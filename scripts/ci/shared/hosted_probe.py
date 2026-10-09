#!/usr/bin/env python3
"""hosted 负向探针：对真实产出 payload/envelope 做篡改、错身份、symlink、路径逃逸拒绝验证。

只读真实字节并生成篡改副本，不执行任何 payload 内容；每个用例都必须被 cli verify-receipt
以稳定错误码拒绝（退出 3）。任一用例未被拒绝即整体退出非零——正向对照失败同样非零，
避免「全部 skipped 变绿」。

用法：
  python3 scripts/ci/shared/hosted_probe.py --receipt <已校验的正向 receipt.json> [--cli <cli.py>]
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


class Probe:
    def __init__(self, receipt_path: str, cli: str):
        with open(receipt_path, encoding="utf-8") as handle:
            self.template = json.load(handle)
        self.cli = cli
        self.root = self.template["root"]
        self.rel = self.template["path"]
        self.expected = self.template["expected"]
        self.results = []
        self.failures = []

    # ---- 基础设施 ----
    def baseline_envelope(self) -> dict:
        with open(os.path.join(self.root, "envelope.json"), encoding="utf-8") as handle:
            return json.load(handle)["envelope"]

    def run_cli(self, receipt: dict):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="bbt-probe-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(receipt, handle)
            env = dict(os.environ)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            return subprocess.run(
                [sys.executable, self.cli, "verify-receipt", "--file", path],
                capture_output=True, text=True, env=env,
            )
        finally:
            os.unlink(path)

    def copy_root(self) -> str:
        tmp = tempfile.mkdtemp(prefix="bbt-probe-root-")
        target = os.path.join(tmp, "received")
        shutil.copytree(self.root, target)
        return target

    def receipt(self, root, envelope=None, rel=None, expected=None) -> dict:
        return {
            "root": root,
            "path": self.rel if rel is None else rel,
            "envelope": self.baseline_envelope() if envelope is None else envelope,
            "expected": self.expected if expected is None else expected,
        }

    def record(self, name, expect, proc):
        ok = proc.returncode == 3 and expect in proc.stdout
        self.results.append((name, expect, proc.returncode, "refused" if ok else "NOT-REFUSED"))
        if not ok:
            self.failures.append(
                f"{name}: expected {expect} exit=3, got exit={proc.returncode} stdout={proc.stdout.strip()}"
            )

    def check(self, name, expect, receipt):
        self.record(name, expect, self.run_cli(receipt))

    def payload_path(self, root) -> str:
        return os.path.join(root, self.rel)

    # ---- 用例 ----
    def positive_control(self):
        proc = self.run_cli(self.receipt(self.root))
        ok = proc.returncode == 0
        self.results.append(("positive-control", "exit0", proc.returncode, "ok" if ok else "FAILED"))
        if not ok:
            self.failures.append(f"positive control failed: exit={proc.returncode} {proc.stdout.strip()}{proc.stderr.strip()}")

    def expected_with_digest(self, digest):
        """隔离单个不变量的用例：期望摘要与用例 payload 保持一致，以命中具体校验分支。"""
        expected = dict(self.expected)
        expected["payload_digest"] = digest
        return expected

    def case_bytes_tampered(self):
        root = self.copy_root()
        with open(self.payload_path(root), "ab") as handle:
            handle.write(b" ")
        self.check("payload-bytes-tampered", "E_ART_DIGEST_MISMATCH", self.receipt(root))

    def case_envelope_digest_swapped(self):
        # payload 被换且 envelope 摘要同步改写：与 producer job 声明的真实摘要不一致 → 拒绝。
        root = self.copy_root()
        with open(self.payload_path(root), "ab") as handle:
            handle.write(b" ")
        data = open(self.payload_path(root), "rb").read()
        env = copy.deepcopy(self.baseline_envelope())
        env["artifact_digest"] = _sha256(data)
        self.check("envelope-digest-swapped", "E_ART_IDENTITY_MISMATCH", self.receipt(root, envelope=env))

    def case_payload_not_object(self):
        root = self.copy_root()
        data = b"[]"
        with open(self.payload_path(root), "wb") as handle:
            handle.write(data)
        digest = _sha256(data)
        env = copy.deepcopy(self.baseline_envelope())
        env["artifact_digest"] = digest
        self.check("payload-not-object", "E_ART_TYPE",
                   self.receipt(root, envelope=env, expected=self.expected_with_digest(digest)))

    def _rewrite_manifest(self, mutate):
        root = self.copy_root()
        with open(self.payload_path(root), encoding="utf-8") as handle:
            doc = json.load(handle)
        mutate(doc)
        data = json.dumps(doc, ensure_ascii=False).encode("utf-8")
        with open(self.payload_path(root), "wb") as handle:
            handle.write(data)
        digest = _sha256(data)
        env = copy.deepcopy(self.baseline_envelope())
        env["artifact_digest"] = digest
        return root, env, self.expected_with_digest(digest)

    def case_manifest_unknown_field(self):
        root, env, expected = self._rewrite_manifest(lambda d: d.update({"extra": 1}))
        self.check("manifest-unknown-field", "E_ART_MANIFEST_UNKNOWN",
                   self.receipt(root, envelope=env, expected=expected))

    def case_manifest_missing_field(self):
        root, env, expected = self._rewrite_manifest(lambda d: d.pop("job"))
        self.check("manifest-missing-field", "E_ART_MANIFEST_MISSING",
                   self.receipt(root, envelope=env, expected=expected))

    def case_manifest_mismatch_envelope(self):
        # manifest 内部自洽但与其 envelope 不一致（payload 被换）→ 交叉绑定拒绝。
        root, env, expected = self._rewrite_manifest(lambda d: d.update({"run_id": int(d["run_id"]) + 1}))
        self.check("manifest-mismatch-envelope", "E_ART_MANIFEST_MISMATCH",
                   self.receipt(root, envelope=env, expected=expected))

    def case_envelope_unknown_field(self):
        env = copy.deepcopy(self.baseline_envelope())
        env["unexpected"] = "x"
        self.check("envelope-unknown-field", "E_ENV_UNKNOWN_FIELD", self.receipt(self.root, envelope=env))

    def case_envelope_path_escape(self):
        env = copy.deepcopy(self.baseline_envelope())
        env["artifact_path"] = "../manifest.json"
        self.check("envelope-path-escape", "E_ENV_PATH", self.receipt(self.root, envelope=env))

    def case_path_mismatch(self):
        self.check("receipt-path-mismatch", "E_ART_PATH_MISMATCH",
                   self.receipt(self.root, rel="envelope.json"))

    def case_expected_missing_key(self):
        expected = {k: v for k, v in self.expected.items() if k != "payload_digest"}
        self.check("expected-missing-digest", "E_ART_TYPE", self.receipt(self.root, expected=expected))

    def case_wrong_source_identity(self):
        env = copy.deepcopy(self.baseline_envelope())
        env["producer_source_sha"] = "0" * 40
        self.check("wrong-source-identity", "E_ART_IDENTITY_MISMATCH", self.receipt(self.root, envelope=env))

    def case_wrong_run_identity(self):
        # 只消费本 run producer：其他 run/attempt 的身份必须被拒。
        env = copy.deepcopy(self.baseline_envelope())
        env["producer_run_id"] = int(env["producer_run_id"]) + 1
        self.check("wrong-run-identity", "E_ART_IDENTITY_MISMATCH", self.receipt(self.root, envelope=env))

    def case_wrong_attempt_identity(self):
        env = copy.deepcopy(self.baseline_envelope())
        env["producer_run_attempt"] = int(env["producer_run_attempt"]) + 1
        self.check("wrong-attempt-identity", "E_ART_IDENTITY_MISMATCH", self.receipt(self.root, envelope=env))

    def case_missing_payload(self):
        root = self.copy_root()
        os.unlink(self.payload_path(root))
        self.check("payload-missing", "E_ART_MISSING", self.receipt(root))

    def case_symlink_payload(self):
        root = self.copy_root()
        target = self.payload_path(root)
        os.unlink(target)
        os.symlink("envelope.json", target)
        self.check("payload-symlink", "E_ART_SYMLINK", self.receipt(root))

    def case_symlink_dir_escape(self):
        # 目录 symlink 逃逸：payload 本身是普通文件，但 realpath 落在可信 root 之外。
        root = self.copy_root()
        outside = tempfile.mkdtemp(prefix="bbt-probe-outside-")
        with open(os.path.join(outside, self.rel), "wb") as handle:
            handle.write(b'{"x":1}')
        os.symlink(outside, os.path.join(root, "link"))
        rel = "link/" + self.rel
        env = copy.deepcopy(self.baseline_envelope())
        env["artifact_path"] = rel
        self.check("symlink-dir-escape", "E_ART_PATH_ESCAPE",
                   self.receipt(root, envelope=env, rel=rel))

    def run(self) -> int:
        self.positive_control()
        for case in (
            self.case_bytes_tampered,
            self.case_envelope_digest_swapped,
            self.case_payload_not_object,
            self.case_manifest_unknown_field,
            self.case_manifest_missing_field,
            self.case_manifest_mismatch_envelope,
            self.case_envelope_unknown_field,
            self.case_envelope_path_escape,
            self.case_path_mismatch,
            self.case_expected_missing_key,
            self.case_wrong_source_identity,
            self.case_wrong_run_identity,
            self.case_wrong_attempt_identity,
            self.case_missing_payload,
            self.case_symlink_payload,
            self.case_symlink_dir_escape,
        ):
            try:
                case()
            except Exception as exc:  # 探针自身异常同样不得算作「已证明拒绝」。
                name = getattr(case, "__name__", "case")
                self.results.append((name, "-", "-", "ERROR"))
                self.failures.append(f"{name}: probe crashed: {exc!r}")
        for name, expect, code, status in self.results:
            print(f"{status:12s} {name:28s} expect={expect:26s} exit={code}")
        if self.failures:
            for line in self.failures:
                print("PROBE FAILURE: " + line, file=sys.stderr)
            print(f"PROBE FAILED ({len(self.failures)} cases did not fail closed)", file=sys.stderr)
            return 1
        print(f"PROBE OK ({len(self.results)} cases, all fail-closed as expected)")
        return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="bbtools-hosted-probe")
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--cli", default=os.path.join(HERE, "cli.py"))
    args = parser.parse_args(argv)
    return Probe(args.receipt, args.cli).run()


if __name__ == "__main__":
    sys.exit(main())
