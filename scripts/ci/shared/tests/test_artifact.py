"""真实产物链路测试：produce 真实 bytes 摘要 + 接收侧 fail-closed + hosted 负向探针可复跑。

只用标准库，全部在临时目录内真实生成/篡改文件与真实子进程（不 mock 成功、不 grep 名字）。
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SHARED = os.path.dirname(HERE)
sys.path.insert(0, SHARED)

import artifact  # noqa: E402
from contract_errors import ContractError  # noqa: E402

CLI = os.path.join(SHARED, "cli.py")
PROBE = os.path.join(SHARED, "hosted_probe.py")
SOURCE_SHA = "5c10c68603260e3204c337638b1e3bf10658661a"
AUTOMATION_SHA = "a60edfb4f7cd77e69926be475e2c45e324a3db2b"
MANIFEST = {
    "manifest_version": 1,
    "source_repo": "yqm-307/bbtools-coroutine",
    "source_sha": SOURCE_SHA,
    "source_head": SOURCE_SHA,
    "automation_repo": "yqm-307/bbt-framework",
    "automation_sha": AUTOMATION_SHA,
    "run_id": 123456789,
    "run_attempt": 1,
    "job": "verify",
}


def run_cli(args, payload=None, stdin_text=None):
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, CLI, *args],
        input=stdin_text if stdin_text is not None else (json.dumps(payload) if payload is not None else None),
        capture_output=True, text=True, env=env,
    )


class ProduceTests(unittest.TestCase):
    def test_produce_writes_real_bytes_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = os.path.join(tmp, "out")
            gh = os.path.join(tmp, "gh_output")
            proc = run_cli(["produce", "--out-dir", out_dir, "--github-output", gh], MANIFEST)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            result = json.loads(proc.stdout)
            data = open(os.path.join(out_dir, "manifest.json"), "rb").read()
            # 摘要必须等于对真实字节重算的 SHA256，且与 envelope/输出一致。
            self.assertEqual(result["artifact_digest"], "sha256:" + hashlib.sha256(data).hexdigest())
            self.assertEqual(result["envelope"]["artifact_digest"], result["artifact_digest"])
            self.assertEqual(result["envelope"]["artifact_path"], "manifest.json")
            self.assertEqual(result["manifest"], MANIFEST)
            # payload 是有限元数据，不含源码内容，体积很小。
            self.assertLess(len(data), 1024)
            self.assertIn(f"artifact_digest={result['artifact_digest']}", open(gh, encoding="utf-8").read())
            self.assertIn("envelope.json", os.listdir(out_dir))

    def test_produce_rejects_head_mismatch_and_schema_violations(self):
        cases = [
            ("source-head-mismatch", {**MANIFEST, "source_head": "b" * 40}, "E_ART_SOURCE_HEAD"),
            ("unknown-field", {**MANIFEST, "extra": 1}, "E_ART_MANIFEST_UNKNOWN"),
            ("missing-field", {k: v for k, v in MANIFEST.items() if k != "job"}, "E_ART_MANIFEST_MISSING"),
            ("bad-sha", {**MANIFEST, "source_sha": "nothex"}, "E_ART_SHA"),
            ("zero-run-id", {**MANIFEST, "run_id": 0}, "E_ART_TYPE"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for name, payload, code in cases:
                with self.subTest(case=name):
                    out_dir = os.path.join(tmp, name)
                    proc = run_cli(["produce", "--out-dir", out_dir], payload)
                    self.assertEqual(proc.returncode, 3, proc.stdout)
                    self.assertIn(code, proc.stdout)
                    self.assertFalse(os.path.exists(os.path.join(out_dir, "manifest.json")))

    def test_produce_rejects_non_object_payload(self):
        proc = run_cli(["produce", "--out-dir", "/tmp/bbt-should-not-exist"], stdin_text="[]")
        self.assertEqual(proc.returncode, 3)
        self.assertIn("E_ART_TYPE", proc.stdout)
        self.assertFalse(os.path.exists("/tmp/bbt-should-not-exist"))


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = os.path.join(self._tmp.name, "received")
        self.produced = artifact.produce(self.root, MANIFEST)
        with open(os.path.join(self.root, "envelope.json"), "w", encoding="utf-8") as handle:
            json.dump({"envelope": self.produced["envelope"]}, handle)
        self.expected = {
            "repo": MANIFEST["source_repo"],
            "source_sha": MANIFEST["source_sha"],
            "workflow_sha": MANIFEST["automation_sha"],
            "job": MANIFEST["job"],
            "run_id": MANIFEST["run_id"],
            "run_attempt": MANIFEST["run_attempt"],
            "payload_digest": self.produced["artifact_digest"],
        }

    def tearDown(self):
        self._tmp.cleanup()

    def receipt_file(self, **override):
        doc = {
            "root": self.root,
            "path": override.pop("path", "manifest.json"),
            "envelope": override.pop("envelope", self.produced["envelope"]),
            "expected": override.pop("expected", self.expected),
        }
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(doc, handle)
        return path

    def test_positive_receipt_passes_cli(self):
        proc = run_cli(["verify-receipt", "--file", self.receipt_file()])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["payload_digest"], self.produced["artifact_digest"])
        self.assertEqual(out["manifest"], MANIFEST)

    def test_identity_and_path_rejections(self):
        cases = [
            ("wrong-source", {"envelope": {**self.produced["envelope"], "producer_source_sha": "0" * 40}},
             "E_ART_IDENTITY_MISMATCH"),
            ("wrong-run", {"envelope": {**self.produced["envelope"], "producer_run_id": 999}},
             "E_ART_IDENTITY_MISMATCH"),
            ("path-mismatch", {"path": "envelope.json"}, "E_ART_PATH_MISMATCH"),
            ("path-escape", {"envelope": {**self.produced["envelope"], "artifact_path": "../x.json"}},
             "E_ENV_PATH"),
            ("missing-expected-key", {"expected": {k: v for k, v in self.expected.items() if k != "run_id"}},
             "E_ART_TYPE"),
        ]
        for name, override, code in cases:
            with self.subTest(case=name):
                proc = run_cli(["verify-receipt", "--file", self.receipt_file(**override)])
                self.assertEqual(proc.returncode, 3, proc.stdout)
                self.assertIn(code, proc.stdout)

    def test_bytes_tamper_and_symlink_rejected(self):
        payload = os.path.join(self.root, "manifest.json")
        original = open(payload, "rb").read()
        try:
            with open(payload, "ab") as handle:
                handle.write(b"x")
            proc = run_cli(["verify-receipt", "--file", self.receipt_file()])
            self.assertEqual(proc.returncode, 3)
            self.assertIn("E_ART_DIGEST_MISMATCH", proc.stdout)
            with open(payload, "wb") as handle:
                handle.write(original)
            os.unlink(payload)
            os.symlink("envelope.json", payload)
            proc = run_cli(["verify-receipt", "--file", self.receipt_file()])
            self.assertEqual(proc.returncode, 3)
            self.assertIn("E_ART_SYMLINK", proc.stdout)
        finally:
            if os.path.islink(payload):
                os.unlink(payload)
            with open(payload, "wb") as handle:
                handle.write(original)

    def test_verify_receipt_unit_path_escape_and_root(self):
        with self.assertRaises(ContractError) as cm:
            artifact.verify_receipt("/definitely/not/here", "manifest.json",
                                   self.produced["envelope"], self.expected)
        self.assertEqual(cm.exception.code, "E_ART_ROOT")

        outside = tempfile.mkdtemp()
        with open(os.path.join(outside, "manifest.json"), "wb") as handle:
            handle.write(b'{"x":1}')
        link = os.path.join(self.root, "link")
        os.symlink(outside, link)
        envelope = {**self.produced["envelope"], "artifact_path": "link/manifest.json"}
        with self.assertRaises(ContractError) as cm:
            artifact.verify_receipt(self.root, "link/manifest.json", envelope, self.expected)
        self.assertEqual(cm.exception.code, "E_ART_PATH_ESCAPE")

    def test_manifest_unknown_field_rejected_with_matching_digest(self):
        doc = {**MANIFEST, "sneaky": True}
        data = json.dumps(doc, ensure_ascii=False).encode("utf-8")
        with open(os.path.join(self.root, "manifest.json"), "wb") as handle:
            handle.write(data)
        digest = artifact.sha256_digest(data)
        envelope = {**self.produced["envelope"], "artifact_digest": digest}
        expected = {**self.expected, "payload_digest": digest}
        proc = run_cli(["verify-receipt", "--file", self.receipt_file(envelope=envelope, expected=expected)])
        self.assertEqual(proc.returncode, 3)
        self.assertIn("E_ART_MANIFEST_UNKNOWN", proc.stdout)


class HostedProbeTests(unittest.TestCase):
    def _baseline(self):
        tmp = tempfile.TemporaryDirectory()
        root = os.path.join(tmp.name, "received")
        produced = artifact.produce(root, MANIFEST)
        with open(os.path.join(root, "envelope.json"), "w", encoding="utf-8") as handle:
            json.dump({"envelope": produced["envelope"]}, handle)
        receipt = {
            "root": root,
            "path": "manifest.json",
            "envelope": produced["envelope"],
            "expected": {
                "repo": MANIFEST["source_repo"], "source_sha": MANIFEST["source_sha"],
                "workflow_sha": MANIFEST["automation_sha"], "job": MANIFEST["job"],
                "run_id": MANIFEST["run_id"], "run_attempt": MANIFEST["run_attempt"],
                "payload_digest": produced["artifact_digest"],
            },
        }
        path = os.path.join(tmp.name, "receipt.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(receipt, handle)
        return tmp, root, produced, path

    def test_probe_refuses_all_tamper_cases(self):
        tmp, root, _produced, receipt = self._baseline()
        with tmp:
            env = dict(os.environ)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            proc = subprocess.run([sys.executable, PROBE, "--receipt", receipt],
                                  capture_output=True, text=True, env=env)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("PROBE OK", proc.stdout)
            self.assertIn("positive-control", proc.stdout)
            for code in ("E_ART_DIGEST_MISMATCH", "E_ART_SYMLINK", "E_ART_PATH_ESCAPE",
                         "E_ART_IDENTITY_MISMATCH", "E_ART_MANIFEST_UNKNOWN", "E_ART_MISSING"):
                self.assertIn(code, proc.stdout)

    def test_probe_is_not_vacuous(self):
        # 篡改基线 payload 后，探针的正向对照必须失败 → 整体非零，禁止"全绿"。
        tmp, root, _produced, receipt = self._baseline()
        with tmp:
            with open(os.path.join(root, "manifest.json"), "ab") as handle:
                handle.write(b"!")
            env = dict(os.environ)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            proc = subprocess.run([sys.executable, PROBE, "--receipt", receipt],
                                  capture_output=True, text=True, env=env)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("PROBE FAILED", proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
