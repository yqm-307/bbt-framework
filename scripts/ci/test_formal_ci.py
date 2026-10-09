#!/usr/bin/env python3
"""正式 `.github/workflows/ci.yml`（唯一权威普通入口）离线冒烟与直接耦合测试。

本文件是现役普通 CI（由已独立审查的 hosted 影子候选升级而来，Issue #50/#53）的**直接
测试**：只验证「CI 配方 / archive 接收侧门禁 / 分类路由 / 静态与结构契约 / 手动任务保留」，
**不跑 C++ 全量构建、不下载源码、不连网**（真实 build→test 走父级授权的在线 CI，在线未验
见 `docs/ci/formal-ci.md`）。stdlib 为主，PyYAML 可选增强。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v scripts/ci/test_formal_ci.py
  # 结构契约需 PyYAML（缺省显式 SKIP，不等于通过）；分类路由冒烟需指向已发布 callee：
  UV_OFFLINE=1 UV_CACHE_DIR=<cache> uv run --no-project --with pyyaml \\
    python3 scripts/ci/test_formal_ci.py
  PYTHONDONTWRITEBYTECODE=1 BBT_CI_SHARED_DIR=<framework>/scripts/ci/shared \\
    python3 scripts/ci/test_formal_ci.py

覆盖：
- 配方守卫：脚本 bash -n、python compile、prepare_boost --print-plan 固定值、fail-closed 拒绝；
- archive 接收侧：sha256 校验 + 闭集成员/路径穿越/绝对路径/逃逸 symlink/设备成员拒绝
  （真实调用 scripts/ci/run_framework_gate.sh verify-archive，synthetic 归档非真实 C++ 产物）；
- changed_files 路由：push/PR 真实 diff 归一、未知保守回退、超预算整集回退；
- 正式 workflow 文本契约（stdlib，恒跑，直接读真实文件）：触发面/身份/权限/concurrency/
  hosted 与手动 runner/固定 SHA pin/callee 复用/required-optional/docs-only/always result/
  artifact 闭包/producer attempt/no-cache/判据命令/内联 bash -n/无影子双跑；
- 正式 workflow 结构契约（需 PyYAML，缺省显式 SKIP）；
- 手动任务保留契约：toolchain-verify / resource-acceptance 及其 inputs/资源边界不删不改；
- producer→receipt 语义：执行真实 workflow heredoc + 真实 shared cli.py，复用原 producer attempt；
- 分类路由冒烟（需 BBT_CI_SHARED_DIR）：用**同一个真实 cli.py** 校验正式受限输入与
  required/optional 判定，本仓不复制契约。
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKTREE = os.path.normpath(os.path.join(HERE, "..", ".."))
CI_YML = os.path.join(WORKTREE, ".github", "workflows", "ci.yml")
CLASSIFY_YML = os.path.join(WORKTREE, ".github", "workflows", "bbtools-classify-v1.yml")
PREPARE_BOOST = os.path.join(HERE, "prepare_boost.sh")
GATE = os.path.join(HERE, "run_framework_gate.sh")
CHANGED = os.path.join(HERE, "changed_files.py")
FETCH_DEPS = os.path.join(WORKTREE, "scripts", "fetch_deps.sh")
PREPARE_PROTOBUF = os.path.join(WORKTREE, "scripts", "prepare_protobuf.sh")
LOCAL_BUILD = os.path.join(WORKTREE, "scripts", "local_build.sh")
BUILD_STACK = os.path.join(WORKTREE, "scripts", "build_stack.sh")
RUN_CTEST = os.path.join(WORKTREE, "scripts", "run_ctest.sh")
VERIFY_TOOLCHAIN = os.path.join(WORKTREE, "scripts", "verify_toolchain.sh")
DEPS_LOCK = os.path.join(WORKTREE, "deps.lock")
TOOLCHAIN_LOCK = os.path.join(WORKTREE, "docker", "toolchain.lock")

CALLEE_SHA = "1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7"
CALLEE = f"yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@{CALLEE_SHA}"
CHECKOUT_SHA = "11d5960a326750d5838078e36cf38b85af677262"
UPLOAD_SHA = "ea165f8d65b6e75b540449e92b4886f43607fa02"
DOWNLOAD_SHA = "d3f86a106a0bac45b974a628896c90dbdf5c8093"
SHA_PIN_RE = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
HOSTED_RUNNER = "ubuntu-24.04"
MANUAL_RUNNER = "arc-s4-framework"

ENV = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")


def raw(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def body(path: str) -> str:
    """去掉说明性注释行后的实际 YAML 内容（注释中可提及被禁写法）。"""
    return "\n".join(
        line for line in raw(path).splitlines() if not line.lstrip().startswith("#")
    )


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)


def _extract_run_blocks(text: str):
    """按缩进提取每个 `run: |` 块，并按 YAML 块标量语义去公共缩进（保留 heredoc 终止符）。"""
    blocks = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        match = re.match(r"^(\s*)run:\s*\|\s*$", lines[i])
        if not match:
            i += 1
            continue
        run_indent = len(match.group(1))
        i += 1
        buf = []
        while i < len(lines):
            line = lines[i]
            if line.strip() and (len(line) - len(line.lstrip())) <= run_indent:
                break
            buf.append(line)
            i += 1
        nonempty = [line for line in buf if line.strip()]
        if nonempty:
            base = min(len(line) - len(line.lstrip()) for line in nonempty)
            buf = [line[base:] if len(line) >= base else line for line in buf]
        blocks.append("\n".join(buf))
    return blocks


def _sha256(path: str) -> str:
    import hashlib

    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def _make_tar(path: str, entries):
    """entries: list of (name, kind, payload/linkname)。kind: file|dir|sym|chr。"""
    with tarfile.open(path, "w:gz") as tf:
        for name, kind, payload in entries:
            if kind == "file":
                data = payload.encode()
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            elif kind == "dir":
                info = tarfile.TarInfo(name)
                info.type = tarfile.DIRTYPE
                tf.addfile(info)
            elif kind == "sym":
                info = tarfile.TarInfo(name)
                info.type = tarfile.SYMTYPE
                info.linkname = payload
                tf.addfile(info)
            elif kind == "chr":
                info = tarfile.TarInfo(name)
                info.type = tarfile.CHRTYPE
                info.devmajor, info.devminor = 1, 3
                tf.addfile(info)
            else:  # pragma: no cover
                raise AssertionError(kind)


GOOD_ENTRIES = [
    (".ci-work", "dir", None),
    (".ci-work/project-build", "dir", None),
    (".ci-work/project-build/tests", "dir", None),
    (".ci-work/project-build/tests/rpc_xlang_server", "file", "elf"),
    (".ci-work/deps-manifest.txt", "file", "coroutine path=/x sha=deadbeef"),
    ("boost-prefix/lib", "dir", None),
    ("boost-prefix/lib/libboost_context.so.1.90.0", "file", "so"),
    ("boost-prefix/lib/libboost_context.so", "sym", "libboost_context.so.1.90.0"),
]


def _verify_archive(archive, expected, dest, prefixes=(".ci-work", "boost-prefix/lib")):
    cmd = ["bash", GATE, "verify-archive", "--archive", archive,
           "--expected-sha256", expected, "--dest", dest]
    for p in prefixes:
        cmd += ["--allow-prefix", p]
    return run(cmd, env=ENV)


# --------------------------------------------------------------------------- #
# 配方守卫
# --------------------------------------------------------------------------- #
class RecipeGuardTests(unittest.TestCase):
    def test_shell_recipes_parse(self):
        for path in (PREPARE_BOOST, GATE, FETCH_DEPS, PREPARE_PROTOBUF, LOCAL_BUILD,
                     BUILD_STACK, RUN_CTEST, VERIFY_TOOLCHAIN):
            with self.subTest(path=os.path.basename(path)):
                proc = run(["bash", "-n", path], env=ENV)
                self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_python_recipes_compile(self):
        for path in (CHANGED, os.path.abspath(__file__)):
            with self.subTest(path=os.path.basename(path)):
                compile(raw(path), path, "exec")

    def test_prepare_boost_print_plan_is_side_effect_free_and_pinned(self):
        proc = run(["bash", PREPARE_BOOST, "--print-plan"], env=ENV)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        plan = dict(
            line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line
        )
        self.assertEqual(plan["boost_version"], "1.90.0")
        self.assertEqual(
            plan["boost_sha256"],
            "5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9",
        )
        self.assertEqual(plan["boost_libs"], "context")
        self.assertEqual(plan["boost_version_num"], "109000")
        self.assertEqual(
            plan["archive_url"],
            "https://archives.boost.io/release/1.90.0/source/boost_1_90_0.tar.gz",
        )

    def test_prepare_boost_pins_match_toolchain_lock(self):
        # 正式 Boost 固定值必须与本仓 docker/toolchain.lock 同源同值（防漂移）。
        lock = raw(TOOLCHAIN_LOCK)
        self.assertIn("BOOST_VERSION=1.90.0", lock)
        self.assertIn(
            "BOOST_SHA256=5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9",
            lock,
        )

    def test_prepare_boost_refuses_existing_prefix(self):
        with tempfile.TemporaryDirectory() as td:
            existing = os.path.join(td, "prefix")
            os.makedirs(existing)
            proc = run(["bash", PREPARE_BOOST, existing], env=ENV)
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertTrue(os.path.isdir(existing), "拒绝时不得改动已存在前缀")

    def test_prepare_boost_refuses_dangling_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            link = os.path.join(td, "dangling")
            os.symlink(os.path.join(td, "never-created"), link)
            proc = run(["bash", PREPARE_BOOST, link], env=ENV)
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertTrue(os.path.islink(link))

    def test_prepare_boost_rejects_missing_prefix_and_bad_args(self):
        self.assertEqual(run(["bash", PREPARE_BOOST], env=ENV).returncode, 2)
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(
                run(["bash", PREPARE_BOOST, os.path.join(td, "p"), "--jobs", "x"], env=ENV).returncode,
                2,
            )
            self.assertEqual(
                run(["bash", PREPARE_BOOST, os.path.join(td, "p"), "--nope"], env=ENV).returncode,
                2,
            )

    def test_gate_rejects_unknown_subcommand_and_missing_args(self):
        self.assertEqual(run(["bash", GATE, "bogus"], env=ENV).returncode, 2)
        self.assertEqual(run(["bash", GATE], env=ENV).returncode, 2)
        # verify-archive 缺 --archive / --allow-prefix → 拒绝。
        self.assertEqual(
            run(["bash", GATE, "verify-archive", "--expected-sha256", "0" * 64,
                 "--dest", "/tmp/x"], env=ENV).returncode,
            2,
        )

    def test_gate_build_rejects_missing_prerequisites(self):
        # 缺必需参数 → 拒绝（fail-closed，不产出半成品归档）。
        self.assertEqual(
            run(["bash", GATE, "build", "--work-dir", "/tmp/w"], env=ENV).returncode, 2
        )
        with tempfile.TemporaryDirectory() as td:
            proc = run(
                ["bash", GATE, "build",
                 "--work-dir", os.path.join(td, "w"),
                 "--deps-dir", os.path.join(td, "deps"),
                 "--protobuf-prefix", os.path.join(td, "pb"),
                 "--boost-prefix", os.path.join(td, "boost"),
                 "--archive", os.path.join(td, "a.tar.gz"),
                 "--pack-root", td,
                 "--automation-sha", "a" * 40],
                env=ENV,
            )
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertFalse(os.path.exists(os.path.join(td, "a.tar.gz")))

    def test_gate_build_rejects_invalid_jobs(self):
        with tempfile.TemporaryDirectory() as td:
            proc = run(
                ["bash", GATE, "build",
                 "--work-dir", os.path.join(td, "w"),
                 "--deps-dir", os.path.join(td, "deps"),
                 "--protobuf-prefix", os.path.join(td, "pb"),
                 "--boost-prefix", os.path.join(td, "boost"),
                 "--archive", os.path.join(td, "a.tar.gz"),
                 "--pack-root", td, "--jobs", "0",
                 "--automation-sha", "a" * 40],
                env=ENV,
            )
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)


# --------------------------------------------------------------------------- #
# archive 接收侧门禁（synthetic 归档，非真实 C++ 产物）
# --------------------------------------------------------------------------- #
class VerifyArchiveTests(unittest.TestCase):
    def _good(self, td):
        archive = os.path.join(td, "good.tar.gz")
        _make_tar(archive, GOOD_ENTRIES)
        return archive, _sha256(archive)

    def test_valid_archive_extracts_in_tree(self):
        with tempfile.TemporaryDirectory() as td:
            archive, sha = self._good(td)
            dest = os.path.join(td, "dest")
            proc = _verify_archive(archive, sha, dest)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertTrue(
                os.path.isfile(os.path.join(dest, ".ci-work/project-build/tests/rpc_xlang_server"))
            )
            link = os.path.join(dest, "boost-prefix/lib/libboost_context.so")
            self.assertTrue(os.path.islink(link))
            self.assertEqual(os.readlink(link), "libboost_context.so.1.90.0")

    def test_tampered_bytes_and_bad_expected_sha_refused(self):
        with tempfile.TemporaryDirectory() as td:
            archive, sha = self._good(td)
            dest = os.path.join(td, "dest")
            self.assertNotEqual(_verify_archive(archive, "0" * 64, dest).returncode, 0)
            self.assertNotEqual(_verify_archive(archive, "", dest).returncode, 0)
            self.assertNotEqual(_verify_archive(archive, "zz", dest).returncode, 0)
            with open(archive, "ab") as handle:
                handle.write(b"tamper")
            self.assertNotEqual(_verify_archive(archive, sha, dest).returncode, 0)

    def _assert_refused(self, td, entries, needle):
        archive = os.path.join(td, "bad.tar.gz")
        _make_tar(archive, entries)
        dest = os.path.join(td, "dest")
        proc = _verify_archive(archive, _sha256(archive), dest)
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(needle, proc.stderr + proc.stdout)
        # 拒绝路径不得把越界成员写进 dest。
        self.assertFalse(os.path.exists("/tmp/pwn"))

    def test_path_traversal_refused(self):
        with tempfile.TemporaryDirectory() as td:
            self._assert_refused(
                td, [(".ci-work/../../etc/pwn", "file", "x")], "非法成员名"
            )

    def test_absolute_member_refused(self):
        with tempfile.TemporaryDirectory() as td:
            self._assert_refused(td, [("/tmp/pwn", "file", "x")], "非法成员名")

    def test_member_outside_closed_set_refused(self):
        with tempfile.TemporaryDirectory() as td:
            self._assert_refused(td, [("src/evil.py", "file", "x")], "成员超出闭集")

    def test_escaping_symlink_refused(self):
        with tempfile.TemporaryDirectory() as td:
            self._assert_refused(
                td, [(".ci-work/link", "sym", "../../etc/passwd")], "symlink"
            )

    def test_device_member_refused(self):
        with tempfile.TemporaryDirectory() as td:
            self._assert_refused(
                td, [(".ci-work/devnull", "chr", None)], "非闭集成员类型"
            )

    def test_empty_archive_refused(self):
        with tempfile.TemporaryDirectory() as td:
            archive = os.path.join(td, "empty.tar.gz")
            _make_tar(archive, [])
            proc = _verify_archive(archive, _sha256(archive), os.path.join(td, "dest"))
            self.assertNotEqual(proc.returncode, 0)


# --------------------------------------------------------------------------- #
# changed_files 路由
# --------------------------------------------------------------------------- #
def _git(repo, *args):
    return run(["git", "-C", repo, *args], env=ENV)


def _init_repo(repo):
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "formal@test")
    _git(repo, "config", "user.name", "formal")
    _git(repo, "config", "commit.gpgsign", "false")


def _commit(repo, relpath, content, message):
    full = os.path.join(repo, relpath)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as handle:
        handle.write(content)
    _git(repo, "add", relpath)
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def _changed_files(repo, env_extra):
    out_path = os.path.join(repo, ".ci-output")
    env = dict(ENV, GITHUB_OUTPUT=out_path, **env_extra)
    proc = run([sys.executable, CHANGED], env=env, cwd=repo)
    with open(out_path, encoding="utf-8") as handle:
        values = dict(
            line.split("=", 1) for line in handle.read().splitlines() if "=" in line
        )
    return proc, values


class ChangedFilesRoutingTests(unittest.TestCase):
    def test_push_range_yields_real_paths(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "docs/ci/a.md", "a", "docs")
            second = _commit(repo, "framework/src/x.cc", "x", "code")
            proc, values = _changed_files(
                repo,
                {"EVENT_NAME": "push", "BEFORE": first, "SHA": second, "REF_NAME": "main"},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["classifier_status"], "ok")
            self.assertEqual(json.loads(values["changed_files_json"]), ["framework/src/x.cc"])
            self.assertEqual(
                json.loads(values["event_json"]),
                {"name": "push", "head_ref": "main"},
            )

    def test_pull_request_three_dot(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            base = _commit(repo, "docs/ci/a.md", "a", "base")
            _git(repo, "update-ref", "refs/remotes/origin/main", base)
            _git(repo, "checkout", "-q", "-b", "feat")
            head = _commit(repo, "docs/ci/formal-ci.md", "n", "note")
            proc, values = _changed_files(
                repo,
                {"EVENT_NAME": "pull_request", "BASE_REF": "main", "SHA": head},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(
                json.loads(values["changed_files_json"]), ["docs/ci/formal-ci.md"]
            )
            self.assertEqual(
                json.loads(values["event_json"]),
                {"name": "pull_request", "base_ref": "main"},
            )

    def test_unknown_range_falls_back_conservative(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "framework/x.cc", "x", "one")
            proc, values = _changed_files(
                repo,
                {"EVENT_NAME": "push", "BEFORE": "f" * 40, "SHA": first},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["changed_files_json"], "[]")
            self.assertEqual(values["classifier_status"], "unknown")

    def test_oversized_change_set_falls_back_to_empty(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "framework/gen0.cc", "0", "base")
            for i in range(1, 6):
                _commit(repo, f"framework/gen{i}.cc", "x", f"c{i}")
            head = _git(repo, "rev-parse", "HEAD").stdout.strip()
            out_path = os.path.join(repo, ".o2")
            env = dict(ENV, GITHUB_OUTPUT=out_path, EVENT_NAME="push",
                       BEFORE=first, SHA=head)
            script = (
                "import sys; sys.path.insert(0, %r); "
                "import changed_files as m; m.MAX_FILES = 2; sys.exit(m.main())" % HERE
            )
            proc = run([sys.executable, "-c", script], env=env, cwd=repo)
            with open(out_path, encoding="utf-8") as handle:
                values = dict(
                    line.split("=", 1)
                    for line in handle.read().splitlines() if "=" in line
                )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["changed_files_json"], "[]")


# --------------------------------------------------------------------------- #
# 正式 workflow 文本契约（stdlib，恒跑，直接读真实文件）
# --------------------------------------------------------------------------- #
class FormalWorkflowTextContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = raw(CI_YML)
        cls.stripped = body(CI_YML)

    def test_name_and_single_authoritative_identity(self):
        self.assertIn("\nname: ci\n", self.text)
        # 正式 artifact 名（bbt-build-<run_id>），不再是影子名；无影子双跑残留。
        self.assertIn("bbt-build-${{ github.run_id }}", self.stripped)
        self.assertNotIn("bbt-shadow-build-", self.stripped)
        self.assertNotIn("ci-shadow-v1", self.stripped)

    def test_triggers_are_pr_push_main_and_manual(self):
        self.assertIn("pull_request:", self.stripped)
        self.assertIn("push:", self.stripped)
        self.assertIn("workflow_dispatch:", self.stripped)
        self.assertIn("branches: [main]", self.stripped)
        # 手动入口保留 resource_acceptance 布尔输入。
        self.assertIn("resource_acceptance:", self.stripped)
        for forbidden in ("schedule:", "merge_group"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, self.stripped)

    def test_permissions_minimal_no_write_no_secrets(self):
        self.assertIn("permissions: {}", self.stripped)
        self.assertNotIn("secrets: inherit", self.text)
        self.assertNotIn("${{ secrets.", self.text)
        self.assertNotRegex(self.text, r"(?m)^\s*secrets:\s*$")
        self.assertNotIn("id-token", self.text)
        self.assertIn("persist-credentials: false", self.text)

    def test_concurrency_main_no_cancel_and_pr_only_cancel(self):
        # main/手动：group 含 run_id → 逐提交独立、互不取消；PR：只取消同一 PR。
        self.assertIn("group: ci-${{", self.stripped)
        self.assertIn("github.run_id", self.stripped)
        self.assertIn("cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
                      self.stripped)

    def test_runner_split_hosted_and_preserved_manual(self):
        runs_on = re.findall(r"(?m)^\s*runs-on:\s*(\S+)\s*$", self.stripped)
        self.assertTrue(runs_on, "应存在本地 job 的 runs-on")
        self.assertEqual(set(runs_on), {HOSTED_RUNNER, MANUAL_RUNNER})
        # 普通重型 job 必须 hosted。
        self.assertGreaterEqual(runs_on.count(HOSTED_RUNNER), 3)
        self.assertEqual(runs_on.count(MANUAL_RUNNER), 2)

    def test_all_uses_are_full_sha_pinned_and_expected(self):
        uses = re.findall(r"(?m)^\s*uses:\s*(\S+)\s*$", self.stripped)
        self.assertTrue(uses)
        for item in uses:
            with self.subTest(uses=item):
                self.assertRegex(item, SHA_PIN_RE)
        for pin in (f"actions/checkout@{CHECKOUT_SHA}",
                    f"actions/upload-artifact@{UPLOAD_SHA}",
                    f"actions/download-artifact@{DOWNLOAD_SHA}"):
            with self.subTest(pin=pin):
                self.assertIn(pin, self.stripped)

    def test_reuses_published_callee_not_local(self):
        self.assertIn(CALLEE, self.stripped)
        self.assertNotIn("./.github/workflows", self.stripped)
        self.assertEqual(
            self.stripped.count(CALLEE), 2, "应在 plan 与 result 各复用一次 callee"
        )

    def test_no_caller_supplied_shell_or_jobs_input(self):
        self.assertIn("scripts/ci/changed_files.py", self.stripped)
        self.assertNotIn("workflow_call", self.stripped)
        self.assertIn("source_sha: ${{ github.sha }}", self.stripped)

    def test_required_and_optional_disjoint_and_build_test_optional(self):
        self.assertIn("required_checks_json: '[\"changes\",\"plan\"]'", self.stripped)
        self.assertIn("optional_checks_json: '[\"build\",\"test\"]'", self.stripped)
        self.assertIn("needs.plan.outputs.classification != 'docs-only'", self.stripped)
        self.assertIn("needs.plan.result == 'success'", self.stripped)

    def test_result_job_aggregates_always_and_passes_all_job_results(self):
        self.assertIn("if: ${{ always() }}", self.stripped)
        self.assertIn(
            "format('{{\"changes\":\"{0}\",\"plan\":\"{1}\",\"build\":\"{2}\",\"test\":\"{3}\"}}'",
            self.stripped,
        )
        for ref in ("needs.changes.result", "needs.plan.result", "needs.build.result",
                    "needs.test.result"):
            with self.subTest(ref=ref):
                self.assertIn(ref, self.stripped)

    def test_result_job_has_conservative_fallbacks(self):
        self.assertIn("|| '[]'", self.stripped)
        self.assertIn("|| 'unknown'", self.stripped)
        self.assertIn('format(\'{{"name":"{0}"}}\', github.event_name)', self.stripped)

    def test_artifact_cross_runner_closure_commands_present(self):
        # same-run only：下载名含 run_id；artifact 命名正式。
        self.assertIn("name: bbt-build-${{ github.run_id }}", self.stripped)
        self.assertIn("download-artifact@", self.stripped)
        self.assertIn("upload-artifact@", self.stripped)
        self.assertIn("retention-days: 1", self.stripped)
        self.assertIn("compression-level: 0", self.stripped)
        self.assertIn("if-no-files-found: error", self.stripped)
        # 先校验 sha256 后解包（verify-archive），并做闭集成员约束。
        self.assertIn("run_framework_gate.sh verify-archive", self.stripped)
        self.assertIn("--expected-sha256 \"$EXPECTED_ARCHIVE_SHA256\"", self.stripped)
        self.assertIn("--allow-prefix \".ci-work\"", self.stripped)
        self.assertIn("--allow-prefix \"boost-prefix/lib\"", self.stripped)
        # provenance 复用已发布 shared 契约，不新增第二 public API：
        # build 侧 produce 在 gate 脚本，接收侧 verify-receipt 在 workflow。
        self.assertRegex(raw(GATE), r"scripts/ci/shared/cli\.py\"?\s+produce")
        self.assertIn("scripts/ci/shared/cli.py verify-receipt", self.stripped)

    def test_producer_attempt_output_wiring(self):
        self.assertIn(
            "producer_run_attempt: ${{ steps.pack.outputs.producer_run_attempt }}",
            self.stripped,
        )
        self.assertIn(
            "EXPECTED_PRODUCER_RUN_ATTEMPT: ${{ needs.build.outputs.producer_run_attempt }}",
            self.stripped,
        )
        self.assertIn("|archive_bytes|producer_run_attempt)=", self.stripped)
        self.assertIn("printf 'producer_run_attempt=%s\\n' \"$run_attempt\"", raw(GATE))

    def test_receipt_reuses_producer_attempt_not_consumer(self):
        # 执行真实 workflow 中的 receipt heredoc 和 shared CLI；不编译/执行产物。
        blocks = [b for b in _extract_run_blocks(self.text)
                  if "cli.py verify-receipt" in b]
        self.assertEqual(len(blocks), 1)
        script = blocks[0].split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        cli = os.path.join(WORKTREE, "scripts", "ci", "shared", "cli.py")
        with tempfile.TemporaryDirectory() as td:
            work = os.path.join(td, "work")
            meta = os.path.join(work, "artifact-meta")
            source = os.path.join(td, "input.json")
            receipt = os.path.join(td, "receipt.json")
            sha = "a" * 40  # 明确合成身份，仅用于离线契约测试。
            with open(source, "w", encoding="utf-8") as handle:
                json.dump({"manifest_version": 1, "source_repo": "example/repo",
                           "source_sha": sha, "source_head": sha,
                           "automation_repo": "example/repo", "automation_sha": sha,
                           "run_id": 123, "run_attempt": 1, "job": "build"}, handle)
            produced = run([sys.executable, cli, "produce", "--file", source,
                            "--out-dir", meta], env=ENV)
            self.assertEqual(produced.returncode, 0, produced.stderr)
            with open(os.path.join(meta, "envelope.json"), encoding="utf-8") as handle:
                digest = json.load(handle)["envelope"]["artifact_digest"]
            env = dict(ENV, BBT_WORK_DIR=work, GITHUB_REPOSITORY="example/repo",
                       GITHUB_SHA=sha, GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="2",
                       EXPECTED_PROVENANCE_DIGEST=digest)
            for attempt, should_pass in (("1", True), ("2", False), ("", False), (None, False)):
                with self.subTest(producer_attempt=attempt):
                    env.pop("EXPECTED_PRODUCER_RUN_ATTEMPT", None)
                    if attempt is not None:
                        env["EXPECTED_PRODUCER_RUN_ATTEMPT"] = attempt
                    made = run([sys.executable, "-c", script, receipt], env=env)
                    if made.returncode:
                        self.assertFalse(should_pass, made.stderr)
                        continue
                    verified = run([sys.executable, cli, "verify-receipt", "--file", receipt], env=env)
                    self.assertEqual(verified.returncode == 0, should_pass,
                                     verified.stdout + verified.stderr)

    def test_gate_default_repo_dir_is_repository_root(self):
        script = raw(GATE)
        line = next(line for line in script.splitlines()
                    if '[ -n "$repo_dir" ] || repo_dir=' in line)
        # 仅执行真实默认值赋值，不运行构建脚本。
        with tempfile.TemporaryDirectory() as td:
            probe = os.path.join(td, "probe.sh")
            with open(probe, "w", encoding="utf-8") as handle:
                handle.write('repo_dir=""\n' + line.replace('${BASH_SOURCE[0]}', GATE)
                             + '\nprintf "%s" "$repo_dir"\n')
            proc = run(["bash", probe], env=ENV)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(proc.stdout, WORKTREE)

    def test_hosted_boost_assertion_requires_private_prefix(self):
        block = next(b for b in _extract_run_blocks(self.text) if "boost_links=" in b)
        probe = block[block.index("boost_links="):block.index("# bbt 必须解析到")]
        with tempfile.TemporaryDirectory() as td:
            env = dict(ENV, BBT_BUILD_DIR=td, BOOST_PREFIX=os.path.join(td, "boost"))
            for path, should_pass in ((env["BOOST_PREFIX"] + "/lib/libboost_context.so.1.90.0", True),
                                      ("/usr/lib/libboost_context.so.1.90.0", False),
                                      ("not found", False)):
                with self.subTest(path=path):
                    env["TEST_LINK"] = "libboost_context.so.1.90.0 => " + path
                    script = 'ldd() { printf "%s\\n" "$TEST_LINK"; }; missing=0\n' + probe + '\nexit "$missing"'
                    proc = run(["bash", "-c", script], env=env)
                    self.assertEqual(proc.returncode == 0, should_pass, proc.stderr)

    def test_no_cache(self):
        for forbidden in ("actions/cache", "ccache", "cache:"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, self.stripped)

    def test_preserves_cross_runner_gate_commands(self):
        for needle in (
            "bash scripts/fetch_deps.sh",
            'bash scripts/prepare_protobuf.sh "$BBT_PROTOBUF_PREFIX"',
            'bash scripts/ci/prepare_boost.sh "$BOOST_PREFIX" --jobs "$BBT_JOBS"',
            "run_framework_gate.sh build",
            'ctest --test-dir "$BBT_BUILD_DIR" -j1 --timeout 300 --output-on-failure --no-tests=error',
            "100% tests passed, 0 tests failed",
            "(Skipped|Not Run)",
            "rpc_xlang_server",
            "getvalue/getvalue_server",
            "libboost_context",
        ):
            with self.subTest(needle=needle):
                self.assertIn(needle, self.stripped)

    def test_build_recipe_delegation_and_fixed_dependencies(self):
        # 正式入口负责准备依赖；gate 负责调用唯一构建配方，不要求旧内联块重复存在。
        for needle in ('bash scripts/fetch_deps.sh',
                       'bash scripts/prepare_protobuf.sh "$BBT_PROTOBUF_PREFIX"',
                       'bash scripts/ci/run_framework_gate.sh build'):
            with self.subTest(workflow=needle):
                self.assertIn(needle, self.stripped)
        gate = body(GATE)
        for needle in ('bash "$repo_dir/scripts/local_build.sh"',
                       'export BBT_NEED_TEST=ON', 'export BBT_SKIP_CTEST=1',
                       '-DBBT_PROTOBUF_PREFIX=$protobuf_prefix'):
            with self.subTest(gate=needle):
                self.assertIn(needle, gate)
        for dependency in ('coroutine', 'infra'):
            self.assertRegex(raw(DEPS_LOCK), rf'{dependency} repo=\S+ sha=[0-9a-f]{{40}}')

    def test_delegated_dual_service_negative_gate_preserved(self):
        self.assertIn('bash scripts/ci/run_framework_gate.sh build', self.stripped)
        gate = body(GATE)
        for needle in ('-DBBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON',
                       "grep -q '^BBT_ENABLE_DUAL_SERVICE_EXAMPLE:BOOL=OFF$'",
                       '缺少前置条件', '-DBBT_HIREDIS_PREFIX',
                       'examples/dual_service', 'if [ "$rc" -eq 0 ]; then'):
            with self.subTest(needle=needle):
                self.assertIn(needle, gate)

    def test_build_and_test_link_source_evidence_preserved(self):
        for source in (self.stripped, body(GATE)):
            for needle in ('deps-manifest.txt', 'link-sources.txt',
                           'if grep -q /usr/local/', 'exit 7'):
                with self.subTest(needle=needle, source='workflow' if source == self.stripped else 'gate'):
                    self.assertIn(needle, source)

    def test_example_exec_bits_and_actual_build_directory_preserved(self):
        block = next(b for b in _extract_run_blocks(self.text) if 'rpc_candidates=' in b)
        for needle in ('rpc_xlang_server', 'Test_framework_*',
                       'getvalue/getvalue_server', 'getvalue/getvalue_caller',
                       'two_service/two_service', 'lifecycle_matrix/lifecycle_fixture',
                       'BBT_BUILD_DIR=$actual_build_dir', 'chmod +x "$bin"',
                       'if [ ! -f "$bin" ]; then'):
            with self.subTest(needle=needle):
                self.assertIn(needle, block)

    def test_ldd_missing_and_external_bbt_guards_preserved(self):
        block = next(b for b in _extract_run_blocks(self.text) if 'boost_links=' in b)
        for needle in ("grep -q 'not found'", '/usr/local/.*bbt',
                       'missing=1', 'exit "$missing"'):
            with self.subTest(needle=needle):
                self.assertIn(needle, block)

    def test_manual_jobs_preserved_with_inputs_and_resource_boundaries(self):
        # 手动工具链验收与双服务资源验收任务、inputs/资源边界保留，不删除假完成。
        self.assertIn("verify_toolchain.sh --in-image --lock \"$GITHUB_WORKSPACE/docker/toolchain.lock\"",
                      self.stripped)
        self.assertIn("if: github.event_name == 'workflow_dispatch'", self.stripped)
        self.assertIn("inputs.resource_acceptance == true", self.stripped)
        for needle in (
            'RESOURCE_RECIPE_SHA: "1543bbd0aca4defa1be701b45aea5ba2cc2d0d86"',
            "redis@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499",
            "mongo@sha256:4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2",
            ".resource-recipe",
            "run_acceptance.sh",
        ):
            with self.subTest(needle=needle):
                self.assertIn(needle, self.stripped)

    def test_inline_run_blocks_pass_bash_n(self):
        blocks = _extract_run_blocks(self.text)
        self.assertTrue(blocks, "应提取到至少一个 run 块")
        for script in blocks:
            head = script.strip().splitlines()[0] if script.strip() else ""
            with self.subTest(head=head):
                fd, tmp = tempfile.mkstemp(suffix=".sh")
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as handle:
                        handle.write(script)
                    proc = run(["bash", "-n", tmp], env=ENV)
                    self.assertEqual(proc.returncode, 0, proc.stderr + script)
                finally:
                    os.unlink(tmp)


# --------------------------------------------------------------------------- #
# 正式 workflow 结构契约（需 PyYAML）
# --------------------------------------------------------------------------- #
try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _loader():
    assert yaml is not None

    class Base(yaml.SafeLoader):
        pass

    Base.yaml_implicit_resolvers = {
        ch: [e for e in entries if e[0] != "tag:yaml.org,2002:bool"]
        for ch, entries in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }
    Base.add_implicit_resolver(
        "tag:yaml.org,2002:bool",
        re.compile(r"^(?:true|false|True|False|TRUE|FALSE)$"),
        list("tTfF"),
    )

    class Strict(Base):
        def construct_mapping(self, node, deep=False):
            seen = set()
            for key_node, _ in node.value:
                key = self.construct_object(key_node, deep=deep)
                if key in seen:
                    raise ValueError(f"duplicate key: {key}")
                seen.add(key)
            return super().construct_mapping(node, deep)

    return Strict


@unittest.skipIf(yaml is None, "PyYAML 不可用：结构契约显式 SKIP（不等于通过）")
class FormalWorkflowStructureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        assert yaml is not None
        cls.wf = yaml.load(raw(CI_YML), Loader=_loader())
        cls.text = raw(CI_YML)

    def test_toplevel_shape_and_jobs(self):
        self.assertEqual(self.wf["name"], "ci")
        self.assertEqual(set(self.wf), {"name", "on", "permissions", "concurrency", "env", "jobs"})
        self.assertEqual(
            set(self.wf["jobs"]),
            {"changes", "plan", "build", "test", "result", "toolchain-verify", "resource-acceptance"},
        )

    def test_triggers(self):
        on = self.wf["on"]
        self.assertEqual(set(on), {"push", "pull_request", "workflow_dispatch"})
        self.assertEqual(on["push"]["branches"], ["main"])
        self.assertEqual(on["pull_request"]["branches"], ["main"])
        self.assertIn("resource_acceptance", on["workflow_dispatch"]["inputs"])

    def test_permissions_minimal(self):
        self.assertEqual(self.wf["permissions"], {})
        for name, job in self.wf["jobs"].items():
            with self.subTest(job=name):
                self.assertNotIn("write", str(job.get("permissions", {})))
                if "uses" not in job:
                    self.assertEqual(job.get("permissions"), {"contents": "read"})

    def test_reusable_jobs_have_no_runs_on(self):
        for name, job in self.wf["jobs"].items():
            if "uses" in job:
                self.assertNotIn("runs-on", job, f"{name} 为 reusable 调用")
                self.assertEqual(job["uses"], CALLEE)
                self.assertEqual(job["with"]["profile"], "hosted")
                self.assertIn("github.sha", job["with"]["source_sha"])

    def test_hosted_and_manual_runner_split(self):
        for name in ("changes", "build", "test"):
            with self.subTest(job=name):
                self.assertEqual(self.wf["jobs"][name]["runs-on"], HOSTED_RUNNER)
        for name in ("toolchain-verify", "resource-acceptance"):
            with self.subTest(job=name):
                self.assertEqual(self.wf["jobs"][name]["runs-on"], MANUAL_RUNNER)
                self.assertIn("workflow_dispatch", str(self.wf["jobs"][name]["if"]))

    def test_build_test_chain_and_results(self):
        build = self.wf["jobs"]["build"]
        test = self.wf["jobs"]["test"]
        self.assertEqual(build["needs"], ["changes", "plan"])
        self.assertEqual(test["needs"], ["changes", "plan", "build"])
        self.assertIn("docs-only", str(build["if"]))
        self.assertIn("docs-only", str(test["if"]))
        self.assertIn("archive_sha256", build["outputs"])
        self.assertIn("provenance_digest", build["outputs"])
        result = self.wf["jobs"]["result"]
        self.assertEqual(result["if"], "${{ always() }}")
        self.assertEqual(set(result["needs"]), {"changes", "plan", "build", "test"})
        self.assertEqual(json.loads(result["with"]["required_checks_json"]), ["changes", "plan"])
        self.assertEqual(json.loads(result["with"]["optional_checks_json"]), ["build", "test"])

    def test_required_check_job_names_preserved(self):
        # job 名保持现役 required check 名，避免 required context 悬空。
        self.assertEqual(self.wf["jobs"]["changes"]["name"], "变更类型检测")
        self.assertEqual(self.wf["jobs"]["build"]["name"], "Build (pinned deps)")
        self.assertEqual(self.wf["jobs"]["test"]["name"], "Test (ctest -j1)")


# --------------------------------------------------------------------------- #
# 分类路由冒烟：真实复用 framework callee 逻辑（不复制契约）
# --------------------------------------------------------------------------- #
SHARED = os.environ.get("BBT_CI_SHARED_DIR", "")


def _formal_payload(changed_files_json, classifier_status="ok"):
    return {
        "repo": "yqm-307/bbt-framework",
        "source_sha": "a" * 40,
        "profile": "hosted",
        "concurrency": "2",
        "timeout_minutes": "60",
        "event_json": '{"name":"pull_request","base_ref":"main"}',
        "changed_files_json": changed_files_json,
        "required_checks_json": '["changes","plan"]',
        "optional_checks_json": '["build","test"]',
        "classifier_status": classifier_status,
    }


@unittest.skipUnless(
    SHARED and os.path.isfile(os.path.join(SHARED, "cli.py")),
    "BBT_CI_SHARED_DIR 未指向已发布 callee scripts/ci/shared：分类路由冒烟显式 SKIP",
)
class ClassificationRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cli = os.path.join(SHARED, "cli.py")

    def _classify(self, changed, status="ok"):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(_formal_payload(changed, status), handle)
            path = handle.name
        proc = run([sys.executable, self.cli, "classify", "--file", path], env=ENV)
        os.unlink(path)
        return proc

    def _evaluate(self, plan, results):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump({"plan": plan, "results": results}, handle)
            path = handle.name
        proc = run([sys.executable, self.cli, "evaluate", "--file", path], env=ENV)
        os.unlink(path)
        return proc

    def _plan_for(self, changed, status="ok"):
        proc = self._classify(changed, status)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        return json.loads(proc.stdout)["plan"]

    def test_docs_only_routing(self):
        proc = self._classify('["docs/ci/formal-ci.md"]')
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["classification"], "docs-only")

    def test_code_routing(self):
        self.assertEqual(
            json.loads(self._classify('["framework/src/Framework.cc"]').stdout)["classification"],
            "code",
        )

    def test_classifier_failure_is_unknown_not_docs(self):
        self.assertEqual(
            json.loads(self._classify('["docs/ci/formal-ci.md"]', status="failed").stdout)["classification"],
            "unknown",
        )
        self.assertEqual(json.loads(self._classify("[]").stdout)["classification"], "unknown")

    def test_docs_only_allows_build_test_skip_and_is_success(self):
        plan = self._plan_for('["docs/ci/formal-ci.md"]')
        proc = self._evaluate(
            plan,
            {"changes": "success", "plan": "success", "build": "skipped", "test": "skipped"},
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_code_build_failure_is_failure(self):
        plan = self._plan_for('["framework/src/x.cc"]')
        proc = self._evaluate(
            plan,
            {"changes": "success", "plan": "success", "build": "failure", "test": "skipped"},
        )
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_code_test_skipped_is_failure(self):
        plan = self._plan_for('["framework/src/x.cc"]')
        proc = self._evaluate(
            plan,
            {"changes": "success", "plan": "success", "build": "success", "test": "skipped"},
        )
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_required_skipped_is_failure(self):
        plan = self._plan_for('["framework/src/x.cc"]')
        proc = self._evaluate(
            plan,
            {"changes": "success", "plan": "skipped", "build": "skipped", "test": "skipped"},
        )
        self.assertEqual(proc.returncode, 1)

    def test_unknown_classification_forbids_skip(self):
        plan = self._plan_for("[]", status="failed")
        proc = self._evaluate(
            plan,
            {"changes": "success", "plan": "success", "build": "skipped", "test": "skipped"},
        )
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_code_build_test_success_is_success(self):
        plan = self._plan_for('["framework/src/x.cc"]')
        proc = self._evaluate(
            plan,
            {"changes": "success", "plan": "success", "build": "success", "test": "success"},
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
