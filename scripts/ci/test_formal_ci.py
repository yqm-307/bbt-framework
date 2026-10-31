#!/usr/bin/env python3
"""正式 `.github/workflows/ci.yml`（唯一权威普通入口）离线冒烟与直接耦合测试。

本文件是现役普通 CI（fw-p3a 内联 callee 链删除、fw-p3b 重写为单 runner
build_test + 本仓 result.py）的**直接测试**：只验证「CI 配方 / 内联分类与
结果判定 / 静态与结构契约 / 手动任务保留（manual.yml）」，**不跑 C++ 全量
构建、不下载源码、不连网**（真实 build→test 走父级授权的在线 CI）。stdlib
为主，PyYAML 可选增强。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v scripts/ci/test_formal_ci.py
  # 结构契约需 PyYAML（缺省显式 SKIP，不等于通过）：
  UV_OFFLINE=1 UV_CACHE_DIR=<cache> uv run --no-project --with pyyaml \
    python3 scripts/ci/test_formal_ci.py

覆盖：
- 配方守卫：脚本 bash -n、python compile、prepare_boost --print-plan 固定值、
  fail-closed 拒绝；
- gate verify-archive 接收侧配方（独立子命令仍在仓内）：sha256 校验 + 闭集
  成员/路径穿越/绝对路径/逃逸 symlink/设备成员拒绝（synthetic 归档非真实
  C++ 产物）；正式 workflow 为单 runner，不再跨 runner 传归档；
- changed_files 路由：push/PR 真实 diff 归一、本仓内联分类、未知保守回退、
  超预算整集回退；
- 正式 workflow 文本契约（stdlib，恒跑，直接读真实 ci.yml 与 manual.yml）：
  触发面/身份/权限/concurrency/hosted runner/固定 SHA pin/无 callee 复用/
  内联分类与 result.py required-optional/docs-only 跳过门禁/always result/
  单 runner build_test 判据命令/内联 bash -n/无影子双跑/无跨 runner artifact；
- 正式 workflow 结构契约（需 PyYAML，缺省显式 SKIP）；
- 手动任务保留契约：manual.yml 中 toolchain-verify / resource-acceptance 及
  其 inputs/资源边界不删不改；
- gate 内联 provenance：执行真实 heredoc 块，manifest+envelope 绑定 producer
  身份，digest 只对实际写入字节计算；
- 结果判定端到端：直接执行本仓 scripts/ci/result.py CLI，required/optional
  判定与 docs-only 放行语义不依赖已删除的 shared callee。
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
MANUAL_YML = os.path.join(WORKTREE, ".github", "workflows", "manual.yml")
PREPARE_BOOST = os.path.join(HERE, "prepare_boost.sh")
GATE = os.path.join(HERE, "run_framework_gate.sh")
CHANGED = os.path.join(HERE, "changed_files.py")
RESULT = os.path.join(HERE, "result.py")
FETCH_DEPS = os.path.join(WORKTREE, "scripts", "fetch_deps.sh")
PREPARE_PROTOBUF = os.path.join(WORKTREE, "scripts", "prepare_protobuf.sh")
LOCAL_BUILD = os.path.join(WORKTREE, "scripts", "local_build.sh")
BUILD_STACK = os.path.join(WORKTREE, "scripts", "build_stack.sh")
RUN_CTEST = os.path.join(WORKTREE, "scripts", "run_ctest.sh")
VERIFY_TOOLCHAIN = os.path.join(WORKTREE, "scripts", "verify_toolchain.sh")
DEPS_LOCK = os.path.join(WORKTREE, "deps.lock")
TOOLCHAIN_LOCK = os.path.join(WORKTREE, "docker", "toolchain.lock")

CHECKOUT_SHA = "11d5960a326750d5838078e36cf38b85af677262"
SHA_PIN_RE = re.compile(r"^actions/[^@\s]+@[0-9a-f]{40}$")
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
        cls.manual_text = raw(MANUAL_YML)
        cls.manual_stripped = body(MANUAL_YML)

    def test_name_and_single_authoritative_identity(self):
        self.assertIn("\nname: ci\n", self.text)
        # 无影子双跑、无跨 runner artifact 名残留（build_test 单 runner，
        # 归档只在 runner 内中转，不上传 artifact）。
        self.assertNotIn("bbt-shadow-build-", self.stripped)
        self.assertNotIn("ci-shadow-v1", self.stripped)
        self.assertNotIn("bbt-build-", self.stripped)
        self.assertNotIn("upload-artifact", self.stripped)
        self.assertNotIn("download-artifact", self.stripped)

    def test_triggers_are_pr_push_main_and_manual(self):
        self.assertIn("pull_request:", self.stripped)
        self.assertIn("push:", self.stripped)
        self.assertIn("workflow_dispatch:", self.stripped)
        self.assertIn("branches: [main]", self.stripped)
        for forbidden in ("schedule:", "merge_group"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, self.stripped)
        # 手动 resource_acceptance 输入随 manual.yml 迁移，不在正式 ci.yml。
        self.assertNotIn("resource_acceptance:", self.stripped)
        self.assertIn("resource_acceptance:", self.manual_stripped)

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
        # 正式 ci.yml 全部 hosted（手动重型 job 已迁移到 manual.yml）。
        self.assertEqual(set(runs_on), {HOSTED_RUNNER})
        self.assertGreaterEqual(runs_on.count(HOSTED_RUNNER), 3)
        # 手动 job 保留在 manual.yml，仍跑专用 runner。
        manual_runs_on = re.findall(r"(?m)^\s*runs-on:\s*(\S+)\s*$",
                                    self.manual_stripped)
        self.assertEqual(manual_runs_on, [MANUAL_RUNNER, MANUAL_RUNNER])

    def test_all_uses_are_full_sha_pinned_and_expected(self):
        # 只使用 actions/* 全 40-hex pin；无 reusable workflow uses（callee 链已删）。
        for label, text in (("ci", self.stripped), ("manual", self.manual_stripped)):
            uses = re.findall(r"(?m)^\s*uses:\s*(\S+)\s*$", text)
            self.assertTrue(uses, f"{label} 应存在 uses:")
            for item in uses:
                with self.subTest(workflow=label, uses=item):
                    self.assertRegex(item, SHA_PIN_RE)
        self.assertIn(f"actions/checkout@{CHECKOUT_SHA}", self.stripped)
        self.assertIn(f"actions/checkout@{CHECKOUT_SHA}", self.manual_stripped)

    def test_no_reusable_callee_or_local_workflow_call(self):
        # classify/result callee 链已删除：无 workflow_call 触发器，无
        # `owner/repo/.github/workflows/...@sha` 或 `./.github/workflows` 复用。
        self.assertNotIn("workflow_call", self.stripped)
        self.assertNotIn("workflow_call", self.manual_stripped)
        self.assertNotIn("/.github/workflows/", self.stripped)
        self.assertNotIn("./.github/workflows", self.stripped)
        self.assertNotIn("bbtools-classify", self.stripped)
        for forbidden in ("required_checks_json:", "optional_checks_json:",
                          "source_sha:", "profile:"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, self.stripped)

    def test_no_caller_supplied_shell_or_jobs_input(self):
        # 分类在本仓 changes job 内联执行（changed_files.py），不经 callee 传参。
        self.assertIn("python3 scripts/ci/changed_files.py", self.stripped)
        self.assertIn("steps.diff.outputs.classification", self.stripped)
        self.assertIn("steps.diff.outputs.changed_files_json", self.stripped)
        self.assertIn("steps.diff.outputs.classifier_status", self.stripped)

    def test_required_and_optional_disjoint_and_build_test_optional(self):
        # result job 内联 result.py：required/optional 集合写死在 heredoc。
        self.assertIn('"required": ["changes", "docs_check"]', self.stripped)
        self.assertIn('"optional": ["build_test"]', self.stripped)
        self.assertIn("needs.changes.outputs.classification != 'docs-only'", self.stripped)
        self.assertIn("needs.changes.result == 'success'", self.stripped)

    def test_result_job_aggregates_always_and_passes_all_job_results(self):
        self.assertIn("if: ${{ always() }}", self.stripped)
        self.assertIn("needs: [changes, docs_check, build_test]", self.stripped)
        self.assertIn(
            "format('{{\"changes\":\"{0}\",\"docs_check\":\"{1}\",\"build_test\":\"{2}\"}}'",
            self.stripped,
        )
        for ref in ("needs.changes.result", "needs.docs_check.result",
                    "needs.build_test.result"):
            with self.subTest(ref=ref):
                self.assertIn(ref, self.stripped)
        # 汇聚走本仓 result.py（fail-closed，非零即红），不再经 callee。
        self.assertIn("python3 scripts/ci/result.py --file \"$payload\"", self.stripped)

    def test_result_job_has_conservative_fallbacks(self):
        # changes 失败时 outputs 为空 → classification 回退 unknown → 执行全部。
        self.assertIn("|| 'unknown'", self.stripped)
        self.assertIn(
            "CLASSIFICATION: ${{ needs.changes.outputs.classification || 'unknown' }}",
            self.stripped,
        )

    def test_gate_build_emits_key_value_outputs(self):
        # gate build 的 key=value 输出保留（单 runner 内消费/日志审计），
        # 摘要仍只对真实写入字节计算。
        gate = raw(GATE)
        for needle in ("printf 'build_archive_sha256=%s\\n'",
                       "printf 'provenance_digest=%s\\n'",
                       "printf 'producer_run_attempt=%s\\n' \"$run_attempt\"",
                       "printf 'archive_bytes=%s\\n'"):
            with self.subTest(needle=needle):
                self.assertIn(needle, gate)

    def test_gate_inline_produce_emits_manifest_envelope_and_real_digest(self):
        # 执行 gate 脚本中内联 produce 块（不再调用 shared cli.py）：
        # manifest.json + envelope.json + artifact_digest 必须真实产出，
        # digest 是对实际写入字节的 sha256，envelope 绑定 producer 身份。
        script = raw(GATE)
        marker = 'python3 - "$meta_in" "$meta_dir" "$meta_out" <<\'PY\''
        block = script.split(marker, 1)[1].split("\nPY\n", 1)[0]
        with tempfile.TemporaryDirectory() as td:
            meta_in = os.path.join(td, "in.json")
            meta_dir = os.path.join(td, "artifact-meta")
            emit_path = os.path.join(td, "emit.out")
            sha = "a" * 40  # 明确合成身份，仅用于离线契约测试。
            manifest = {"manifest_version": 1, "source_repo": "example/repo",
                        "source_sha": sha, "source_head": sha,
                        "automation_repo": "example/repo", "automation_sha": sha,
                        "run_id": 123, "run_attempt": 1, "job": "build"}
            with open(meta_in, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle)
            proc = run([sys.executable, "-c", block, meta_in, meta_dir, emit_path],
                       env=ENV)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            import hashlib

            payload = os.path.join(meta_dir, "manifest.json")
            self.assertTrue(os.path.isfile(payload))
            with open(payload, "rb") as handle:
                data = handle.read()
            expected = "sha256:" + hashlib.sha256(data).hexdigest()
            with open(os.path.join(meta_dir, "envelope.json"), encoding="utf-8") as handle:
                env = json.load(handle)["envelope"]
            self.assertEqual(env["artifact_digest"], expected)
            self.assertEqual(env["artifact_path"], "manifest.json")
            self.assertEqual(env["producer_run_attempt"], 1)
            self.assertEqual(env["producer_source_sha"], sha)
            self.assertEqual(env["workflow_sha"], sha)
            with open(emit_path, encoding="utf-8") as handle:
                self.assertIn("artifact_digest=" + expected, handle.read())

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

    def test_preserves_build_test_gate_commands(self):
        # 单 runner build_test 判据命令原样保留（deps/前缀/gate build/ctest
        # 门禁/no-SKIP/链接来源）。跨 runner 专属的 exec-bit 恢复块已随
        # artifact 通路退役，不在本断言内。
        for needle in (
            "bash scripts/fetch_deps.sh",
            'bash scripts/prepare_protobuf.sh "$BBT_PROTOBUF_PREFIX"',
            'bash scripts/ci/prepare_boost.sh "$BOOST_PREFIX" --jobs "$BBT_JOBS"',
            "run_framework_gate.sh build",
            'ctest --test-dir "$BBT_BUILD_DIR" -j1 --timeout 300 --output-on-failure --no-tests=error',
            "100% tests passed, 0 tests failed",
            "(Skipped|Not Run)",
            "rpc_xlang_server",
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

    def test_example_binaries_checked_via_ldd_block(self):
        # 单 runner 下构建产物不经归档传输，exec-bit 恢复块退役；本 job 仍
        # 对 Test_framework_* 与 rpc_xlang_server 做 ldd fail-fast 复核。
        block = next(b for b in _extract_run_blocks(self.text) if 'boost_links=' in b)
        for needle in ("rpc_xlang_server", "Test_framework_*",
                       "verified binaries: $found",
                       'if [ "$found" -eq 0 ]', "exit 9"):
            with self.subTest(needle=needle):
                self.assertIn(needle, block)

    def test_ldd_missing_and_external_bbt_guards_preserved(self):
        block = next(b for b in _extract_run_blocks(self.text) if 'boost_links=' in b)
        for needle in ("grep -q 'not found'", '/usr/local/.*bbt',
                       'missing=1', 'exit "$missing"'):
            with self.subTest(needle=needle):
                self.assertIn(needle, block)

    def test_manual_jobs_preserved_with_inputs_and_resource_boundaries(self):
        # 手动工具链验收与双服务资源验收任务迁移到 manual.yml：
        # inputs/资源边界（recipe SHA、Redis/Mongo digest、专用前缀、清理）不变。
        text = self.manual_stripped
        self.assertIn("verify_toolchain.sh --in-image --lock \"$GITHUB_WORKSPACE/docker/toolchain.lock\"",
                      text)
        self.assertIn("if: github.event_name == 'workflow_dispatch'", text)
        self.assertIn("inputs.resource_acceptance == true", text)
        for needle in (
            'RESOURCE_RECIPE_SHA: "1543bbd0aca4defa1be701b45aea5ba2cc2d0d86"',
            "redis@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499",
            "mongo@sha256:4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2",
            ".resource-recipe",
            "run_acceptance.sh",
        ):
            with self.subTest(needle=needle):
                self.assertIn(needle, text)
        # 手动任务不得回流到正式 ci.yml（非 required 面不变）。
        for forbidden in ("arc-s4-framework", "resource-acceptance:",
                          "toolchain-verify:", "RESOURCE_RECIPE_SHA"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, self.stripped)

    def test_inline_run_blocks_pass_bash_n(self):
        for label, text in (("ci", self.text), ("manual", self.manual_text)):
            blocks = _extract_run_blocks(text)
            self.assertTrue(blocks, f"{label} 应提取到至少一个 run 块")
            for script in blocks:
                head = script.strip().splitlines()[0] if script.strip() else ""
                with self.subTest(workflow=label, head=head):
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
        cls.manual = yaml.load(raw(MANUAL_YML), Loader=_loader())
        cls.text = raw(CI_YML)

    def test_toplevel_shape_and_jobs(self):
        self.assertEqual(self.wf["name"], "ci")
        self.assertEqual(set(self.wf), {"name", "on", "permissions", "concurrency", "env", "jobs"})
        self.assertEqual(
            set(self.wf["jobs"]),
            {"changes", "docs_check", "build_test", "result"},
        )
        self.assertEqual(self.manual["name"], "manual")
        self.assertEqual(
            set(self.manual["jobs"]),
            {"toolchain-verify", "resource-acceptance"},
        )

    def test_triggers(self):
        on = self.wf["on"]
        self.assertEqual(set(on), {"push", "pull_request", "workflow_dispatch"})
        self.assertEqual(on["push"]["branches"], ["main"])
        self.assertEqual(on["pull_request"]["branches"], ["main"])
        # 正式 ci.yml 的 workflow_dispatch 无 inputs；resource_acceptance 在 manual.yml。
        self.assertNotIn(
            "resource_acceptance", (on["workflow_dispatch"] or {}).get("inputs") or {})
        mon = self.manual["on"]
        self.assertEqual(set(mon), {"workflow_dispatch"})
        self.assertIn("resource_acceptance", mon["workflow_dispatch"]["inputs"])

    def test_permissions_minimal(self):
        for label, wf in (("ci", self.wf), ("manual", self.manual)):
            self.assertEqual(wf["permissions"], {})
            for name, job in wf["jobs"].items():
                with self.subTest(workflow=label, job=name):
                    self.assertNotIn("write", str(job.get("permissions", {})))
                    self.assertEqual(job.get("permissions"), {"contents": "read"})

    def test_no_reusable_jobs_anywhere(self):
        for label, wf in (("ci", self.wf), ("manual", self.manual)):
            for name, job in wf["jobs"].items():
                with self.subTest(workflow=label, job=name):
                    self.assertNotIn("uses", job, f"{label}/{name} 不得是 reusable 调用")

    def test_hosted_and_manual_runner_split(self):
        for name in ("changes", "docs_check", "build_test", "result"):
            with self.subTest(job=name):
                self.assertEqual(self.wf["jobs"][name]["runs-on"], HOSTED_RUNNER)
        for name in ("toolchain-verify", "resource-acceptance"):
            with self.subTest(job=name):
                self.assertEqual(self.manual["jobs"][name]["runs-on"], MANUAL_RUNNER)
                self.assertIn("workflow_dispatch", str(self.manual["jobs"][name]["if"]))

    def test_build_test_and_result_chain(self):
        build_test = self.wf["jobs"]["build_test"]
        self.assertEqual(build_test["needs"], ["changes"])
        self.assertIn("docs-only", str(build_test["if"]))
        self.assertIn("changes.result", str(build_test["if"]))
        result = self.wf["jobs"]["result"]
        self.assertEqual(result["if"], "${{ always() }}")
        self.assertEqual(set(result["needs"]), {"changes", "docs_check", "build_test"})
        # result job 直接运行本仓 result.py；required/optional 写死在 heredoc。
        run_text = "\n".join(step.get("run", "") for step in result["steps"])
        self.assertIn("scripts/ci/result.py", run_text)
        self.assertIn('"required": ["changes", "docs_check"]', run_text)
        self.assertIn('"optional": ["build_test"]', run_text)

    def test_required_check_job_names_preserved(self):
        # job 名保持现役 required check 名，避免 required context 悬空。
        self.assertEqual(self.wf["jobs"]["changes"]["name"], "变更类型检测")
        self.assertEqual(self.wf["jobs"]["docs_check"]["name"], "Docs check")
        self.assertEqual(self.wf["jobs"]["build_test"]["name"],
                         "Build + Test (pinned deps, ctest -j1)")
        self.assertNotIn("name", self.wf["jobs"]["result"])


# --------------------------------------------------------------------------- #
# 结果判定端到端：直接执行本仓 result.py CLI（shared callee 已按 D3 删除，
# required/optional 语义由 result.py 承接，本类走真实 CLI 不复制契约）。
# --------------------------------------------------------------------------- #
def _result_cli(results, required, optional, classification):
    payload = {
        "results": results,
        "required": required,
        "optional": optional,
        "classification": classification,
    }
    return run([sys.executable, RESULT], input=json.dumps(payload), env=ENV)


class ResultContractRoutingTests(unittest.TestCase):
    """needs.*.result → verdict 的端到端判定（与 result job heredoc 同一 payload 形状）。"""

    REQUIRED = ["changes", "docs_check"]
    OPTIONAL = ["build_test"]

    def _eval(self, results, classification="code"):
        return _result_cli(results, self.REQUIRED, self.OPTIONAL, classification)

    def test_all_success_is_success(self):
        proc = self._eval({"changes": "success", "docs_check": "success",
                           "build_test": "success"})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_docs_only_allows_build_test_skip_and_is_success(self):
        proc = self._eval({"changes": "success", "docs_check": "success",
                           "build_test": "skipped"}, "docs-only")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_code_build_test_failure_is_failure(self):
        proc = self._eval({"changes": "success", "docs_check": "success",
                           "build_test": "failure"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_code_build_test_skipped_is_failure(self):
        proc = self._eval({"changes": "success", "docs_check": "success",
                           "build_test": "skipped"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_required_skipped_is_failure(self):
        for results in (
            {"changes": "skipped", "docs_check": "success", "build_test": "success"},
            {"changes": "success", "docs_check": "skipped", "build_test": "success"},
        ):
            with self.subTest(results=results):
                proc = self._eval(results, "docs-only")
                self.assertEqual(proc.returncode, 1)

    def test_unknown_classification_forbids_skip(self):
        proc = self._eval({"changes": "success", "docs_check": "success",
                           "build_test": "skipped"}, "unknown")
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_contract_rejects_exit_3(self):
        # 空 required / 集合外 job / 非法状态 / required∩optional → rc 3 拒绝。
        for payload in (
            {"results": {"changes": "success"}, "required": []},
            {"results": {"changes": "success", "other": "success"},
             "required": ["changes"]},
            {"results": {"changes": "bogus"}, "required": ["changes"]},
            {"results": {"changes": "success"},
             "required": ["changes"], "optional": ["changes"]},
            "not json",
            {"results": {}},
        ):
            with self.subTest(payload=payload):
                proc = run([sys.executable, RESULT], input=json.dumps(payload),
                           env=ENV)
                self.assertEqual(proc.returncode, 3, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
