"""候选 reusable workflow / canary caller 的静态契约测试（需要 PyYAML）。

检查：重复 key、workflow_call 类型、输出契约、最小权限、无 secrets:inherit、
hosted-only runs-on、远程 uses 完整 SHA pin、本地 callee 引用仅限 caller、
canary 仅分支 push 触发、真实产物上传/下载固定 SHA 与范围、payload 摘要与归档
digest 严格区分、report 不吞失败、内联脚本 bash -n 语法。
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKTREE = os.path.normpath(os.path.join(HERE, "..", "..", "..", ".."))
WORKFLOW_DIR = os.path.join(WORKTREE, ".github", "workflows")
CLASSIFY = os.path.join(WORKFLOW_DIR, "bbtools-classify-v1.yml")
VERIFY = os.path.join(WORKFLOW_DIR, "bbtools-verify-v1.yml")
CANARY = os.path.join(WORKFLOW_DIR, "bbtools-canary-v1.yml")
CALLER = os.path.join(WORKTREE, "docs", "ci", "caller-example-unpublished.yml")
PUBLISH_PATCH_DOC = os.path.join(WORKTREE, "docs", "ci", "caller-canary-publish-patch.md")
API_DOC = os.path.join(WORKTREE, "docs", "ci", "api.md")
CHANGELOG_DOC = os.path.join(WORKTREE, "docs", "ci", "changelog.md")

SHARED = os.path.normpath(os.path.join(HERE, ".."))
sys.path.insert(0, SHARED)
import input_contract  # noqa: E402

try:
    import yaml
except ImportError:  # pragma: no cover - 用 uv 运行时应可用
    yaml = None

SHA_PIN_RE = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
LOCAL_REF_RE = re.compile(r"^\./\.github/workflows/[A-Za-z0-9._-]+\.yml$")
ALLOWED_INPUT_TYPES = {"string", "boolean", "number"}
REUSABLE = (CLASSIFY, VERIFY)
CALLERS = (CALLER, CANARY)

# #50 T3.1 已发布隔离分支（ci/issue-50-hosted-canary）上的真实模板 commit：canary 两处 callee
# 引用已机械 pin 到该完整 40-hex SHA（在线 run 37870795091，caller d003182… != callee 5b04115…）。
# 重新 pin 时同步更新此处与 docs/ci/（不引入配置/注册表）。
PUBLISHED_CALLEE_SHA = "5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8"
CALLEE_REPO = "yqm-307/bbt-framework"
CANARY_CALLEES = {"classify": "bbtools-classify-v1.yml", "verify": "bbtools-verify-v1.yml"}
PUBLISHED_REF_RE = re.compile(
    r"^(?P<repo>[^/@\s]+/[^/@\s]+)/\.github/workflows/"
    r"(?P<path>[A-Za-z0-9._-]+\.yml)@(?P<sha>[0-9a-f]{40})$"
)


class DuplicateKeyError(Exception):
    pass


def _make_loader():
    class Base(yaml.SafeLoader):
        pass

    # 关闭 YAML 1.1 把 on/off/yes/no 当布尔；只保留 true/false。
    Base.yaml_implicit_resolvers = {
        ch: [entry for entry in entries if entry[0] != "tag:yaml.org,2002:bool"]
        for ch, entries in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }
    Base.add_implicit_resolver(
        "tag:yaml.org,2002:bool",
        re.compile(r"^(?:true|false|True|False|TRUE|FALSE)$"),
        list("tTfF"),
    )

    class Strict(Base):
        def construct_mapping(self, node, deep=False):
            seen = {}
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                if key in seen:
                    raise DuplicateKeyError(str(key))
                seen[key] = self.construct_object(value_node, deep=deep)
            return seen

    return Strict


def load_workflow(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.load(handle, Loader=_make_loader())


def raw(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def body(path):
    """去掉说明性注释行后的实际 YAML 内容（注释里可以提及被禁用的写法）。"""
    return "\n".join(
        line for line in raw(path).splitlines() if not line.lstrip().startswith("#")
    )


def iter_steps(workflow):
    for job in workflow.get("jobs", {}).values():
        for step in job.get("steps", []) or []:
            yield step


def iter_uses(workflow):
    for job in workflow.get("jobs", {}).values():
        if "uses" in job:
            yield job["uses"]
        for step in job.get("steps", []) or []:
            if "uses" in step:
                yield step["uses"]


def parse_published_ref(uses):
    """解析「已发布 reusable 引用」：<owner>/<repo>/.github/workflows/<file>.yml@<40-hex>。"""
    return PUBLISHED_REF_RE.match(uses)


def is_canary_callee_ref(job_name, uses):
    """严格判定 canary callee 引用：本仓 + 正确 callee 路径 + 当前已发布完整 40-hex SHA。

    拒绝分支/tag/短 SHA/跨仓/错误路径/不同 callee SHA/本地 ./ 引用。
    """
    match = parse_published_ref(uses)
    return bool(
        match
        and match.group("repo") == CALLEE_REPO
        and match.group("path") == CANARY_CALLEES.get(job_name)
        and match.group("sha") == PUBLISHED_CALLEE_SHA
    )


@unittest.skipIf(yaml is None, "PyYAML 不可用（用 uv 运行）")
class WorkflowStaticTests(unittest.TestCase):
    def test_no_duplicate_keys(self):
        for path in (*REUSABLE, *CALLERS):
            with self.subTest(path=os.path.basename(path)):
                load_workflow(path)  # 重复 key 会抛 DuplicateKeyError

    def test_workflow_call_present_with_typed_inputs(self):
        for path in REUSABLE:
            with self.subTest(path=os.path.basename(path)):
                wf = load_workflow(path)
                self.assertIn("on", wf)
                call = wf["on"]["workflow_call"]
                inputs = call["inputs"]
                self.assertTrue(inputs)
                for name, spec in inputs.items():
                    self.assertIn(spec.get("type"), ALLOWED_INPUT_TYPES, name)
                    self.assertIn("description", spec, name)

    def test_workflow_call_outputs_contract(self):
        for path in REUSABLE:
            with self.subTest(path=os.path.basename(path)):
                wf = load_workflow(path)
                outputs = wf["on"]["workflow_call"]["outputs"]
                self.assertTrue(outputs)
                for name, spec in outputs.items():
                    self.assertIn("value", spec, name)
                    self.assertIn("jobs.", spec["value"], name)

    def test_no_secrets_inherit(self):
        for path in (*REUSABLE, *CALLERS):
            with self.subTest(path=os.path.basename(path)):
                text = raw(path)
                self.assertNotIn("secrets: inherit", text)
                self.assertNotRegex(text, r"(?m)^\s*secrets:\s*$")
                self.assertNotIn("${{ secrets.", text)

    def test_permissions_are_minimal(self):
        for path in (*REUSABLE, CANARY):
            wf = load_workflow(path)
            with self.subTest(path=os.path.basename(path)):
                top = wf.get("permissions", {})
                self.assertNotIn("write", str(top))
                for job in wf["jobs"].values():
                    self.assertNotIn("write", str(job.get("permissions", {})))

    def test_runs_on_is_hosted_only(self):
        for path in REUSABLE:
            wf = load_workflow(path)
            with self.subTest(path=os.path.basename(path)):
                for job in wf["jobs"].values():
                    runs_on = job.get("runs-on")
                    self.assertEqual(runs_on, "ubuntu-latest")
                    self.assertNotIn("${{", str(runs_on))

    def test_remote_uses_are_full_sha_pinned(self):
        # 远程 action/reusable 必须完整 40-hex pin；本地 ./ 引用只允许出现在 caller。
        for path in (*REUSABLE, *CALLERS):
            wf = load_workflow(path)
            for uses in iter_uses(wf):
                with self.subTest(path=os.path.basename(path), uses=uses):
                    if LOCAL_REF_RE.match(uses):
                        self.assertIn(path, CALLERS, "local callee ref only allowed in caller")
                    else:
                        self.assertRegex(uses, SHA_PIN_RE, uses)

    def test_canary_trigger_is_branch_push_only(self):
        wf = load_workflow(CANARY)
        self.assertEqual(set(wf["on"]), {"push"})
        self.assertEqual(wf["on"]["push"]["branches"], ["ci/issue-50-hosted-canary"])
        text = body(CANARY)
        for forbidden in ("pull_request", "workflow_dispatch", "schedule", "merge_group", "tags:"):
            self.assertNotIn(forbidden, text, forbidden)
        self.assertEqual(wf.get("permissions"), {})

    def test_canary_calls_published_callees_with_explicit_inputs(self):
        # #50 T3.1：canary 两处 uses 已 pin 到已发布隔离分支模板的完整 SHA（caller != callee）。
        wf = load_workflow(CANARY)
        self.assertEqual(set(wf["jobs"]), set(CANARY_CALLEES))
        shas = set()
        for name, job in wf["jobs"].items():
            with self.subTest(job=name):
                match = parse_published_ref(job["uses"])
                self.assertIsNotNone(match, f"must be a published full-SHA ref: {job['uses']}")
                self.assertTrue(is_canary_callee_ref(name, job["uses"]), job["uses"])
                shas.add(match.group("sha"))
                self.assertNotIn("secrets", job)
                with_block = job["with"]
                for key in ("repo", "source_sha", "profile", "required_checks_json"):
                    self.assertIn(key, with_block)
                self.assertEqual(with_block["profile"], "hosted")
                self.assertEqual(job["permissions"], {"contents": "read"})
        self.assertEqual(shas, {PUBLISHED_CALLEE_SHA}, "both callees must pin the same published commit")

    def test_canary_callee_ref_rejects_bad_pins(self):
        # 已发布形状的严格反例：分支/tag/短 SHA/跨仓/错误路径/不同 callee SHA/非本仓 workflows 路径/
        # 本地 ./ 引用都必须被拒（正向对照一并校验）。
        good = f"{CALLEE_REPO}/.github/workflows/bbtools-verify-v1.yml@{PUBLISHED_CALLEE_SHA}"
        self.assertTrue(is_canary_callee_ref("verify", good))
        bad = {
            "branch": f"{CALLEE_REPO}/.github/workflows/bbtools-verify-v1.yml@main",
            "tag": f"{CALLEE_REPO}/.github/workflows/bbtools-verify-v1.yml@v1",
            "short_sha": f"{CALLEE_REPO}/.github/workflows/bbtools-verify-v1.yml@{PUBLISHED_CALLEE_SHA[:7]}",
            "cross_repo": f"yqm-307/bbtools-infra/.github/workflows/bbtools-verify-v1.yml@{PUBLISHED_CALLEE_SHA}",
            "wrong_path": f"{CALLEE_REPO}/.github/workflows/bbtools-classify-v1.yml@{PUBLISHED_CALLEE_SHA}",
            "different_callee_sha": f"{CALLEE_REPO}/.github/workflows/bbtools-verify-v1.yml@{'a' * 40}",
            "non_workflow_path": f"{CALLEE_REPO}/evil/bbtools-verify-v1.yml@{PUBLISHED_CALLEE_SHA}",
            "path_traversal": f"{CALLEE_REPO}/.github/workflows/../bbtools-verify-v1.yml@{PUBLISHED_CALLEE_SHA}",
            "local_ref": "./.github/workflows/bbtools-verify-v1.yml",
        }
        for label, uses in bad.items():
            with self.subTest(case=label):
                self.assertFalse(is_canary_callee_ref("verify", uses), uses)

    def test_verify_upload_download_are_pinned_and_scoped(self):
        text = raw(VERIFY)
        self.assertIn("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02", text)
        self.assertIn("actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093", text)
        wf = load_workflow(VERIFY)
        verify_steps = wf["jobs"]["verify"]["steps"]
        upload = [s for s in verify_steps if "upload-artifact@" in s.get("uses", "")]
        self.assertEqual(len(upload), 1)
        self.assertEqual(upload[0]["with"]["path"], "out/")
        self.assertEqual(upload[0]["with"]["name"], "bbt-canary-payload-v1")
        self.assertEqual(upload[0]["with"]["if-no-files-found"], "error")
        self.assertLessEqual(int(upload[0]["with"]["retention-days"]), 2)
        consume_steps = wf["jobs"]["consume"]["steps"]
        download = [s for s in consume_steps if "download-artifact@" in s.get("uses", "")]
        self.assertEqual(len(download), 1)
        self.assertEqual(download[0]["with"]["name"], "bbt-canary-payload-v1")
        # 只允许本 run：绝不传 run-id/repository 指向任意 run。
        self.assertNotIn("run-id", download[0]["with"])
        self.assertNotIn("repository", download[0]["with"])
        self.assertEqual(wf["jobs"]["consume"]["needs"], "verify")

    def test_verify_payload_digest_is_distinct_from_archive_digest(self):
        wf = load_workflow(VERIFY)
        outputs = wf["jobs"]["verify"]["outputs"]
        self.assertEqual(outputs["payload_digest"], "${{ steps.produce.outputs.artifact_digest }}")
        self.assertEqual(outputs["archive_digest"], "${{ steps.upload.outputs.artifact-digest }}")
        text = raw(VERIFY)
        # 接收侧 expected 的摘要来自 producer job 声明的真实 payload 摘要。
        self.assertIn("BBT_PAYLOAD_DIGEST: ${{ needs.verify.outputs.payload_digest }}", text)
        # 归档 digest 只作运行证据：不得进入 envelope 构造或 expected。
        self.assertIn("BBT_ARCHIVE_DIGEST: ${{ needs.verify.outputs.archive_digest }}", text)
        self.assertNotIn('"artifact_digest": os.environ["BBT_ARCHIVE_DIGEST"]', text)

    def test_verify_never_executes_the_downloaded_payload(self):
        wf = load_workflow(VERIFY)
        consume = wf["jobs"]["consume"]["steps"]
        for step in consume:
            script = step.get("run", "")
            with self.subTest(step=step.get("name")):
                # 只允许通过 cli verify-receipt / hosted_probe 读取；不得直接执行 received 内容。
                self.assertNotRegex(script, r"(?m)^\s*(?:bash|sh|source|\.)\s+\S*received")
                self.assertNotIn("chmod +x", script)
                self.assertNotRegex(script, r"(?m)^\s*cd\s+\S*received")
        for step in consume:
            if "checkout@" in step.get("uses", ""):
                self.assertNotIn("source", str(step["with"].get("path", "")))

    def test_verify_report_does_not_swallow_failures(self):
        self.assertNotIn("always()", body(VERIFY))
        wf = load_workflow(VERIFY)
        report = wf["jobs"]["report"]
        self.assertEqual(report["needs"], ["verify", "consume"])
        check = [s for s in report["steps"] if "needs.verify.result" in str(s.get("if", ""))]
        self.assertTrue(check, "report must explicitly fail when producer/consumer fail")

    def test_verify_refuses_to_consume_failed_producer(self):
        wf = load_workflow(VERIFY)
        guard = [s for s in wf["jobs"]["consume"]["steps"]
                 if "success" in str(s.get("if", ""))]
        self.assertTrue(guard, "consume must refuse a producer that did not succeed")

    def test_verify_produce_reads_actual_source_head(self):
        text = raw(VERIFY)
        self.assertIn("rev-parse HEAD", text)
        self.assertIn('"manifest_version": 1', text)
        self.assertIn("--out-dir", text)

    def test_inline_scripts_pass_bash_n(self):
        for path in REUSABLE:
            wf = load_workflow(path)
            for step in iter_steps(wf):
                script = step.get("run")
                if not script:
                    continue
                with self.subTest(path=os.path.basename(path), step=step.get("name")):
                    fd, tmp = tempfile.mkstemp(suffix=".sh")
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as handle:
                            handle.write(script)
                        proc = subprocess.run(["bash", "-n", tmp], capture_output=True, text=True)
                        self.assertEqual(proc.returncode, 0, proc.stderr)
                    finally:
                        os.unlink(tmp)

    def test_caller_example_references_published_callee(self):
        # caller 示例引用已发布隔离分支模板的真实完整 SHA，并如实标注未合入 main / 非 C++ 验收。
        text = raw(CALLER)
        wf = load_workflow(CALLER)
        uses = [j["uses"] for j in wf["jobs"].values() if "uses" in j]
        self.assertTrue(uses, "caller example must show a reusable call")
        for ref in uses:
            match = parse_published_ref(ref)
            self.assertIsNotNone(match, ref)
            self.assertEqual(match.group("repo"), CALLEE_REPO)
            self.assertIn(match.group("path"), set(CANARY_CALLEES.values()))
            self.assertEqual(match.group("sha"), PUBLISHED_CALLEE_SHA)
        for marker in ("隔离分支", "main", "C++"):
            self.assertIn(marker, text, marker)

    def test_status_docs_distinguish_published_branch_from_unmerged_main(self):
        # 真实状态（run 37870605859 本地同 commit success / 37870795091 caller != callee success）：
        # 模板已发布到隔离分支 ci/issue-50-hosted-canary（完整 SHA 5b04115…）并被在线 hosted canary 实跑；
        # 未合入 main、非 C++/perf 验收、consumer 未迁移。workflow 头部的候选期 UNPUBLISHED 标记是历史
        # （本轮不改 workflow 文本），真实状态以 docs/ci 为准。
        docs = "\n".join((raw(API_DOC), raw(CHANGELOG_DOC), raw(PUBLISH_PATCH_DOC)))
        self.assertIn(PUBLISHED_CALLEE_SHA, docs)
        self.assertIn("隔离分支", docs)
        self.assertNotIn("未在任何远端发布", docs)
        self.assertTrue(("UNPUBLISHED" in raw(CANARY)) or ("未发布" in raw(CANARY)))

    def test_identity_resolved_from_job_context(self):
        # C1：automation 身份必须取自 callee 作用域 job.workflow_*，不得解析 caller 关联变量。
        for path in REUSABLE:
            text = raw(path)
            # 只校验实际 YAML 内容，忽略说明性注释。
            body = "\n".join(
                line for line in text.splitlines() if not line.lstrip().startswith("#")
            )
            with self.subTest(path=os.path.basename(path)):
                self.assertIn("job.workflow_repository", body)
                self.assertIn("job.workflow_sha", body)
                self.assertNotIn("GITHUB_WORKFLOW_REF", body)
                self.assertNotIn("GITHUB_WORKFLOW_SHA", body)
                self.assertNotIn("github.workflow_ref", body)
                self.assertNotIn("github.workflow_sha", body)

    def test_declared_inputs_are_all_in_typed_contract(self):
        # C3/C6：workflow_call 声明的每个输入都必须落在 CLI 输入闭集内（死字段会被拒）。
        defaults = {
            "repo": "yqm-307/bbt-framework",
            "source_sha": "a60edfb4f7cd77e69926be475e2c45e324a3db2b",
            "profile": "hosted",
            "concurrency": "1",
            "timeout_minutes": "20",
            "event_json": '{"name":"workflow_dispatch"}',
            "changed_files_json": "[]",
            "required_checks_json": '["lint"]',
            "optional_checks_json": "[]",
            "classifier_status": "ok",
            "results_json": "",
        }
        for path in REUSABLE:
            wf = load_workflow(path)
            names = list(wf["on"]["workflow_call"]["inputs"])
            with self.subTest(path=os.path.basename(path)):
                payload = {name: defaults.get(name, "") for name in names}
                input_contract.coerce_workflow_inputs(payload)

    def test_verify_has_no_dead_producer_manifest_input(self):
        self.assertNotIn("producer_manifest_json", raw(VERIFY))

    def test_artifact_digest_is_not_fabricated(self):
        # C5 升级：摘要只能来自真实 payload 字节，不再由调用方传入。
        text = raw(VERIFY)
        self.assertNotIn("BBT_ARTIFACT_DIGEST", text)
        self.assertNotIn("0" * 64, text)
        self.assertIn("sys.exit(1)", text)
        self.assertIn("verify-receipt", text)
        self.assertIn("hosted_probe.py", text)
        # envelope 的 artifact_digest 来自 produce 步骤输出，非字面量。
        wf = load_workflow(VERIFY)
        produce = [s for s in wf["jobs"]["verify"]["steps"] if s.get("id") == "produce"]
        self.assertEqual(len(produce), 1)
        self.assertIn("cli.py produce", produce[0]["run"])
        self.assertNotRegex(produce[0]["run"], r"sha256:[0-9a-f]{64}")

    def test_canary_publish_patch_template_applies(self):
        # 从真实已发布 canary 还原本地引用，再应用文档中的两行机械替换。
        # 任一引用缺失/重复/改为坏 pin 都必须失败，其他字节必须保持不变。
        text = raw(CANARY)
        assert yaml is not None  # 本类在无 PyYAML 时整体 skip。
        local_text = text
        replacements = []
        for name, ref in CANARY_CALLEES.items():
            local = f"./.github/workflows/{ref}"
            published = f"{CALLEE_REPO}/{local[2:]}@{PUBLISHED_CALLEE_SHA}"
            before, after = f"uses: {local}", f"uses: {published}"
            self.assertEqual(text.count(after), 1, name)
            self.assertNotIn(before, text)
            local_text = local_text.replace(after, before)
            replacements.append((before, after))
        local_wf = yaml.load(local_text, Loader=_make_loader())
        self.assertEqual(set(local_wf["jobs"]), set(CANARY_CALLEES))
        for name, ref in CANARY_CALLEES.items():
            self.assertEqual(local_wf["jobs"][name]["uses"], f"./.github/workflows/{ref}")
            self.assertRegex(local_wf["jobs"][name]["uses"], LOCAL_REF_RE)
        patched = local_text
        for before, after in replacements:
            self.assertEqual(patched.count(before), 1)
            patched = patched.replace(before, after)
        self.assertEqual(patched, text)
        doc = raw(PUBLISH_PATCH_DOC)
        self.assertIn(PUBLISHED_CALLEE_SHA, doc)
        self.assertIn("bbtools-canary-v1.yml", doc)


if __name__ == "__main__":
    unittest.main(verbosity=2)
