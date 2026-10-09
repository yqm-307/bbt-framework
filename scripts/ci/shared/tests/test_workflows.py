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


def load_text(text):
    return yaml.load(text, Loader=_make_loader())


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

    def test_canary_calls_local_callees_with_explicit_inputs(self):
        wf = load_workflow(CANARY)
        self.assertEqual(set(wf["jobs"]), {"classify", "verify"})
        callers = {"classify": "bbtools-classify-v1.yml", "verify": "bbtools-verify-v1.yml"}
        for name, job in wf["jobs"].items():
            with self.subTest(job=name):
                self.assertEqual(job["uses"], f"./.github/workflows/{callers[name]}")
                self.assertNotIn("secrets", job)
                with_block = job["with"]
                for key in ("repo", "source_sha", "profile", "required_checks_json"):
                    self.assertIn(key, with_block)
                self.assertEqual(with_block["profile"], "hosted")
                self.assertEqual(load_workflow(CANARY)["jobs"][name]["permissions"], {"contents": "read"})

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

    def test_caller_example_marked_unpublished(self):
        text = raw(CALLER)
        self.assertTrue(("未发布" in text) or ("UNPUBLISHED" in text))

    def test_canary_marked_unpublished(self):
        text = raw(CANARY)
        self.assertTrue(("未发布" in text) or ("UNPUBLISHED" in text))

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
        # 为父级第二轮 caller != callee 准备：本地引用可机械替换为已发布完整 SHA 引用。
        text = raw(CANARY)
        local = re.findall(r"uses: (\./\.github/workflows/[A-Za-z0-9._-]+\.yml)", text)
        self.assertEqual(sorted(local), [
            "./.github/workflows/bbtools-classify-v1.yml",
            "./.github/workflows/bbtools-verify-v1.yml",
        ])
        published = "a" * 40  # 仅用于验证替换形状；不构成任何已发布 SHA 声明。
        patched = text
        for ref in local:
            patched = patched.replace(
                f"uses: {ref}",
                f"uses: yqm-307/bbt-framework/{ref[2:]}@{published}",
            )
        wf = load_text(patched)
        for name, job in wf["jobs"].items():
            with self.subTest(job=name):
                self.assertRegex(job["uses"], SHA_PIN_RE)
        body = raw(PUBLISH_PATCH_DOC)
        self.assertIn("<PUBLISHED_COMMIT_SHA_40HEX>", body)
        self.assertIn("bbtools-canary-v1.yml", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
