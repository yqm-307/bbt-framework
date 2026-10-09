"""纯逻辑契约测试：表驱动正反 fixtures + CLI 冒烟 + 敏感 canary 不外泄。

仅用标准库；直接针对候选源码运行，不读仓库、不联网、不执行源仓脚本。
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SHARED = os.path.dirname(HERE)
sys.path.insert(0, SHARED)

import classification  # noqa: E402
import envelope  # noqa: E402
import input_contract  # noqa: E402
import result_contract  # noqa: E402
from contract_errors import ContractError  # noqa: E402

FIXTURES = os.path.join(HERE, "fixtures")
CLI = os.path.join(SHARED, "cli.py")
WORKFLOW_DIR = os.path.normpath(os.path.join(SHARED, "..", "..", "..", ".github", "workflows"))
VERIFY_YML = os.path.join(WORKFLOW_DIR, "bbtools-verify-v1.yml")
CLASSIFY_YML = os.path.join(WORKFLOW_DIR, "bbtools-classify-v1.yml")
CANARY = "ghp_0000000000000000000000000000000000"
BASE_INPUTS = {
    "repo": "yqm-307/bbt-framework",
    "source_sha": "a60edfb4f7cd77e69926be475e2c45e324a3db2b",
    "profile": "hosted",
}


def load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as handle:
        return json.load(handle)


def resolve(case, data, key):
    value = case[key]
    if isinstance(value, str) and value in data:
        return data[value]
    return value


class InputContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = load("input_cases.json")

    def test_cases(self):
        for case in self.data["cases"]:
            with self.subTest(case=case["name"]):
                if case["expect"] == "ok":
                    out = input_contract.coerce_workflow_inputs(case["payload"])
                    self.assertEqual(out["source_sha"], out["source_sha"].lower())
                    self.assertIn("changed_files", out)
                else:
                    with self.assertRaises(ContractError) as cm:
                        input_contract.coerce_workflow_inputs(case["payload"])
                    self.assertEqual(cm.exception.code, case["code"])
                    self.assertNotIn(CANARY, str(cm.exception))


class ClassificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = load("plan_cases.json")

    def test_cases(self):
        for case in self.data["cases"]:
            with self.subTest(case=case["name"]):
                payload = dict(BASE_INPUTS)
                payload.update(case["payload"])
                norm = input_contract.validate_inputs(payload)
                if "expect_plan" in case:
                    plan = classification.plan_from_inputs(norm)
                    expected = case["expect_plan"]
                    for key, value in expected.items():
                        self.assertEqual(plan[key], value, f"{case['name']}:{key}")
                else:
                    with self.assertRaises(ContractError) as cm:
                        classification.plan_from_inputs(norm)
                    self.assertEqual(cm.exception.code, case["code"])

    def test_plan_is_deterministic(self):
        payload = dict(BASE_INPUTS)
        payload.update(self.data["cases"][0]["payload"])
        norm = input_contract.validate_inputs(payload)
        self.assertEqual(classification.plan_from_inputs(norm), classification.plan_from_inputs(norm))


class ResultContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = load("result_cases.json")

    def test_cases(self):
        for case in self.data["cases"]:
            with self.subTest(case=case["name"]):
                plan = self.data["plans"][case["plan"]]
                if "expect_verdict" in case:
                    out = result_contract.evaluate_plan(plan, case["results"])
                    self.assertEqual(out["verdict"], case["expect_verdict"])
                    if "expect_failed" in case:
                        self.assertEqual(sorted(out["failed"]), sorted(case["expect_failed"]))
                else:
                    with self.assertRaises(ContractError) as cm:
                        result_contract.evaluate_plan(plan, case["results"])
                    self.assertEqual(cm.exception.code, case["code"])

    def test_all_skipped_guard_on_hand_built_plan(self):
        plan = {
            "classification": "docs-only", "required": [], "optional": ["a"],
            "run": [], "skip": ["a"], "allow_skip": True, "allow_skip_reason": "docs-only",
        }
        with self.assertRaises(ContractError) as cm:
            result_contract.evaluate_plan(plan, {"a": "skipped"})
        self.assertEqual(cm.exception.code, "E_ALL_SKIPPED")


class EnvelopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = load("envelope_cases.json")

    def test_envelope_cases(self):
        for case in self.data["envelope_cases"]:
            with self.subTest(case=case["name"]):
                env = resolve(case, self.data, "envelope")
                if case["expect"] == "ok":
                    out = envelope.validate_envelope(env)
                    for field in case.get("expect_runtime_unknown", []):
                        self.assertEqual(out[field], "unknown")
                else:
                    with self.assertRaises(ContractError) as cm:
                        envelope.validate_envelope(env)
                    self.assertEqual(cm.exception.code, case["code"])
                    self.assertNotIn(CANARY, str(cm.exception))

    def test_binding_cases(self):
        for case in self.data["binding_cases"]:
            with self.subTest(case=case["name"]):
                env = resolve(case, self.data, "envelope")
                expected = resolve(case, self.data, "expected")
                if case["expect"] == "ok":
                    envelope.validate_binding(env, expected)
                else:
                    with self.assertRaises(ContractError) as cm:
                        envelope.validate_binding(env, expected)
                    self.assertEqual(cm.exception.code, case["code"])

    def test_rerun_cases(self):
        for case in self.data["rerun_cases"]:
            with self.subTest(case=case["name"]):
                producer = resolve(case, self.data, "producer")
                rerun = resolve(case, self.data, "rerun")
                expected = resolve(case, self.data, "expected")
                if case["expect"] == "ok":
                    identity = envelope.verify_rerun_reuse(producer, rerun, expected)
                    self.assertEqual(identity["producer_run_id"], producer["producer_run_id"])
                    self.assertEqual(identity["producer_run_attempt"], producer["producer_run_attempt"])
                else:
                    with self.assertRaises(ContractError) as cm:
                        envelope.verify_rerun_reuse(producer, rerun, expected)
                    self.assertEqual(cm.exception.code, case["code"])


def _run_cli(args, payload):
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, CLI, *args],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env=env,
    )


class CliSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valid = {
            "repo": "yqm-307/bbt-framework",
            "source_sha": "a60edfb4f7cd77e69926be475e2c45e324a3db2b",
            "profile": "hosted",
            "concurrency": "2",
            "timeout_minutes": "20",
            "event": {"name": "pull_request", "base_ref": "main"},
            "changed_files_json": "[\"docs/ci/api.md\"]",
            "required_checks_json": "[\"lint\",\"unit\"]",
            "optional_checks_json": "[\"heavy\",\"perf\"]",
            "classifier_status": "ok",
        }

    def test_classify_docs_only_ok(self):
        proc = _run_cli(["classify"], self.valid)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertTrue(out["ok"])
        self.assertEqual(out["classification"], "docs-only")
        self.assertIn("heavy", out["plan"]["skip"])

    def test_classify_output_is_stable(self):
        first = _run_cli(["classify"], self.valid).stdout
        second = _run_cli(["classify"], self.valid).stdout
        self.assertEqual(first, second)

    def test_reject_is_nonzero_and_no_echo(self):
        bad = dict(self.valid)
        bad["changed_files_json"] = json.dumps([f"secrets/{CANARY}.txt"])
        proc = _run_cli(["classify"], bad)
        self.assertEqual(proc.returncode, 3)
        self.assertNotIn(CANARY, proc.stdout)
        self.assertNotIn(CANARY, proc.stderr)
        self.assertIn("E_INPUT_SECRET", proc.stdout)

    def test_evaluate_cli_verdict(self):
        payload = {
            "plan": {
                "classification": "docs-only", "required": ["lint"],
                "optional": ["heavy"], "run": ["lint"], "skip": ["heavy"],
                "allow_skip": True, "allow_skip_reason": "docs-only",
            },
            "results": {"lint": "success", "heavy": "skipped"},
        }
        proc = _run_cli(["evaluate"], payload)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_verify_rerun_cli(self):
        data = load("envelope_cases.json")
        payload = {
            "producer": data["valid_envelope"],
            "rerun": data["valid_envelope"],
            "expected": data["expected"],
        }
        proc = _run_cli(["verify-rerun"], payload)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(json.loads(proc.stdout)["ok"])

    def test_validate_envelope_cli_rejects_bad_digest(self):
        data = load("envelope_cases.json")
        bad = dict(data["valid_envelope"])
        bad["artifact_digest"] = "deadbeef"
        proc = _run_cli(["validate-envelope"], {"envelope": bad})
        self.assertEqual(proc.returncode, 3)
        self.assertIn("E_ENV_DIGEST", proc.stdout)


# GitHub workflow_call 把每个输入以字符串形态放进 toJSON(inputs)；下为 callee 默认形态。
GITHUB_CLASSIFY_INPUTS = {
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
GITHUB_VERIFY_INPUTS = {
    key: value for key, value in GITHUB_CLASSIFY_INPUTS.items() if key != "results_json"
}
HEREDOC_RE = re.compile(r"<<'PY'\n(.*?)\n[ \t]*PY[ \t]*(?:\n|$)", re.DOTALL)


def extract_heredocs(path):
    """提取 workflow 内联 `<<'PY' ... PY` 脚本正文（去 YAML 缩进），用于真实接线测试。"""
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    return [textwrap.dedent(body) for body in HEREDOC_RE.findall(text)]


def _eval_payload(status):
    return {
        "plan": {
            "classification": "docs-only", "required": ["lint"], "optional": ["heavy"],
            "run": ["lint"], "skip": ["heavy"], "allow_skip": True, "allow_skip_reason": "docs-only",
        },
        "results": {"lint": status, "heavy": "success"},
    }


class GithubShapedRoutingTests(unittest.TestCase):
    """用 GitHub 真实 toJSON(inputs) 形态直接驱动 cli.py / workflow 内联脚本（非仅静态 YAML）。"""

    def test_classify_github_default_inputs_succeeds(self):
        # C2+C6：默认调用（results_json=""）必须成功且不含 evaluation。
        proc = _run_cli(["classify"], GITHUB_CLASSIFY_INPUTS)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertTrue(out["ok"])
        self.assertEqual(out["classification"], "unknown")  # 空变更集保守 unknown
        self.assertNotIn("evaluation", out)

    def test_classify_empty_object_results_treated_as_absent(self):
        # C2：显式 "{}" 与缺失等价，不得再报 E_RESULT_EMPTY。
        payload = dict(GITHUB_CLASSIFY_INPUTS)
        payload["results_json"] = "{}"
        payload["changed_files_json"] = '["docs/ci/api.md"]'
        proc = _run_cli(["classify"], payload)
        self.assertEqual(proc.returncode, 0, proc.stdout)
        out = json.loads(proc.stdout)
        self.assertEqual(out["classification"], "docs-only")
        self.assertNotIn("evaluation", out)

    def test_classify_with_real_results_evaluates(self):
        payload = dict(GITHUB_CLASSIFY_INPUTS)
        payload["changed_files_json"] = '["docs/ci/api.md"]'
        payload["results_json"] = '{"lint":"success"}'
        proc = _run_cli(["classify"], payload)
        self.assertEqual(proc.returncode, 0, proc.stdout)
        self.assertEqual(json.loads(proc.stdout)["evaluation"]["verdict"], "success")

    def test_verify_default_input_shape_is_in_closed_set(self):
        # C3+C6：verify 的 toJSON(inputs) 形态必须全部落在输入闭集内（无死字段）。
        proc = _run_cli(["classify"], GITHUB_VERIFY_INPUTS)
        self.assertEqual(proc.returncode, 0, proc.stdout)
        self.assertNotIn("E_INPUT_UNKNOWN_FIELD", proc.stdout)

    def test_dead_input_field_is_rejected_by_contract(self):
        # 回归守卫：若 workflow 重新声明 producer_manifest_json，闭集拒绝（配合 test_workflows 静态断言）。
        payload = dict(GITHUB_VERIFY_INPUTS)
        payload["producer_manifest_json"] = ""
        proc = _run_cli(["classify"], payload)
        self.assertEqual(proc.returncode, 3, proc.stdout)
        self.assertIn("E_INPUT_UNKNOWN_FIELD", proc.stdout)

    def test_classify_result_detects_absent_and_present_results(self):
        # C2+C6 接线：result job 的 detect 脚本必须区分「无 results」与「有 results」。
        heredocs = extract_heredocs(CLASSIFY_YML)
        self.assertEqual(len(heredocs), 2, "classify workflow should have detect+evaluate heredocs")
        detect = heredocs[0]
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        for results_json, expected in (("", "false"), ('{"lint":"success"}', "true")):
            with self.subTest(results_json=results_json), tempfile.TemporaryDirectory() as tmp:
                inputs = dict(GITHUB_CLASSIFY_INPUTS)
                inputs["results_json"] = results_json
                out_file = os.path.join(tmp, "gh_output")
                run_env = dict(env)
                run_env["BBT_WORKFLOW_INPUTS"] = json.dumps(inputs)
                proc = subprocess.run(
                    [sys.executable, "-", out_file], input=detect,
                    capture_output=True, text=True, env=run_env,
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                with open(out_file, encoding="utf-8") as handle:
                    self.assertIn(f"has_results={expected}", handle.read())

    def test_evaluate_failure_exit_is_nonzero(self):
        # C4+C6：required failure 必须非零，不得判绿。
        proc = _run_cli(["evaluate"], _eval_payload("failure"))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_evaluate_success_exit_is_zero(self):
        proc = _run_cli(["evaluate"], _eval_payload("success"))
        self.assertEqual(proc.returncode, 0, proc.stdout)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_validate_envelope_rejects_missing_digest(self):
        # C5 CLI 面：空摘要必须拒绝。
        data = load("envelope_cases.json")
        bad = dict(data["valid_envelope"])
        bad["artifact_digest"] = ""
        proc = _run_cli(["validate-envelope"], {"envelope": bad})
        self.assertEqual(proc.returncode, 3, proc.stdout)
        self.assertIn("E_ENV_MISSING", proc.stdout)

    @staticmethod
    def _builder_env():
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env.update({
            "BBT_SOURCE_REPO": "yqm-307/bbt-framework",
            "BBT_SOURCE_SHA": "a60edfb4f7cd77e69926be475e2c45e324a3db2b",
            "BBT_AUTOMATION_REPO": "yqm-307/bbt-framework",
            "BBT_AUTOMATION_SHA": "fedcba9876543210fedcba9876543210fedcba98",
            "BBT_RUN_ID": "123456",
            "BBT_RUN_ATTEMPT": "1",
        })
        return env

    def _heredoc(self, path, marker):
        """按稳定标记选择 workflow 内联 heredoc（verify 现有 produce/receipt/report 三段）。"""
        for body in extract_heredocs(path):
            if marker in body:
                return body
        self.fail(f"no heredoc containing {marker!r} in {os.path.basename(path)}")

    def test_verify_produce_heredoc_writes_real_manifest_inputs(self):
        # 真实元数据来源：实际 checkout HEAD（作为 argv[2] 传入）+ job 上下文，非调用方伪造。
        body = self._heredoc(VERIFY_YML, '"manifest_version": 1')
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "manifest-inputs.json")
            head = "b" * 40
            proc = subprocess.run(
                [sys.executable, "-", inputs, head], input=body,
                capture_output=True, text=True, env=self._builder_env(),
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            with open(inputs, encoding="utf-8") as handle:
                doc = json.load(handle)
            self.assertEqual(doc["source_head"], head)
            self.assertEqual(doc["source_sha"], "a60edfb4f7cd77e69926be475e2c45e324a3db2b")
            self.assertEqual(doc["automation_sha"], "fedcba9876543210fedcba9876543210fedcba98")
            self.assertEqual(doc["job"], "verify")

    def test_verify_receipt_builder_fail_closed_without_producer_digest(self):
        # 接收侧：producer 声明的真实摘要缺失即拒绝，绝不接受调用方/artifact 自报摘要。
        body = self._heredoc(VERIFY_YML, "BBT_PAYLOAD_DIGEST")
        with tempfile.TemporaryDirectory() as tmp:
            receipt = os.path.join(tmp, "receipt.json")
            env = self._builder_env()
            env["BBT_RECEIVED_DIR"] = tmp
            env.pop("BBT_PAYLOAD_DIGEST", None)
            proc = subprocess.run(
                [sys.executable, "-", receipt], input=body,
                capture_output=True, text=True, env=env,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertFalse(os.path.exists(receipt))

    def test_cli_rejects_non_json_input_stably(self):
        # C8：非 JSON 输入必须稳定 E_INPUT_JSON、非零、无 traceback、不回显输入。
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        proc = subprocess.run(
            [sys.executable, CLI, "classify"], input="{not json",
            capture_output=True, text=True, env=env,
        )
        self.assertEqual(proc.returncode, 3)
        self.assertIn("E_INPUT_JSON", proc.stdout)
        self.assertNotIn("Traceback", proc.stderr)
        self.assertNotIn("{not json", proc.stdout)

    def test_cli_missing_file_is_stable(self):
        missing = os.path.join(tempfile.gettempdir(), "bbtools-does-not-exist.json")
        proc = _run_cli(["classify", "--file", missing], {})
        self.assertEqual(proc.returncode, 3)
        self.assertIn("E_INPUT_JSON", proc.stdout)

    def test_repo_dot_segments_rejected(self):
        # C7：dot 段 repo 必须拒绝，合法 owner/name 不受影响。
        for repo in ("../x", "a/..", "a/.", "..", "."):
            payload = dict(GITHUB_CLASSIFY_INPUTS)
            payload["repo"] = repo
            with self.subTest(repo=repo):
                proc = _run_cli(["classify"], payload)
                self.assertEqual(proc.returncode, 3, proc.stdout)
                self.assertIn("E_INPUT_REPO", proc.stdout)


def _classify_eval_script():
    """从真实 workflow YAML 提取 result job 的 evaluate 内联脚本（真实执行，不做静态断言）。"""
    for body in extract_heredocs(CLASSIFY_YML):
        if "BBT_PLAN_JSON" in body:
            return body
    raise AssertionError("classify workflow has no evaluate heredoc referencing BBT_PLAN_JSON")


def _classify_detect_script():
    for body in extract_heredocs(CLASSIFY_YML):
        if "has_results" in body:
            return body
    raise AssertionError("classify workflow has no detect heredoc writing has_results")


def _github_classify_inputs(changed_files, results_json, *, required=("changes", "plan"),
                            optional=("build",), classifier_status="ok"):
    """真实 caller toJSON(inputs) 形态：镜像线上 infra 运行（required=changes/plan, optional=build）。"""
    inputs = dict(GITHUB_CLASSIFY_INPUTS)
    inputs["changed_files_json"] = json.dumps(list(changed_files))
    inputs["results_json"] = results_json
    inputs["required_checks_json"] = json.dumps(list(required))
    inputs["optional_checks_json"] = json.dumps(list(optional))
    inputs["classifier_status"] = classifier_status
    return inputs


def _run_classify_job(inputs):
    """真实重放 classify job：cli.py classify 写 GITHUB_OUTPUT，返回其 result_json 输出值（= plan_json）。"""
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    with tempfile.TemporaryDirectory() as tmp:
        gh = os.path.join(tmp, "gh_output")
        proc = subprocess.run(
            [sys.executable, CLI, "classify", "--github-output", gh],
            input=json.dumps(inputs), capture_output=True, text=True, env=env,
        )
        if proc.returncode != 0:
            raise AssertionError(f"cli classify failed: {proc.returncode}: {proc.stdout}{proc.stderr}")
        with open(gh, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("result_json="):
                    return line[len("result_json="):].rstrip("\n")
    raise AssertionError("cli classify produced no result_json github-output")


def _run_detect_job(inputs):
    """真实执行 result job 的 detect 内联脚本，返回其 GITHUB_OUTPUT 内容。"""
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["BBT_WORKFLOW_INPUTS"] = json.dumps(inputs)
    with tempfile.TemporaryDirectory() as tmp:
        gh = os.path.join(tmp, "gh_output")
        with open(gh, "w", encoding="utf-8"):
            pass
        proc = subprocess.run(
            [sys.executable, "-", gh], input=_classify_detect_script(),
            capture_output=True, text=True, env=env,
        )
        if proc.returncode != 0:
            raise AssertionError(f"detect heredoc failed: {proc.stderr}")
        with open(gh, encoding="utf-8") as handle:
            return handle.read()


def _evaluate_wiring(inputs, plan_json):
    """真实接线重放：workflow evaluate 内联脚本 -> payload -> 真实 cli.py evaluate。

    返回 (exit_code, verdict|None, payload_written)。内联脚本 fail-closed（不写 payload）时不跑 CLI。
    """
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["BBT_WORKFLOW_INPUTS"] = json.dumps(inputs)
    env["BBT_PLAN_JSON"] = plan_json
    with tempfile.TemporaryDirectory() as tmp:
        payload = os.path.join(tmp, "bbtools-eval.json")
        prep = subprocess.run(
            [sys.executable, "-", payload], input=_classify_eval_script(),
            capture_output=True, text=True, env=env,
        )
        if prep.returncode != 0 or not os.path.exists(payload):
            return prep.returncode or 1, None, False
        proc = subprocess.run(
            [sys.executable, CLI, "evaluate", "--file", payload],
            capture_output=True, text=True, env=env,
        )
        verdict = None
        try:
            verdict = json.loads(proc.stdout)["verdict"]
        except (ValueError, KeyError, TypeError):
            verdict = None
        return proc.returncode, verdict, True


class ClassifyResultConsumerWiringTests(unittest.TestCase):
    """#50 结果契约接线回归：plan_json 是 cli classify 的 CLI 外层信封（{ok,classification,plan,evaluation}），
    result job 必须先提取内部 plan 再门禁。

    端到端重放：真实 cli.py classify 输出 -> 真实 YAML evaluate 内联脚本提取 plan -> 真实 cli.py evaluate。
    旧实现把外层信封当 plan 传给 evaluate_plan，直接 KeyError('/'required')/伪失败，本类会红。
    """

    REQUIRED_OK = '{"changes":"success","plan":"success","build":"success"}'

    def test_classify_job_output_is_cli_envelope_with_inner_plan(self):
        # 固化根因：classify 的 result_json（即 plan_json 输出值）是 CLI 信封，plan 在内部。
        inputs = _github_classify_inputs([".github/workflows/x.yml"], self.REQUIRED_OK)
        envelope = json.loads(_run_classify_job(inputs))
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["classification"], "code")
        self.assertIsInstance(envelope["plan"], dict)
        self.assertIn("required", envelope["plan"])

    def test_all_required_and_optional_success_is_green(self):
        inputs = _github_classify_inputs([".github/workflows/x.yml"], self.REQUIRED_OK)
        code, verdict, wrote = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertTrue(wrote, "evaluate payload must be written from the classify envelope")
        self.assertEqual(code, 0)
        self.assertEqual(verdict, "success")

    def test_required_failure_is_red(self):
        inputs = _github_classify_inputs(
            [".github/workflows/x.yml"], '{"changes":"failure","plan":"success","build":"success"}')
        code, verdict, _ = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertNotEqual(code, 0)
        self.assertEqual(verdict, "failure")

    def test_required_cancelled_is_red(self):
        inputs = _github_classify_inputs(
            [".github/workflows/x.yml"], '{"changes":"success","plan":"cancelled","build":"success"}')
        code, verdict, _ = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertNotEqual(code, 0)
        self.assertEqual(verdict, "failure")

    def test_required_skipped_is_red(self):
        inputs = _github_classify_inputs(
            [".github/workflows/x.yml"], '{"changes":"success","plan":"skipped","build":"success"}')
        code, verdict, _ = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertNotEqual(code, 0)
        self.assertEqual(verdict, "failure")

    def test_docs_only_optional_skipped_is_allowed_green(self):
        inputs = _github_classify_inputs(
            ["docs/ci/api.md"], '{"changes":"success","plan":"success","build":"skipped"}')
        code, verdict, _ = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertEqual(code, 0)
        self.assertEqual(verdict, "success")

    def test_code_optional_skipped_is_red(self):
        inputs = _github_classify_inputs(
            [".github/workflows/x.yml"], '{"changes":"success","plan":"success","build":"skipped"}')
        code, verdict, _ = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertNotEqual(code, 0)
        self.assertEqual(verdict, "failure")

    def test_unknown_optional_skipped_is_red(self):
        # 空变更集 => unknown（保守执行），optional 跳过不判绿。
        inputs = _github_classify_inputs(
            [], '{"changes":"success","plan":"success","build":"skipped"}')
        self.assertEqual(json.loads(_run_classify_job(inputs))["classification"], "unknown")
        code, verdict, _ = _evaluate_wiring(inputs, _run_classify_job(inputs))
        self.assertNotEqual(code, 0)
        self.assertEqual(verdict, "failure")

    def test_classification_only_call_stays_green_without_evaluation(self):
        # 无 results：detect 必须判 false -> workflow 不执行 evaluate step -> 成功；输出无 evaluation。
        inputs = _github_classify_inputs([".github/workflows/x.yml"], "")
        self.assertIn("has_results=false", _run_detect_job(inputs))
        envelope = json.loads(_run_classify_job(inputs))
        self.assertNotIn("evaluation", envelope)
        self.assertIn("plan", envelope)
        # 评估步骤确实被真实存在且以 has_results 为门（否则 classify-only 会被迫评估/失败）。
        raw_yaml = open(CLASSIFY_YML, encoding="utf-8").read()
        self.assertIn("steps.detect.outputs.has_results == 'true'", raw_yaml)

    def test_malformed_plan_json_is_never_green(self):
        # 任何非「带内部 plan 的 CLI 信封」都不得伪绿：内联脚本 fail-closed，绝不写 payload 蒙混评估。
        inputs = _github_classify_inputs([".github/workflows/x.yml"], self.REQUIRED_OK)
        for bad in ("{}", "[]", "not json", '{"plan":"nope"}', '{"ok":true}', ""):
            with self.subTest(plan_json=bad):
                code, _, wrote = _evaluate_wiring(inputs, bad)
                self.assertNotEqual(code, 0)
                self.assertFalse(wrote)


if __name__ == "__main__":
    unittest.main(verbosity=2)
