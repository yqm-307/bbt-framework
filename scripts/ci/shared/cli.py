#!/usr/bin/env python3
"""bbtools-classify/verify-v1 本地候选 CLI。

薄封装：stdin/文件读受限 JSON，stdout 只输出 JSON；拒绝时输出固定诊断并返回
非零（3），绝不回显被拒输入。用于 workflow 内联调用与离线冒烟。

退出码语义：
- 0：成功（`evaluate` 仅当 verdict=success 时）；
- 1：契约通过但门禁失败（`evaluate` verdict=failure/cancelled/跳过不绿）；
- 3：契约拒绝（`ContractError`，含输入非 JSON/文件读取失败）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import artifact  # noqa: E402
import classification  # noqa: E402
import envelope  # noqa: E402
import input_contract  # noqa: E402
import result_contract  # noqa: E402
from contract_errors import ContractError  # noqa: E402


def _read_text(path):
    if path in (None, "-"):
        return sys.stdin.read()
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _load(path):
    """读取并解析受限 JSON；文件读取/解析失败统一为稳定契约错误，不回显输入。"""
    try:
        text = _read_text(path)
    except (OSError, UnicodeDecodeError):
        raise ContractError("E_INPUT_JSON")
    try:
        return json.loads(text)
    except ValueError:  # 含 json.JSONDecodeError
        raise ContractError("E_INPUT_JSON")


def _load_object(path, required=()):
    payload = _load(path)
    if not isinstance(payload, dict) or any(key not in payload for key in required):
        raise ContractError("E_INPUT_TYPE")
    return payload


def _emit(obj, github_output=None):
    text = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    print(text)
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write("ok=true\n")
            for key in ("classification", "verdict", "code"):
                if key in obj:
                    handle.write(f"{key}={obj[key]}\n")
            handle.write("result_json=" + text + "\n")


def cmd_validate_inputs(args):
    norm = input_contract.coerce_workflow_inputs(_load(args.file))
    _emit({"ok": True, "inputs": norm}, args.github_output)
    return 0


def cmd_classify(args):
    norm = input_contract.coerce_workflow_inputs(_load(args.file))
    plan = classification.plan_from_inputs(norm)
    out = {"ok": True, "classification": plan["classification"], "plan": plan}
    # 分类与结果评估解耦：仅当调用方提供真实非空 results 时才评估；空/缺失不评估。
    if norm.get("results"):
        out["evaluation"] = result_contract.evaluate_plan(plan, norm["results"])
    _emit(out, args.github_output)
    return 0


def cmd_evaluate(args):
    payload = _load_object(args.file, required=("plan", "results"))
    evaluation = result_contract.evaluate_plan(payload["plan"], payload["results"])
    _emit({"ok": True, **evaluation}, args.github_output)
    # verdict=failure 是真实门禁失败：输出仍稳定，但退出非零，禁止把 failure 判绿。
    return 0 if evaluation["verdict"] == "success" else 1


def cmd_validate_envelope(args):
    payload = _load_object(args.file, required=("envelope",))
    if "expected" in payload:
        envelope.validate_binding(payload["envelope"], payload["expected"])
    identity = envelope.producer_identity(payload["envelope"])
    _emit({"ok": True, "identity": identity}, args.github_output)
    return 0


def cmd_verify_rerun(args):
    payload = _load_object(args.file, required=("producer", "rerun"))
    identity = envelope.verify_rerun_reuse(
        payload["producer"], payload["rerun"], payload.get("expected", {})
    )
    _emit({"ok": True, "producer_identity": identity}, args.github_output)
    return 0


def cmd_produce(args):
    """产出真实有限元数据 payload + envelope（摘要来自实际写入字节，不接受外部摘要）。"""
    out = artifact.produce(args.out_dir, _load(args.file))
    with open(os.path.join(args.out_dir, "envelope.json"), "w", encoding="utf-8") as handle:
        json.dump({"envelope": out["envelope"]}, handle, ensure_ascii=False, sort_keys=True)
    result = {
        "ok": True,
        "artifact_digest": out["artifact_digest"],
        "artifact_path": out["artifact_path"],
        "manifest": out["manifest"],
        "envelope": out["envelope"],
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if args.github_output:
        envelope_json = json.dumps({"envelope": out["envelope"]}, ensure_ascii=False, sort_keys=True)
        with open(args.github_output, "a", encoding="utf-8") as handle:
            handle.write("ok=true\n")
            handle.write("artifact_digest=" + out["artifact_digest"] + "\n")
            handle.write("artifact_path=" + out["artifact_path"] + "\n")
            handle.write("result_json=" + envelope_json + "\n")
    return 0


def cmd_verify_receipt(args):
    """接收侧：校验真实 payload 字节与 producer 身份；拒绝时退出 3 且不回显输入。"""
    payload = _load(args.file)
    if not isinstance(payload, dict) or set(payload) - {"root", "path", "envelope", "expected"}:
        raise ContractError("E_ART_TYPE")
    if any(key not in payload for key in ("root", "path", "envelope", "expected")):
        raise ContractError("E_ART_TYPE")
    result = artifact.verify_receipt(
        payload["root"], payload["path"], payload["envelope"], payload["expected"]
    )
    _emit({"ok": True, **result}, args.github_output)
    return 0


def build_parser():
    parser = argparse.ArgumentParser(prog="bbtools-ci-shared")
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add(name, handler):
        node = sub.add_parser(name)
        node.add_argument("--file", default="-")
        node.add_argument("--github-output", default=None)
        node.set_defaults(handler=handler)

    add("validate-inputs", cmd_validate_inputs)
    add("classify", cmd_classify)
    add("evaluate", cmd_evaluate)
    add("validate-envelope", cmd_validate_envelope)
    add("verify-rerun", cmd_verify_rerun)
    add("verify-receipt", cmd_verify_receipt)
    produce = sub.add_parser("produce")
    produce.add_argument("--file", default="-")
    produce.add_argument("--out-dir", required=True)
    produce.add_argument("--github-output", default=None)
    produce.set_defaults(handler=cmd_produce)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except ContractError as err:
        print(json.dumps({"ok": False, "code": err.code, "message": err.message}, ensure_ascii=False, sort_keys=True))
        return 3


if __name__ == "__main__":
    sys.exit(main())
