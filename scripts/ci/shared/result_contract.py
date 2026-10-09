"""结果契约：必跑集失败闭合（failure/cancelled/空值/意外 skipped 不判绿）。

只消费调用方收集的受限 results 映射（job -> status）；不读 GitHub 上下文、
不执行任何命令。plan 由 classification.build_plan 产出并已校验。
"""
from __future__ import annotations

from contract_errors import ContractError

VALID_STATUSES = {"success", "failure", "cancelled", "skipped"}


def evaluate_plan(plan: dict, results: dict) -> dict:
    """返回 {verdict, failed, ran, skipped}；任何必跑非 success 即 failure。"""
    if not isinstance(results, dict) or not results:
        raise ContractError("E_RESULT_EMPTY")

    allowed = set(plan["required"]) | set(plan["optional"])
    unknown = set(results) - allowed
    if unknown:
        raise ContractError("E_RESULT_UNKNOWN_JOB", count=len(unknown))
    for status in results.values():
        if status not in VALID_STATUSES:
            raise ContractError("E_RESULT_STATUS")

    failed = []
    for job in plan["required"]:
        if results.get(job, "") != "success":
            failed.append(job)
    for job in plan["optional"]:
        status = results.get(job, "")
        if status == "success":
            continue
        if status in ("skipped", ""):
            if plan["allow_skip"]:
                continue
            failed.append(job)
            continue
        failed.append(job)  # failure / cancelled

    # 防御性不变量：无任何真实 success 不得判绿（即使调用方手工构造空 required 计划）。
    if not failed and not any(results.get(j, "") == "success" for j in allowed):
        raise ContractError("E_ALL_SKIPPED")

    return {
        "verdict": "success" if not failed else "failure",
        "failed": failed,
        "ran": [j for j in allowed if results.get(j, "") in ("success", "failure")],
        "skipped": [j for j in allowed if results.get(j, "") in ("skipped", "")],
    }
