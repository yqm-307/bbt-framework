"""纯分类逻辑：由调用方提供的受限变更数据推导执行计划。

不读仓库、不联网、不执行命令、不接受任意 YAML/jobs 定义；只做确定性映射。
docs-only 才允许 heavy/perf 跳过；code 或 unknown（classifier 失败）保守执行全部。
"""
from __future__ import annotations

from contract_errors import ContractError

DOC_SUFFIXES = (".md", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".txt")
DOC_PREFIXES = ("docs/",)
DOC_BASENAMES = {"LICENSE", "NOTICE", "COPYING", "CODEOWNERS", "AUTHORS"}


def classify_path(path: str) -> str:
    """单路径分类：doc 或 code。"""
    base = path.rsplit("/", 1)[-1]
    if path.startswith(DOC_PREFIXES):
        return "doc"
    if path.lower().endswith(DOC_SUFFIXES):
        return "doc"
    if base in DOC_BASENAMES:
        return "doc"
    return "code"


def classify_changes(changed_files) -> str:
    """变更集分类：docs-only | code | unknown。"""
    if not changed_files:
        # 无变更数据视为 unknown，保守执行，不擅自判绿。
        return "unknown"
    if all(classify_path(p) == "doc" for p in changed_files):
        return "docs-only"
    return "code"


def classify_status(status: str, changed_files) -> str:
    """结合 classifier 状态：失败/未知一律 conservative unknown。"""
    if status != "ok":
        return "unknown"
    return classify_changes(changed_files)


def build_plan(classification: str, required, optional) -> dict:
    """构造校验过的执行计划。required 不可为空；禁止 all-skipped 变绿。"""
    if not required:
        raise ContractError("E_REQUIRED_EMPTY")
    if set(required) & set(optional):
        raise ContractError("E_CHECK_OVERLAP")
    if classification == "docs-only":
        run = list(required)
        skip = list(optional)
        allow_skip = True
        reason = "docs-only"
    else:  # code / unknown -> 保守执行全部，禁止任何跳过
        run = list(required) + list(optional)
        skip = []
        allow_skip = False
        reason = None
    return {
        "classification": classification,
        "required": list(required),
        "optional": list(optional),
        "run": run,
        "skip": skip,
        "allow_skip": allow_skip,
        "allow_skip_reason": reason,
    }


def plan_from_inputs(norm: dict) -> dict:
    classification = classify_status(norm["classifier_status"], norm["changed_files"])
    return build_plan(classification, norm["required_checks"], norm["optional_checks"])
