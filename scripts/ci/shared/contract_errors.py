"""稳定契约错误：只携带错误码与固定诊断，绝不回显调用方输入或敏感值。

所有拒绝路径统一 `raise ContractError(code)`；`code` 是稳定契约的一部分，
`message` 为不含任何输入值的固定诊断（仅可附加非敏感的计数 n=...）。
调用方、调用仓与测试只依赖 `code`，不解析 message。
"""
from __future__ import annotations

_MESSAGES = {
    # ---- typed 输入契约 ----
    "E_INPUT_TYPE": "input payload must be a JSON object",
    "E_INPUT_JSON": "a declared JSON-array/object input is not valid JSON",
    "E_INPUT_UNKNOWN_FIELD": "input contains fields outside the typed contract",
    "E_INPUT_REPO": "repo must be owner/name with allowed characters",
    "E_INPUT_SHA": "source_sha must be a full 40-hex commit SHA",
    "E_INPUT_PROFILE": "profile is not supported by this template revision",
    "E_PROFILE_NO_ADMISSION": "self-hosted profile has no admission evidence; rejected",
    "E_INPUT_CONCURRENCY": "concurrency must be a positive in-range integer",
    "E_INPUT_TIMEOUT": "timeout_minutes must be a positive in-range integer",
    "E_INPUT_EVENT": "event is not a supported bounded event object",
    "E_INPUT_CHANGED_FILES": "changed_files must be a bounded list of repo-relative paths",
    "E_INPUT_PATH": "a path is absolute, escaping, backslash or otherwise unsafe",
    "E_INPUT_CHECK_ID": "a check id is not a safe bounded identifier",
    "E_INPUT_STATUS": "classifier_status must be one of ok|failed|unknown",
    "E_INPUT_SECRET": "input looks like a credential/secret value; refused",
    # ---- 执行计划契约 ----
    "E_REQUIRED_EMPTY": "required check set is empty; cannot be all-skipped green",
    "E_CHECK_OVERLAP": "a check id appears in both required and optional sets",
    # ---- 结果契约 ----
    "E_RESULT_EMPTY": "results map is empty",
    "E_RESULT_UNKNOWN_JOB": "results contain job ids outside the validated plan",
    "E_RESULT_STATUS": "a job status is not one of success|failure|cancelled|skipped",
    "E_ALL_SKIPPED": "every job is skipped/empty; refusing to report green",
    # ---- 产物 envelope 契约 ----
    "E_ENV_TYPE": "envelope must be a JSON object",
    "E_ENV_UNKNOWN_FIELD": "envelope contains fields outside the typed contract",
    "E_ENV_MISSING": "envelope is missing a required producer/identity field",
    "E_ENV_REPO": "producer_repo must be owner/name",
    "E_ENV_SHA": "a producer/workflow SHA is not a full 40-hex commit",
    "E_ENV_JOB": "job id is empty or unsafe",
    "E_ENV_RUN_ID": "producer_run_id must be a positive integer",
    "E_ENV_RUN_ATTEMPT": "producer_run_attempt must be a positive integer",
    "E_ENV_DIGEST": "artifact_digest must be sha256:<64 hex>",
    "E_ENV_PATH": "artifact_path is absolute, escaping or unsafe",
    "E_ENV_RUNTIME": "an optional runtime field is malformed",
    "E_ENV_SECRET": "envelope contains a credential/secret-like value; refused",
    "E_ENV_IDENTITY_MISMATCH": "producer identity does not match expected source/workflow/job",
    "E_ENV_REBOUND": "downstream rerun rebound producer identity to its own attempt",
    # ---- 真实产物 manifest / receipt 契约 ----
    "E_ART_TYPE": "artifact payload/receipt is malformed or not a bounded object",
    "E_ART_SHA": "a manifest SHA field is not a full 40-hex commit",
    "E_ART_SOURCE_HEAD": "manifest source_head does not match the declared source_sha",
    "E_ART_MANIFEST_UNKNOWN": "manifest contains fields outside the closed metadata schema",
    "E_ART_MANIFEST_MISSING": "manifest is missing a required metadata field",
    "E_ART_MANIFEST_MISMATCH": "manifest metadata does not match the producer envelope",
    "E_ART_ROOT": "artifact root is missing, not a directory or unsafe",
    "E_ART_PATH": "artifact path is absolute, escaping or otherwise unsafe",
    "E_ART_PATH_MISMATCH": "artifact path does not match the envelope-declared path",
    "E_ART_PATH_ESCAPE": "artifact path resolves outside the trusted artifact root",
    "E_ART_SYMLINK": "artifact payload is a symbolic link; refused",
    "E_ART_MISSING": "artifact payload file is missing or not a regular file",
    "E_ART_DIGEST_MISMATCH": "payload bytes do not match the envelope artifact_digest",
    "E_ART_IDENTITY_MISMATCH": "artifact producer identity does not match expected source/callee/run",
}


class ContractError(Exception):
    """契约拒绝。`code` 稳定；`message` 永不含调用方输入值。"""

    def __init__(self, code: str, count: int | None = None):
        self.code = code
        base = _MESSAGES.get(code, "contract rejected")
        self.message = base if count is None else f"{base} (n={count})"
        super().__init__(f"{code}: {self.message}")
