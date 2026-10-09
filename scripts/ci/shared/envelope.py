"""产物 envelope 契约：校验 producer 身份与受限运行信息。

身份字段（producer repo/source SHA、workflow SHA、run id/attempt、job、artifact
digest/path）缺失即拒绝；非身份运行信息（toolchain/tests/retry/cache/artifact id）
缺失记 "unknown"，绝不伪造。下游重跑必须复用原成功 producer 身份，不得改绑到
消费者自身 attempt。source 与 automation（workflow SHA）分离绑定。
"""
from __future__ import annotations

import re

from contract_errors import ContractError
from input_contract import CHECK_ID_RE, REPO_RE, SHA_RE, reject_secret_like, validate_rel_path

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

IDENTITY_FIELDS = [
    "producer_repo",
    "producer_source_sha",
    "producer_run_id",
    "producer_run_attempt",
    "workflow_sha",
    "job",
    "artifact_digest",
    "artifact_path",
]
RUNTIME_FIELDS = ("toolchain", "tests", "retry", "cache_id", "artifact_id")
ALLOWED_ENV_KEYS = set(IDENTITY_FIELDS) | set(RUNTIME_FIELDS)
MAX_RUNTIME_STR = 256


def _is_pos_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def validate_envelope(env) -> dict:
    """结构化校验 envelope，返回规范化对象（缺失运行信息为 "unknown"）。"""
    if not isinstance(env, dict):
        raise ContractError("E_ENV_TYPE")
    unknown = set(env) - ALLOWED_ENV_KEYS
    if unknown:
        raise ContractError("E_ENV_UNKNOWN_FIELD", count=len(unknown))

    missing = [f for f in IDENTITY_FIELDS if f not in env or env[f] in (None, "")]
    if missing:
        raise ContractError("E_ENV_MISSING", count=len(missing))

    repo = env["producer_repo"]
    if not isinstance(repo, str) or not REPO_RE.match(repo):
        raise ContractError("E_ENV_REPO")
    reject_secret_like(repo, "E_ENV_SECRET")

    for field in ("producer_source_sha", "workflow_sha"):
        val = env[field]
        if not isinstance(val, str) or not SHA_RE.match(val):
            raise ContractError("E_ENV_SHA")

    job = env["job"]
    if not isinstance(job, str) or not CHECK_ID_RE.match(job):
        raise ContractError("E_ENV_JOB")
    reject_secret_like(job, "E_ENV_SECRET")

    if not _is_pos_int(env["producer_run_id"]):
        raise ContractError("E_ENV_RUN_ID")
    if not _is_pos_int(env["producer_run_attempt"]):
        raise ContractError("E_ENV_RUN_ATTEMPT")

    digest = env["artifact_digest"]
    if not isinstance(digest, str) or not DIGEST_RE.match(digest):
        raise ContractError("E_ENV_DIGEST")

    validate_rel_path(env["artifact_path"], code="E_ENV_PATH", secret_code="E_ENV_SECRET")

    out = {f: env[f] for f in IDENTITY_FIELDS}
    for field in RUNTIME_FIELDS:
        if field not in env or env[field] == "unknown":
            out[field] = "unknown"  # 缺失或已归一 -> unknown，绝不伪造
            continue
        val = env[field]
        if field == "retry":
            if not (isinstance(val, int) and not isinstance(val, bool) and val >= 0):
                raise ContractError("E_ENV_RUNTIME")
        else:
            if not isinstance(val, str) or len(val) > MAX_RUNTIME_STR:
                raise ContractError("E_ENV_RUNTIME")
            reject_secret_like(val, "E_ENV_SECRET")
        out[field] = val
    return out


def producer_identity(env) -> dict:
    """返回可被下游复用的权威 producer 身份子集。"""
    normalized = validate_envelope(env)
    return {f: normalized[f] for f in IDENTITY_FIELDS}


def validate_binding(env, expected) -> dict:
    """把 envelope 身份绑定到期望的 source/workflow/job（source 与 automation 分离）。"""
    normalized = validate_envelope(env)
    if not isinstance(expected, dict):
        raise ContractError("E_ENV_TYPE")
    actual = {
        "repo": normalized["producer_repo"],
        "source_sha": normalized["producer_source_sha"],
        "workflow_sha": normalized["workflow_sha"],
        "job": normalized["job"],
    }
    for key, val in actual.items():
        if key in expected and expected[key] != val:
            raise ContractError("E_ENV_IDENTITY_MISMATCH")
    return normalized


def verify_rerun_reuse(producer_env, rerun_env, expected) -> dict:
    """全量/仅失败下游重跑：必须复用原成功 producer 身份，禁止改绑消费者 attempt。

    - producer/rerun 都必须绑定同一 expected（错误 producer/source/digest/路径即拒）；
    - 两者 identity 必须逐字段相等，否则视为改绑（E_ENV_REBOUND）。
    """
    producer = validate_binding(producer_env, expected)
    rerun = validate_binding(rerun_env, expected)
    if producer_identity(producer) != producer_identity(rerun):
        raise ContractError("E_ENV_REBOUND")
    return producer_identity(producer)
