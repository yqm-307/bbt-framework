"""typed 输入契约：校验 workflow_call 传入的受限输入，fail-closed，不回显输入。

只接受固定有限字段集；拒绝任意命令/YAML/jobs、路径逃逸、越界预算、未知字段与
疑似凭据值。所有拒绝只抛 ContractError(code)，message 不含输入值。
"""
from __future__ import annotations

import json
import re

from contract_errors import ContractError

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
# owner/name：两段均须以字母数字开头/结尾，其余允许 `_.-`；显式排除 `.`/`..`/空段等 dot 段。
REPO_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?/[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?$")
CHECK_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

ALLOWED_EVENTS = {"pull_request", "push", "merge_group", "workflow_dispatch", "schedule"}
ALLOWED_PROFILES = {"hosted"}
CLASSIFIER_STATUSES = {"ok", "failed", "unknown"}

MAX_CONCURRENCY = 32
MAX_TIMEOUT = 360
MAX_CHANGED_FILES = 5000
MAX_PATH_LEN = 512
MAX_CHECK_IDS = 64
MAX_EVENT_FIELD_LEN = 128
MAX_REPO_LEN = 200

# classify/verify 公共 typed 输入字段（闭集）。
ALLOWED_INPUT_KEYS = {
    "repo", "source_sha", "profile", "concurrency", "timeout_minutes",
    "event", "changed_files", "required_checks", "optional_checks",
    "classifier_status", "results",
}
EVENT_KEYS = {"name", "base_ref", "head_ref", "action"}

# 出现即拒绝：凭据/秘密指纹（大小写不敏感比对）。绝不把这些值写进 message。
_SECRET_MARKERS = (
    "ghp_", "gho_", "ghs_", "ghu_", "github_pat_", "xoxb-", "xoxp-",
    "akia", "-----begin", "private key", "password=", "passwd=",
    "secret=", "token=", "bearer ",
)


def reject_secret_like(value, code: str = "E_INPUT_SECRET") -> None:
    """若字符串疑似凭据则拒绝（指定 code）；不返回也不记录该值。"""
    if not isinstance(value, str):
        return
    low = value.lower()
    for marker in _SECRET_MARKERS:
        if marker in low:
            raise ContractError(code)


def validate_rel_path(path, code: str = "E_INPUT_PATH", secret_code: str = "E_INPUT_SECRET") -> str:
    """校验仓库相对路径：禁止绝对路径、`..` 逃逸、反斜杠、空段与超长。"""
    if not isinstance(path, str) or not path or len(path) > MAX_PATH_LEN:
        raise ContractError(code)
    if "\x00" in path or "\\" in path:
        raise ContractError(code)
    if path.startswith("/") or path.startswith("~"):
        raise ContractError(code)
    if len(path) >= 2 and path[1] == ":":
        raise ContractError(code)
    if any(part in ("", ".", "..") for part in path.split("/")):
        raise ContractError(code)
    reject_secret_like(path, secret_code)
    return path


def _validate_check_list(ids, code: str) -> list:
    if not isinstance(ids, list) or len(ids) > MAX_CHECK_IDS:
        raise ContractError(code)
    out = []
    for item in ids:
        if not isinstance(item, str) or not CHECK_ID_RE.match(item):
            raise ContractError("E_INPUT_CHECK_ID")
        out.append(item)
    if len(set(out)) != len(out):
        raise ContractError(code)
    return out


def _validate_event(event) -> dict:
    if not isinstance(event, dict):
        raise ContractError("E_INPUT_EVENT")
    if set(event) - EVENT_KEYS:
        raise ContractError("E_INPUT_EVENT")
    name = event.get("name")
    if name not in ALLOWED_EVENTS:
        raise ContractError("E_INPUT_EVENT")
    out = {"name": name}
    for key in ("base_ref", "head_ref", "action"):
        if key in event:
            val = event[key]
            if not isinstance(val, str) or len(val) > MAX_EVENT_FIELD_LEN:
                raise ContractError("E_INPUT_EVENT")
            reject_secret_like(val)
            out[key] = val
    return out


def validate_inputs(payload) -> dict:
    """规范化并校验 typed 输入，返回闭集规范化对象。"""
    if not isinstance(payload, dict):
        raise ContractError("E_INPUT_TYPE")
    unknown = set(payload) - ALLOWED_INPUT_KEYS
    if unknown:
        raise ContractError("E_INPUT_UNKNOWN_FIELD", count=len(unknown))

    repo = payload.get("repo")
    if not isinstance(repo, str) or len(repo) > MAX_REPO_LEN or not REPO_RE.match(repo):
        raise ContractError("E_INPUT_REPO")
    reject_secret_like(repo)

    sha = payload.get("source_sha")
    if not isinstance(sha, str) or not SHA_RE.match(sha):
        raise ContractError("E_INPUT_SHA")

    profile = payload.get("profile")
    if profile not in ALLOWED_PROFILES:
        if isinstance(profile, str) and profile.replace("-", "").replace("_", "") == "selfhosted":
            raise ContractError("E_PROFILE_NO_ADMISSION")
        raise ContractError("E_INPUT_PROFILE")

    concurrency = payload.get("concurrency", 1)
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or not 1 <= concurrency <= MAX_CONCURRENCY:
        raise ContractError("E_INPUT_CONCURRENCY")

    timeout = payload.get("timeout_minutes", 20)
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= MAX_TIMEOUT:
        raise ContractError("E_INPUT_TIMEOUT")

    event = _validate_event(payload.get("event", {"name": "workflow_dispatch"}))

    changed = payload.get("changed_files", [])
    if not isinstance(changed, list) or len(changed) > MAX_CHANGED_FILES:
        raise ContractError("E_INPUT_CHANGED_FILES")
    changed = [validate_rel_path(p) for p in changed]

    required = _validate_check_list(payload.get("required_checks", []), "E_INPUT_CHECK_ID")
    optional = _validate_check_list(payload.get("optional_checks", []), "E_INPUT_CHECK_ID")

    status = payload.get("classifier_status", "ok")
    if status not in CLASSIFIER_STATUSES:
        raise ContractError("E_INPUT_STATUS")

    results = payload.get("results", None)
    if results is not None and not isinstance(results, dict):
        raise ContractError("E_INPUT_TYPE")

    return {
        "repo": repo,
        "source_sha": sha,
        "profile": profile,
        "concurrency": concurrency,
        "timeout_minutes": timeout,
        "event": event,
        "changed_files": changed,
        "required_checks": required,
        "optional_checks": optional,
        "classifier_status": status,
        "results": results,
    }


_COERCE_JSON = {
    "changed_files_json": ("changed_files", list),
    "required_checks_json": ("required_checks", list),
    "optional_checks_json": ("optional_checks", list),
    "event_json": ("event", dict),
    "results_json": ("results", dict),
}
_COERCE_INT = {"concurrency": "concurrency", "timeout_minutes": "timeout_minutes"}


def coerce_workflow_inputs(raw) -> dict:
    """把 GitHub workflow_call 的字符串输入（JSON 串/数字串）强制为 typed 值再校验。"""
    if not isinstance(raw, dict):
        raise ContractError("E_INPUT_TYPE")
    out = {}
    for key, val in raw.items():
        if key in _COERCE_JSON:
            target, kind = _COERCE_JSON[key]
            if isinstance(val, str):
                text = val.strip()
                if text == "":
                    val = {} if kind is dict else []
                else:
                    try:
                        val = json.loads(text)
                    except Exception:
                        raise ContractError("E_INPUT_JSON")
            if not isinstance(val, kind):
                raise ContractError("E_INPUT_JSON")
            # 空对象 results 视为「未提供结果」：规范为 None，与分类解耦，避免默认调用即失败。
            if target == "results" and val == {}:
                val = None
            out[target] = val
        elif key in _COERCE_INT:
            if isinstance(val, str):
                if not val.strip().isdigit():
                    raise ContractError("E_INPUT_CONCURRENCY" if key == "concurrency" else "E_INPUT_TIMEOUT")
                val = int(val)
            out[_COERCE_INT[key]] = val
        else:
            out[key] = val
    return validate_inputs(out)
