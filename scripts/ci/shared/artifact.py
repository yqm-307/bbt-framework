"""真实产物链路：有限源码元数据 manifest、真实 bytes 摘要与接收侧 fail-closed 校验。

边界（有意收窄，不做通用执行器/迁移系统）：
- manifest 是闭集的小型元数据（source repo/SHA 与实际 checkout HEAD、callee SHA、run 绑定），
  不扫描、不打包源码，也不含秘密；
- `artifact_digest` 只对**实际写入的 payload 字节**计算 SHA256，绝不接受调用方传入摘要；
- 接收侧严格按「envelope 结构 → 期望身份/run/attempt → 路径 → 文件类型 → 字节摘要 →
  JSON 形状 → manifest 交叉绑定」顺序校验，任一失败即拒绝；
- 全流程不执行 payload 内容，验证完成前不消费 artifact。
"""
from __future__ import annotations

import hashlib
import json
import os

from contract_errors import ContractError
from input_contract import CHECK_ID_RE, REPO_RE, SHA_RE, reject_secret_like, validate_rel_path

MANIFEST_VERSION = 1
MANIFEST_FIELDS = (
    "manifest_version",
    "source_repo",
    "source_sha",
    "source_head",
    "automation_repo",
    "automation_sha",
    "run_id",
    "run_attempt",
    "job",
)
# payload 在 artifact 根内的唯一相对路径（envelope.artifact_path 使用同一常量）。
PAYLOAD_REL_PATH = "manifest.json"
MAX_PAYLOAD_BYTES = 4096
SHA256_PREFIX = "sha256:"

# 接收侧期望身份键（全部必需；缺键/未知键即 E_ART_TYPE，不允许静默放宽比对）。
EXPECTED_KEYS = (
    "repo",
    "source_sha",
    "workflow_sha",
    "job",
    "run_id",
    "run_attempt",
    "payload_digest",
)
_EXPECTED_TO_ENVELOPE = {
    "repo": "producer_repo",
    "source_sha": "producer_source_sha",
    "workflow_sha": "workflow_sha",
    "job": "job",
    "run_id": "producer_run_id",
    "run_attempt": "producer_run_attempt",
    "payload_digest": "artifact_digest",
}
_MANIFEST_TO_ENVELOPE = (
    ("source_repo", "producer_repo"),
    ("source_sha", "producer_source_sha"),
    ("automation_sha", "workflow_sha"),
    ("run_id", "producer_run_id"),
    ("run_attempt", "producer_run_attempt"),
    ("job", "job"),
)


def _is_pos_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def sha256_digest(data: bytes) -> str:
    """真实 bytes 摘要（唯一摘要口径）。"""
    return SHA256_PREFIX + hashlib.sha256(data).hexdigest()


def validate_manifest(obj) -> dict:
    """校验有限源码元数据 manifest：非 object/未知字段/缺字段/类型错一律拒绝。"""
    if not isinstance(obj, dict):
        raise ContractError("E_ART_TYPE")
    unknown = set(obj) - set(MANIFEST_FIELDS)
    if unknown:
        raise ContractError("E_ART_MANIFEST_UNKNOWN", count=len(unknown))
    missing = [f for f in MANIFEST_FIELDS if f not in obj or obj[f] in (None, "")]
    if missing:
        raise ContractError("E_ART_MANIFEST_MISSING", count=len(missing))

    version = obj["manifest_version"]
    if isinstance(version, bool) or version != MANIFEST_VERSION:
        raise ContractError("E_ART_TYPE")

    for field in ("source_repo", "automation_repo"):
        val = obj[field]
        if not isinstance(val, str) or not REPO_RE.match(val):
            raise ContractError("E_ART_TYPE")
        reject_secret_like(val)

    for field in ("source_sha", "source_head", "automation_sha"):
        val = obj[field]
        if not isinstance(val, str) or not SHA_RE.match(val):
            raise ContractError("E_ART_SHA")

    # 实际 checkout HEAD 必须与声明的 source SHA 一致，否则不接受为可信 source 元数据。
    if obj["source_head"] != obj["source_sha"]:
        raise ContractError("E_ART_SOURCE_HEAD")

    if not isinstance(obj["job"], str) or not CHECK_ID_RE.match(obj["job"]):
        raise ContractError("E_ART_TYPE")
    if not _is_pos_int(obj["run_id"]) or not _is_pos_int(obj["run_attempt"]):
        raise ContractError("E_ART_TYPE")
    return {f: obj[f] for f in MANIFEST_FIELDS}


def manifest_bytes(manifest: dict) -> bytes:
    """payload 的唯一规范化字节序列；摘要必须针对这些字节计算。"""
    return json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def write_payload(out_dir: str, manifest: dict) -> tuple:
    """写入 payload（固定路径 manifest.json）并返回 (path, 真实 bytes 摘要)。"""
    if not isinstance(out_dir, str) or not out_dir:
        raise ContractError("E_ART_TYPE")
    norm = validate_manifest(manifest)
    data = manifest_bytes(norm)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, PAYLOAD_REL_PATH)
    with open(path, "wb") as handle:
        handle.write(data)
    return path, sha256_digest(data)


def build_envelope(manifest: dict, digest: str) -> dict:
    """由 manifest + 真实 bytes 摘要构造 producer envelope。"""
    return {
        "producer_repo": manifest["source_repo"],
        "producer_source_sha": manifest["source_sha"],
        "producer_run_id": manifest["run_id"],
        "producer_run_attempt": manifest["run_attempt"],
        "workflow_sha": manifest["automation_sha"],
        "job": manifest["job"],
        "artifact_digest": digest,
        "artifact_path": PAYLOAD_REL_PATH,
    }


def produce(out_dir: str, manifest_obj) -> dict:
    """产出 payload + envelope：摘要来自真实写入字节，绝不来自调用方输入。"""
    import envelope as envelope_module

    manifest = validate_manifest(manifest_obj)
    path, digest = write_payload(out_dir, manifest)
    env = build_envelope(manifest, digest)
    envelope_module.validate_envelope(env)
    return {
        "manifest": manifest,
        "envelope": env,
        "artifact_digest": digest,
        "artifact_path": PAYLOAD_REL_PATH,
        "payload_file": path,
    }


def validate_expected(expected) -> dict:
    """期望身份必须完整给出（缺键/未知键即拒绝，避免比对被静默弱化）。"""
    if not isinstance(expected, dict):
        raise ContractError("E_ART_TYPE")
    if set(expected) - set(EXPECTED_KEYS) or any(k not in expected for k in EXPECTED_KEYS):
        raise ContractError("E_ART_TYPE")
    return {k: expected[k] for k in EXPECTED_KEYS}


def verify_receipt(root: str, rel_path: str, envelope_obj, expected) -> dict:
    """接收侧校验真实 payload 与 producer 身份；返回身份/摘要/manifest，拒绝即 ContractError。"""
    import envelope as envelope_module

    if not isinstance(root, str) or not root:
        raise ContractError("E_ART_ROOT")
    rel = validate_rel_path(rel_path, code="E_ART_PATH")
    env = envelope_module.validate_envelope(envelope_obj)
    if env["artifact_path"] != rel:
        raise ContractError("E_ART_PATH_MISMATCH")

    exp = validate_expected(expected)
    for key, env_key in _EXPECTED_TO_ENVELOPE.items():
        # 含 producer run/attempt：只接受本 run 成功 producer，不接受任意 run/改绑身份。
        if env[env_key] != exp[key]:
            raise ContractError("E_ART_IDENTITY_MISMATCH")

    if os.path.islink(root) or not os.path.isdir(root):
        raise ContractError("E_ART_ROOT")
    root_real = os.path.realpath(root)
    target = os.path.join(root, rel)
    if os.path.islink(target):
        raise ContractError("E_ART_SYMLINK")
    if not os.path.isfile(target):
        raise ContractError("E_ART_MISSING")
    real = os.path.realpath(target)
    if real != root_real and not real.startswith(root_real + os.sep):
        raise ContractError("E_ART_PATH_ESCAPE")

    with open(target, "rb") as handle:
        data = handle.read(MAX_PAYLOAD_BYTES + 1)
    if len(data) > MAX_PAYLOAD_BYTES:
        raise ContractError("E_ART_TYPE")
    if sha256_digest(data) != env["artifact_digest"]:
        raise ContractError("E_ART_DIGEST_MISMATCH")

    try:
        obj = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise ContractError("E_ART_TYPE")
    if not isinstance(obj, dict):
        raise ContractError("E_ART_TYPE")
    manifest = validate_manifest(obj)
    for key, env_key in _MANIFEST_TO_ENVELOPE:
        if manifest[key] != env[env_key]:
            raise ContractError("E_ART_MANIFEST_MISMATCH")

    return {
        "identity": {field: env[field] for field in envelope_module.IDENTITY_FIELDS},
        "payload_digest": env["artifact_digest"],
        "manifest": manifest,
    }
