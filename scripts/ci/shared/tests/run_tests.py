#!/usr/bin/env python3
"""运行 scripts/ci/shared 全部单测（纯逻辑 + workflow 静态契约）。

用法（推荐用既有 uv-cache，禁止联网新增依赖）：
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/shared/tests/run_tests.py
或（需要 PyYAML 时，复用只读 uv-cache）：
  UV_OFFLINE=1 UV_CACHE_DIR=<cache> uv run --no-project --with pyyaml \
    python3 scripts/ci/shared/tests/run_tests.py
"""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

if __name__ == "__main__":
    suite = unittest.TestLoader().discover(HERE, pattern="test_*.py", top_level_dir=HERE)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"\nRUN={result.testsRun} FAIL={len(result.failures)} ERROR={len(result.errors)} "
          f"SKIP={len(result.skipped)}")
    sys.exit(0 if result.wasSuccessful() else 1)
