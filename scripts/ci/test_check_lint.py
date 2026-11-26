#!/usr/bin/env python3
"""check_lint.py 离线契约测试（仅标准库，不跑真实 C++ 构建）。

策略：在临时目录造一个迷你仓（.config/lint.toml + 源文件），pub lint 工具
默认用 third-party 里的真实副本走真实扫描；只有"坏协议"用例用同路径替身
注入损坏报告或异常退出码。断言：
- 有效 JSON 报告 → 0，普通 lint 候选（T501）不影响通过；
- 词法覆盖缺口（T000 宏未展开）→ 0，但输出 ::warning:: 与诊断行；
- 缺配置、没扫到任何源码、读文件错误（无效 UTF-8）、损坏报告、
  上游退出码非 0/2 → 一律不得返回 0。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v scripts/ci/test_check_lint.py
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
CHECK_LINT = HERE / 'check_lint.py'
REAL_TOOL_DIR = REPO_ROOT / 'third-party/bbtools-pub/tools/lint'
TOOL_REL = Path('third-party/bbtools-pub/tools/lint')

# 迷你配置：T501 提供确定性普通候选；排除 third-party 避免扫到工具自身。
CONFIG = """\
schema_version = 2
timeout_seconds = 60

[scan]
rules = ["T501"]

[scope]
exclude = ["third-party/*"]
"""

CLEAN_SOURCE = """\
namespace demo {

int add(int a, int b) { return a + b; }

}  // namespace demo
"""

# T501 普通候选：VersionV2 命中历史版本命名正则；不构成覆盖缺口。
CANDIDATE_SOURCE = """\
namespace demo {

int VersionV2 = 1;

}  // namespace demo
"""

# 带参数宏命中 T000 "preprocessor branches/macros not expanded" 词法缺口。
# 注意：数字字面量会先被 LEXEMES 遮蔽，"#define X 1" 不会触发该缺口，
# 这里用括号形式保证遮蔽后仍可识别为宏定义。
MACRO_SOURCE = """\
#define FLAG(x)

namespace demo {

int flag() { return 1; }

}  // namespace demo
"""

# 坏协议替身 A：stdout 不是 JSON，正常退出 0。
STUB_CORRUPT_REPORT = """\
import sys
print('this is not a json report')
sys.exit(0)
"""

# 坏协议替身 B：上游异常退出码 1（非 0/2）。
STUB_ABNORMAL_EXIT = """\
import sys
sys.exit(1)
"""


def write(path, content, binary=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    if binary:
        path.write_bytes(content)
    else:
        path.write_text(content, encoding='utf-8')


def build_repo(root, sources, *, config=True, stub=None):
    """在 root 造迷你仓：配置 + 源文件 + 真实 lint 工具（或同路径替身）。"""
    if config:
        write(root / '.config/lint.toml', CONFIG)
    if stub is None:
        shutil.copytree(REAL_TOOL_DIR, root / TOOL_REL)
    else:
        write(root / TOOL_REL / 'lint.py', stub)
    for rel, content in sources.items():
        if isinstance(content, bytes):
            write(root / rel, content, binary=True)
        else:
            write(root / rel, content)


def run_check(root):
    return subprocess.run(
        [sys.executable, '-I', str(CHECK_LINT), '--root', str(root)],
        capture_output=True, text=True, timeout=120)


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='check-lint-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def check(self):
        return run_check(self.root)


class TestRealScan(Case):
    def test_valid_report_with_plain_candidates_passes(self):
        build_repo(self.root, {'src/util.cc': CLEAN_SOURCE,
                               'src/version.cc': CANDIDATE_SOURCE})
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        # 普通 lint 候选只发布，不影响退出码。
        self.assertIn('lint: src/version.cc:3 T501', result.stdout)
        self.assertNotIn('::warning::', result.stdout)

    def test_lexical_gap_warns_but_passes(self):
        build_repo(self.root, {'src/macro.cc': MACRO_SOURCE})
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('::warning::pub lint coverage incomplete: 1 of 1 C++ files',
                      result.stdout)
        self.assertIn('lint: src/macro.cc:1 T000 '
                      'incomplete: preprocessor branches/macros not expanded',
                      result.stdout)


class TestInputFailures(Case):
    def test_missing_config_fails(self):
        build_repo(self.root, {'src/util.cc': CLEAN_SOURCE}, config=False)
        result = self.check()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('check_lint:', result.stderr)

    def test_no_sources_scanned_fails(self):
        build_repo(self.root, {'docs/note.md': '# nothing to scan\n'})
        result = self.check()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('no source files scanned', result.stderr)

    def test_invalid_utf8_input_error_fails(self):
        # 无效 UTF-8 触发 T000 输入错误，不属于词法覆盖缺口，必须失败。
        build_repo(self.root, {'src/good.cc': CLEAN_SOURCE,
                               'src/bad.cc': b'\xff\xfe\x00bad'})
        result = self.check()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('input error or unrecognized coverage gap', result.stderr)


class TestBadProtocol(Case):
    def test_corrupt_report_fails(self):
        build_repo(self.root, {'src/util.cc': CLEAN_SOURCE},
                   stub=STUB_CORRUPT_REPORT)
        result = self.check()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('check_lint:', result.stderr)

    def test_upstream_abnormal_exit_fails(self):
        build_repo(self.root, {'src/util.cc': CLEAN_SOURCE},
                   stub=STUB_ABNORMAL_EXIT)
        result = self.check()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('exited 1', result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
