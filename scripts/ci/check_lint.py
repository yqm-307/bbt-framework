#!/usr/bin/env python3
"""Publish pub lint candidates without treating coverage gaps as a clean scan."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


# Pub's T000 also covers input errors. Only known lexer limitations may warn.
LEXICAL_GAP = re.compile(
    r"incomplete: (?:unterminated or unsupported literal/comment|"
    r"preprocessor branches/macros not expanded|unmatched delimiter|"
    r"unclosed delimiter|unclassified member of [A-Za-z_][A-Za-z_0-9]*|"
    r"unsupported type declaration)"
)


def check(root):
    tool = root / "third-party/bbtools-pub/tools/lint/lint.py"
    config = root / ".config/lint.toml"
    result = subprocess.run(
        [sys.executable, "-IB", str(tool), "--root", str(root),
         "--config", str(config), "--format", "json"],
        capture_output=True, text=True, check=False,
    )
    for line in result.stderr.splitlines():
        print(f"pub lint: {line}", file=sys.stderr)
    if result.returncode not in (0, 2):
        raise ValueError(f"pub lint exited {result.returncode}")

    report = json.loads(result.stdout)
    coverage = report["coverage"]
    files = coverage["files_scanned"]
    partial = coverage["partial_files"]
    findings = report["findings"]
    if (report["schema_version"] != 1 or type(files) is not int or files <= 0
            or not isinstance(partial, list) or not isinstance(findings, list)):
        raise ValueError("invalid report or no source files scanned")

    gaps = set()
    for finding in findings:
        path, line, rule, message = (finding[key] for key in ("file", "line", "rule", "message"))
        if (not all(isinstance(value, str) and value for value in (path, rule, message))
                or type(line) is not int or line < 1):
            raise ValueError("invalid finding in report")
        detail = f"{path}:{line} {rule} {message}"
        print("lint: " + detail.replace("\n", "\\n").replace("\r", "\\r"))
        if rule == "T000":
            if not LEXICAL_GAP.fullmatch(message):
                raise ValueError("T000 reported an input error or unrecognized coverage gap")
            gaps.add(path)

    if set(partial) != gaps or (result.returncode == 2) != bool(gaps):
        raise ValueError("coverage and exit status do not match the findings")
    if gaps:
        print(f"::warning::pub lint coverage incomplete: {len(gaps)} of {files} C++ files; see T000 diagnostics")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    try:
        return check(args.root.resolve(strict=True))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"check_lint: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
