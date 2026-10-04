#!/usr/bin/env python3
"""bbt-framework Issue #4（P0-A）：GetValue 跨进程验收驱动器。

由 ctest 调用：
  getvalue_run.py --server-bin <bin> --client-py <client> --proto <get_value.proto>
                  --protoc <protoc> --protoc-libdir <dir>

职责：
  1. 重复生成 proto（两次）并 diff —— 无 schema 漂移机器判据；
  2. spawn C++ server（动态端口/元数据/journal），等 readiness；
  3. 运行 Python 标准库客户端，断言退出码；
  4. 读 journal：断言预算过期/协议错误的请求未进 handler，且所有记录
     auth_state=unauthenticated_loopback、peer_principal 未被 metadata 升级；
  5. 收尾：关 stdin 让 server 优雅关闭，清理临时文件。
"""

import argparse
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile
import time


def run_protoc_drift(protoc, protoc_libdir, proto, proto_root, work):
    out1 = os.path.join(work, "gen1")
    out2 = os.path.join(work, "gen2")
    for out in (out1, out2):
        os.makedirs(out, exist_ok=True)
        env = dict(os.environ, LD_LIBRARY_PATH=protoc_libdir)
        rc = subprocess.run(
            [protoc, f"--proto_path={proto_root}", f"--cpp_out={out}", proto],
            env=env, capture_output=True, text=True)
        if rc.returncode != 0:
            print(f"protoc failed: {rc.stderr}", file=sys.stderr)
            sys.exit(9)
    rel = os.path.relpath(proto, proto_root)
    rel_base = rel[:-len(".proto")] if rel.endswith(".proto") else rel
    for name in (rel_base + ".pb.h", rel_base + ".pb.cc"):
        a = os.path.join(out1, name)
        b = os.path.join(out2, name)
        if not (os.path.exists(a) and os.path.exists(b)):
            print(f"protoc drift: generated {name} missing", file=sys.stderr)
            sys.exit(9)
        if not filecmp.cmp(a, b, shallow=False):
            print(f"protoc drift detected in {name}", file=sys.stderr)
            sys.exit(9)
    return {"status": "pass", "artifact": rel_base + ".pb.{h,cc}"}


def read_kv(path):
    out = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k] = v
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--client-py", required=True)
    ap.add_argument("--proto", required=True)
    ap.add_argument("--proto-root", required=True)
    ap.add_argument("--protoc", required=True)
    ap.add_argument("--protoc-libdir", required=True)
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    work = tempfile.mkdtemp(prefix="getvalue_")
    port_file = os.path.join(work, "port")
    meta_file = os.path.join(work, "meta")
    journal_file = os.path.join(work, "journal")
    results_file = os.path.join(work, "results.json")
    server = None
    try:
        drift = run_protoc_drift(args.protoc, args.protoc_libdir, args.proto,
                                 args.proto_root, work)
        print(f"protoc-drift {drift}", file=sys.stderr)

        server = subprocess.Popen(
            [args.server_bin, "server", port_file, meta_file, journal_file],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        deadline = time.monotonic() + args.timeout
        port = 0
        while time.monotonic() < deadline:
            if server.poll() is not None:
                print(f"server exited early rc={server.returncode}",
                      file=sys.stderr)
                sys.exit(2)
            try:
                with open(port_file) as f:
                    port = int(f.read().strip())
                if port > 0:
                    break
            except (OSError, ValueError):
                pass
            time.sleep(0.05)
        if not port:
            print("server did not write port file", file=sys.stderr)
            sys.exit(2)
        meta = read_kv(meta_file)
        print(f"server up port={port} meta={meta}", file=sys.stderr)

        client = subprocess.run(
            [sys.executable, args.client_py, "--port", str(port),
             "--request-schema", meta.get("request_schema", ""),
             "--response-schema", meta.get("response_schema", ""),
             "--results", results_file],
            capture_output=True, text=True, timeout=args.timeout)
        sys.stdout.write(client.stdout)
        sys.stderr.write(client.stderr)
        if client.returncode != 0:
            print(f"client failed rc={client.returncode}", file=sys.stderr)
            sys.exit(3)

        # journal 断言：哪些请求真正进过 handler（handler_entered）。
        entered = set()
        peer_upgraded = []
        auth_ok = True
        if os.path.exists(journal_file):
            with open(journal_file) as f:
                for line in f:
                    rec = dict(
                        kv.split("=", 1) for kv in line.strip().split(" ")
                        if "=" in kv)
                    if rec.get("event") == "handler_entered":
                        entered.add(rec.get("req", ""))
                        if rec.get("peer_principal", ""):
                            peer_upgraded.append(rec.get("req"))
                    if rec.get("auth_state") != "unauthenticated_loopback":
                        auth_ok = False
        # 业务/负载校验在 handler 内，故这些请求会进 handler；
        # budget/schema/路由/wire 层被拒的请求不得进 handler。
        expect_entered = {"sc-known", "sc-miss", "sc-empty",
                          "sc-badpayload", "sc-fwd", "sc-v2", "sc-budgetlarge"}
        must_not_enter = {"sc-budget0", "sc-budget1", "sc-schema", "sc-nomethod",
                          "sc-trunc", "sc-ctype"}
        missing = expect_entered - entered
        leaked = must_not_enter & entered
        if missing:
            print(f"journal: expected handler-entered missing {sorted(missing)}",
                  file=sys.stderr)
            sys.exit(4)
        if leaked:
            print(f"journal: expired/protocol-rejected request entered handler "
                  f"{sorted(leaked)}", file=sys.stderr)
            sys.exit(4)
        if peer_upgraded:
            print(f"journal: peer_principal upgraded from metadata "
                  f"{peer_upgraded}", file=sys.stderr)
            sys.exit(4)
        if not auth_ok:
            print("journal: missing/incorrect auth_state marker", file=sys.stderr)
            sys.exit(4)
        print(f"journal-check entered={sorted(entered)} "
              f"not-entered={sorted(must_not_enter)} auth_state=ok",
              file=sys.stderr)

        if os.path.exists(results_file):
            with open(results_file) as f:
                print(f"client-results {f.read().strip()}", file=sys.stderr)
    finally:
        if server is not None:
            if server.stdin:
                server.stdin.close()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
                print("server did not stop cleanly", file=sys.stderr)
                sys.exit(5)
            if server.returncode != 0:
                print(f"server rc={server.returncode}", file=sys.stderr)
                sys.exit(5)
        shutil.rmtree(work, ignore_errors=True)
    print("GETVALUE-DRIVER-PASS auth_state=unauthenticated_loopback")


if __name__ == "__main__":
    main()
