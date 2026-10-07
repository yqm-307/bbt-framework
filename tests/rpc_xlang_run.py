#!/usr/bin/env python3
"""framework #4：跨语言 round-trip 驱动器。

由 ctest 调用：
  rpc_xlang_run.py --server-bin <path> --client-py <path> --pb2-dir <dir>

流程：spawn 服务端（stdin 管道保持）、等 port_file、跑 python 客户端、
断言退出码与输出、关闭服务端 stdin 让其按序 teardown，最后清理临时文件。
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--client-py", required=True)
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    with tempfile.NamedTemporaryFile(
            prefix="rpc_xlang_port_", delete=False) as tf:
        port_file = tf.name
    os.unlink(port_file)

    server = subprocess.Popen(
        [args.server_bin, "server", port_file],
        stdin=subprocess.PIPE,
        stdout=sys.stderr, stderr=sys.stderr)
    try:
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
        print(f"server up port={port}", file=sys.stderr)

        client = subprocess.run(
            [sys.executable, args.client_py, "--port", str(port)],
            capture_output=True, text=True, timeout=args.timeout)
        sys.stdout.write(client.stdout)
        sys.stderr.write(client.stderr)
        if client.returncode != 0:
            print(f"client failed rc={client.returncode}", file=sys.stderr)
            sys.exit(3)
    finally:
        if server.stdin:
            server.stdin.close()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
            print("server did not stop cleanly", file=sys.stderr)
            sys.exit(4)
        if server.returncode != 0:
            print(f"server rc={server.returncode}", file=sys.stderr)
            sys.exit(5)
        try:
            os.unlink(port_file)
        except OSError:
            pass
    print("DRIVER-PASS")


if __name__ == "__main__":
    main()
