#!/usr/bin/env python3
"""真实存储验收：自有固定镜像容器、独立服务进程、ProtoWireV1 请求与 Mongo 读回。"""
import argparse
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

# 导入原驱动只复用客户端/进程工具，缓存产物不能写进示例源码目录。
sys.dont_write_bytecode = True
import run as cluster

IMAGES = {
    "redis": "redis@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499",
    "mongo": "mongo@sha256:4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2",
}
NAMES = {role: "bbt.example.ecommerce.v1." + role.title() + "Service"
         for role in ("gateway", "order", "catalog")}
NAMES["settlement"] = "ecommerce.settlement"


class StorageNode(cluster.Node):
    def __init__(self, binary, mode, consul_url, log_path, storage_args):
        self.mode = mode
        self.log_path = str(log_path)
        self.lines = []
        self.ready_info = None
        self._cv = threading.Condition()
        self._log = open(log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [binary, mode, "--consul", consul_url, "--storage"] + storage_args,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=1, text=True)
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()


def command(args, timeout=30):
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"command {args[0]} rc={result.returncode}: {result.stdout}")
    return result.stdout.strip()


def order_payload(order_id, sku="sku-book", qty=2, discount=100):
    return (cluster._f_len(1, order_id) + cluster._f_len(2, sku) +
            cluster._f_varint(3, qty & ((1 << 64) - 1)) + cluster._f_varint(4, discount))


def decode_record(data):
    names = {1: "order_id", 2: "sku", 3: "qty", 4: "total_cents",
             5: "discount_cents", 6: "payable_cents", 7: "state",
             8: "catalog_instance", 9: "order_instance", 10: "settlement_instance"}
    result = {"discount_cents": 0, "payable_cents": 0}
    for field, wire, value in cluster._parse_fields(data):
        if field in names:
            result[names[field]] = value.decode() if wire == 2 else value
    return result


def rpc(node, service, method, payload, request_type):
    request_id = "wire-" + uuid.uuid4().hex
    prefix = "bbt.example.ecommerce.v1."
    body = cluster._f_varint(1, 1) + cluster._f_len(2, request_id)
    body += cluster._f_len(3, NAMES[service]) + cluster._f_len(4, method)
    body += cluster._f_varint(5, 4000) + cluster._f_len(6, prefix + request_type)
    body += cluster._f_len(7, prefix + "Record") + cluster._f_len(8, payload)
    port = int(node.ready_info["endpoint"].rsplit(":", 1)[1])
    start = time.monotonic()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=7)
    try:
        conn.request("POST", "/rpc", body, {"Content-Type": cluster.CONTENT_TYPE})
        response = conn.getresponse()
        raw = response.read()
        env = cluster.dec_envelope(raw)
        result = {"ok": env["success"], "elapsed": time.monotonic() - start,
                  "http_status": response.status, "request_id": request_id}
        if env.get("error"):
            result["error"] = env["error"]
        if env["success"]:
            result["record"] = decode_record(env.get("payload", b""))
        return result
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--consul", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    args.binary = str(Path(args.binary).resolve())
    args.consul = str(Path(args.consul).resolve())
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    work = output.parent / ("storage-work-" + uuid.uuid4().hex[:12])
    work.mkdir()
    run_id = "ec_" + uuid.uuid4().hex[:16]
    result = {"status": "fail", "rc": 1, "checks": [], "failures": [],
              "run_id": run_id, "images": {}, "nodes": [], "containers": {},
              "workdir": str(work)}
    nodes = []
    containers = {}
    consul = None

    def check(name, passed, evidence):
        result["checks"].append({"name": name, "passed": bool(passed), "evidence": evidence})
        if not passed:
            result["failures"].append(name)
            raise RuntimeError(name)

    def start(role, storage_args):
        if consul is None:
            raise RuntimeError("consul not started")
        node = StorageNode(args.binary, role, consul.url(),
                           work / f"{role}-{len(nodes)}.log", storage_args)
        nodes.append(node)
        ready = node.wait_ready(20)
        check(f"{role}_ready_{len(nodes)}", ready is not None,
              {"ready": ready, "pid": node.proc.pid, "rc": node.proc.poll()})
        result["nodes"].append(ready)
        return node

    def query(node, method, order_id):
        return rpc(node, "gateway", method, cluster._f_len(1, order_id), "LookupRequest")

    def routes_ready(node, methods):
        deadline = time.monotonic() + 15
        observed = {}
        while time.monotonic() < deadline:
            observed = {m: query(node, m, "route-probe") for m in methods}
            if all(x.get("error", {}).get("message") == "record not found" for x in observed.values()):
                return observed
            time.sleep(0.1)
        raise RuntimeError("routes did not become ready: " + json.dumps(observed))

    def views_ready(order_node, gateway_node):
        def views(node):
            latest = {}
            for line in node.events():
                if line.startswith("event=view"):
                    fields = dict(t.split("=", 1) for t in line.split() if "=" in t)
                    latest[fields["service"]] = fields
            return latest
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            ov, gv = views(order_node), views(gateway_node)
            catalog_view = ov.get(NAMES["catalog"], {})
            settlement_view = ov.get(NAMES["settlement"], {})
            gateway_view = gv.get(NAMES["settlement"], {})
            if (catalog_view.get("endpoints") == "1" and settlement_view.get("endpoints") == "1" and
                    gateway_view.get("endpoints") == "1" and
                    int(settlement_view["revision"]) >= int(gateway_view["revision"])):
                return
            time.sleep(0.1)
        raise RuntimeError("order downstream discovery views not ready")

    def mongo_eval(script):
        return command(["docker", "exec", containers["mongo"], "mongosh", "--quiet",
                        "--eval", f"const d=db.getSiblingDB('{run_id}'); " + script])

    def count(order_id):
        return json.loads(mongo_eval(
            f"print(JSON.stringify({{orders:d.orders.countDocuments({{_id:'{order_id}'}}),"
            f"bills:d.bills.countDocuments({{_id:'{order_id}'}})}}))"))

    started = time.monotonic()
    try:
        for kind, image in IMAGES.items():
            inspected = json.loads(command(["docker", "image", "inspect", image]))[0]
            check(kind + "_image_local", image in inspected.get("RepoDigests", []),
                  {"id": inspected["Id"], "repo_digests": inspected.get("RepoDigests", [])})
            result["images"][kind] = inspected["Id"]
            port = 6379 if kind == "redis" else 27017
            name = f"{run_id}-{kind}"
            containers[kind] = name
            cid = command(["docker", "run", "--pull=never", "-d", "--name", name,
                           "--label", "bbt.ecommerce.run=" + run_id,
                           "-p", f"127.0.0.1::{port}", image])
            containers[kind] = cid
            mapped = command(["docker", "port", cid, str(port) + "/tcp"])
            check(kind + "_loopback", mapped.startswith("127.0.0.1:"), mapped)
            result["containers"][kind] = {"id": cid, "endpoint": mapped}
        deadline = time.monotonic() + 35
        ready = False
        ping = None
        while time.monotonic() < deadline:
            ping = subprocess.run(["docker", "exec", containers["mongo"], "mongosh", "--quiet",
                                   "--eval", "quit(db.adminCommand({ping:1}).ok ? 0 : 1)"],
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=8)
            if ping.returncode == 0:
                ready = True
                break
            time.sleep(0.2)
        check("mongo_ready", ready, {"rc": ping.returncode if ping else None})
        check("redis_ready", command(["docker", "exec", containers["redis"], "redis-cli", "PING"]) == "PONG", "PING")
        consul = cluster.ConsulAgent(args.consul, str(work))
        consul.start()
        check("consul_ready", consul.ready(), {"pid": consul.pid})
        storage_args = ["--redis", result["containers"]["redis"]["endpoint"],
                        "--mongo", "mongodb://" + result["containers"]["mongo"]["endpoint"] + "/?retryWrites=false",
                        "--database", run_id, "--key-prefix", run_id]
        catalog = start("catalog", storage_args)
        settlement = start("settlement", storage_args)
        order = start("order", storage_args)
        gateway = start("gateway", storage_args)
        routes_ready(gateway, ["GetOrder", "GetBill"])
        views_ready(order, gateway)
        # 没有写请求的自动重试：每次 CreateOrder 都是显式验收动作。
        created = rpc(gateway, "gateway", "CreateOrder", order_payload("order-one"), "OrderRequest")
        check("create_settled", created.get("ok") and created["record"]["state"] == "settled" and
              created["record"]["total_cents"] == 2598 and created["record"]["payable_cents"] == 2498, created)
        original = created["record"]
        replay = rpc(gateway, "gateway", "CreateOrder", order_payload("order-one"), "OrderRequest")
        check("same_payload_same_result", replay.get("record") == original, replay)
        conflict = rpc(gateway, "gateway", "CreateOrder", order_payload("order-one", qty=3), "OrderRequest")
        check("payload_conflict", not conflict["ok"] and "id_conflict" in conflict.get("error", {}).get("message", ""), conflict)
        got = query(gateway, "GetOrder", "order-one")
        bill = query(gateway, "GetBill", "order-one")
        check("mongo_query", got.get("record") == original and bill.get("record", {}).get("payable_cents") == 2498, {"order": got, "bill": bill})
        counts = count("order-one")
        check("mongo_unique", counts == {"orders": 1, "bills": 1}, counts)
        second = rpc(gateway, "gateway", "CreateOrder", order_payload("order-two"), "OrderRequest")
        check("second_order", second["ok"], second)
        cache_events = [x for x in catalog.events() if x.startswith("event=cache")]
        check("cache_miss_hit", any("result=miss" in x for x in cache_events) and any("result=hit" in x for x in cache_events), cache_events)
        cached = command(["docker", "exec", containers["redis"], "redis-cli", "GET", run_id + ":price:sku-book"])
        check("redis_actual_value", cached == "1299", cached)
        for name, payload in [
            ("zero_qty", order_payload("invalid-zero", qty=0)),
            ("negative_qty", order_payload("invalid-negative", qty=-1)),
            ("overflow", order_payload("invalid-overflow", qty=(1 << 63) - 1)),
            ("discount", order_payload("invalid-discount", discount=999999)),
            ("unknown_sku", order_payload("invalid-sku", sku="unknown")),
            ("empty_id", order_payload("")),
        ]:
            rejected = rpc(gateway, "gateway", "CreateOrder", payload, "OrderRequest")
            check(name + "_rejected", not rejected["ok"], rejected)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            concurrent_results = list(pool.map(lambda _: rpc(gateway, "gateway", "CreateOrder", order_payload("order-concurrent"), "OrderRequest"), range(2)))
        check("concurrent_replay", all(x["ok"] for x in concurrent_results) and concurrent_results[0]["record"] == concurrent_results[1]["record"] and
              count("order-concurrent") == {"orders": 1, "bills": 1}, concurrent_results)
        # 直接调用独立结算进程，证明其自身也有载荷冲突保护。
        payload = cluster._f_len(1, order_payload("order-one", qty=3)) + cluster._f_varint(2, 2598)
        conflict = rpc(settlement, "settlement", "Settle", payload, "BillRequest")
        check("settlement_conflict", not conflict["ok"] and "id_conflict" in conflict.get("error", {}).get("message", ""), conflict)
        rc, message = settlement.stop()
        check("settlement_stop", rc == 0, {"rc": rc, "message": message})
        partial = rpc(gateway, "gateway", "CreateOrder", order_payload("order-pending"), "OrderRequest")
        check("downstream_bounded_error", not partial["ok"] and partial["elapsed"] < 7, partial)
        pending = query(gateway, "GetOrder", "order-pending")
        check("partial_queryable", pending.get("record", {}).get("state") == "pending" and
              count("order-pending") == {"orders": 1, "bills": 0}, pending)
        settlement = start("settlement", storage_args)
        routes_ready(gateway, ["GetBill"])
        views_ready(order, gateway)
        resumed = rpc(gateway, "gateway", "CreateOrder", order_payload("order-pending"), "OrderRequest")
        check("explicit_replay_completes_pending", resumed.get("record", {}).get("state") == "settled" and
              count("order-pending") == {"orders": 1, "bills": 1}, resumed)
        for node in [catalog, order, settlement]:
            rc, message = node.stop()
            check(node.mode + "_restart_stop", rc == 0, {"rc": rc, "message": message})
        catalog = start("catalog", storage_args)
        settlement = start("settlement", storage_args)
        order = start("order", storage_args)
        routes_ready(gateway, ["GetOrder", "GetBill"])
        views_ready(order, gateway)
        after = query(gateway, "GetOrder", "order-one")
        after_bill = query(gateway, "GetBill", "order-one")
        check("restart_persistence", after.get("record") == original and after_bill.get("record") == bill["record"],
              {"order": after, "bill": after_bill})
        replay = rpc(gateway, "gateway", "CreateOrder", order_payload("order-one"), "OrderRequest")
        check("restart_idempotency", replay.get("record") == original and count("order-one") == {"orders": 1, "bills": 1}, replay)
        hops = [dict(t.split("=", 1) for t in line.split() if "=" in t)
                for node in nodes for line in node.events()
                if line.startswith("event=storage_hop") and "order_id=order-one " in line and "ok=1 " in line]
        first_hops = {role: next((h for h in hops if h["service"] == role and h["method"] == method), None)
                      for role, method in [("gateway", "CreateOrder"), ("order", "CreateOrder"), ("catalog", "Price"), ("settlement", "Settle")]}
        check("different_process_typed_hops", all(first_hops.values()) and
              len({h["pid"] for h in first_hops.values() if h is not None}) == 4 and
              len({h["request_id"] for h in first_hops.values() if h is not None}) == 4, first_hops)
        # 验证显式存储模式缺配置时失败，不回落报价。
        rejected = subprocess.run([args.binary, "order", "--consul", consul.url(), "--storage"],
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=8)
        check("storage_fail_closed", rejected.returncode != 0 and "storage requires" in rejected.stdout,
              {"rc": rejected.returncode, "output": rejected.stdout})
        # 实际停止本次后端，再证明新进程拒绝进入 ready；不触碰共享服务。
        for backend, role in [("redis", "catalog"), ("mongo", "order")]:
            command(["docker", "stop", "--time", "3", containers[backend]], timeout=10)
            failed_start = subprocess.run(
                [args.binary, role, "--consul", consul.url(), "--storage"] + storage_args,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=18)
            check(backend + "_unavailable_fail_closed", failed_start.returncode != 0 and
                  "event=ready" not in failed_start.stdout,
                  {"rc": failed_start.returncode, "output": failed_start.stdout})
    except Exception as error:
        result["failures"].append(str(error))
    finally:
        for node in reversed(nodes):
            rc, message = node.stop()
            result.setdefault("process_rc", []).append({"role": node.mode, "pid": node.proc.pid, "rc": rc})
            if rc != 0:
                result["failures"].append(f"{node.mode} cleanup rc={rc}: {message}")
            node.cleanup()
        if consul is not None:
            consul.stop()
            result["consul_rc"] = consul.proc.returncode
        for kind, cid in containers.items():
            try:
                label = command(["docker", "inspect", "--format", '{{ index .Config.Labels "bbt.ecommerce.run" }}', cid])
                if label != run_id:
                    raise RuntimeError("container ownership mismatch")
                command(["docker", "rm", "-f", "-v", cid])
                exists = subprocess.run(["docker", "inspect", cid], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)
                if exists.returncode == 0:
                    raise RuntimeError("owned container survived cleanup")
                result.setdefault("cleanup", {})[kind] = {"removed": True, "inspect_rc": exists.returncode}
            except Exception as error:
                result["failures"].append("cleanup: " + str(error))
        result["duration_s"] = round(time.monotonic() - started, 3)
        result["status"] = "pass" if not result["failures"] else "fail"
        result["rc"] = 0 if result["status"] == "pass" else 1
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "rc": result["rc"], "checks": len(result["checks"]),
                      "failures": result["failures"], "output": str(output)}, ensure_ascii=False))
    return result["rc"]


if __name__ == "__main__":
    sys.exit(main())
