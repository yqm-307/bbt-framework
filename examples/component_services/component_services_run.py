#!/usr/bin/env python3
"""bbt-framework 组件式服务示例的验收驱动器（Python 标准库，无第三方依赖）。

由 ctest 调用：
  component_services_run.py --bin <component_services> [--results <json>]

职责（全部是真实进程 + 公开 v2 API 的运行证据）：
  1. 运行 shared 场景：一个 App 内两个 Service 共享同一组件实例、运行期
     热拔插、hook/observer 生存期、缺失槽位错误、hook 失败记录、有限
     deadline 收口；
  2. 并发启动两个独立 node 进程：同一业务头各自装配自己的组件实例，
     断言 PID 不同、各自购物车总额只由本进程配置决定（不共享对象）；
  3. 解析 fixture journal 的 `event=check name=... ok=1|0` 与业务事件，
     断言必需检查项全部出现且通过，最后写 results JSON。

异常路径 finally 回收自有个子进程，不触碰他人进程；每个子进程有硬超时。
"""

import argparse
import json
import os
import subprocess
import sys
import time

CASE_TIMEOUT_S = 90

# shared 场景必须出现的检查项（C++ 侧逐条真实断言后写入 journal）。
REQUIRED_SHARED_CHECKS = [
    "install_pricing_v1",
    "find_pricing",
    "observer_token_active",
    "install_catalog_with_declared_binding",
    "install_cart",
    "co_start",
    "app_running",
    "shared_single_instance",
    "component_hooks_install_start",
    "find_services",
    "runtime_bind_cart",
    "cart_binding_borrowable",
    "two_services_share_one_instance",
    "cart_hook_write_visible",
    "observer_saw_running",
    "missing_slot_not_found",
    "unbind_both_subjects",
    "old_handle_closed",
    "uninstall_v1",
    "component_hooks_stop_uninstall",
    "old_instance_destroyed",
    "observer_saw_destroyed",
    "observer_token_revoke",
    "find_after_uninstall_not_found",
    "bind_after_uninstall_not_found",
    "install_pricing_v2_same_slot",
    "single_live_instance_after_swap",
    "reinstalled_identity_changed",
    "reinstalled_running_while_app_running",
    "rebind_both_subjects_new_instance",
    "new_binding_borrowable",
    "new_instance_fresh_state",
    "observer_service_token",
    "observer_token_explicit_revoke",
    "co_stop",
    "app_stopped",
    "no_pending_cleanup",
    "no_lifecycle_failures",
    "no_events_after_revoke",
    "all_instances_destroyed",
    "both_versions_stopped",
]

REQUIRED_FAILURE_CHECKS = [
    "install_failing_component",
    "hook_failure_surfaces_error",
    "partial_start_reports_running",
    "hook_failure_state_failed",
    "hook_failure_recorded",
    "failure_app_clean_stop",
]

REQUIRED_NODE_CHECKS = [
    "node_install_component",
    "node_install_services",
    "node_co_start",
    "node_bind_cart",
    "node_independent_state",
    "node_clean_stop",
]

_FAILURES = []


def check(cond, label, detail=""):
    if not cond:
        _FAILURES.append(f"{label}: {detail}" if detail else label)
        print(f"FAIL {label} {detail}", file=sys.stderr)
    else:
        print(f"PASS {label} {detail}")
    return cond


def read_journal(path):
    """把 `event=... k=v` 行解析成 dict 列表；值不含空格（fixture 侧约定）。"""
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = {}
            for token in line.split(" "):
                if "=" in token:
                    k, v = token.split("=", 1)
                    rec[k] = v
            entries.append(rec)
    return entries


def run_child(argv, timeout_s=CASE_TIMEOUT_S):
    """运行自有子进程，返回 (rc, stdout, stderr)；超时即杀并记失败。"""
    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    try:
        out, err = proc.communicate(timeout=timeout_s)
        return proc.returncode, out, err
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        return None, out, err
    finally:
        if proc.poll() is None:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _FAILURES.append(f"child_not_reaped: pid={proc.pid}")
                print(f"FAIL child_not_reaped pid={proc.pid}", file=sys.stderr)


def spool(stdout, stderr):
    for line in (stdout or "").splitlines():
        print(f"  fixture| {line}")
    for line in (stderr or "").splitlines():
        print(f"  fixture! {line}")


def checks_by_name(entries):
    return {
        e["name"]: e
        for e in entries
        if e.get("event") == "check" and "name" in e
    }


def verify_checks(entries, required, scenario):
    seen = checks_by_name(entries)
    missing = [n for n in required if n not in seen]
    failed = [n for n in required if n in seen and seen[n].get("ok") != "1"]
    check(not missing, f"{scenario}.required_checks_present",
          f"missing={','.join(missing) if missing else 'none'}")
    check(not failed, f"{scenario}.required_checks_passed",
          f"failed={','.join(failed) if failed else 'none'}")
    unexpected = [
        e["name"] for e in entries
        if e.get("event") == "check" and e.get("ok") == "0"
    ]
    check(not unexpected, f"{scenario}.no_failed_checks",
          f"failed={','.join(unexpected) if unexpected else 'none'}")
    return seen


def scenario_shared(binary, tmpdir):
    print("\n=== shared: 同进程组件复用 + 热拔插 + hook/observer ===")
    journal = os.path.join(tmpdir, "shared.journal")
    rc, out, err = run_child([binary, "shared", "--journal", journal])
    spool(out, err)
    entries = read_journal(journal)
    check(rc == 0, "shared.exit_code", f"rc={rc}")
    seen = verify_checks(entries, REQUIRED_SHARED_CHECKS, "shared")

    hooks = [
        e for e in entries
        if e.get("event") == "component_hook" and "phase" in e
    ]
    phases = [e["phase"] for e in hooks]
    check("install" in phases and "start" in phases,
          "shared.component_hook_install_and_start", f"phases={phases}")
    # 第一版组件必须 stop 在 uninstall 之前。
    if "stop" in phases and "uninstall" in phases:
        check(phases.index("stop") < phases.index("uninstall"),
              "shared.hook_order_stop_before_uninstall", f"phases={phases}")
    else:
        check(False, "shared.hook_order_stop_before_uninstall",
              f"phases={phases}")

    installs = [
        e for e in entries
        if e.get("event") == "component_hook" and e.get("phase") == "install"
        and e.get("name") == "pricing"
    ]
    seqs = [e.get("instance_seq") for e in installs]
    check(seqs[:2] == ["1", "2"],
          "shared.two_instances_same_slot", f"instance_seqs={seqs}")

    binding_events = [e for e in entries if e.get("event") == "binding_hook"]
    bound = [e for e in binding_events if e.get("bound") == "1"]
    unbound = [e for e in binding_events if e.get("bound") == "0"]
    check(bool(bound) and bool(unbound), "shared.binding_hook_both_directions",
          f"bound={len(bound)} unbound={len(unbound)}")

    cart = [e for e in entries if e.get("event") == "cart_binding"]
    totals = [e.get("cart_total") for e in cart if "cart_total" in e]
    check(totals == ["2000", "3000"], "shared.cart_totals_per_version",
          f"totals={totals}")

    rebinding = [
        e for e in binding_events
        if e.get("service") == "catalog" and e.get("borrow") == "1"
    ]
    check(bool(rebinding), "shared.catalog_rebind_borrows_new_instance",
          f"events={len(rebinding)}")

    fail_events = [e for e in entries if e.get("event") == "hook_failure_recorded"]
    check(len(fail_events) == 1, "shared.hook_failure_event",
          f"events={len(fail_events)}")

    end = [e for e in entries if e.get("event") == "run_end"]
    check(bool(end) and end[-1].get("rc") == "0", "shared.run_end_clean",
          f"end={end[-1] if end else 'none'}")
    return {"status": "pass" if rc == 0 and not _FAILURES else "fail",
            "checks": len(seen)}


def scenario_failure_app(binary, tmpdir):
    # shared 场景已包含失败 App 段落（同一进程内第二个 App），这里只核对
    # 失败段落的检查项确实来自真实运行而不是漏跑。
    entries = read_journal(os.path.join(tmpdir, "shared.journal"))
    seen = checks_by_name(entries)
    segment = []
    active = False
    for entry in entries:
        if entry.get("event") == "scenario_start" and entry.get("mode") == "hook_failure":
            active = True
        if active and entry.get("event") == "check":
            segment.append(entry)
    failure_seen = checks_by_name(segment)
    missing = [n for n in REQUIRED_FAILURE_CHECKS if n not in failure_seen]
    failed = [n for n in REQUIRED_FAILURE_CHECKS
              if n in failure_seen and failure_seen[n].get("ok") != "1"]
    before = len(_FAILURES)
    verify_checks(segment, REQUIRED_FAILURE_CHECKS, "hook_failure")
    return {"status": "pass" if not missing and not failed and len(_FAILURES) == before else "fail",
            "checks": len(segment)}


def scenario_price_args(binary, tmpdir):
    print("\n=== price arguments: validation and int-safe boundary ===")
    for tag, value in [("empty", ""), ("nonnumeric", "not-a-price"),
                       ("trailing", "12junk"), ("negative", "-1"),
                       ("int_max", str(2**31 - 1)),
                       ("overflow", "9" * 100)]:
        journal = os.path.join(tmpdir, f"price-{tag}.journal")
        rc, out, err = run_child(
            [binary, "node", "--journal", journal, "--price", value])
        check(rc == 90, f"price.{tag}_rejected", f"rc={rc} stderr={err.strip()}")
        check(not os.path.exists(journal), f"price.{tag}_no_journal",
              "journal_not_opened")

    for tag, price in [("zero", 0), ("upper_bound", (2**31 - 1) // 2)]:
        journal = os.path.join(tmpdir, f"price-{tag}.journal")
        rc, out, err = run_child(
            [binary, "node", "--journal", journal, "--price", str(price)])
        entries = read_journal(journal)
        node = name_or_none([e for e in entries
                             if e.get("event") == "cross_process_node"])
        check(rc == 0 and node is not None and node.get("total") == str(2 * price),
              f"price.{tag}_accepted",
              f"rc={rc} total={node.get('total') if node else 'missing'} stderr={err.strip()}")


def scenario_nodes(binary, tmpdir):
    print("\n=== node: 两个独立进程复用同一业务模块 ===")
    specs = [(700, "a"), (1300, "b")]
    procs = []
    try:
        for price, tag in specs:
            journal = os.path.join(tmpdir, f"node-{tag}.journal")
            procs.append(
                (
                    tag,
                    price,
                    journal,
                    subprocess.Popen(
                        [binary, "node", "--journal", journal,
                         "--price", str(price)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                    ),
                )
            )
        results = []
        deadline = time.monotonic() + CASE_TIMEOUT_S
        for tag, price, journal, proc in procs:
            remaining = max(1.0, deadline - time.monotonic())
            try:
                out, err = proc.communicate(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()
                out, err = proc.communicate()
                check(False, f"node.{tag}.timeout", "killed")
                results.append(None)
                continue
            spool(out, err)
            check(proc.returncode == 0, f"node.{tag}.exit_code",
                  f"rc={proc.returncode}")
            entries = read_journal(journal)
            verify_checks(entries, REQUIRED_NODE_CHECKS, f"node.{tag}")
            nodes = [
                e for e in entries if e.get("event") == "cross_process_node"
            ]
            check(len(nodes) == 1, f"node.{tag}.journal_event",
                  f"events={len(nodes)}")
            results.append((name_or_none(nodes), price, entries))
    finally:
        for _, _, _, proc in procs:
            if proc.poll() is None:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    _FAILURES.append(f"child_not_reaped: pid={proc.pid}")
                    print(f"FAIL child_not_reaped pid={proc.pid}", file=sys.stderr)
    pids = [r[0]["pid"] for r in results if r]
    totals = [int(r[0]["total"]) for r in results if r]
    prices = [r[1] for r in results if r]
    check(len(pids) == 2 and pids[0] != pids[1],
          "node.distinct_pids", f"pids={pids}")
    check(totals == [2 * p for p in prices], "node.independent_totals",
          f"prices={prices} totals={totals}")
    check(all(r[0].get("instance_seq") == "1" for r in results if r),
          "node.independent_instance_seq", "each_process_starts_at_1")
    return {"status": "pass" if not _FAILURES else "fail",
            "pids": pids, "totals": totals}


def name_or_none(nodes):
    return nodes[-1] if nodes else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--results")
    args = ap.parse_args()

    if not os.path.isfile(args.bin):
        print(f"driver: binary not found: {args.bin}", file=sys.stderr)
        return 9

    tmpdir = os.path.join(
        os.environ.get("TMPDIR", "/tmp"), f"component_services.{os.getpid()}"
    )
    os.makedirs(tmpdir, exist_ok=True)

    results = {}
    try:
        results["shared"] = scenario_shared(args.bin, tmpdir)
        results["hook_failure"] = scenario_failure_app(args.bin, tmpdir)
        scenario_price_args(args.bin, tmpdir)
        results["nodes"] = scenario_nodes(args.bin, tmpdir)
    except Exception as exc:  # noqa: BLE001 - 驱动器要给出可判定的失败
        _FAILURES.append(f"driver_exception: {exc}")
        print(f"FAIL driver_exception {exc}", file=sys.stderr)

    ok = not _FAILURES
    payload = {
        "status": "pass" if ok else "fail",
        "failures": _FAILURES,
        "scenarios": results,
    }
    if args.results:
        with open(args.results, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"\ncomponent services acceptance: {'PASS' if ok else 'FAIL'}")
    if not ok:
        for f in _FAILURES:
            print(f"  - {f}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
