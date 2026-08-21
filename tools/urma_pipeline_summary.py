#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# urma_pipeline_summary.py — Parse bpftrace output from urma_pipeline_trace.sh
# and print a hierarchical pipeline breakdown similar to distributed-trace views.
#
# Usage:
#   python3 tools/urma_pipeline_summary.py < /tmp/trace.log
#   python3 tools/urma_pipeline_summary.py /tmp/trace.log
#
# Computes:
#   - Per-stage avg/p50/p90/p99 from @lat lhist
#   - Exclusive (self) time = total − sum(children)
#   - Tree-like hierarchical display

import re
import sys

# ---------------------------------------------------------------------------
# Pipeline definition — known call hierarchy from brpc + URMA source.
#
# Each entry: (display_name, function_name_in_bpftrace, parent_display)
# parent_display=None → top-level node.
#
# The function_name must match the string used in @lat["name"] in the .bt file.
# ---------------------------------------------------------------------------
PIPELINE = [
    # --- Client side ---
    ("Client总时长", "1.CallMethod", None),
    ("  编码(发送前)", "2.PackRpcRequest", "Client总时长"),
    ("  写socket", "3.SocketWrite", "Client总时长"),
    ("    URMA发送", "4.CutFromIOBufList", "  写socket"),
    ("  等待对端(网络+服务端)", None, "Client总时长"),  # exclusive
    ("  读取+解码", "10.ProcessRpcResponse", "Client总时长"),
    # --- Server side ---
    ("Server总时长", "8.ProcessRpcRequest", None),
    ("  URMA收包", "14.urma_poll_jfc", "Server总时长"),
    ("  消息分发", "5.DispatchReceivedBytes", "Server总时长"),
    ("  解码", "7.ParseRpcMessage", "Server总时长"),
    ("  编码应答+发送", "9.SendRpcResponse", "Server总时长"),
    ("    URMA发送", "4.CutFromIOBufList", "  编码应答+发送"),
    ("      post_send", "11.urma_post_jetty_send_wr", "    URMA发送"),
    # --- URMA transport (shared, shown for reference) ---
    ("URMA发送(post)", "11.urma_post_jetty_send_wr", None),
    ("URMA轮询(poll)", "14.urma_poll_jfc", None),
    ("URMA等待(wait)", "16.urma_wait_jfc", None),
    ("URMA重武装(rearm)", "15.urma_rearm_jfc", None),
    ("URMA应答(ack)", "17.urma_ack_jfc", None),
    ("URMA接收post(post_jfr)", "13.urma_post_jfr_wr", None),
    ("URMA接收(recv)", "12.urma_post_jetty_recv_wr", None),
]


# ---------------------------------------------------------------------------
# Parsing (reused from urma_trace_summary.py logic)
# ---------------------------------------------------------------------------


def parse_num(s):
    s = s.strip()
    if s.endswith("K"):
        return int(s[:-1]) * 1024
    if s.endswith("M"):
        return int(s[:-1]) * 1024 * 1024
    return int(s)


def parse_bucket_line(line):
    line = line.split("|")[0].strip()
    m = re.match(r"\[([^\],]+),\s*\.\.\.\)\s+(\d+)", line)
    if m:
        return (parse_num(m.group(1)), float("inf"), int(m.group(2)))
    m = re.match(r"\(\.\.\.,\s*([^\)]+)\)\s+(\d+)", line)
    if m:
        return (0, parse_num(m.group(1)), int(m.group(2)))
    m = re.match(r"\[([^\],]+),\s*([^\]]+)\)\s+(\d+)", line)
    if m:
        return (parse_num(m.group(1)), parse_num(m.group(2)), int(m.group(3)))
    m = re.match(r"\[([^\]]+)\]\s+(\d+)", line)
    if m:
        v = parse_num(m.group(1))
        return (v, v + 1, int(m.group(2)))
    return None


def parse(data):
    lat_data = {}
    cnt = {}
    sum_ns = {}
    max_ns = {}
    slow = {}

    cur_func = None
    cur_buckets = []

    for line in data.split("\n"):
        m = re.match(r"@lat\[([^\]]+)\]:", line)
        if m:
            if cur_func and cur_buckets:
                lat_data[cur_func] = cur_buckets
            cur_func = m.group(1).strip()
            cur_buckets = []
            continue

        if cur_func is not None:
            b = parse_bucket_line(line)
            if b is not None:
                cur_buckets.append(b)
                continue
            stripped = line.split("|")[0].strip()
            if stripped and not stripped.startswith("["):
                if cur_buckets:
                    lat_data[cur_func] = cur_buckets
                cur_func = None
                cur_buckets = []

        for name, target in (
            ("@cnt", cnt),
            ("@sum", sum_ns),
            ("@max", max_ns),
            ("@slow", slow),
        ):
            m = re.match(r"%s\[([^\]]+)\]:\s*(\d+)" % name, line)
            if m:
                target[m.group(1).strip()] = int(m.group(2))

    if cur_func and cur_buckets:
        lat_data[cur_func] = cur_buckets

    return lat_data, cnt, sum_ns, max_ns, slow


# ---------------------------------------------------------------------------
# Percentile
# ---------------------------------------------------------------------------


def percentile(buckets, p):
    total = sum(c for _, _, c in buckets)
    if total == 0:
        return 0.0, False
    target = p / 100.0 * total
    cum = 0
    for low, high, count in buckets:
        if cum + count >= target:
            if count == 0:
                return float(low), False
            if high == float("inf"):
                return float(low), True
            frac = (target - cum) / count
            return low + frac * (high - low), False
        cum += count
    last = buckets[-1] if buckets else (0, 0, 0)
    return float(last[1]), last[1] == float("inf")


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def fmt_us(val, is_overflow=False):
    if is_overflow:
        return f">={val:.0f}us"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}s"
    if val >= 1_000:
        return f"{val / 1000:.1f}ms"
    if val >= 10:
        return f"{val:.0f}us"
    if val >= 1:
        return f"{val:.1f}us"
    return f"{val:.2f}us"


def fmt_count(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


# ---------------------------------------------------------------------------
# Build tree and compute exclusive times
# ---------------------------------------------------------------------------


class Stage:
    def __init__(self, name, func_name):
        self.name = name
        self.func_name = func_name
        self.children = []
        self.parent = None
        self.calls = 0
        self.avg_us = 0.0
        self.p50 = 0.0
        self.p90 = 0.0
        self.p99 = 0.0
        self.max_us = 0.0
        self.sum_ns = 0
        self.slow = 0
        self.exclusive_us = 0.0  # computed later
        self.is_exclusive = func_name is None  # synthetic stage


def build_pipeline(lat_data, cnt, sum_ns, max_ns, slow):
    """Build pipeline tree from parsed data. Returns list of root stages."""
    stages = {}
    roots = []

    # Create stage objects
    for display, func_name, parent_name in PIPELINE:
        s = Stage(display, func_name)
        stages[display] = s

        if func_name and func_name in lat_data:
            buckets = lat_data[func_name]
            s.calls = sum(c for _, _, c in buckets)
            p50, _ = percentile(buckets, 50)
            p90, _ = percentile(buckets, 90)
            p99, _ = percentile(buckets, 99)
            s.p50 = p50
            s.p90 = p90
            s.p99 = p99
            s.sum_ns = sum_ns.get(func_name, 0)
            s.avg_us = s.sum_ns / 1000.0 / s.calls if s.calls > 0 else 0.0
            s.max_us = max_ns.get(func_name, 0) / 1000.0
            s.slow = slow.get(func_name, 0)

        # Link to parent
        if parent_name and parent_name in stages:
            s.parent = stages[parent_name]
            stages[parent_name].children.append(s)
        else:
            roots.append(s)

    # Compute exclusive times (post-order)
    def compute_exclusive(s):
        child_sum = 0.0
        for c in s.children:
            compute_exclusive(c)
            child_sum += c.avg_us
        if s.is_exclusive:
            # Synthetic stage: exclusive = parent.avg - sum(siblings with data)
            if s.parent:
                sib_sum = sum(
                    c.avg_us for c in s.parent.children if c != s and not c.is_exclusive
                )
                s.avg_us = max(0.0, s.parent.avg_us - sib_sum)
                # Estimate p50/p99 as proportion of parent
                if s.parent.calls > 0:
                    s.calls = s.parent.calls
                    s.p50 = max(0.0, s.parent.p50 - sib_sum)
                    s.p90 = max(0.0, s.parent.p90 - sib_sum)
                    s.p99 = max(0.0, s.parent.p99 - sib_sum)
        else:
            s.exclusive_us = max(0.0, s.avg_us - child_sum)

    for r in roots:
        compute_exclusive(r)

    return roots


# ---------------------------------------------------------------------------
# Print tree
# ---------------------------------------------------------------------------


def print_tree(roots):
    for root in roots:
        if root.calls == 0 and not root.is_exclusive:
            continue
        print_tree_node(root, "")
        print()


def print_tree_node(node, indent):
    """Print a node and its children recursively."""
    prefix = indent
    if node.is_exclusive:
        # Synthetic "exclusive" stage
        star = " ★" if node.avg_us > 1000 else ""
        print(
            f"{prefix}{node.name:<24} avg={fmt_us(node.avg_us):>10} "
            f"p50={fmt_us(node.p50):>10} p90={fmt_us(node.p90):>10} "
            f"p99={fmt_us(node.p99):>10} (n={fmt_count(node.calls)}){star}"
        )
    else:
        star = " ★" if node.slow > 0 or node.p99 > 1000 else ""
        excl = ""
        if node.children and node.exclusive_us > 0:
            excl = f"  [self={fmt_us(node.exclusive_us)}]"
        print(
            f"{prefix}{node.name:<24} avg={fmt_us(node.avg_us):>10} "
            f"p50={fmt_us(node.p50):>10} p90={fmt_us(node.p90):>10} "
            f"p99={fmt_us(node.p99):>10} (n={fmt_count(node.calls)}){star}{excl}"
        )

    for child in node.children:
        print_tree_node(child, indent + "  ")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            data = f.read()
    else:
        data = sys.stdin.read()

    lat_data, cnt, sum_ns, max_ns, slow = parse(data)

    if not lat_data:
        print("No @lat data found. Is this bpftrace pipeline output?", file=sys.stderr)
        sys.exit(1)

    roots = build_pipeline(lat_data, cnt, sum_ns, max_ns, slow)

    print("── Pipeline Latency Breakdown ──")
    print("(★ = slow calls or P99>1ms; [self] = exclusive time)\n")
    print_tree(roots)

    # Print raw function table for reference
    print("\n── Raw Function Table ──\n")
    hdr = (
        f"{'Function':<28} {'Calls':>10} {'Avg':>10} {'P50':>10} "
        f"{'P90':>10} {'P99':>10} {'Max':>10} {'Slow':>6}"
    )
    print(hdr)
    print("-" * len(hdr))
    for func in sorted(lat_data.keys()):
        buckets = lat_data[func]
        calls = sum(c for _, _, c in buckets)
        p50, _ = percentile(buckets, 50)
        p90, _ = percentile(buckets, 90)
        p99, _ = percentile(buckets, 99)
        avg = sum_ns.get(func, 0) / 1000.0 / calls if calls > 0 else 0.0
        mx = max_ns.get(func, 0) / 1000.0
        sw = slow.get(func, 0)
        flag = " *" if (sw > 0 or p99 > 1000) else ""
        print(
            f"{func:<28} {fmt_count(calls):>10} {fmt_us(avg):>10} "
            f"{fmt_us(p50):>10} {fmt_us(p90):>10} {fmt_us(p99):>10} "
            f"{fmt_us(mx):>10} {sw:>6}{flag}"
        )


if __name__ == "__main__":
    main()
