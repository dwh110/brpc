#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# urma_trace_summary.py — Parse bpftrace output from urma_uprobe_trace.bt and
# print a per-function summary table with P50/P90/P99/P99.9 percentiles.
#
# Usage:
#   python3 tools/urma_trace_summary.py < /tmp/trace.log
#   python3 tools/urma_trace_summary.py /tmp/trace.log
#
# Compatible with both lhist() (linear 1us buckets) and hist() (log2 buckets).
# Reads @lat, @cnt, @sum, @max, @slow maps from the bpftrace FINAL REPORT.

import re
import sys


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_num(s):
    """Parse a bpftrace hist/lhist bucket boundary with optional K/M suffix.

    bpftrace hist() log2 buckets use K=1024, M=1024*1024 for large values
    (e.g. 256K = 262144). lhist() uses plain integers.
    """
    s = s.strip()
    if s.endswith("K"):
        return int(s[:-1]) * 1024
    if s.endswith("M"):
        return int(s[:-1]) * 1024 * 1024
    return int(s)


def parse_bucket_line(line):
    """Parse one lhist/hist bucket line. Returns (low, high, count) or None.

    Handles these bpftrace bucket formats:
      [low, high)    count   — linear/log2 range bucket (low/high may have K/M suffix)
      [value]        count   — single-value bucket (hist log2, e.g. [0], [1])
      (..., high)    count   — lhist underflow
      [low, ...)     count   — lhist overflow
    The ASCII bar (|@@@@...) is stripped before parsing.
    """
    line = line.split("|")[0].strip()
    # [low, ...) count  — lhist overflow (check BEFORE normal range!)
    m = re.match(r"\[([^\],]+),\s*\.\.\.\)\s+(\d+)", line)
    if m:
        return (parse_num(m.group(1)), float("inf"), int(m.group(2)))
    # (..., high) count  — lhist underflow (treat as [0, high))
    m = re.match(r"\(\.\.\.,\s*([^\)]+)\)\s+(\d+)", line)
    if m:
        return (0, parse_num(m.group(1)), int(m.group(2)))
    # [low, high) count  — normal range bucket
    m = re.match(r"\[([^\],]+),\s*([^\]]+)\)\s+(\d+)", line)
    if m:
        return (parse_num(m.group(1)), parse_num(m.group(2)), int(m.group(3)))
    # [value] count  — single-value bucket (hist log2)
    m = re.match(r"\[([^\]]+)\]\s+(\d+)", line)
    if m:
        v = parse_num(m.group(1))
        return (v, v + 1, int(m.group(2)))
    return None


def parse(data):
    """Parse bpftrace output text.

    Returns (lat_data, cnt, sum_ns, max_ns, slow) where:
      lat_data: {func: [(low, high, count), ...]}
      cnt/sum_ns/max_ns/slow: {func: int}
    """
    lat_data = {}
    cnt = {}
    sum_ns = {}
    max_ns = {}
    slow = {}

    cur_func = None
    cur_buckets = []

    for line in data.split("\n"):
        # @lat[func]:  — start of a latency histogram block
        m = re.match(r"@lat\[([^\]]+)\]:", line)
        if m:
            if cur_func and cur_buckets:
                lat_data[cur_func] = cur_buckets
            cur_func = m.group(1).strip()
            cur_buckets = []
            continue

        # If inside a @lat block, try to parse a bucket line
        if cur_func is not None:
            b = parse_bucket_line(line)
            if b is not None:
                cur_buckets.append(b)
                continue
            # A non-bucket, non-bar line ends the block
            stripped = line.split("|")[0].strip()
            if stripped and not stripped.startswith("["):
                if cur_buckets:
                    lat_data[cur_func] = cur_buckets
                cur_func = None
                cur_buckets = []

        # @cnt[func]: value  (and @sum, @max, @slow — same format)
        for name, target in (
            ("@cnt", cnt),
            ("@sum", sum_ns),
            ("@max", max_ns),
            ("@slow", slow),
        ):
            m = re.match(r"%s\[([^\]]+)\]:\s*(\d+)" % name, line)
            if m:
                target[m.group(1).strip()] = int(m.group(2))

    # Flush the last @lat block
    if cur_func and cur_buckets:
        lat_data[cur_func] = cur_buckets

    return lat_data, cnt, sum_ns, max_ns, slow


# ---------------------------------------------------------------------------
# Percentile computation
# ---------------------------------------------------------------------------


def percentile(buckets, p):
    """Compute the p-th percentile (0..100) via linear interpolation across
    cumulative bucket counts. Returns (value_us, is_overflow).

    For overflow buckets (high == inf), if the percentile falls there, returns
    (low, True) meaning ">= low us".
    """
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
    # Beyond last bucket
    last = buckets[-1] if buckets else (0, 0, 0)
    return float(last[1]), last[1] == float("inf")


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def fmt_lat(val_us, is_overflow=False):
    """Format a latency value in microseconds with smart unit."""
    if is_overflow:
        return f">={val_us:.0f}us"
    if val_us >= 1_000_000:
        return f"{val_us / 1_000_000:.1f}s"
    if val_us >= 1_000:
        return f"{val_us / 1000:.1f}ms"
    if val_us >= 10:
        return f"{val_us:.0f}us"
    if val_us >= 1:
        return f"{val_us:.1f}us"
    return f"{val_us:.2f}us"


def fmt_count(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def fmt_total(ms):
    """Format cumulative time (ms) with smart unit."""
    if ms >= 1_000:
        return f"{ms / 1000:.1f}s"
    if ms >= 1:
        return f"{ms:.0f}ms"
    return f"{ms:.1f}ms"


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
        print(
            "No @lat data found. Is this bpftrace output from urma_uprobe_trace.bt?",
            file=sys.stderr,
        )
        sys.exit(1)

    rows = []
    for func in sorted(lat_data.keys()):
        buckets = lat_data[func]
        calls = sum(c for _, _, c in buckets)
        p50, of50 = percentile(buckets, 50)
        p90, of90 = percentile(buckets, 90)
        p99, of99 = percentile(buckets, 99)
        p999, of999 = percentile(buckets, 99.9)
        mx_us = max_ns.get(func, 0) / 1000.0  # ns -> us
        sw = slow.get(func, 0)
        total_ms = sum_ns.get(func, 0) / 1e6  # ns -> ms
        rows.append(
            (
                func,
                calls,
                p50,
                of50,
                p90,
                of90,
                p99,
                of99,
                p999,
                of999,
                mx_us,
                sw,
                total_ms,
            )
        )

    hdr = (
        f"{'Function':<28} {'Calls':>10} {'P50':>9} {'P90':>9} "
        f"{'P99':>9} {'P99.9':>10} {'Max':>10} {'Slow':>6} {'Total':>10}"
    )
    print(hdr)
    print("-" * len(hdr))

    for (
        func,
        calls,
        p50,
        of50,
        p90,
        of90,
        p99,
        of99,
        p999,
        of999,
        mx_us,
        sw,
        total_ms,
    ) in rows:
        flag = " *" if (sw > 0 or p999 > 1000) else ""
        print(
            f"{func:<28} {fmt_count(calls):>10} "
            f"{fmt_lat(p50, of50):>9} {fmt_lat(p90, of90):>9} "
            f"{fmt_lat(p99, of99):>9} {fmt_lat(p999, of999):>10} "
            f"{fmt_lat(mx_us):>10} {sw:>6} {fmt_total(total_ms):>10}{flag}"
        )

    print()
    print("Units: us=microseconds, ms=milliseconds, s=seconds.")
    print(
        "Calls  = count of latency samples in @lat (sampled subset if SAMPLE_RATE>1)."
    )
    print("Total  = cumulative time from @sum.")
    print("Slow   = count of calls >1ms (@slow).")
    print(
        "*      = function has slow calls or P99.9 > 1ms (investigate warm-up/outliers)."
    )


if __name__ == "__main__":
    main()
