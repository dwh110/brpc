#!/usr/bin/env python
"""Unified E2E latency trace script for URMA io_mode=2 1KB QPS=1000.

Workflow stages (controllable via flags):
  1. upload  - upload modified source files to server (202)
  2. build   - bazel build ub_test_server/client on 202
  3. sync    - copy client binary to 204
  4. test    - start server, run client, capture [URMA-E2E] logs
  5. analyze - parse logs, print 19-stage latency distribution

Usage:
  python run_e2e_trace.py                   # full pipeline (upload+build+sync+test+analyze)
  python run_e2e_trace.py --skip-upload     # skip step 1
  python run_e2e_trace.py --skip-build      # skip steps 1-3 (reuse existing binaries)
  python run_e2e_trace.py --analyze-only    # only analyze local e2e_client_trace.log
  python run_e2e_trace.py --test-only       # only run test + analyze (reuse binaries)

All trace output is gated by a single gflag: --urma_trace_latency=true.
[URMA-E2E] logs are emitted only on the client side (Controller::EndRPC).
"""
import argparse
import contextlib
import io
import os
import re
import sys
import tempfile
import time

import paramiko

# --- Environment config ---
SERVER_HOST = '141.61.17.202'
CLIENT_HOST = '141.61.17.204'
USER = 'd00836578'
PASS = 'dwh123456'
PORT = 10918

REMOTE_BRPC = '/home/d00836578/brpc_workspace/brpc_urma/brpc'
REMOTE_CLIENT = '/home/d00836578/brpc_workspace/brpc_urma/ub_test_client'
SERVER_BIN = f'{REMOTE_BRPC}/bazel-bin/example/ub_test_server'
CLIENT_BIN_REMOTE = f'{REMOTE_BRPC}/bazel-bin/example/ub_test_client'

# Source files to upload (local relative path -> remote absolute path)
FILES_TO_UPLOAD = [
    ('src/brpc/controller.cpp',                     f'{REMOTE_BRPC}/src/brpc/controller.cpp'),
    ('src/brpc/controller.h',                       f'{REMOTE_BRPC}/src/brpc/controller.h'),
    ('src/brpc/input_message_base.h',               f'{REMOTE_BRPC}/src/brpc/input_message_base.h'),
    ('src/brpc/input_messenger.cpp',                f'{REMOTE_BRPC}/src/brpc/input_messenger.cpp'),
    ('src/brpc/input_messenger.h',                  f'{REMOTE_BRPC}/src/brpc/input_messenger.h'),
    ('src/brpc/transport.h',                        f'{REMOTE_BRPC}/src/brpc/transport.h'),
    ('src/brpc/channel.cpp',                        f'{REMOTE_BRPC}/src/brpc/channel.cpp'),
    ('src/brpc/policy/baidu_rpc_protocol.cpp',      f'{REMOTE_BRPC}/src/brpc/policy/baidu_rpc_protocol.cpp'),
    ('src/brpc/urma/urma_endpoint.cpp',             f'{REMOTE_BRPC}/src/brpc/urma/urma_endpoint.cpp'),
    ('src/brpc/urma/urma_endpoint.h',               f'{REMOTE_BRPC}/src/brpc/urma/urma_endpoint.h'),
    ('src/brpc/urma/urma_helper.cpp',               f'{REMOTE_BRPC}/src/brpc/urma/urma_helper.cpp'),
    ('src/brpc/urma/urma_one_sided.h',              f'{REMOTE_BRPC}/src/brpc/urma/urma_one_sided.h'),
    ('example/BUILD.bazel',                         f'{REMOTE_BRPC}/example/BUILD.bazel'),
    ('BUILD.bazel',                                 f'{REMOTE_BRPC}/BUILD.bazel'),
    ('bazel/config/BUILD.bazel',                    f'{REMOTE_BRPC}/bazel/config/BUILD.bazel'),
]

BUILD_CMD = (
    'cd {brpc} && '
    'export http_proxy="http://141.1.37.126:7777" && '
    'export https_proxy="http://141.1.37.126:7777" && '
    'export CPLUS_INCLUDE_PATH=//usr/include/ub/:/usr/include/ub/umdk/:'
    '//usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && '
    'bazel build //example:ub_test_server //example:ub_test_client '
    '--define=BRPC_WITH_URMA=true --define=BRPC_E2E_TRACE=true '
    '--repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0 -c opt '
    '--define=BUTIL_USE_CPU_FREQUENCY=true 2>&1'
).format(brpc=REMOTE_BRPC)

# Test parameters
IO_MODE = 2
REQ_SIZE = 1024
RSP_SIZE = 1024
EXPECTED_QPS = 1000
TEST_SECONDS = 5
SEND_BUF_KB = 2048
RECV_BUF_KB = 2048
NUM_THREADS = 16

# Project root is one level up from this script's directory (tracepoint/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Output log file
LOCAL_LOG = os.path.join(SCRIPT_DIR, 'output', 'e2e_client_trace.log')
LOCAL_ANALYSIS = os.path.join(SCRIPT_DIR, 'output', 'e2e_analysis.txt')

# All 19 stages (matching Controller::LogAndStatE2E output)
ALL_STAGES = [
    'c_ser', 'c_queue', 'c_post', 'uplink',
    's_event', 's_cq', 's_msg', 's_bthread',
    's_deser', 's_svc', 's_ser', 's_queue', 's_post',
    'downlink',
    'c_event', 'c_cq', 'c_msg', 'c_bthread',
    'c_deser', 'c_done',
]
CLIENT_LOCAL_STAGES = [
    'c_ser', 'c_queue', 'c_post',
    'c_event', 'c_cq', 'c_msg', 'c_bthread', 'c_deser', 'c_done',
]
WARMUP_SKIP = 100


class Tee:
    """Redirect stdout to both original stdout and a buffer."""
    def __init__(self, *streams):
        self.streams = streams
    def write(self, data):
        for s in self.streams:
            s.write(data)
    def flush(self):
        for s in self.streams:
            s.flush()


# --- SSH helpers ---
def ssh_exec(host, cmd, timeout=300):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=USER, password=PASS, timeout=10)
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode()
    err = stderr.read().decode()
    code = stdout.channel.recv_exit_status()
    ssh.close()
    return code, out, err


def upload_file(host, local, remote):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=USER, password=PASS, timeout=10)
    sftp = ssh.open_sftp()
    sftp.put(local, remote)
    sftp.close()
    ssh.close()


def scp_between_hosts(src_host, src_path, dst_host, dst_path):
    """Relay a file from src_host to dst_host via local temp."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(src_host, username=USER, password=PASS, timeout=10)
    sftp = ssh.open_sftp()
    with sftp.file(src_path, 'rb') as f:
        data = f.read()
    sftp.close()
    ssh.close()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(dst_host, username=USER, password=PASS, timeout=10)
    sftp = ssh.open_sftp()
    with sftp.file(dst_path + '.tmp', 'wb') as f:
        f.write(data)
    sftp.close()
    ssh.exec_command(f'mv {dst_path}.tmp {dst_path} && chmod +x {dst_path}')
    ssh.close()


# --- Stage 1: upload ---
def stage_upload():
    print("=" * 60)
    print("Stage 1: Upload source files to 202")
    print("=" * 60)
    local_base = PROJECT_ROOT
    for local_rel, remote_abs in FILES_TO_UPLOAD:
        local_abs = os.path.join(local_base, local_rel.replace('/', os.sep))
        if not os.path.exists(local_abs):
            print(f"  SKIP (not found): {local_rel}")
            continue
        upload_file(SERVER_HOST, local_abs, remote_abs)
        print(f"  Uploaded: {local_rel}")
    print("Upload complete.")


# --- Stage 2: build ---
def stage_build():
    print("\n" + "=" * 60)
    print("Stage 2: Build on 202")
    print("=" * 60)
    code, out, err = ssh_exec(SERVER_HOST, f'{BUILD_CMD} | tail -30', timeout=600)
    print(out[-2000:] if len(out) > 2000 else out)
    if err:
        print("STDERR:", err[-1000:])
    if code != 0:
        print(f"BUILD FAILED (code={code})")
        sys.exit(1)
    print("Build succeeded.")


# --- Stage 3: sync client binary ---
def stage_sync():
    print("\n" + "=" * 60)
    print("Stage 3: Copy client binary to 204")
    print("=" * 60)
    scp_between_hosts(SERVER_HOST, CLIENT_BIN_REMOTE, CLIENT_HOST, REMOTE_CLIENT)
    print(f"Client copied to {CLIENT_HOST}:{REMOTE_CLIENT}")


# --- Stage 4: test ---
def stage_test():
    print("\n" + "=" * 60)
    print(f"Stage 4: Run test (io_mode={IO_MODE}, qps={EXPECTED_QPS}, size={REQ_SIZE})")
    print("=" * 60)

    # Kill existing server
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null; sleep 1', timeout=10)
    ssh_exec(SERVER_HOST, '> /tmp/ub_test_server.log', timeout=5)

    # Start server
    srv_cmd = (
        f'nohup numactl -C 96-111 -m 1 {SERVER_BIN} '
        f'--port {PORT} --use_urma=true --rsp_size={RSP_SIZE} '
        f'--num_threads={NUM_THREADS} --urma_io_mode={IO_MODE} '
        f'--urma_max_sge_len=65536 '
        f'--urma_send_buf_size={SEND_BUF_KB} '
        f'--urma_recv_buf_size={RECV_BUF_KB} '
        f'--urma_trace_latency=true '
        f'> /tmp/ub_test_server.log 2>&1 &'
    )
    ssh_exec(SERVER_HOST, srv_cmd, timeout=10)
    time.sleep(3)

    code, pid_out, _ = ssh_exec(SERVER_HOST, f"pgrep -f 'ub_test_server.*--port {PORT}'", timeout=10)
    if not pid_out.strip():
        print("ERROR: Server failed to start")
        _, log, _ = ssh_exec(SERVER_HOST, 'cat /tmp/ub_test_server.log', timeout=10)
        print(log[:1000])
        sys.exit(1)
    print(f"Server started (pid={pid_out.split()[0]})")

    # Run client
    cli_cmd = (
        f'numactl -C 96-111 -m 1 {REMOTE_CLIENT} '
        f'--servers={SERVER_HOST}:{PORT} '
        f'--use_urma=true --urma_io_mode={IO_MODE} '
        f'--urma_max_sge_len=65536 '
        f'--urma_send_buf_size={SEND_BUF_KB} '
        f'--urma_recv_buf_size={RECV_BUF_KB} '
        f'--rpc_timeout_ms=2000 --connect_timeout_ms=6000 '
        f'--test_seconds={TEST_SECONDS} --max_retry=10 --queue_depth=10 '
        f'--req_size={REQ_SIZE} --dummy_port=0 '
        f'--expected_qps={EXPECTED_QPS} --initial_tokens=0 '
        f'--urma_trace_latency=true 2>&1 | tee /tmp/e2e_client.log'
    )
    print(f"Running client ({TEST_SECONDS}s, qps={EXPECTED_QPS}, {REQ_SIZE}B)...")
    code, cli_out, _ = ssh_exec(CLIENT_HOST, cli_cmd, timeout=60)

    for line in cli_out.split('\n'):
        if 'Avg-Latency:' in line:
            print("RESULT:", line.strip())
            break

    # Kill server
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null', timeout=10)

    # Fetch [URMA-E2E] logs from client
    code, logs, _ = ssh_exec(CLIENT_HOST, 'grep -h "URMA-E2E" /tmp/e2e_client.log', timeout=30)
    lines = logs.strip().split('\n') if logs.strip() else []
    print(f"Client [URMA-E2E] lines: {len(lines)}")

    # Save locally
    with open(LOCAL_LOG, 'w') as f:
        f.write(logs)
    print(f"Logs saved to {LOCAL_LOG}")

    return lines


# --- Stage 5: analyze ---
def parse_line(line):
    m = re.search(r'\[URMA-E2E\]\s*(.*)', line)
    if not m:
        return None
    body = m.group(1)
    fields = {}
    for kv in re.finditer(r'(\w+)=(\-?\d+)', body):
        fields[kv.group(1)] = int(kv.group(2))
    return fields


def stats(vals, name):
    if not vals:
        print(f"  {name:<14} (no data)")
        return None
    vs = sorted(vals)
    n = len(vs)
    avg = sum(vs) / n
    p50 = vs[n // 2]
    p99 = vs[int(n * 0.99)] if n > 100 else vs[-1]
    filtered = [v for v in vs if v < 10000]
    if filtered and len(filtered) > n * 0.9:
        favg = sum(filtered) / len(filtered)
        fp50 = sorted(filtered)[len(filtered) // 2]
        fp99 = sorted(filtered)[int(len(filtered) * 0.99)] if len(filtered) > 100 else sorted(filtered)[-1]
        print(f"  {name:<14} avg={avg:>8.1f}  p50={p50:>6}  p99={p99:>6}  "
              f"min={vs[0]:>5}  max={vs[-1]:>6}  "
              f"[filtered avg={favg:.1f} p50={fp50} p99={fp99} n={len(filtered)}]")
    else:
        print(f"  {name:<14} avg={avg:>8.1f}  p50={p50:>6}  p99={p99:>6}  "
              f"min={vs[0]:>5}  max={vs[-1]:>6}")
    return avg


def print_waterfall(records):
    """Print a waterfall chart showing avg latency per stage in RPC execution order.

    Each stage is drawn as a horizontal bar proportional to its avg duration.
    Stages with 0 or unreliable data (uplink/downlink due to clock skew, or
    server stages not transmitted via RpcMeta) are marked but not drawn.
    """
    # Stage display order with human-readable labels and group separators
    waterfall_stages = [
        # Client send path
        ('c_ser',    'Client serialize',     'client'),
        ('c_queue',  'Client queue',         'client'),
        ('c_post',   'Client URMA post',     'client'),
        # Network + server (residual, not individually measurable)
        ('uplink',   'Uplink (net)',         'network'),
        ('s_event',  'Server event',         'server'),
        ('s_cq',     'Server CQ drain',      'server'),
        ('s_msg',    'Server msg recv',      'server'),
        ('s_bthread','Server bthread',       'server'),
        ('s_deser',  'Server deser',         'server'),
        ('s_svc',    'Server service',       'server'),
        ('s_ser',    'Server serialize',     'server'),
        ('s_queue',  'Server queue',         'server'),
        ('s_post',   'Server URMA post',     'server'),
        ('downlink', 'Downlink (net)',       'network'),
        # Client recv path
        ('c_event',  'Client event',         'client'),
        ('c_cq',     'Client CQ drain',      'client'),
        ('c_msg',    'Client msg recv',      'client'),
        ('c_bthread','Client bthread',       'client'),
        ('c_deser',  'Client deser',         'client'),
        ('c_done',   'Client done',          'client'),
    ]

    # Compute avg for each stage
    stage_avgs = {}
    for stage_key, _, _ in waterfall_stages:
        vals = [r[stage_key] for r in records if stage_key in r and r[stage_key] >= 0]
        if vals:
            stage_avgs[stage_key] = sum(vals) / len(vals)
        else:
            stage_avgs[stage_key] = None

    avg_total = sum(r['total'] for r in records) / len(records)

    # Network residual = total - sum(client local stages)
    client_sum = sum(stage_avgs.get(s, 0) or 0 for s in CLIENT_LOCAL_STAGES)
    network_residual = avg_total - client_sum if avg_total > client_sum else 0

    # Bar scale: max bar width = 50 chars
    MAX_BAR_WIDTH = 50
    max_stage_val = max(
        (v for v in stage_avgs.values() if v is not None and 0 < v < 10000),
        default=1
    )
    # Include network residual in scale consideration
    max_stage_val = max(max_stage_val, network_residual)

    print("\n" + "=" * 80)
    print("WATERFALL CHART (avg latency per stage, in RPC execution order)")
    print("=" * 80)
    print(f"  Total avg: {avg_total:.1f} us  |  bar scale: {max_stage_val:.1f} us")
    print()

    group_colors = {'client': '', 'server': '', 'network': ''}
    group_labels = {
        'client': 'CLIENT',
        'server': 'SERVER',
        'network': 'NET  ',
    }

    prev_group = None
    for stage_key, label, group in waterfall_stages:
        # Print group separator
        if group != prev_group:
            if prev_group is not None:
                print()
            prev_group = group

        avg = stage_avgs[stage_key]

        # Skip unreliable stages (clock-skewed uplink/downlink, untransmitted server)
        if stage_key in ('uplink', 'downlink'):
            print(f"  {group_labels[group]} {label:<22} ~{network_residual:>6.1f} us  "
                  f"[network residual, clock-skewed]")
            continue

        if avg is None:
            print(f"  {group_labels[group]} {label:<22} {'---':>7}     [no data]")
            continue

        if avg >= 10000:
            print(f"  {group_labels[group]} {label:<22} {avg:>7.1f} us  [unreliable]")
            continue

        if avg == 0 and group == 'server':
            print(f"  {group_labels[group]} {label:<22} {'0.0':>7} us  [not transmitted]")
            continue

        bar_width = int(avg / max_stage_val * MAX_BAR_WIDTH) if max_stage_val > 0 else 0
        bar_width = max(bar_width, 1)  # at least 1 char for visible stages
        bar = '#' * bar_width
        pct = 100 * avg / avg_total if avg_total > 0 else 0
        print(f"  {group_labels[group]} {label:<22} {avg:>7.1f} us  {bar:<{MAX_BAR_WIDTH}}  ({pct:>4.1f}%)")

    # Network residual summary line
    print()
    net_bar_width = int(network_residual / max_stage_val * MAX_BAR_WIDTH) if max_stage_val > 0 else 0
    net_bar = '=' * net_bar_width
    net_pct = 100 * network_residual / avg_total if avg_total > 0 else 0
    print(f"  {'NET  '} {'Network (residual)':<22} {network_residual:>7.1f} us  {net_bar:<{MAX_BAR_WIDTH}}  ({net_pct:>4.1f}%)")
    print(f"  {'NET  '} {'Client local sum':<22} {client_sum:>7.1f} us")
    print(f"  {'NET  '} {'Total':<22} {avg_total:>7.1f} us")


def stage_analyze(lines=None):
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = Tee(old_stdout, buf)
    try:
        _stage_analyze_impl(lines)
    finally:
        sys.stdout = old_stdout
        analysis_text = buf.getvalue()
        buf.close()
    os.makedirs(os.path.dirname(LOCAL_ANALYSIS), exist_ok=True)
    with open(LOCAL_ANALYSIS, 'w') as f:
        f.write(analysis_text)
    print(f"Analysis saved to {LOCAL_ANALYSIS}")


def _stage_analyze_impl(lines):
    print("\n" + "=" * 60)
    print("Stage 5: Analyze E2E trace")
    print("=" * 60)

    if lines is None:
        if not os.path.exists(LOCAL_LOG):
            print(f"ERROR: {LOCAL_LOG} not found. Run with --test-only or full pipeline first.")
            sys.exit(1)
        with open(LOCAL_LOG, 'r') as f:
            lines = f.readlines()

    records = []
    for line in lines:
        f = parse_line(line)
        if f and f.get('total', 0) > 0:
            records.append(f)

    if len(records) <= WARMUP_SKIP:
        print(f"Only {len(records)} records, insufficient for warmup skip ({WARMUP_SKIP}).")
        if not records:
            print("No valid records. Sample lines:")
            for line in lines[:5]:
                print("  RAW:", line.strip())
            return

    records = records[WARMUP_SKIP:]
    print(f"Records (after warmup skip {WARMUP_SKIP}): {len(records)}")

    # --- Client-side stages (monotonic, reliable) ---
    print("\n" + "-" * 60)
    print("CLIENT-SIDE STAGES (monotonic)")
    print("-" * 60)
    for stage in CLIENT_LOCAL_STAGES:
        vals = [r[stage] for r in records if stage in r and r[stage] >= 0]
        stats(vals, stage)

    totals = [r['total'] for r in records if r.get('total', 0) > 0]
    stats(totals, 'total')

    # --- Network residual ---
    print("\n" + "-" * 60)
    print("NETWORK LATENCY (residual = total - sum(client stages))")
    print("-" * 60)
    residuals = []
    for r in records:
        client_sum = sum(r.get(s, 0) for s in CLIENT_LOCAL_STAGES)
        residuals.append(r['total'] - client_sum)
    stats(residuals, 'network')

    # --- All 19 stages (includes server-side from RpcMeta user_fields) ---
    print("\n" + "-" * 60)
    print("ALL 19 STAGES (server stages may be 0 if not transmitted)")
    print("-" * 60)
    print(f"  {'Stage':<14} {'avg(us)':<10} {'p50(us)':<10} {'p99(us)':<10} {'min':<6} {'max':<6}")
    print("  " + "-" * 58)
    for stage in ALL_STAGES + ['total']:
        vals = sorted([r[stage] for r in records if stage in r and r[stage] >= 0])
        if not vals:
            print(f"  {stage:<14} (no data)")
            continue
        avg = sum(vals) / len(vals)
        p50 = vals[len(vals) // 2]
        p99 = vals[int(len(vals) * 0.99)] if len(vals) > 100 else vals[-1]
        print(f"  {stage:<14} {avg:<10.1f} {p50:<10} {p99:<10} {vals[0]:<6} {vals[-1]:<6}")

    # --- Breakdown ---
    avg_client = sum(sum(r.get(s, 0) for s in CLIENT_LOCAL_STAGES) for r in records) / len(records)
    avg_network = sum(residuals) / len(residuals) if residuals else 0
    avg_total = sum(totals) / len(totals) if totals else 0
    print("\n" + "-" * 60)
    print("BREAKDOWN (averages)")
    print("-" * 60)
    print(f"  Client local: {avg_client:.1f} us ({100*avg_client/avg_total:.0f}%)")
    print(f"  Network:      {avg_network:.1f} us ({100*avg_network/avg_total:.0f}%)")
    print(f"  Total:        {avg_total:.1f} us")

    # --- Total latency distribution ---
    print("\n" + "-" * 60)
    print("TOTAL LATENCY DISTRIBUTION")
    print("-" * 60)
    ts = sorted(totals)
    n = len(ts)
    for pct in [50, 75, 90, 95, 99, 99.9, 99.99]:
        idx = min(int(n * pct / 100), n - 1)
        print(f"  p{pct}: {ts[idx]} us")
    print(f"  avg: {sum(ts)/n:.1f} us")
    print(f"  min: {ts[0]} us")
    print(f"  max: {ts[-1]} us")

    # --- Sample records ---
    print("\n" + "-" * 60)
    print("SAMPLE RECORDS (first 5)")
    print("-" * 60)
    for i, r in enumerate(records[:5]):
        parts = [f"{s}={r.get(s, 0)}" for s in ALL_STAGES + ['total']]
        print(f"  [{i}] " + " ".join(parts))

    # --- Waterfall chart ---
    print_waterfall(records)


# --- Main ---
def main():
    parser = argparse.ArgumentParser(description='Unified E2E URMA latency trace')
    parser.add_argument('--skip-upload', action='store_true', help='Skip source upload stage')
    parser.add_argument('--skip-build', action='store_true', help='Skip upload+build+sync (reuse binaries)')
    parser.add_argument('--test-only', action='store_true', help='Only run test + analyze')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze local e2e_client_trace.log')
    args = parser.parse_args()

    if args.analyze_only:
        stage_analyze()
        return

    if args.test_only or args.skip_build:
        lines = stage_test()
        stage_analyze(lines)
        return

    if not args.skip_upload:
        stage_upload()
    else:
        print("Skipping upload stage.")

    stage_build()
    stage_sync()
    lines = stage_test()
    stage_analyze(lines)


if __name__ == '__main__':
    main()
