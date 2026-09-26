#!/usr/bin/env python
"""Stage 1 verification: inline_threshold 2048→8192 + read_jetty_sq 256→1024.

Expected effects:
  - 4K/8K: WriteZeroCopy (two-segment RTT) → WriteInline (single WRITE_IMM)
    Expected -10~14us, 4K should beat UBS v2 (20us)
  - 8M qps=1000: SQ pressure reduced (1024 vs 512), expected stable < 4005us
"""
import paramiko
import time
import os
import re

SERVER_HOST = '141.61.17.202'
CLIENT_HOST = '141.61.17.204'
USER = 'd00836578'
PASS = 'dwh123456'
PORT = 10918

SERVER_BIN = '/home/d00836578/brpc_workspace/brpc_urma/brpc/bazel-bin/example/ub_test_server'
CLIENT_BIN = '/home/d00836578/brpc_workspace/brpc_urma/ub_test_client'

DEVICE = 'udmac0d1e2'

# 64KB buffer: 8MB/64KB = 128 blocks
BUFFER_SIZE = 65536
BUFFER_COUNT = 8192
SEND_BUF_KB = 8192
RECV_BUF_KB = 8192
CHUNK_PAYLOAD = 2095104
SQ_SIZE = 1024
# Stage1 方案 G: 512→1024，降低 8M qps=1000 SQ 窗口压力
READ_JETTY_SQ = 1024

# inline_threshold 不传参，使用代码新默认值 8192（方案 A）

QPS_VALUES = [1000, 500]
SIZES = [1024, 4096, 8192, 102400, 204800, 1048576, 8388608]

RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'stage1_results.csv')

CSV_HEADER = ("pass,urma_io_mode,expected_qps,size,Avg-Latency,50th-Latency,"
              "90th-Latency,99th-Latency,99.9th-Latency,99.99th-Latency,"
              "Max-Latency,Throughput,QPS,Server_CPU_avg,Server_CPU_max,"
              "Client_CPU_avg,Client_CPU_max,Client_Mem_avg,Client_Mem_max,"
              "Error_rate,Total_Sent,Total_Errors")

# UBS v2 3次重复均值 (from doc 13.4.1 / 13.4.2)
UBS_V2 = {
    500: {1024: 16.33, 4096: 20.00, 8192: 28.67, 102400: 48.67,
          204800: 75.00, 1048576: 356.67, 8388608: 3153.33},
    1000: {1024: 17.00, 4096: 20.00, 8192: 29.00, 102400: 48.67,
           204800: 75.67, 1048576: 359.00, 8388608: 4005.00},
}

# Stage1 前的 baseline (from doc 14.2)
BASELINE = {
    500: {1024: 20, 4096: 33, 8192: 33, 102400: 54,
          204800: 78, 1048576: 362, 8388608: 3278},
    1000: {1024: 19, 4096: 32, 8192: 33, 102400: 54,
           204800: 79, 1048576: 357, 8388608: 4181},
}


def ssh_connect(host):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=USER, password=PASS, timeout=30)
    return ssh


def ssh_exec(host, cmd, timeout=120):
    ssh = ssh_connect(host)
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    stdout.channel.set_combine_stderr(True)
    out = stdout.read().decode()
    code = stdout.channel.recv_exit_status()
    ssh.close()
    return code, out


def kill_server():
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null; sleep 1', timeout=15)


def start_server():
    kill_server()
    srv_cmd = (
        'nohup numactl -C 96-111 -m 1 ' + SERVER_BIN + ' '
        '--port ' + str(PORT) + ' --use_urma=true --rsp_size=0 '
        '--num_threads=16 --urma_io_mode=2 '
        '--urma_max_sge_len=65536 '
        '--urma_buffer_size=' + str(BUFFER_SIZE) + ' '
        '--urma_buffer_count=' + str(BUFFER_COUNT) + ' '
        '--urma_send_buf_size=' + str(SEND_BUF_KB) + ' '
        '--urma_recv_buf_size=' + str(RECV_BUF_KB) + ' '
        '--urma_chunk_payload_size=' + str(CHUNK_PAYLOAD) + ' '
        '--urma_sq_size=' + str(SQ_SIZE) + ' '
        '--urma_read_jetty_sq_size=' + str(READ_JETTY_SQ) + ' '
        '--urma_use_polling=true '
        '--urma_device=' + DEVICE + ' '
        '--urma_client_handshake_version=3 '
        '--urma_dual_jetty=true '
        '--urma_use_zerocopy_read=true '
        '> /tmp/ub_test_server.log 2>&1 &'
    )
    ssh_exec(SERVER_HOST, srv_cmd, timeout=15)
    time.sleep(4)
    code, pid_out = ssh_exec(SERVER_HOST,
        "pgrep -f 'ub_test_server.*--port " + str(PORT) + "'", timeout=10)
    if not pid_out.strip():
        print("  ERROR: server failed to start!")
        _, log = ssh_exec(SERVER_HOST, 'tail -30 /tmp/ub_test_server.log', timeout=10)
        print(log)
        return False
    print("  Server started (pid=" + pid_out.strip().split()[0] + ")")
    return True


def run_client(qps, size):
    cli_cmd = (
        'numactl -C 96-111 -m 1 ' + CLIENT_BIN + ' '
        '--servers=' + SERVER_HOST + ':' + str(PORT) + ' '
        '--use_urma=true --urma_io_mode=2 '
        '--urma_max_sge_len=65536 '
        '--urma_buffer_size=' + str(BUFFER_SIZE) + ' '
        '--urma_buffer_count=' + str(BUFFER_COUNT) + ' '
        '--urma_send_buf_size=' + str(SEND_BUF_KB) + ' '
        '--urma_recv_buf_size=' + str(RECV_BUF_KB) + ' '
        '--urma_chunk_payload_size=' + str(CHUNK_PAYLOAD) + ' '
        '--urma_sq_size=' + str(SQ_SIZE) + ' '
        '--urma_read_jetty_sq_size=' + str(READ_JETTY_SQ) + ' '
        '--urma_use_polling=true '
        '--urma_device=' + DEVICE + ' '
        '--urma_client_handshake_version=3 '
        '--urma_dual_jetty=true '
        '--urma_use_zerocopy_read=true '
        '--rpc_timeout_ms=30000 --connect_timeout_ms=6000 '
        '--test_seconds=20 --max_retry=10 --queue_depth=10 '
        '--req_size=' + str(size) + ' --dummy_port=0 '
        '--expected_qps=' + str(qps) + ' --initial_tokens=0 2>&1'
    )
    cli_timeout = 300 if size >= 1048576 else 120
    code, out = ssh_exec(CLIENT_HOST, cli_cmd, timeout=cli_timeout)
    return out


def parse_result(out):
    result_line = ""
    for line in out.split('\n'):
        if 'Avg-Latency:' in line:
            result_line = line
            break
    if not result_line:
        return None

    def extract(pattern):
        m = re.search(pattern, result_line)
        return m.group(1) if m else "N/A"

    return {
        'avg_lat': extract(r'Avg-Latency:\s*([0-9.]+)'),
        'p50': extract(r'50th-Latency:\s*([0-9.]+)'),
        'p90': extract(r'90th-Latency:\s*([0-9.]+)'),
        'p99': extract(r'99th-Latency:\s*([0-9.]+)'),
        'p999': extract(r'99\.9th-Latency:\s*([0-9.]+)'),
        'p9999': extract(r'99\.99th-Latency:\s*([0-9.]+)'),
        'max_lat': extract(r'Max-Latency:\s*([0-9.]+)'),
        'throughput': extract(r'Throughput:\s*([0-9.]+)'),
        'qps': extract(r'QPS:\s*([0-9.]+)'),
        'srv_cpu': extract(r'Server CPU\(avg/max\):\s*([0-9.]+)'),
        'srv_cpu_max': extract(r'Server CPU\(avg/max\):\s*[0-9.]+/([0-9.]+)'),
        'cli_cpu': extract(r'Client CPU\(avg/max\):\s*([0-9.]+)'),
        'cli_cpu_max': extract(r'Client CPU\(avg/max\):\s*[0-9.]+/([0-9.]+)'),
        'cli_mem': extract(r'Client Memory\(avg/max\):\s*([0-9.]+)'),
        'cli_mem_max': extract(r'Client Memory\(avg/max\):\s*[0-9.]+/([0-9.]+)'),
        'err_rate': extract(r'Error rate:\s*([0-9.]+)'),
        'total_sent': extract(r'Total Sent:\s*([0-9]+)'),
        'total_errors': extract(r'Total Errors:\s*([0-9]+)'),
    }


def run_test(qps, size):
    print("\n" + "="*60)
    print("[Stage1] inline_threshold=8192, read_jetty_sq=1024")
    print("  qps=" + str(qps) + ", size=" + str(size))
    print("="*60)

    if not start_server():
        return "stage1,2," + str(qps) + "," + str(size) + ",SERVER_FAILED,,,,,,,,,,,,,,,,,,\n"

    out = run_client(qps, size)

    lines = out.strip().split('\n')
    for l in lines[-5:]:
        print("  " + l)

    m = parse_result(out)
    if not m:
        print("  ERROR: No result line found")
        return "stage1,2," + str(qps) + "," + str(size) + ",NO_RESULT,,,,,,,,,,,,,,,,,,\n"

    row = ("stage1,2," + str(qps) + "," + str(size) + ","
           + m['avg_lat'] + "," + m['p50'] + "," + m['p90'] + "," + m['p99'] + ","
           + m['p999'] + "," + m['p9999'] + "," + m['max_lat'] + "," + m['throughput'] + ","
           + m['qps'] + "," + m['srv_cpu'] + "," + m['srv_cpu_max'] + ","
           + m['cli_cpu'] + "," + m['cli_cpu_max'] + "," + m['cli_mem'] + "," + m['cli_mem_max'] + ","
           + m['err_rate'] + "," + m['total_sent'] + "," + m['total_errors'] + "\n")
    print("  -> avg_lat=" + m['avg_lat'] + "us, qps=" + m['qps'] + ", err=" + m['err_rate'] + "%")
    return row


def main():
    print("Stage 1 verification: inline_threshold 8192 + read_jetty_sq 1024")
    print("  Config: buffer_size=" + str(BUFFER_SIZE) + " read_jetty_sq=" + str(READ_JETTY_SQ)
          + " SQ=" + str(SQ_SIZE) + " (inline_threshold=8192 from code default)")
    print("  Expected: 4K/8K -10~14us (WriteZeroCopy→WriteInline), 8M stable")
    print()

    results = [CSV_HEADER + "\n"]
    total = len(QPS_VALUES) * len(SIZES)

    idx = 0
    for qps in QPS_VALUES:
        for size in SIZES:
            idx += 1
            print("\n[" + str(idx) + "/" + str(total) + "]")
            try:
                row = run_test(qps, size)
            except Exception as e:
                print("EXCEPTION: " + str(e))
                row = "stage1,2," + str(qps) + "," + str(size) + ",EXCEPTION,,,,,,,,,,,,,,,,,,\n"
            results.append(row)
            with open(RESULTS_FILE, 'w') as f:
                f.writelines(results)
            kill_server()
            time.sleep(3)

    print("\n" + "="*60)
    print("STAGE 1 COMPLETED")
    print("="*60)
    print("Results saved to " + RESULTS_FILE)

    # Print comparison
    print("\n" + "="*90)
    print("COMPARISON: Stage1 vs Baseline vs UBS v2 (avg_latency_us)")
    print("="*90)
    print(f"{'qps':>5} {'size':>10} {'baseline':>10} {'stage1':>10} {'delta':>8} {'ubs_v2':>10} {'vs_ubs':>8} {'err%':>8} {'verdict':>12}")
    print("-"*90)
    pass_count = 0
    for row in results[1:]:
        parts = row.strip().split(',')
        if len(parts) < 22:
            continue
        qps = int(parts[2])
        size = int(parts[3])
        avg_lat = parts[4]
        err = parts[19]
        base = BASELINE.get(qps, {}).get(size, 0)
        ubs = UBS_V2.get(qps, {}).get(size, 0)
        try:
            lat_f = float(avg_lat)
            delta = lat_f - base
            vs_ubs = lat_f - ubs
            verdict = "BEAT" if vs_ubs < 0 else ("TIE" if vs_ubs == 0 else "gap")
            if vs_ubs < 0:
                pass_count += 1
            print(f"{qps:>5} {size:>10} {base:>10} {avg_lat:>10} {delta:>+8.1f} {ubs:>10.2f} {vs_ubs:>+8.1f} {err:>8} {verdict:>12}")
        except:
            print(f"{qps:>5} {size:>10} {base:>10} {avg_lat:>10} {'N/A':>8} {ubs:>10.2f} {'N/A':>8} {err:>8} {'N/A':>12}")

    print("\n" + "="*90)
    print(f"SUMMARY: {pass_count}/{len(results)-1} beat UBS v2")
    print("="*90)


if __name__ == '__main__':
    main()
