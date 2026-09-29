#!/usr/bin/env python
"""Run O1+O2+O3 test with 64KB buffer size to reduce block count.

8MB / 64KB = 128 blocks (vs 8KB buffer = 1024 blocks).
This matches UBS v2's 64KB mini_block_size.
"""
import paramiko
import time
import sys
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

# 64KB buffer to reduce block count: 8MB/64KB = 128 blocks
BUFFER_SIZE = 65536      # 64KB (was 8KB)
BUFFER_COUNT = 8192      # 64KB * 8192 = 512MB total (same as 8KB * 65536)
SEND_BUF_KB = 8192
RECV_BUF_KB = 8192
CHUNK_PAYLOAD = 2095104
SQ_SIZE = 1024
READ_JETTY_SQ = 512

# Test all sizes, both qps
QPS_VALUES = [1000, 500]
SIZES = [1024, 4096, 8192, 102400, 204800, 1048576, 8388608]

RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'o123_64k_sq512_results.csv')

CSV_HEADER = ("pass,urma_io_mode,expected_qps,size,Avg-Latency,50th-Latency,"
              "90th-Latency,99th-Latency,99.9th-Latency,99.99th-Latency,"
              "Max-Latency,Throughput,QPS,Server_CPU_avg,Server_CPU_max,"
              "Client_CPU_avg,Client_CPU_max,Client_Mem_avg,Client_Mem_max,"
              "Error_rate,Total_Sent,Total_Errors,UBS_v2_Latency,Ratio")

# UBS v2 reference latencies (avg, us) for comparison.
UBS_V2_LAT = {
    1024: 17, 4096: 21, 8192: 29, 102400: 49,
    204800: 75, 1048576: 361, 8388608: 3940,
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


def start_server(size):
    kill_server()
    srv_cmd = (
        'nohup numactl -C 96-111 -m 1 ' + SERVER_BIN + ' '
        '--port ' + str(PORT) + ' --use_urma=true --rsp_size=' + str(size) + ' '
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
        '--rpc_timeout_ms=5000 --connect_timeout_ms=6000 '
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
    print("[O1+O2+O3 64KB buf] io_mode=2, qps=" + str(qps) + ", size=" + str(size))
    print("="*60)

    if not start_server(size):
        return "o123_64k,2," + str(qps) + "," + str(size) + ",SERVER_FAILED,,,,,,,,,,,,,,,,,,,,\n"

    out = run_client(qps, size)

    lines = out.strip().split('\n')
    for l in lines[-5:]:
        print("  " + l)

    m = parse_result(out)
    if not m:
        print("  ERROR: No result line found")
        return "o123_64k,2," + str(qps) + "," + str(size) + ",NO_RESULT,,,,,,,,,,,,,,,,,,,,\n"

    ubs_lat = UBS_V2_LAT.get(size, 0)
    try:
        ratio = "{:.2f}".format(float(m['avg_lat']) / ubs_lat) if ubs_lat > 0 else "N/A"
    except (ValueError, ZeroDivisionError):
        ratio = "N/A"

    row = ("o123_64k,2," + str(qps) + "," + str(size) + ","
           + m['avg_lat'] + "," + m['p50'] + "," + m['p90'] + "," + m['p99'] + ","
           + m['p999'] + "," + m['p9999'] + "," + m['max_lat'] + "," + m['throughput'] + ","
           + m['qps'] + "," + m['srv_cpu'] + "," + m['srv_cpu_max'] + ","
           + m['cli_cpu'] + "," + m['cli_cpu_max'] + "," + m['cli_mem'] + "," + m['cli_mem_max'] + ","
           + m['err_rate'] + "," + m['total_sent'] + "," + m['total_errors'] + ","
           + str(ubs_lat) + "," + ratio + "\n")
    print("  -> avg_lat=" + m['avg_lat'] + "us, qps=" + m['qps'] + ", err=" + m['err_rate'] + "%"
          + ", ubs_v2=" + str(ubs_lat) + "us, ratio=" + ratio + "x")
    return row


def main():
    print("O1+O2+O3 optimization test with 64KB buffer")
    print("  Config: buffer_size=" + str(BUFFER_SIZE) + " buffer_count=" + str(BUFFER_COUNT)
          + " send_buf=" + str(SEND_BUF_KB) + "KB recv_buf=" + str(RECV_BUF_KB)
          + "KB chunk=" + str(CHUNK_PAYLOAD) + " SQ=" + str(SQ_SIZE)
          + " read_jetty_sq=" + str(READ_JETTY_SQ))
    print("  QPS: " + str(QPS_VALUES))
    print("  Sizes: " + str(SIZES))

    results = [CSV_HEADER + "\n"]
    total = len(QPS_VALUES) * len(SIZES)

    idx = 0
    for qps in QPS_VALUES:
        for size in SIZES:
            idx += 1
            print("\n[" + str(idx) + "/" + str(total) + "] ")
            try:
                row = run_test(qps, size)
            except Exception as e:
                print("EXCEPTION: " + str(e))
                row = "o123_64k,2," + str(qps) + "," + str(size) + ",EXCEPTION,,,,,,,,,,,,,,,,,,,,\n"
            results.append(row)
            with open(RESULTS_FILE, 'w') as f:
                f.writelines(results)
            kill_server()
            time.sleep(3)

    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60)
    print("Results saved to " + RESULTS_FILE)

    # Print comparison table with UBS v2.
    print("\n" + "="*90)
    print("COMPARISON: brpc O1+O2+O3 64KB buf vs UBS v2 (avg_latency_us)")
    print("="*90)
    hdr = (f"{'qps':>5} {'size':>10} {'brpc_lat':>10} {'ubs_v2':>10} "
           f"{'ratio':>8} {'qps':>8} {'err%':>8} {'p99':>8} {'p999':>8} {'max':>8}")
    print(hdr)
    print("-" * len(hdr))
    for row in results[1:]:
        parts = row.strip().split(',')
        if len(parts) < 24:
            continue
        qps = parts[2]
        size = parts[3]
        avg_lat = parts[4]
        p99 = parts[7]
        p999 = parts[8]
        max_lat = parts[10]
        qps_val = parts[12]
        err = parts[19]
        ubs = parts[22]
        ratio = parts[23]
        print(f"{qps:>5} {size:>10} {avg_lat:>10} {ubs:>10} {ratio:>7}x "
              f"{qps_val:>8} {err:>8} {p99:>8} {p999:>8} {max_lat:>8}")


if __name__ == '__main__':
    main()
