#!/usr/bin/env python
"""Run ub_test performance matrix for TCP and RDMA transports on 202 (server) + 204 (client).

Transports:
  - TCP:  --use_urma=false --use_rdma=false
  - RDMA: --use_urma=false --use_rdma=true --rdma_device=mlx5_2

Matrix: transport {tcp,rdma} x qps {1000,500} x size {1024..8388608}
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
PORT = 10928  # different from URMA script (10918) to avoid collision

SERVER_BIN = '/home/d00836578/brpc_workspace/brpc_urma/brpc/bazel-bin/example/ub_test_server'
CLIENT_BIN = '/home/d00836578/brpc_workspace/brpc_urma/ub_test_client'

BRPC_DIR = '/home/d00836578/brpc_workspace/brpc_urma/brpc'

# Build with both RDMA and URMA enabled so the same binary supports all modes.
BUILD_CMD = (
    'export http_proxy="http://141.1.37.126:7777" && '
    'export https_proxy="http://141.1.37.126:7777" && '
    'export CPLUS_INCLUDE_PATH=//usr/include/ub/:/usr/include/ub/umdk/:'
    '//usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && '
    'bazel build -c opt //example:ub_test_server //example:ub_test_client '
    '--define BRPC_WITH_URMA=true '
    '--define BRPC_WITH_RDMA=true '
    '--repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0 '
    '--define BUTIL_USE_CPU_FREQUENCY=true'
)

RDMA_DEVICE = 'rocep42s0f0'
RDMA_POOL_SIZE = 32  # MB; 202 has max locked memory limit of 64MB

QPS_VALUES = [1000, 500]
SIZES = [1024, 4096, 8192, 102400, 204800, 1048576, 8388608]
TRANSPORTS = ['tcp', 'rdma']

RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'ub_test_results_tcp_rdma.csv')

CSV_HEADER = ("transport,expected_qps,size,Avg-Latency,50th-Latency,"
              "90th-Latency,99th-Latency,99.9th-Latency,99.99th-Latency,"
              "Max-Latency,Throughput,QPS,Server_CPU_avg,Server_CPU_max,"
              "Client_CPU_avg,Client_CPU_max,Client_Mem_avg,Client_Mem_max,"
              "Error_rate")


def ssh_exec(host, cmd, timeout=120):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=USER, password=PASS, timeout=10)
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode()
    err = stderr.read().decode()
    code = stdout.channel.recv_exit_status()
    ssh.close()
    return code, out, err


def ssh_scp(src_host, src_path, dst_host, dst_path):
    """Copy a file from src_host to dst_host via local relay."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(src_host, username=USER, password=PASS, timeout=10)
    sftp = ssh.open_sftp()
    with sftp.file(src_path, 'rb') as f:
        data = f.read()
    sftp.close()
    ssh.close()
    # Upload to destination
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(dst_host, username=USER, password=PASS, timeout=10)
    sftp = ssh.open_sftp()
    with sftp.file(dst_path + '.tmp', 'wb') as f:
        f.write(data)
    sftp.close()
    ssh.exec_command(f'mv {dst_path}.tmp {dst_path} && chmod +x {dst_path}')
    ssh.close()


def build_and_sync():
    """Build ub_test_server/client on 202, then copy client to 204."""
    print("\n" + "=" * 60)
    print("Building on 202 (RDMA + URMA enabled)...")
    print("=" * 60)
    code, out, err = ssh_exec(
        SERVER_HOST, f'cd {BRPC_DIR} && {BUILD_CMD}', timeout=600)
    print(out[-2000:])
    if err:
        print("STDERR:", err[-1000:])
    if code != 0:
        print(f"ERROR: Build failed (code={code})")
        sys.exit(1)
    print("Build succeeded on 202.")

    print("\nCopying ub_test_client to 204...")
    ssh_scp(SERVER_HOST,
            f'{BRPC_DIR}/bazel-bin/example/ub_test_client',
            CLIENT_HOST, CLIENT_BIN)
    print(f"Client copied to {CLIENT_HOST}:{CLIENT_BIN}")


def build_server_cmd(transport, size):
    """Build the server command for the given transport and size."""
    parts = [
        f'nohup numactl -C 96-111 -m 1 {SERVER_BIN}',
        f'--port {PORT}',
        f'--rsp_size={size}',
        f'--num_threads=16',
    ]
    if transport == 'tcp':
        parts.append('--use_urma=false --use_rdma=false')
    elif transport == 'rdma':
        parts.append(f'--use_urma=false --use_rdma=true --rdma_device={RDMA_DEVICE} '
                     f'--rdma_memory_pool_initial_size_mb={RDMA_POOL_SIZE} '
                     f'--rdma_memory_pool_increase_size_mb={RDMA_POOL_SIZE}')
    parts.append('> /tmp/ub_test_server.log 2>&1 &')
    return ' '.join(parts)


def build_client_cmd(transport, qps, size):
    """Build the client command for the given transport, qps, and size."""
    parts = [
        f'numactl -C 96-111 -m 1 {CLIENT_BIN}',
        f'--servers={SERVER_HOST}:{PORT}',
        f'--rpc_timeout_ms=2000 --connect_timeout_ms=6000',
        f'--test_seconds=20 --max_retry=10 --queue_depth=10',
        f'--req_size={size} --dummy_port=0',
        f'--expected_qps={qps} --initial_tokens=0',
    ]
    if transport == 'tcp':
        parts.append('--use_urma=false --use_rdma=false')
    elif transport == 'rdma':
        parts.append(f'--use_urma=false --use_rdma=true --rdma_device={RDMA_DEVICE} '
                     f'--rdma_memory_pool_initial_size_mb={RDMA_POOL_SIZE} '
                     f'--rdma_memory_pool_increase_size_mb={RDMA_POOL_SIZE}')
    parts.append('2>&1')
    return ' '.join(parts)


def run_test(transport, qps, size):
    print(f"\n{'='*60}")
    print(f"Running: transport={transport}, qps={qps}, size={size}")
    print(f"{'='*60}")

    # Kill existing server
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null; sleep 1', timeout=10)

    # Start server on 202
    print(f"Starting server on 202 (transport={transport}, rsp_size={size})...")
    srv_cmd = build_server_cmd(transport, size)
    ssh_exec(SERVER_HOST, srv_cmd, timeout=10)
    time.sleep(3)

    # Verify server is running
    code, pid_out, _ = ssh_exec(SERVER_HOST,
        f"pgrep -f 'ub_test_server.*--port {PORT}'", timeout=10)
    pid_out = pid_out.strip()
    if not pid_out:
        print("ERROR: Server failed to start")
        _, log, _ = ssh_exec(SERVER_HOST, 'cat /tmp/ub_test_server.log', timeout=10)
        print(log[:500])
        return error_row(transport, qps, size)
    print(f"Server started (pid={pid_out.split()[0]})")

    # Run client on 204
    print(f"Starting client on 204 (transport={transport}, req_size={size}, qps={qps})...")
    cli_cmd = build_client_cmd(transport, qps, size)
    cli_timeout = 300 if size >= 1048576 else 120
    code, cli_out, cli_err = ssh_exec(CLIENT_HOST, cli_cmd, timeout=cli_timeout)

    # Parse result line
    result_line = ""
    for line in cli_out.split('\n'):
        if 'Avg-Latency:' in line:
            result_line = line
            break

    if not result_line:
        print("ERROR: No result line found")
        print(cli_out[-500:])
        row = error_row(transport, qps, size)
    else:
        def extract(pattern):
            m = re.search(pattern, result_line)
            return m.group(1) if m else "N/A"

        avg_lat = extract(r'Avg-Latency:\s*([0-9.]+)')
        p50 = extract(r'50th-Latency:\s*([0-9.]+)')
        p90 = extract(r'90th-Latency:\s*([0-9.]+)')
        p99 = extract(r'99th-Latency:\s*([0-9.]+)')
        p999 = extract(r'99\.9th-Latency:\s*([0-9.]+)')
        p9999 = extract(r'99\.99th-Latency:\s*([0-9.]+)')
        max_lat = extract(r'Max-Latency:\s*([0-9.]+)')
        throughput = extract(r'Throughput:\s*([0-9.]+)')
        qps_val = extract(r'QPS:\s*([0-9.]+)')
        srv_cpu = extract(r'Server CPU\(avg/max\):\s*([0-9.]+)')
        srv_cpu_max = extract(r'Server CPU\(avg/max\):\s*[0-9.]+/([0-9.]+)')
        cli_cpu = extract(r'Client CPU\(avg/max\):\s*([0-9.]+)')
        cli_cpu_max = extract(r'Client CPU\(avg/max\):\s*[0-9.]+/([0-9.]+)')
        cli_mem = extract(r'Client Memory\(avg/max\):\s*([0-9.]+)')
        cli_mem_max = extract(r'Client Memory\(avg/max\):\s*[0-9.]+/([0-9.]+)')
        err_rate = extract(r'Error rate:\s*([0-9.]+)')

        row = (f"{transport},{qps},{size},{avg_lat},{p50},{p90},{p99},"
               f"{p999},{p9999},{max_lat},{throughput},{qps_val},"
               f"{srv_cpu},{srv_cpu_max},{cli_cpu},{cli_cpu_max},"
               f"{cli_mem},{cli_mem_max},{err_rate}")
        print(f"  -> avg_lat={avg_lat}us, qps={qps_val}, err_rate={err_rate}%")

    # Kill server
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null', timeout=10)
    time.sleep(2)

    return row


def error_row(transport, qps, size):
    return (f"{transport},{qps},{size},ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,"
            f"ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR")


def main():
    # Build and sync binaries first.
    build_and_sync()

    results = [CSV_HEADER]

    total = len(TRANSPORTS) * len(QPS_VALUES) * len(SIZES)
    idx = 0

    for transport in TRANSPORTS:
        for qps in QPS_VALUES:
            for size in SIZES:
                idx += 1
                print(f"\n[{idx}/{total}] ", end="")
                try:
                    row = run_test(transport, qps, size)
                except Exception as e:
                    print(f"EXCEPTION: {e}")
                    row = error_row(transport, qps, size)
                    row = row.replace("ERROR", "EXCEPTION")
                results.append(row)

                # Write intermediate results
                with open(RESULTS_FILE, 'w') as f:
                    f.write('\n'.join(results) + '\n')

    # Print final results
    print("\n" + "=" * 60)
    print("All tests completed. Results:")
    print("=" * 60)
    for line in results:
        print(line)

    print(f"\nResults saved to {RESULTS_FILE}")


if __name__ == '__main__':
    main()
