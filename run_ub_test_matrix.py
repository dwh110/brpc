#!/usr/bin/env python
"""Run ub_test performance matrix on 202 (server) + 204 (client) via paramiko.
Mirrors run_ub_test.sh: io_mode {0,1,2} x qps {1000,500} x size {1024..8388608}.
"""
import paramiko
import time
import sys
import os

SERVER_HOST = '141.61.17.202'
CLIENT_HOST = '141.61.17.204'
USER = 'd00836578'
PASS = 'dwh123456'
PORT = 10918

SERVER_BIN = '/home/d00836578/brpc_workspace/brpc_urma/brpc/bazel-bin/example/ub_test_server'
CLIENT_BIN = '/home/d00836578/brpc_workspace/brpc_urma/ub_test_client'

BRPC_DIR = '/home/d00836578/brpc_workspace/brpc_urma/brpc'

# Source files to upload before building.
LOCAL_FILES = [
    'src/brpc/urma/urma_one_sided.h',
    'src/brpc/urma/urma_endpoint.h',
    'src/brpc/urma/urma_endpoint.cpp',
    'src/brpc/urma/urma_helper.cpp',
    'src/brpc/urma_transport.cpp',
    'src/brpc/controller.h',
    'src/brpc/controller.cpp',
    'src/brpc/input_message_base.h',
    'src/brpc/input_messenger.h',
    'src/brpc/input_messenger.cpp',
    'src/brpc/transport.h',
    'src/brpc/channel.cpp',
    'src/brpc/policy/baidu_rpc_protocol.cpp',
]
REMOTE_URMA_DIR = '/home/d00836578/brpc_workspace/brpc_urma/brpc/src/brpc/urma/'
REMOTE_TRANSPORT_DIR = '/home/d00836578/brpc_workspace/brpc_urma/brpc/src/brpc/'

# One-sided buffer sizes (KB). 2MB supports ~16 concurrent 127KB chunks.
SEND_BUF_KB = 2048
RECV_BUF_KB = 2048

BUILD_CMD = (
    'export http_proxy="http://141.1.37.126:7777" && '
    'export https_proxy="http://141.1.37.126:7777" && '
    'export CPLUS_INCLUDE_PATH=//usr/include/ub/:/usr/include/ub/umdk/:'
    '//usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && '
    'bazel build -c opt //example:ub_test_server //example:ub_test_client '
    '--define BRPC_WITH_URMA=true '
    '--repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0 '
    '--define BUTIL_USE_CPU_FREQUENCY=true'
)

IO_MODES = [0, 1, 2]
QPS_VALUES = [1000, 500]
SIZES = [1024, 4096, 8192, 102400, 204800, 1048576, 8388608]

RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ub_test_results.csv')

CSV_HEADER = ("urma_io_mode,expected_qps,size,Avg-Latency,50th-Latency,"
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
    # Download from source
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


def upload_sources():
    """Upload modified source files to 202 before building."""
    print("\n" + "=" * 60)
    print("Uploading source files to 202...")
    print("=" * 60)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SERVER_HOST, username=USER, password=PASS, timeout=10)
    sftp = ssh.open_sftp()
    local_base = os.path.dirname(os.path.abspath(__file__))
    for f in LOCAL_FILES:
        local_path = os.path.join(local_base, f.replace('/', os.sep))
        # Files in src/brpc/urma/ go to REMOTE_URMA_DIR;
        # files directly in src/brpc/ go to REMOTE_TRANSPORT_DIR.
        if f.startswith('src/brpc/urma/'):
            remote_path = REMOTE_URMA_DIR + os.path.basename(f)
        else:
            remote_path = REMOTE_TRANSPORT_DIR + os.path.basename(f)
        sftp.put(local_path, remote_path)
        print(f"  Uploaded: {os.path.basename(f)} -> {remote_path}")
    sftp.close()
    ssh.close()
    print("All source files uploaded.")


def build_and_sync():
    """Upload sources, build ub_test_server/client on 202, copy client to 204."""
    upload_sources()

    print("\n" + "=" * 60)
    print("Building on 202...")
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


def run_test(io_mode, qps, size):
    print(f"\n{'='*60}")
    print(f"Running: io_mode={io_mode}, qps={qps}, size={size}")
    print(f"{'='*60}")

    # Kill existing server
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null; sleep 1', timeout=10)

    # Start server on 202
    print(f"Starting server on 202 (rsp_size={size}, io_mode={io_mode})...")
    srv_cmd = (f'nohup numactl -C 96-111 -m 1 {SERVER_BIN} '
               f'--port {PORT} --use_urma=true --rsp_size={size} '
               f'--num_threads=16 --urma_io_mode={io_mode} '
               f'--urma_max_sge_len=65536 '
               f'--urma_send_buf_size={SEND_BUF_KB} '
               f'--urma_recv_buf_size={RECV_BUF_KB} '
               f'> /tmp/ub_test_server.log 2>&1 &')
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
        return f"{io_mode},{qps},{size},ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR"
    print(f"Server started (pid={pid_out.split()[0]})")

    # Run client on 204
    print(f"Starting client on 204 (req_size={size}, expected_qps={qps}, io_mode={io_mode})...")
    cli_cmd = (f'numactl -C 96-111 -m 1 {CLIENT_BIN} '
               f'--servers={SERVER_HOST}:{PORT} '
               f'--use_urma=true --urma_io_mode={io_mode} '
               f'--urma_max_sge_len=65536 '
               f'--urma_send_buf_size={SEND_BUF_KB} '
               f'--urma_recv_buf_size={RECV_BUF_KB} '
               f'--rpc_timeout_ms=2000 --connect_timeout_ms=6000 '
               f'--test_seconds=20 --max_retry=10 --queue_depth=10 '
               f'--req_size={size} --dummy_port=0 '
               f'--expected_qps={qps} --initial_tokens=0 2>&1')

    # Use longer timeout for 8MB messages
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
        row = f"{io_mode},{qps},{size},ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR"
    else:
        import re
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

        row = f"{io_mode},{qps},{size},{avg_lat},{p50},{p90},{p99},{p999},{p9999},{max_lat},{throughput},{qps_val},{srv_cpu},{srv_cpu_max},{cli_cpu},{cli_cpu_max},{cli_mem},{cli_mem_max},{err_rate}"
        print(f"  -> avg_lat={avg_lat}us, qps={qps_val}, err_rate={err_rate}%")

    # Kill server
    ssh_exec(SERVER_HOST, 'pkill -f ub_test_server 2>/dev/null', timeout=10)
    time.sleep(2)

    return row


def main():
    # Build and sync binaries first.
    build_and_sync()

    results = [CSV_HEADER]

    total = len(IO_MODES) * len(QPS_VALUES) * len(SIZES)
    idx = 0

    for io_mode in IO_MODES:
        for qps in QPS_VALUES:
            for size in SIZES:
                idx += 1
                print(f"\n[{idx}/{total}] ", end="")
                try:
                    row = run_test(io_mode, qps, size)
                except Exception as e:
                    print(f"EXCEPTION: {e}")
                    row = f"{io_mode},{qps},{size},EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION,EXCEPTION"
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

    # Also save locally
    local_results = os.path.join(os.path.dirname(__file__), 'ub_test_results.csv')
    with open(local_results, 'w') as f:
        f.write('\n'.join(results) + '\n')
    print(f"\nResults saved to {local_results}")


if __name__ == '__main__':
    main()
