#!/usr/bin/env python3
"""Build on 202, sync client to 204, run io_mode=2 large message tests."""
import paramiko
import sys
import time
import os
import re

SERVER_HOST = "141.61.17.202"
CLIENT_HOST = "141.61.17.204"
USER = "d00836578"
PASS = "dwh123456"
PORT = 10086

LOCAL_FILES = [
    "src/brpc/urma/urma_one_sided.h",
    "src/brpc/urma/urma_endpoint.h",
    "src/brpc/urma/urma_endpoint.cpp",
]
REMOTE_DIR = "/home/d00836578/brpc_workspace/brpc_urma/brpc/src/brpc/urma/"

SERVER_BIN = "/home/d00836578/brpc_workspace/brpc_urma/brpc/bazel-bin/example/ub_test_server"
CLIENT_BIN = "/home/d00836578/brpc_workspace/brpc_urma/ub_test_client"

BUILD_CMD = (
    "cd /home/d00836578/brpc_workspace/brpc_urma/brpc && "
    'export http_proxy="http://141.1.37.126:7777" && '
    'export https_proxy="http://141.1.37.126:7777" && '
    "export CPLUS_INCLUDE_PATH=//usr/include/ub/:/usr/include/ub/umdk/:/usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && "
    "bazel build //example:ub_test_server //example:ub_test_client "
    "--define=BRPC_WITH_URMA=true --repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0 -c opt "
    "--define=BUTIL_USE_CPU_FREQUENCY=true 2>&1"
)


def get_ssh_client(host):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=USER, password=PASS, timeout=30)
    return client


def run_cmd(client, cmd, timeout=600):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    output = ""
    for line in iter(stdout.readline, ""):
        line_stripped = line.rstrip()
        print(f"  {line_stripped}")
        output += line
    err = stderr.read().decode()
    if err:
        print(f"  STDERR: {err}")
    exit_code = stdout.channel.recv_exit_status()
    return exit_code, output, err


def main():
    # Step 1: Upload modified source files to 202
    print("=" * 60)
    print("Step 1: Upload source files to 202")
    print("=" * 60)
    server = get_ssh_client(SERVER_HOST)
    sftp = server.open_sftp()

    local_base = "D:/kunpeng/bpc_urma/brpc/"
    for f in LOCAL_FILES:
        local_path = os.path.join(local_base, f.replace("/", "\\"))
        remote_path = REMOTE_DIR + os.path.basename(f)
        sftp.put(local_path, remote_path)
        print(f"  Uploaded: {os.path.basename(f)}")

    sftp.close()
    print("  All files uploaded.")

    # Step 2: Build on 202
    print()
    print("=" * 60)
    print("Step 2: Build on 202")
    print("=" * 60)
    print("Building (this may take several minutes)...")
    exit_code, build_output, build_err = run_cmd(server, BUILD_CMD, timeout=600)
    if exit_code != 0:
        print(f"BUILD FAILED with exit code {exit_code}")
        lines = build_output.strip().split("\n")
        for l in lines[-30:]:
            print(f"  {l}")
        server.close()
        sys.exit(1)
    print("BUILD SUCCEEDED")

    # Step 3: Sync client binary to 204
    print()
    print("=" * 60)
    print("Step 3: Sync client binary to 204")
    print("=" * 60)
    sftp = server.open_sftp()
    local_tmp = os.path.join(os.environ.get("TEMP", "/tmp"), "ub_test_client_tmp")
    remote_client_bin = "/home/d00836578/brpc_workspace/brpc_urma/brpc/bazel-bin/example/ub_test_client"
    print(f"  Downloading client binary from 202...")
    sftp.get(remote_client_bin, local_tmp)
    sftp.close()
    print(f"  Downloaded to {local_tmp}")

    client = get_ssh_client(CLIENT_HOST)
    sftp = client.open_sftp()
    remote_tmp = "/home/d00836578/brpc_workspace/brpc_urma/ub_test_client_new"
    print(f"  Uploading client binary to 204...")
    sftp.put(local_tmp, remote_tmp)
    run_cmd(client, f"mv {remote_tmp} {CLIENT_BIN} && chmod +x {CLIENT_BIN}")
    sftp.close()
    print(f"  Client synced to 204")

    try:
        os.remove(local_tmp)
    except:
        pass

    # Step 4: Run io_mode=2 tests
    print()
    print("=" * 60)
    print("Step 4: Run io_mode=2 tests")
    print("=" * 60)

    test_cases = [
        (1048576, 1000, "1MB qd=10 - key test"),
        (1024, 1000, "1KB regression"),
        (8192, 1000, "8KB regression"),
        (8388608, 1000, "8MB qd=10"),
    ]

    results = []
    for size, qps, desc in test_cases:
        print()
        print(f"--- Test: size={size}, qps={qps} ({desc}) ---")

        run_cmd(server, "pkill -f ub_test_server 2>/dev/null; sleep 1", timeout=10)

        server_cmd = (
            f"nohup numactl -C 96-111 -m 1 {SERVER_BIN} "
            f"--port {PORT} --use_urma=true --rsp_size={size} --num_threads=16 "
            f"--urma_io_mode=2 --urma_max_sge_len=65536 "
            f"--urma_send_buf_size=2048 --urma_recv_buf_size=2048 "
            f"> /tmp/ub_test_server.log 2>&1 &"
        )
        run_cmd(server, server_cmd, timeout=10)
        time.sleep(3)

        _, pid_out, _ = run_cmd(server, f"pgrep -f 'ub_test_server.*--port {PORT}'", timeout=10)
        pid_out = pid_out.strip()
        if not pid_out:
            print("  ERROR: Server failed to start!")
            _, log_out, _ = run_cmd(server, "cat /tmp/ub_test_server.log", timeout=10)
            results.append((size, qps, "SERVER_FAIL", "", ""))
            continue
        print(f"  Server started (pid={pid_out})")

        client_cmd = (
            f"numactl -C 96-111 -m 1 {CLIENT_BIN} "
            f"--servers={SERVER_HOST}:{PORT} --use_urma=true "
            f"--urma_io_mode=2 --urma_max_sge_len=65536 "
            f"--urma_send_buf_size=2048 --urma_recv_buf_size=2048 "
            f"--rpc_timeout_ms=2000 --connect_timeout_ms=6000 "
            f"--test_seconds=20 --max_retry=10 --queue_depth=10 "
            f"--req_size={size} --dummy_port=0 --expected_qps={qps} "
            f"--initial_tokens=0 2>&1"
        )
        exit_code, client_output, _ = run_cmd(client, client_cmd, timeout=120)

        result_line = ""
        for line in client_output.split("\n"):
            if "Avg-Latency:" in line:
                result_line = line.strip()
                break

        if result_line:
            avg_lat = re.search(r"Avg-Latency: ([0-9.]+)", result_line)
            err_rate = re.search(r"Error rate: ([0-9.]+)", result_line)
            qps_val = re.search(r"QPS: ([0-9.]+)", result_line)
            avg = avg_lat.group(1) if avg_lat else "?"
            err = err_rate.group(1) if err_rate else "?"
            qps_v = qps_val.group(1) if qps_val else "?"
            print(f"  RESULT: avg_lat={avg}us, qps={qps_v}, err_rate={err}%")
            results.append((size, qps, avg, qps_v, err))
        else:
            print("  ERROR: No result line found!")
            for l in client_output.strip().split("\n")[-10:]:
                print(f"    {l}")
            results.append((size, qps, "ERROR", "", ""))

        run_cmd(server, "pkill -f ub_test_server 2>/dev/null", timeout=10)
        # Print last 30 lines of server log for debugging.
        if size >= 1048576:
            print("  --- Server log (last 30 lines) ---")
            _, srv_log, _ = run_cmd(server, "tail -30 /tmp/ub_test_server.log", timeout=10)
        time.sleep(2)

    # Summary
    print()
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"{'Size':>10} {'QPS':>6} {'Avg-Lat':>10} {'QPS-actual':>10} {'Err%':>8} {'Status':>10}")
    print("-" * 60)
    for size, qps, avg, qps_v, err in results:
        status = "PASS" if (err not in ("", "?", "ERROR") and float(err) == 0) else "FAIL"
        print(f"{size:>10} {qps:>6} {str(avg):>10} {str(qps_v):>10} {str(err):>8} {status:>10}")

    server.close()
    client.close()


if __name__ == "__main__":
    main()
