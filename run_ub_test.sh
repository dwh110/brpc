#!/bin/bash
# run_ub_test.sh — Run ub_test performance matrix on 202 (server) + 204 (client)
# Matrix: urma_io_mode ∈ {0, 1, 2} × expected_qps ∈ {1000, 500} × size ∈ {1024, 4096, 8192, 102400, 204800, 1048576, 8388608}
# rsp_size = req_size = size for each run.
# urma_io_mode: 0=SEND_ONLY (default), 1=WRITE_ONLY, 2=HYBRID

set -euo pipefail

SERVER_HOST=141.61.17.202
CLIENT_HOST=141.61.17.204
SERVER_USER=d00836578
CLIENT_USER=d00836578
SERVER_PASS=dwh123456
CLIENT_PASS=dwh123456
PORT=10086

SERVER_BIN="/home/d00836578/brpc_workspace/brpc_urma/brpc/bazel-bin/example/ub_test_server"
CLIENT_BIN="/home/d00836578/brpc_workspace/brpc_urma/ub_test_client"

# SSH helper
ssh_server() {
    DISPLAY=:0 SSH_ASKPASS=/tmp/ssh_pass_202.sh SSH_ASKPASS_REQUIRE=force \
        ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER_HOST} "$@"
}

ssh_client() {
    DISPLAY=:0 SSH_ASKPASS=/tmp/ssh_pass_204.sh SSH_ASKPASS_REQUIRE=force \
        ssh -o StrictHostKeyChecking=no ${CLIENT_USER}@${CLIENT_HOST} "$@"
}

IO_MODES=(0 1 2)
QPS_VALUES=(1000 500)
SIZES=(1024 4096 8192 102400 204800 1048576 8388608)

RESULTS_FILE="/tmp/ub_test_results.csv"
echo "urma_io_mode,expected_qps,size,Avg-Latency,50th-Latency,90th-Latency,99th-Latency,99.9th-Latency,99.99th-Latency,Max-Latency,Throughput,QPS,Server_CPU_avg,Server_CPU_max,Client_CPU_avg,Client_CPU_max,Client_Mem_avg,Client_Mem_max,Error_rate" > ${RESULTS_FILE}

run_test() {
    local io_mode=$1
    local qps=$2
    local size=$3

    echo "========================================================"
    echo "Running: urma_io_mode=${io_mode}, expected_qps=${qps}, req_size=${size}, rsp_size=${size}"
    echo "========================================================"

    # Kill any existing server
    ssh_server "pkill -f ub_test_server 2>/dev/null; sleep 1" 2>/dev/null || true

    # Start server on 202
    echo "Starting server on 202 (rsp_size=${size}, io_mode=${io_mode})..."
    ssh_server "nohup numactl -C 96-111 -m 1 ${SERVER_BIN} --port ${PORT} --use_urma=true --rsp_size=${size} --num_threads=16 --urma_io_mode=${io_mode} > /tmp/ub_test_server.log 2>&1 &" 2>/dev/null
    sleep 3

    # Verify server is running
    local server_pid
    server_pid=$(ssh_server "pgrep -f 'ub_test_server.*--port ${PORT}'" 2>/dev/null || echo "")
    if [ -z "$server_pid" ]; then
        echo "ERROR: Server failed to start on 202"
        ssh_server "cat /tmp/ub_test_server.log" 2>/dev/null
        return 1
    fi
    echo "Server started (pid=${server_pid})"

    # Run client on 204
    echo "Starting client on 204 (req_size=${size}, expected_qps=${qps}, io_mode=${io_mode})..."
    local client_output
    client_output=$(ssh_client "numactl -C 96-111 -m 1 ${CLIENT_BIN} \
        --servers=${SERVER_HOST}:${PORT} \
        --use_urma=true \
        --urma_io_mode=${io_mode} \
        --rpc_timeout_ms=2000 \
        --connect_timeout_ms=6000 \
        --test_seconds=20 \
        --max_retry=10 \
        --queue_depth=10 \
        --req_size=${size} \
        --dummy_port=0 \
        --expected_qps=${qps} \
        --initial_tokens=0 2>&1" 2>/dev/null)

    echo "${client_output}"

    # Parse the result line
    local result_line
    result_line=$(echo "${client_output}" | grep "Avg-Latency:" || echo "")

    if [ -z "$result_line" ]; then
        echo "ERROR: No result line found for io_mode=${io_mode}, qps=${qps}, size=${size}"
        echo "${io_mode},${qps},${size},ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR" >> ${RESULTS_FILE}
    else
        # Parse all fields from the result line
        # Format: Avg-Latency: X, 50th-Latency: X, 90th-Latency: X, 99th-Latency: X,
        #         99.9th-Latency: X, 99.99th-Latency: X, Max-Latency: X,
        #         Throughput: XMB/s, QPS: X, Server CPU(avg/max): X/X%,
        #         Client CPU(avg/max): X/X%, Client Memory(avg/max): X/XMB,
        #         Total Sent: X, Total Errors: X, Total Requests: X, Error rate: X%

        local avg_lat=$(echo "${result_line}" | sed -n 's/.*Avg-Latency: \([0-9.]*\).*/\1/p')
        local p50=$(echo "${result_line}" | sed -n 's/.*50th-Latency: \([0-9.]*\).*/\1/p')
        local p90=$(echo "${result_line}" | sed -n 's/.*90th-Latency: \([0-9.]*\).*/\1/p')
        local p99=$(echo "${result_line}" | sed -n 's/.*99th-Latency: \([0-9.]*\).*/\1/p')
        local p999=$(echo "${result_line}" | sed -n 's/.*99\.9th-Latency: \([0-9.]*\).*/\1/p')
        local p9999=$(echo "${result_line}" | sed -n 's/.*99\.99th-Latency: \([0-9.]*\).*/\1/p')
        local max_lat=$(echo "${result_line}" | sed -n 's/.*Max-Latency: \([0-9.]*\).*/\1/p')
        local throughput=$(echo "${result_line}" | sed -n 's/.*Throughput: \([0-9.]*\)MB\/s.*/\1/p')
        local qps_val=$(echo "${result_line}" | sed -n 's/.*QPS: \([0-9.]*\).*/\1/p')
        local srv_cpu=$(echo "${result_line}" | sed -n 's/.*Server CPU(avg\/max): \([0-9.]*\)\/\([0-9.]*\)%.*/\1/p')
        local srv_cpu_max=$(echo "${result_line}" | sed -n 's/.*Server CPU(avg\/max): \([0-9.]*\)\/\([0-9.]*\)%.*/\2/p')
        local cli_cpu=$(echo "${result_line}" | sed -n 's/.*Client CPU(avg\/max): \([0-9.]*\)\/\([0-9.]*\)%.*/\1/p')
        local cli_cpu_max=$(echo "${result_line}" | sed -n 's/.*Client CPU(avg\/max): \([0-9.]*\)\/\([0-9.]*\)%.*/\2/p')
        local cli_mem=$(echo "${result_line}" | sed -n 's/.*Client Memory(avg\/max): \([0-9.]*\)\/\([0-9.]*\)MB.*/\1/p')
        local cli_mem_max=$(echo "${result_line}" | sed -n 's/.*Client Memory(avg\/max): \([0-9.]*\)\/\([0-9.]*\)MB.*/\2/p')
        local err_rate=$(echo "${result_line}" | sed -n 's/.*Error rate: \([0-9.]*\).*/\1/p')

        echo "${io_mode},${qps},${size},${avg_lat},${p50},${p90},${p99},${p999},${p9999},${max_lat},${throughput},${qps_val},${srv_cpu},${srv_cpu_max},${cli_cpu},${cli_cpu_max},${cli_mem},${cli_mem_max},${err_rate}" >> ${RESULTS_FILE}

        echo "  -> Parsed: avg_lat=${avg_lat}us, qps=${qps_val}, err_rate=${err_rate}%"
    fi

    # Kill server
    ssh_server "pkill -f ub_test_server 2>/dev/null" 2>/dev/null || true
    sleep 2
}

# Create SSH password scripts
echo "dwh123456" > /tmp/ssh_pass_202.sh
chmod +x /tmp/ssh_pass_202.sh
echo "dwh123456" > /tmp/ssh_pass_204.sh
chmod +x /tmp/ssh_pass_204.sh

# Run all test combinations: io_mode × qps × size
for io_mode in "${IO_MODES[@]}"; do
    for qps in "${QPS_VALUES[@]}"; do
        for size in "${SIZES[@]}"; do
            run_test "${io_mode}" "${qps}" "${size}"
        done
    done
done

echo ""
echo "========================================================"
echo "All tests completed. Results table:"
echo "========================================================"
echo ""

# Print results as a formatted table
cat ${RESULTS_FILE}

echo ""
echo "Formatted table:"
echo ""

# Use column to format nicely
column -t -s',' ${RESULTS_FILE}
