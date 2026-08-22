#!/bin/bash
# urma_fullstack_trace.sh — full-stack tracing for the brpc→ubsocket→umq→URMA
# chain, combining ubsocket built-in profiling + LD_PRELOAD URMA wrapping.
#
# Architecture:
#   brpc ──→ ubsocket (LD_PRELOAD) ──→ umq ──→ URMA (liburma.so)
#             [built-in prof]          [built-in prof]  [LD_PRELOAD wrap]
#
# Traced layers:
#   1. brpc framework:        ubsocket built-in PROF_START/PROF_END tracepoints
#      (BRPC_CLIENT_CALL, BRPC_SERIALIZE, BRPC_WRITEV, BRPC_DESERIALIZE, etc.)
#   2. ubsocket transport:    ubsocket built-in SplitTrace
#      (CORE_CONNECT, CORE_READ, CORE_WRITE, CORE_EPOLL_*, etc.)
#   3. umq messaging queue:   ubsocket built-in SplitTrace
#      (CORE_WRITE_UMQ_POLL, CORE_EPOLL_UMQ_POLL, etc.)
#   4. URMA hardware:         LD_PRELOAD wrapping (this tool's .so)
#      (urma_post_jetty_send_wr, urma_poll_jfc, urma_wait_jfc, etc.)
#
# Usage:
#   sudo ./urma_fullstack_trace.sh -- ./server --port 8003
#   sudo ./urma_fullstack_trace.sh -- ./client --server=127.0.0.1:8003
#
# Requires:
#   - gcc (for compiling liburma_trace_wrap.so)
#   - libubsocket.so (the ubsocket LD_PRELOAD library)
#   - liburma.so (the URMA transport library)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WRAP_SRC="${SCRIPT_DIR}/urma_ldpreload_wrap.c"
WRAP_SO="/tmp/liburma_trace_wrap.so"

# --- compile the URMA wrapping library ----------------------------------------
if [[ ! -f "${WRAP_SO}" ]] || [[ "${WRAP_SRC}" -nt "${WRAP_SO}" ]]; then
    echo "compiling URMA trace wrap library..."
    command -v gcc >/dev/null 2>&1 || { echo "error: gcc not found" >&2; exit 1; }
    gcc -shared -fPIC -O2 -Wall -o "${WRAP_SO}" "${WRAP_SRC}" -ldl -lpthread
    echo "compiled: ${WRAP_SO}"
fi

# --- locate libubsocket.so ----------------------------------------------------
UBSOCKET_LIB="${UBSOCKET_LIB:-}"
if [[ -z "${UBSOCKET_LIB}" ]]; then
    # Try common paths
    for p in \
        "/usr/local/lib/libubsocket.so" \
        "/usr/lib64/libubsocket.so" \
        "$(ldconfig -p 2>/dev/null | awk '/libubsocket\.so/ {print $NF; exit}')" \
        "${SCRIPT_DIR}/../ubs-comm_rpc-720/build/output/libubsocket.so"; do
        if [[ -f "$p" ]]; then
            UBSOCKET_LIB="$p"
            break
        fi
    done
fi
if [[ -z "${UBSOCKET_LIB}" ]]; then
    echo "error: libubsocket.so not found. Set UBSOCKET_LIB=/path/to/libubsocket.so" >&2
    exit 1
fi
echo "ubsocket: ${UBSOCKET_LIB}"

# --- determine target command --------------------------------------------------
TARGET_CMD=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            cat <<EOF
Usage: $0 [OPTIONS] -- TARGET_CMD [TARGET_ARGS...]

  --             separator: everything after is the target command
  TARGET_CMD     the binary to trace

Examples:
  $0 -- ./bazel-bin/example/urma_performance_server --port 8003
  $0 -- ./bazel-bin/example/urma_performance_client --server=127.0.0.1:8003

Environment variables (ubsocket built-in profiling):
  UBSOCKET_SPLIT_TRACE_ENABLE=true    Enable SplitTrace (default: true)
  UBSOCKET_SPLIT_TRACE_LEVEL=all      Trace level: all|ubsocket|umq (default: all)
  UBSOCKET_SPLIT_TRACE_BUF_CAPACITY  Trace buffer size (default: 65535)
  UBSOCKET_SPLIT_TRACE_DRAIN_INTERVAL_MS  Drain interval (default: 10)
  UBSOCKET_PROF_ENABLE=true           Enable PROF_START/PROF_END stats (default: true)
  UBSOCKET_PROF_MODE=fast             Profiling mode: fast|detail (default: fast)
  UBSOCKET_PROF_DUMP_FILE_PATH        Stats dump path (default: /tmp/ubsocket/profiling)

Environment variables (URMA LD_PRELOAD wrap):
  URMA_TRACE_FILE                      URMA stats output (default: /tmp/urma_ldpreload_*.log)
  UBSOCKET_LIB                         Path to libubsocket.so (auto-detected)
EOF
            exit 0
            ;;
        --) shift; TARGET_CMD="$*"; break ;;
        *)  TARGET_CMD="$*"; break ;;
    esac
done

[[ -z "${TARGET_CMD}" ]] && { echo "error: no target command. Usage: $0 -- ./server [args...]" >&2; exit 1; }

# --- set up environment -------------------------------------------------------
LOGDIR="/tmp/urma_fullstack_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${LOGDIR}"

# URMA LD_PRELOAD wrap output
export URMA_TRACE_FILE="${LOGDIR}/urma_layer.log"

# ubsocket built-in profiling
export UBSOCKET_SPLIT_TRACE_ENABLE="${UBSOCKET_SPLIT_TRACE_ENABLE:-true}"
export UBSOCKET_SPLIT_TRACE_LEVEL="${UBSOCKET_SPLIT_TRACE_LEVEL:-all}"
export UBSOCKET_SPLIT_TRACE_BUF_CAPACITY="${UBSOCKET_SPLIT_TRACE_BUF_CAPACITY:-65535}"
export UBSOCKET_SPLIT_TRACE_DRAIN_INTERVAL_MS="${UBSOCKET_SPLIT_TRACE_DRAIN_INTERVAL_MS:-10}"
export UBSOCKET_PROF_ENABLE="${UBSOCKET_PROF_ENABLE:-true}"
export UBSOCKET_PROF_MODE="${UBSOCKET_PROF_MODE:-fast}"
export UBSOCKET_PROF_DUMP_FILE_PATH="${LOGDIR}/ubsocket_prof.log"

# LD_PRELOAD: trace lib FIRST, then ubsocket lib
# This ensures our URMA wrappers resolve RTLD_NEXT to the real urma functions
export LD_PRELOAD="${WRAP_SO}:${UBSOCKET_LIB}"

echo "========================================"
echo " Full-Stack Trace: brpc→ubsocket→umq→URMA"
echo "========================================"
echo " trace dir:        ${LOGDIR}"
echo " URMA layer log:   ${URMA_TRACE_FILE}"
echo " ubsocket prof:    ${UBSOCKET_PROF_DUMP_FILE_PATH}"
echo " LD_PRELOAD:       ${LD_PRELOAD}"
echo " target:           ${TARGET_CMD}"
echo "========================================"
echo ""

# --- run target ---------------------------------------------------------------
# We DON'T use exec so we can collect stats after the target exits.
bash -c "${TARGET_CMD}" &
TARGET_PID=$!

# Wait for target to exit
wait "${TARGET_PID}" 2>/dev/null || true
TARGET_EXIT=$?

# --- collect results ----------------------------------------------------------
echo ""
echo "========================================"
echo " Trace Results"
echo "========================================"

# URMA layer (from LD_PRELOAD wrap destructor)
if [[ -f "${URMA_TRACE_FILE}" ]]; then
    echo ""
    echo "--- URMA Layer (LD_PRELOAD wrap) ---"
    cat "${URMA_TRACE_FILE}"
fi

# ubsocket built-in profiling
if [[ -f "${UBSOCKET_PROF_DUMP_FILE_PATH}" ]]; then
    echo ""
    echo "--- ubsocket Built-in Profiling ---"
    cat "${UBSOCKET_PROF_DUMP_FILE_PATH}"
elif [[ -d "/tmp/ubsocket/profiling" ]]; then
    echo ""
    echo "--- ubsocket Profiling (default path) ---"
    cat /tmp/ubsocket/profiling/* 2>/dev/null || echo "(no files)"
fi

# SplitTrace data (if ubsocket writes to a trace file)
SPLITTRACE_DIR="${LOGDIR}/splittrace"
if [[ -d "${SPLITTRACE_DIR}" ]]; then
    echo ""
    echo "--- SplitTrace Data ---"
    ls -la "${SPLITTRACE_DIR}/"
fi

echo ""
echo "========================================"
echo " Target exit code: ${TARGET_EXIT}"
echo " All logs in: ${LOGDIR}"
echo "========================================"

exit ${TARGET_EXIT}
