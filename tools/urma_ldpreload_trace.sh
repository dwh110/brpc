#!/bin/bash
# urma_ldpreload_trace.sh — full-stack LD_PRELOAD tracing for URMA + UB
# shared-memory + socket syscalls.
#
# Advantage over uprobe: ~10-20ns per call (function call overhead only,
# no kernel trap). uprobe is ~200-500ns per call.
#
# Usage:
#   sudo ./urma_ldpreload_trace.sh ./server --port 8003
#   sudo ./urma_ldpreload_trace.sh -- ./client --server=127.0.0.1:8003
#
# Traced layers (via LD_PRELOAD interception):
#   1. URMA transport (liburma.so): 7 data-plane + 8 control-plane functions
#   2. UB shared memory (libubsm_sdk.so): 6 key functions
#   3. Socket syscalls (libc): readv, writev, epoll_wait
#
# For brpc framework functions (Channel::CallMethod, etc.), use the
# uprobe-based tools/brpc_lifecycle_trace.sh — those are statically linked
# and cannot be intercepted via LD_PRELOAD.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/urma_ldpreload_wrap.c"
SO="/tmp/liburma_trace_wrap.so"

# --- compile the intercept library -------------------------------------------------
if [[ ! -f "${SO}" ]] || [[ "${SRC}" -nt "${SO}" ]]; then
    echo "compiling LD_PRELOAD library..."
    if ! command -v gcc >/dev/null 2>&1; then
        echo "error: gcc not found" >&2
        exit 1
    fi
    gcc -shared -fPIC -O2 -Wall -o "${SO}" "${SRC}" -ldl -lpthread
    echo "compiled: ${SO}"
fi

# --- determine target command ------------------------------------------------------
# First non-option argument is the binary; rest are its args.
# Use -- to separate our flags from target args.
TARGET_CMD=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            cat <<EOF
Usage: $0 [OPTIONS] -- TARGET_CMD [TARGET_ARGS...]

  --           separator: everything after is the target command
  TARGET_CMD   the binary to trace (e.g. ./server)

Examples:
  $0 -- ./bazel-bin/example/urma_performance_server --port 8003
  $0 -- ./bazel-bin/example/urma_performance_client --server=127.0.0.1:8003
EOF
            exit 0
            ;;
        --) shift; TARGET_CMD="$*"; break ;;
        *)  TARGET_CMD="$*"; break ;;
    esac
done

if [[ -z "${TARGET_CMD}" ]]; then
    echo "error: no target command specified" >&2
    echo "usage: $0 -- ./server [args...]" >&2
    exit 1
fi

# --- set trace output file --------------------------------------------------------
LOGFILE="/tmp/urma_ldpreload_$(date +%Y%m%d_%H%M%S).log"
export URMA_TRACE_FILE="${LOGFILE}"

# --- run with LD_PRELOAD ----------------------------------------------------------
echo "trace output: ${LOGFILE}"
echo "target: ${TARGET_CMD}"
echo "launching with LD_PRELOAD..."

# LD_PRELOAD only works if the .so is in a path accessible at exec time.
# For setuid binaries, use sudo with --preserve-env.
if [[ "$(id -u)" -eq 0 ]]; then
    LD_PRELOAD="${SO}" exec bash -c "${TARGET_CMD}"
else
    LD_PRELOAD="${SO}" exec bash -c "${TARGET_CMD}"
fi

# After the target exits, the destructor in the .so writes stats to ${LOGFILE}.
# But since we used exec, this line is never reached. The destructor runs
# in the child process and writes the file.
