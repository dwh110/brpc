#!/bin/bash
# urma_fullstack_trace.sh — full-stack tracing for brpc→ubsocket→umq→URMA.
#
# Injects into the user's existing ubsocket launch command:
#   1. Prepends liburma_trace_wrap.so to LD_PRELOAD (before libubsocket.so)
#   2. Adds ubsocket profiling env vars (UBSOCKET_SPLIT_TRACE_*, UBSOCKET_PROF_*)
#
# Usage:
#   # Your normal command (with all your env vars):
#   LD_PRELOAD=/home/phz/lib/libubsocket.so UBSOCKET_DEV_NAME="udmac0d1e2" \
#       taskset -c 80-95 ./server --port=8333
#
#   # Just wrap it with this script:
#   LD_PRELOAD=/home/phz/lib/libubsocket.so UBSOCKET_DEV_NAME="udmac0d1e2" \
#       ./urma_fullstack_trace.sh taskset -c 80-95 ./server --port=8333
#
# The script reads your existing LD_PRELOAD and env vars, adds tracing, and
# execs your command. All your UBSOCKET_* env vars pass through unchanged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WRAP_SRC="${SCRIPT_DIR}/urma_ldpreload_wrap.c"
WRAP_SO="/tmp/liburma_trace_wrap.so"

# --- compile the URMA wrapping library ----------------------------------------
if [[ ! -f "${WRAP_SO}" ]] || [[ "${WRAP_SRC}" -nt "${WRAP_SO}" ]]; then
    echo "[trace] compiling URMA wrap library..." >&2
    command -v gcc >/dev/null 2>&1 || { echo "error: gcc not found" >&2; exit 1; }
    gcc -shared -fPIC -O2 -Wall -o "${WRAP_SO}" "${WRAP_SRC}" -ldl -lpthread
    echo "[trace] compiled: ${WRAP_SO}" >&2
fi

# --- set up tracing env vars (don't override user's values) -------------------
LOGDIR="/tmp/urma_fullstack_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${LOGDIR}"

# URMA LD_PRELOAD wrap output file
export URMA_TRACE_FILE="${URMA_TRACE_FILE:-${LOGDIR}/urma_layer.log}"

# ubsocket built-in profiling (only set if not already set by user)
: "${UBSOCKET_SPLIT_TRACE_ENABLE:=true}"
: "${UBSOCKET_SPLIT_TRACE_LEVEL:=all}"
: "${UBSOCKET_SPLIT_TRACE_BUF_CAPACITY:=65535}"
: "${UBSOCKET_SPLIT_TRACE_DRAIN_INTERVAL_MS:=10}"
: "${UBSOCKET_PROF_ENABLE:=true}"
: "${UBSOCKET_PROF_MODE:=fast}"
: "${UBSOCKET_PROF_DUMP_FILE_PATH:=${LOGDIR}/ubsocket_prof.log}"
export UBSOCKET_SPLIT_TRACE_ENABLE UBSOCKET_SPLIT_TRACE_LEVEL
export UBSOCKET_SPLIT_TRACE_BUF_CAPACITY UBSOCKET_SPLIT_TRACE_DRAIN_INTERVAL_MS
export UBSOCKET_PROF_ENABLE UBSOCKET_PROF_MODE UBSOCKET_PROF_DUMP_FILE_PATH
export URMA_TRACE_FILE

# --- inject wrap .so into LD_PRELOAD (before libubsocket.so) -----------------
# Read the user's existing LD_PRELOAD and prepend our wrap lib.
USER_LD_PRELOAD="${LD_PRELOAD:-}"
if [[ -n "${USER_LD_PRELOAD}" ]]; then
    # Check if libubsocket.so is in LD_PRELOAD
    if echo "${USER_LD_PRELOAD}" | grep -q 'libubsocket'; then
        # Insert wrap .so BEFORE libubsocket.so
        export LD_PRELOAD="${WRAP_SO}:${USER_LD_PRELOAD}"
    else
        # No ubsocket in LD_PRELOAD, just prepend
        export LD_PRELOAD="${WRAP_SO}:${USER_LD_PRELOAD}"
    fi
else
    export LD_PRELOAD="${WRAP_SO}"
fi

# --- print trace config --------------------------------------------------------
echo "[trace] ========================================" >&2
echo "[trace] Full-Stack Trace: brpc→ubsocket→umq→URMA" >&2
echo "[trace] ========================================" >&2
echo "[trace] trace dir:     ${LOGDIR}" >&2
echo "[trace] URMA log:      ${URMA_TRACE_FILE}" >&2
echo "[trace] ubsocket prof: ${UBSOCKET_PROF_DUMP_FILE_PATH}" >&2
echo "[trace] LD_PRELOAD:    ${LD_PRELOAD}" >&2
echo "[trace] split_trace:   ${UBSOCKET_SPLIT_TRACE_ENABLE} level=${UBSOCKET_SPLIT_TRACE_LEVEL}" >&2
echo "[trace] prof:          ${UBSOCKET_PROF_ENABLE} mode=${UBSOCKET_PROF_MODE}" >&2
echo "[trace] target cmd:    $*" >&2
echo "[trace] ========================================" >&2
echo "" >&2

# --- exec the target command ---------------------------------------------------
# Use exec so the target inherits all env vars. The target's exit triggers
# the destructor in liburma_trace_wrap.so which writes URMA stats.
# ubsocket's destructor writes its profiling stats too.
exec "$@"
