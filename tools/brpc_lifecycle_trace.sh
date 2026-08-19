#!/bin/bash
# brpc_lifecycle_trace.sh — non-intrusive uprobe tracing of the full brpc
# RPC request lifecycle, from client initiation through URMA transport to
# business processing and response.
#
# Usage:
#   sudo ./brpc_lifecycle_trace.sh -e ./server -- --port 8080   # start & trace
#   sudo ./brpc_lifecycle_trace.sh <pid>                         # trace running PID
#   sudo ./brpc_lifecycle_trace.sh -n my_server                  # trace by name
#
# Stages traced (baidu_rpc_protocol):
#   1.  Client CallMethod        — brpc::Channel::CallMethod
#   2.  Serialize request        — brpc::policy::PackRpcRequest
#   3.  Socket send              — brpc::Socket::Write
#   4.  URMA send                — brpc::UrmaEndpoint::CutFromIOBufList
#   5.  URMA poll/recv           — brpc::UrmaEndpoint::PollCq
#   6.  URMA -> brpc bridge      — brpc::UrmaEndpoint::DispatchReceivedBytes
#   7.  Message dispatch         — brpc::InputMessenger::ProcessNewMessage
#   8.  Parse wire frame        — brpc::policy::ParseRpcMessage
#   9.  Server request handler   — brpc::policy::ProcessRpcRequest
#   10. Send response            — brpc::policy::SendRpcResponse
#   11. Client response handler  — brpc::policy::ProcessRpcResponse
#
# Requires: bpftrace, root, CONFIG_UPROBES, brpc binary not stripped.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [PID]

  -e, --exec CMD  start bpftrace, then exec CMD (captures warm-up/control-plane)
  -n, --name N    trace the process named N
  -p PID          trace this PID
  -b, --bin PATH  force brpc binary path (e.g. ./bazel-bin/example/server)
  -h, --help      this help

Traces 11 stages of the brpc RPC lifecycle via uprobe/uretprobe.
EOF
    exit 1
}

PID=""
NAME=""
EXEC_CMD=""
BRPC_BIN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -e|--exec)  EXEC_CMD="$2"; shift 2 ;;
        -p)         PID="$2"; shift 2 ;;
        -n|--name)  NAME="$2"; shift 2 ;;
        -b|--bin)   BRPC_BIN="$2"; shift 2 ;;
        -h|--help)  usage ;;
        --)         shift; EXEC_CMD="$*"; shift $# ;;
        -*)         echo "unknown option: $1" >&2; usage ;;
        *)          PID="$1"; shift ;;
    esac
done

if [[ -n "${NAME}" ]]; then
    PID="$(pidof -s "${NAME}" 2>/dev/null || pgrep -x -n "${NAME}" 2>/dev/null || true)"
    if [[ -z "${PID}" ]]; then
        echo "no process named '${NAME}' running" >&2
        exit 1
    fi
    echo "resolved name '${NAME}' -> pid ${PID}"
fi

if [[ -n "${EXEC_CMD}" && -n "${PID}" ]]; then
    echo "error: --exec and PID/-n are mutually exclusive" >&2
    exit 1
fi

# --- locate brpc binary --------------------------------------------------------
if [[ -z "${BRPC_BIN}" ]]; then
    if [[ -n "${PID}" ]]; then
        BRPC_BIN="$(readlink -f /proc/${PID}/exe 2>/dev/null || true)"
    fi
fi
if [[ -z "${BRPC_BIN}" ]] && [[ -n "${EXEC_CMD}" ]]; then
    # Extract the binary path from the exec command (first word)
    BRPC_BIN="$(readlink -f "$(echo "${EXEC_CMD}" | awk '{print $1}')" 2>/dev/null || true)"
fi
if [[ -z "${BRPC_BIN}" ]]; then
    echo "error: cannot determine brpc binary path. Use -b /path/to/binary" >&2
    exit 1
fi
if [[ ! -f "${BRPC_BIN}" ]]; then
    echo "error: binary not found: ${BRPC_BIN}" >&2
    exit 1
fi

echo "brpc binary: ${BRPC_BIN}"

# --- sanity checks -----------------------------------------------------------
if ! command -v bpftrace >/dev/null 2>&1; then
    echo "bpftrace not found in PATH" >&2
    exit 1
fi
if ! command -v nm >/dev/null 2>&1; then
    echo "nm not found (binutils required)" >&2
    exit 1
fi
if [[ "$(id -u)" -ne 0 ]]; then
    echo "warning: not running as root; uprobe attach will likely fail" >&2
fi

# --- resolve C++ mangled symbols ----------------------------------------------
# Build a mangled->demangled mapping for all text/weak symbols.
SYMMAP="$(mktemp --tmpdir brpc_symmap.XXXX.tsv)"
trap 'rm -f -- "${SYMMAP}"' EXIT
nm "${BRPC_BIN}" 2>/dev/null | awk '$2=="T" || $2=="W" || $2=="t" || $2=="w" {print $3}' > "${SYMMAP}.mangled"
c++filt < "${SYMMAP}.mangled" > "${SYMMAP}.demangled"
paste "${SYMMAP}.mangled" "${SYMMAP}.demangled" > "${SYMMAP}"
rm -f -- "${SYMMAP}.mangled" "${SYMMAP}.demangled"

# find_sym "pattern" — returns the first mangled symbol whose demangled name
# matches the grep pattern.
find_sym() {
    grep -m1 "$1" "${SYMMAP}" | cut -f1 || true
}

# Stages: "stage_id|display_name|grep_pattern"
STAGES=(
    "1|1.CallMethod|brpc::Channel::CallMethod"
    "2|2.PackRpcRequest|brpc::policy::PackRpcRequest"
    "3|3.SocketWrite|brpc::Socket::Write\b"
    "4|4.UrmaSend|CutFromIOBufList"
    "5|5.UrmaPoll|PollCq"
    "6|6.UrmaDispatch|DispatchReceivedBytes"
    "7|7.ProcessNewMsg|InputMessenger::ProcessNewMessage"
    "8|8.ParseRpcMsg|brpc::policy::ParseRpcMessage"
    "9|9.ProcessRpcReq|brpc::policy::ProcessRpcRequest"
    "10|10.SendRpcResp|brpc::policy::SendRpcResponse"
    "11|11.ProcessRpcResp|brpc::policy::ProcessRpcResponse"
)

echo "resolving symbols..."
FOUND=0
declare -A SYM_MAP
for entry in "${STAGES[@]}"; do
    IFS='|' read -r sid sname spattern <<< "$entry"
    sym="$(find_sym "$spattern")"
    if [[ -n "$sym" ]]; then
        SYM_MAP[$sid]="$sym"
        echo "  [OK]  ${sname} -> ${sym}"
        FOUND=$((FOUND + 1))
    else
        echo "  [SKIP] ${sname} (symbol not found)"
    fi
done
echo "resolved ${FOUND}/${#STAGES[@]} stages"

if [[ "${FOUND}" -eq 0 ]]; then
    echo "error: no symbols found. Is the binary stripped?" >&2
    exit 1
fi

# --- generate bpftrace script -------------------------------------------------
BTFILE="$(mktemp --tmpdir brpc_lifecycle.XXXX.bt)"
trap 'rm -f -- "${SYMMAP}" "${BTFILE}"' EXIT

cat > "${BTFILE}" <<'HEADER'
BEGIN
{
    printf("brpc lifecycle tracer attached. Ctrl-C for final stats.\n\n");
}

HEADER

for entry in "${STAGES[@]}"; do
    IFS='|' read -r sid sname spattern <<< "$entry"
    sym="${SYM_MAP[$sid]:-}"
    if [[ -z "$sym" ]]; then
        continue
    fi
    cat >> "${BTFILE}" <<PROBE

/* ${sname} */
uprobe:${BRPC_BIN}:${sym}
{ @el[tid, ${sid}] = nsecs; }
uretprobe:${BRPC_BIN}:${sym}
/ @el[tid, ${sid}] /
{
    \$d = nsecs - @el[tid, ${sid}];
    @lat["${sname}"] = lhist(\$d / 1000, 0, 1000, 1);
    @sum["${sname}"] = sum(\$d);
    @max["${sname}"] = max(\$d);
    if (\$d > 1000000) {
        @slow["${sname}"] = count();
        printf("[SLOW] ${sname} pid=%lu tid=%lu lat=%llu us\n", pid, tid, \$d/1000);
    }
    delete(@el[tid, ${sid}]);
}
PROBE
done

cat >> "${BTFILE}" <<'FOOTER'

interval:s:5
{
    time("[%H:%M:%S] tracing... (Ctrl-C for final report)\n");
}

END
{
    printf("\n==================== BRPC LIFECYCLE REPORT ====================\n");

    printf("\n--- Total time spent per stage, in ms (@sum / 1e6) ---\n");
    print(@sum);

    printf("\n--- Worst single latency per stage, in ms (@max / 1e6) ---\n");
    print(@max);

    printf("\n--- Slow-call (>1ms) counts (@slow) ---\n");
    print(@slow);

    printf("\n--- Latency histograms (us) per stage (@lat) ---\n");
    print(@lat);

    printf("==============================================================\n");
}
FOOTER

echo ""
echo "generated bpftrace script: ${BTFILE}"
echo ""

# --- dry-run ------------------------------------------------------------------
DRY_ERR="$(mktemp --tmpdir brpc_dryrun.XXXX.log)"
if ! bpftrace -d "${BTFILE}" >/dev/null 2>"${DRY_ERR}"; then
    echo "bpftrace dry-run failed:" >&2
    cat -- "${DRY_ERR}" >&2
    rm -f -- "${DRY_ERR}"
    exit 1
fi
rm -f -- "${DRY_ERR}"

# --- attach -------------------------------------------------------------------
LOGFILE="/tmp/brpc_lifecycle_$(date +%Y%m%d_%H%M%S).log"
echo "trace log: ${LOGFILE}"

trap '' INT
if [[ -n "${EXEC_CMD}" ]]; then
    echo "starting bpftrace, then exec: ${EXEC_CMD}"
    bpftrace "${BTFILE}" 2>&1 | tee "${LOGFILE}" &
    BPID=$!
    sleep 1
    bash -c "${EXEC_CMD}"
    kill -INT "${BPID}" 2>/dev/null || true
    wait "${BPID}" 2>/dev/null || true
elif [[ -n "${PID}" ]]; then
    echo "attaching to pid ${PID}... (Ctrl-C to stop)"
    bpftrace -p "${PID}" "${BTFILE}" 2>&1 | tee "${LOGFILE}" || true
else
    echo "attaching... (Ctrl-C to stop)"
    bpftrace "${BTFILE}" 2>&1 | tee "${LOGFILE}" || true
fi
trap - INT

# --- summary -----------------------------------------------------------------
SUMMARY="${SCRIPT_DIR}/urma_trace_summary.py"
if [[ -f "${SUMMARY}" ]]; then
    PYTHON=""
    for p in python3 python; do
        if command -v "$p" &>/dev/null; then
            PYTHON="$p"; break
        fi
    done
    if [[ -n "${PYTHON}" ]]; then
        echo ""
        echo "=== summary: ${PYTHON} ${SUMMARY} ${LOGFILE} ==="
        "${PYTHON}" "${SUMMARY}" "${LOGFILE}"
    else
        echo "python not found; raw log at ${LOGFILE}" >&2
    fi
fi
