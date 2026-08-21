#!/bin/bash
# urma_pipeline_trace.sh — full-pipeline uprobe tracing for urma_performance,
# covering client encode → URMA send → URMA recv → server decode → business
# → response send, with hierarchical latency breakdown.
#
# Usage:
#   sudo ./urma_pipeline_trace.sh -e ./server -- --port 8003   # start & trace
#   sudo ./urma_pipeline_trace.sh -p <pid>                      # trace running
#   sudo ./urma_pipeline_trace.sh -n my_server                  # by name
#
# Traces both brpc binary functions (C++ mangled) and liburma.so functions.
# Output is piped through urma_pipeline_summary.py for hierarchical display.
#
# Requires: bpftrace, root, nm/c++filt, python3, non-stripped binaries.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [PID]

  -e, --exec CMD  start bpftrace, then exec CMD (captures warm-up)
  -n, --name N    trace process named N
  -p PID          trace this PID
  -b, --bin PATH   force brpc binary path
  -l, --lib PATH   force liburma.so path
  -h, --help      this help
EOF
    exit 1
}

PID=""
NAME=""
EXEC_CMD=""
BRPC_BIN=""
LIBURMA=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -e|--exec)  EXEC_CMD="$2"; shift 2 ;;
        -p)         PID="$2"; shift 2 ;;
        -n|--name)  NAME="$2"; shift 2 ;;
        -b|--bin)   BRPC_BIN="$2"; shift 2 ;;
        -l|--lib)   LIBURMA="$2"; shift 2 ;;
        -h|--help)  usage ;;
        --)         shift; EXEC_CMD="$*"; shift $# ;;
        -*)         echo "unknown option: $1" >&2; usage ;;
        *)          PID="$1"; shift ;;
    esac
done

if [[ -n "${NAME}" ]]; then
    PID="$(pidof -s "${NAME}" 2>/dev/null || pgrep -x -n "${NAME}" 2>/dev/null || true)"
    [[ -z "${PID}" ]] && { echo "no process named '${NAME}'" >&2; exit 1; }
    echo "resolved '${NAME}' -> pid ${PID}"
fi
[[ -n "${EXEC_CMD}" && -n "${PID}" ]] && { echo "--exec and PID are mutually exclusive" >&2; exit 1; }

# --- locate brpc binary --------------------------------------------------------
if [[ -z "${BRPC_BIN}" ]]; then
    if [[ -n "${PID}" ]]; then
        BRPC_BIN="$(readlink -f /proc/${PID}/exe 2>/dev/null || true)"
    elif [[ -n "${EXEC_CMD}" ]]; then
        BRPC_BIN="$(readlink -f "$(echo "${EXEC_CMD}" | awk '{print $1}')" 2>/dev/null || true)"
    fi
fi
[[ -z "${BRPC_BIN}" || ! -f "${BRPC_BIN}" ]] && { echo "error: need -b /path/to/binary" >&2; usage; }
echo "brpc binary: ${BRPC_BIN}"

# --- locate liburma.so ---------------------------------------------------------
if [[ -z "${LIBURMA}" ]]; then
    if [[ -n "${PID}" ]]; then
        LIBURMA="$(awk '/liburma\.so/ {print $6; exit}' /proc/${PID}/maps 2>/dev/null || true)"
    fi
    [[ -z "${LIBURMA}" ]] && LIBURMA="$(ldconfig -p 2>/dev/null | awk '/liburma\.so/ {print $NF; exit}' || true)"
    [[ -z "${LIBURMA}" ]] && LIBURMA="liburma.so"
fi
echo "liburma: ${LIBURMA}"

# --- sanity -------------------------------------------------------------------
command -v bpftrace >/dev/null 2>&1 || { echo "bpftrace not found" >&2; exit 1; }
command -v nm >/dev/null 2>&1 || { echo "nm not found" >&2; exit 1; }
[[ "$(id -u)" -ne 0 ]] && echo "warning: not root" >&2

# --- resolve brpc C++ symbols -------------------------------------------------
SYMMAP="$(mktemp --tmpdir brpc_symmap.XXXX.tsv)"
trap 'rm -f -- "${SYMMAP}" "${BTFILE}"' EXIT
nm "${BRPC_BIN}" 2>/dev/null | awk '$2=="T"||$2=="W"||$2=="t"||$2=="w"{print $3}' > "${SYMMAP}.m"
c++filt < "${SYMMAP}.m" > "${SYMMAP}.d"
paste "${SYMMAP}.m" "${SYMMAP}.d" > "${SYMMAP}"
rm -f -- "${SYMMAP}.m" "${SYMMAP}.d"

find_sym() { grep -m1 "$1" "${SYMMAP}" | cut -f1 || true; }

# brpc pipeline stages: "id|display_name|grep_pattern"
BRPC_STAGES=(
    "101|1.CallMethod|brpc::Channel::CallMethod"
    "102|2.PackRpcRequest|brpc::policy::PackRpcRequest"
    "103|3.SocketWrite|brpc::Socket::Write\b"
    "104|4.CutFromIOBufList|CutFromIOBufList"
    "105|5.DispatchReceivedBytes|DispatchReceivedBytes"
    "106|6.ProcessNewMessage|InputMessenger::ProcessNewMessage"
    "107|7.ParseRpcMessage|brpc::policy::ParseRpcMessage"
    "108|8.ProcessRpcRequest|brpc::policy::ProcessRpcRequest"
    "109|9.SendRpcResponse|brpc::policy::SendRpcResponse"
    "110|10.ProcessRpcResponse|brpc::policy::ProcessRpcResponse"
)

# URMA data-plane stages (plain C names, no nm resolution needed)
URMA_STAGES=(
    "201|11.urma_post_jetty_send_wr|urma_post_jetty_send_wr"
    "202|12.urma_post_jetty_recv_wr|urma_post_jetty_recv_wr"
    "203|13.urma_post_jfr_wr|urma_post_jfr_wr"
    "204|14.urma_poll_jfc|urma_poll_jfc"
    "205|15.urma_rearm_jfc|urma_rearm_jfc"
    "206|16.urma_wait_jfc|urma_wait_jfc"
    "207|17.urma_ack_jfc|urma_ack_jfc"
)

# URMA control-plane stages (warm-up / connection setup)
URMA_CP_STAGES=(
    "301|CP1.urma_init|urma_init"
    "302|CP2.urma_uninit|urma_uninit"
    "303|CP3.urma_get_device_list|urma_get_device_list"
    "304|CP4.urma_free_device_list|urma_free_device_list"
    "305|CP5.urma_query_device|urma_query_device"
    "306|CP6.urma_get_eid_list|urma_get_eid_list"
    "307|CP7.urma_free_eid_list|urma_free_eid_list"
    "308|CP8.urma_create_context|urma_create_context"
    "309|CP9.urma_delete_context|urma_delete_context"
    "310|CP10.urma_user_ctl|urma_user_ctl"
    "311|CP11.urma_create_jfce|urma_create_jfce"
    "312|CP12.urma_delete_jfce|urma_delete_jfce"
    "313|CP13.urma_create_jfc|urma_create_jfc"
    "314|CP14.urma_delete_jfc|urma_delete_jfc"
    "315|CP15.urma_create_jfr|urma_create_jfr"
    "316|CP16.urma_delete_jfr|urma_delete_jfr"
    "317|CP17.urma_create_jetty|urma_create_jetty"
    "318|CP18.urma_delete_jetty|urma_delete_jetty"
    "319|CP19.urma_modify_jetty|urma_modify_jetty"
    "320|CP20.urma_unbind_jetty|urma_unbind_jetty"
    "321|CP21.urma_register_seg|urma_register_seg"
    "322|CP22.urma_unregister_seg|urma_unregister_seg"
    "323|CP23.urma_import_seg|urma_import_seg"
    "324|CP24.urma_unimport_seg|urma_unimport_seg"
    "325|CP25.urma_import_jetty|urma_import_jetty"
    "326|CP26.urma_unimport_jetty|urma_unimport_jetty"
)

echo "resolving symbols..."
FOUND=0
declare -A SYM_MAP
for entry in "${BRPC_STAGES[@]}"; do
    IFS='|' read -r sid sname spattern <<< "$entry"
    sym="$(find_sym "$spattern")"
    if [[ -n "$sym" ]]; then
        SYM_MAP[$sid]="$sym"
        echo "  [OK]  ${sname} -> ${sym}"
        FOUND=$((FOUND + 1))
    else
        echo "  [SKIP] ${sname} (not found)"
    fi
done
for entry in "${URMA_STAGES[@]}"; do
    IFS='|' read -r sid sname sfunc <<< "$entry"
    SYM_MAP[$sid]="$sfunc"
    echo "  [URMA-DP] ${sname} -> ${sfunc}"
    FOUND=$((FOUND + 1))
done
for entry in "${URMA_CP_STAGES[@]}"; do
    IFS='|' read -r sid sname sfunc <<< "$entry"
    SYM_MAP[$sid]="$sfunc"
    echo "  [URMA-CP] ${sname} -> ${sfunc}"
    FOUND=$((FOUND + 1))
done
echo "resolved ${FOUND} stages"

# --- generate bpftrace script -------------------------------------------------
BTFILE="$(mktemp --tmpdir urma_pipeline.XXXX.bt)"

cat > "${BTFILE}" <<'HEADER'
BEGIN
{
    printf("urma pipeline tracer attached. Ctrl-C for final stats.\n\n");
}

HEADER

# brpc binary probes (use resolved mangled symbols)
for entry in "${BRPC_STAGES[@]}"; do
    IFS='|' read -r sid sname spattern <<< "$entry"
    sym="${SYM_MAP[$sid]:-}"
    [[ -z "$sym" ]] && continue
    cat >> "${BTFILE}" <<PROBE

/* ${sname} (brpc binary) */
uprobe:${BRPC_BIN}:${sym}
{ @el[tid, ${sid}] = nsecs; }
uretprobe:${BRPC_BIN}:${sym}
/ @el[tid, ${sid}] /
{
    \$d = nsecs - @el[tid, ${sid}];
    @lat["${sname}"] = lhist(\$d / 1000, 0, 1000, 10);
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

# liburma.so probes (plain C function names)
for entry in "${URMA_STAGES[@]}"; do
    IFS='|' read -r sid sname sfunc <<< "$entry"
    cat >> "${BTFILE}" <<PROBE

/* ${sname} (liburma.so) */
uprobe:${LIBURMA}:${sfunc}
{ @el[tid, ${sid}] = nsecs; }
uretprobe:${LIBURMA}:${sfunc}
/ @el[tid, ${sid}] /
{
    \$d = nsecs - @el[tid, ${sid}];
    @lat["${sname}"] = lhist(\$d / 1000, 0, 1000, 10);
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

# liburma.so control-plane probes (warm-up / connection setup)
for entry in "${URMA_CP_STAGES[@]}"; do
    IFS='|' read -r sid sname sfunc <<< "$entry"
    cat >> "${BTFILE}" <<PROBE

/* ${sname} (liburma.so control-plane) */
uprobe:${LIBURMA}:${sfunc}
{ @el[tid, ${sid}] = nsecs; }
uretprobe:${LIBURMA}:${sfunc}
/ @el[tid, ${sid}] /
{
    \$d = nsecs - @el[tid, ${sid}];
    @lat["${sname}"] = lhist(\$d / 1000, 0, 10000, 100);
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
    time("[%H:%M:%S] tracing... (Ctrl-C for pipeline report)\n");
}

END
{
    printf("\n==================== PIPELINE REPORT ====================\n");
    printf("\n--- Total time per stage (@sum / 1e6 ms) ---\n");
    print(@sum);
    printf("\n--- Max latency per stage (@max / 1e6 ms) ---\n");
    print(@max);
    printf("\n--- Slow calls >1ms (@slow) ---\n");
    print(@slow);
    printf("\n--- Latency histograms (us) (@lat) ---\n");
    print(@lat);
    printf("========================================================\n");
}
FOOTER

echo ""
echo "generated: ${BTFILE}"

# --- dry-run ------------------------------------------------------------------
DRY_ERR="$(mktemp --tmpdir urma_dryrun.XXXX.log)"
if ! bpftrace -d "${BTFILE}" >/dev/null 2>"${DRY_ERR}"; then
    echo "bpftrace dry-run failed:" >&2
    cat -- "${DRY_ERR}" >&2
    rm -f -- "${DRY_ERR}"
    exit 1
fi
rm -f -- "${DRY_ERR}"

# --- attach -------------------------------------------------------------------
LOGFILE="/tmp/urma_pipeline_$(date +%Y%m%d_%H%M%S).log"
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

# --- pipeline summary ---------------------------------------------------------
SUMMARY="${SCRIPT_DIR}/urma_pipeline_summary.py"
if [[ -f "${SUMMARY}" ]]; then
    PYTHON=""
    for p in python3 python; do
        command -v "$p" >/dev/null 2>&1 && { PYTHON="$p"; break; }
    done
    if [[ -n "${PYTHON}" ]]; then
        echo ""
        echo "=== pipeline: ${PYTHON} ${SUMMARY} ${LOGFILE} ==="
        "${PYTHON}" "${SUMMARY}" "${LOGFILE}"
    else
        echo "python not found; raw log at ${LOGFILE}" >&2
    fi
fi
