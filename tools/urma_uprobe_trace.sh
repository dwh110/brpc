#!/bin/bash
# urma_uprobe_trace.sh — non-intrusive uprobe tracing of all URMA APIs that
# brpc actually uses, for brpc/URMA performance analysis.
#
# Usage:
#   sudo ./urma_uprobe_trace.sh                 # trace all processes
#   sudo ./urma_uprobe_trace.sh <pid>           # trace a specific brpc PID
#   sudo ./urma_uprobe_trace.sh -n my_server    # trace by process name
#   sudo ./urma_uprobe_trace.sh -l /opt/umdk/lib/liburma.so.0  # force lib path
#
# Data-plane sampled at 1/SAMPLE_RATE (default 100 = 1%). Override:
#   sudo SAMPLE_RATE=50 ./urma_uprobe_trace.sh -n my_server   # sample 1/50
#
# Output goes to stdout (bpftrace). Pair with:
#   sudo ./urma_uprobe_trace.sh <pid> 2>&1 | tee /tmp/urma_trace.log
#
# Requires: bpftrace (>= v0.11), root, CONFIG_UPROBES, liburma.so not stripped.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="${SCRIPT_DIR}/urma_uprobe_trace.bt"

# Sample rate for data-plane probes (1/N). Higher = less overhead, coarser stats.
SAMPLE_RATE="${SAMPLE_RATE:-100}"

usage() {
    cat <<EOF
Usage: $0 [-n NAME] [-l LIBPATH] [-p PID] [PID]

  PID             trace this brpc PID (positional or -p)
  -n, --name N    trace the process named N (resolved via pidof/pgrep)
  -l, --lib PATH  force liburma.so path (e.g. /opt/umdk/lib/liburma.so.0)
  -h, --help      this help

If neither PID nor -n is given, traces ALL processes using liburma.so.
If -l is omitted, the lib path is auto-detected:
  - from /proc/<PID>/maps when a PID is given, else
  - from ldconfig -p cache.
EOF
    exit 1
}

PID=""
NAME=""
LIB=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p)         PID="$2"; shift 2 ;;
        -n|--name)  NAME="$2"; shift 2 ;;
        -l|--lib)   LIB="$2"; shift 2 ;;
        -h|--help)  usage ;;
        -*)         echo "unknown option: $1" >&2; usage ;;
        *)          PID="$1"; shift ;;
    esac
done

if [[ -n "${NAME}" ]]; then
    if ! command -v pidof >/dev/null 2>&1; then
        echo "pidof not available; install sysvinit-tools or use -p PID" >&2
        exit 1
    fi
    PID="$(pidof -s "${NAME}" 2>/dev/null || true)"
    if [[ -z "${PID}" ]]; then
        PID="$(pgrep -x -n "${NAME}" 2>/dev/null || true)"
    fi
    if [[ -z "${PID}" ]]; then
        echo "no process named '${NAME}' running" >&2
        exit 1
    fi
    echo "resolved name '${NAME}' -> pid ${PID}"
fi

# --- locate liburma.so --------------------------------------------------------
if [[ -z "${LIB}" ]]; then
    if [[ -n "${PID}" ]]; then
        # /proc/PID/maps column 6 is the file path; match liburma.so anywhere.
        LIB="$(awk '/liburma\.so/ {print $6; exit}' "/proc/${PID}/maps" 2>/dev/null || true)"
    fi
    if [[ -z "${LIB}" ]]; then
        # fall back to ldconfig cache
        LIB="$(ldconfig -p 2>/dev/null | awk '/liburma\.so/ {print $NF; exit}' || true)"
    fi
    if [[ -z "${LIB}" ]]; then
        LIB="liburma.so"   # last resort: let bpftrace resolve via ld.so
    fi
fi

# --- sanity checks -----------------------------------------------------------
if ! command -v bpftrace >/dev/null 2>&1; then
    echo "bpftrace not found in PATH" >&2
    exit 1
fi
if [[ "$(id -u)" -ne 0 ]]; then
    echo "warning: not running as root; uprobe attach will likely fail" >&2
fi

# Verify the lib is readable and the symbols we attach to actually exist,
# otherwise bpftrace will silently skip the missing probes.
if [[ -f "${LIB}" ]]; then
    if command -v nm >/dev/null 2>&1; then
        SYMS="$(nm -D --defined-only "${LIB}" 2>/dev/null | awk '{print $NF}' | grep -c '^urma_' || true)"
        echo "lib: ${LIB}  (urmasym count: ${SYMS})"
        if [[ "${SYMS}" -eq 0 ]]; then
            echo "warning: no urma_* symbols in ${LIB}; the lib may be stripped." >&2
            echo "         bpftrace will attach by offset if you provide -l with a debug build." >&2
        fi
    else
        echo "lib: ${LIB}  (nm unavailable, skipping symbol check)"
    fi
else
    echo "lib: ${LIB} (not a regular file; relying on dynamic loader resolution)"
fi

# --- render template ----------------------------------------------------------
RENDERED="$(mktemp --tmpdir urma_uprobe_trace.XXXX.bt)"
trap 'rm -f -- "${RENDERED}"' EXIT
sed -e "s|@__LIBURMA__@|${LIB}|g" \
    -e "s|@__SAMPLE_RATE__@|${SAMPLE_RATE}|g" \
    "${TEMPLATE}" > "${RENDERED}"

echo "sample_rate: 1/${SAMPLE_RATE}  (override via SAMPLE_RATE=N)"

# dry-run: parse/compile only (-d), does not attach. Catches template errors.
DRY_ERR="$(mktemp --tmpdir urma_dryrun.XXXX.log)"
if ! bpftrace -d "${RENDERED}" >/dev/null 2>"${DRY_ERR}"; then
    echo "bpftrace dry-run failed:" >&2
    cat -- "${DRY_ERR}" >&2
    rm -f -- "${DRY_ERR}"
    exit 1
fi
rm -f -- "${DRY_ERR}"

# --- attach -------------------------------------------------------------------
LOGFILE="/tmp/urma_trace_$(date +%Y%m%d_%H%M%S).log"
trap 'rm -f -- "${RENDERED}"' EXIT

echo "attaching... (Ctrl-C to stop and print final report)"
if [[ -n "${PID}" ]]; then
    bpftrace -p "${PID}" "${RENDERED}" 2>&1 | tee "${LOGFILE}"
else
    bpftrace "${RENDERED}" 2>&1 | tee "${LOGFILE}"
fi

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
else
    echo "summary script not found: ${SUMMARY}; raw log at ${LOGFILE}" >&2
fi
