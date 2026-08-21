// SPDX-License-Identifier: Apache-2.0
//
// urma_ldpreload_wrap.c — LD_PRELOAD intercept library for full-stack
// URMA + UB shared-memory + socket-syscall latency tracing.
//
// Advantage over uprobe: ~10-20ns per call (function call overhead only,
// no kernel trap). uprobe is ~200-500ns per call.
//
// Usage:
//   gcc -shared -fPIC -O2 -o liburma_trace_wrap.so tools/urma_ldpreload_wrap.c -ldl -lpthread
//   LD_PRELOAD=./liburma_trace_wrap.so URMA_TRACE_FILE=/tmp/trace.log ./server
//
// Wrapped layers:
//   1. URMA transport (liburma.so): post_jetty_send_wr, poll_jfc, etc.
//   2. UB shared memory (libubsm_sdk.so): ubsmem_initialize, create_region, etc.
//   3. Socket syscalls (libc): readv, writev, epoll_wait
//
// brpc framework functions (Channel::CallMethod, etc.) are statically linked
// and cannot be intercepted via LD_PRELOAD. Use tools/brpc_lifecycle_trace.sh
// (uprobe-based) for those.

#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/epoll.h>

// ===========================================================================
// Stats infrastructure
// ===========================================================================

#define MAX_FUNCS 64

typedef struct {
    _Atomic uint64_t count;
    _Atomic uint64_t total_ns;
    _Atomic uint64_t max_ns;
} func_stats_t;

static func_stats_t g_stats[MAX_FUNCS];
static const char* g_names[MAX_FUNCS];
static _Atomic int g_next_id = 0;

// Per-thread recursion guard: prevents tracing of tracing code's own I/O
static __thread int g_in_trace = 0;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// Register a function name and get its ID. Called once at init.
static int register_func(const char* name) {
    int id = atomic_fetch_add(&g_next_id, 1);
    if (id < MAX_FUNCS) {
        g_names[id] = name;
    }
    return id;
}

#define REG(name) ({                                          \
    static _Atomic int _id = -1;                              \
    int _v = atomic_load(&_id);                               \
    if (_v < 0) {                                             \
        _v = register_func(#name);                            \
        atomic_store(&_id, _v);                               \
    }                                                         \
    _v;                                                       \
})

// Record a timing sample
static inline void record(int fid, uint64_t delta_ns) {
    atomic_fetch_add(&g_stats[fid].count, 1);
    atomic_fetch_add(&g_stats[fid].total_ns, delta_ns);
    uint64_t old = atomic_load(&g_stats[fid].max_ns);
    while (delta_ns > old) {
        if (atomic_compare_exchange_weak(&g_stats[fid].max_ns, &old, delta_ns))
            break;
    }
    // Slow-call log (>1ms)
    if (delta_ns > 1000000ULL && !g_in_trace) {
        g_in_trace = 1;
        fprintf(stderr, "[SLOW] %s lat=%llu us\n",
                g_names[fid], (unsigned long long)(delta_ns / 1000));
        g_in_trace = 0;
    }
}

// Macro to define a wrapper for a function returning int/uint32_t
#define WRAP_INT(name, sig_args, call_args)                              \
    static int (*real_##name)sig_args = NULL;                            \
    int name sig_args {                                                  \
        if (!real_##name)                                                \
            real_##name = (int(*)sig_args)dlsym(RTLD_NEXT, #name);       \
        if (g_in_trace) return real_##name call_args;                    \
        g_in_trace = 1;                                                  \
        int _fid = REG(name);                                           \
        uint64_t _t0 = now_ns();                                         \
        int _r = real_##name call_args;                                  \
        record(_fid, now_ns() - _t0);                                    \
        g_in_trace = 0;                                                  \
        return _r;                                                       \
    }

// Macro for void-returning functions
#define WRAP_VOID(name, sig_args, call_args)                             \
    static void (*real_##name)sig_args = NULL;                          \
    void name sig_args {                                                 \
        if (!real_##name)                                                \
            real_##name = (void(*)sig_args)dlsym(RTLD_NEXT, #name);      \
        if (g_in_trace) { real_##name call_args; return; }              \
        g_in_trace = 1;                                                  \
        int _fid = REG(name);                                           \
        uint64_t _t0 = now_ns();                                         \
        real_##name call_args;                                           \
        record(_fid, now_ns() - _t0);                                    \
        g_in_trace = 0;                                                  \
    }

// Macro for ssize_t-returning functions
#define WRAP_SSIZE(name, sig_args, call_args)                            \
    static ssize_t (*real_##name)sig_args = NULL;                       \
    ssize_t name sig_args {                                              \
        if (!real_##name)                                                \
            real_##name = (ssize_t(*)sig_args)dlsym(RTLD_NEXT, #name);   \
        if (g_in_trace) return real_##name call_args;                    \
        g_in_trace = 1;                                                  \
        int _fid = REG(name);                                           \
        uint64_t _t0 = now_ns();                                         \
        ssize_t _r = real_##name call_args;                              \
        record(_fid, now_ns() - _t0);                                    \
        g_in_trace = 0;                                                  \
        return _r;                                                       \
    }

// ===========================================================================
// URMA data-plane wrappers (liburma.so)
// ===========================================================================

WRAP_INT(urma_post_jetty_send_wr,
         (void* jetty, void* wr, const void** bad_wr),
         (jetty, wr, bad_wr))

WRAP_INT(urma_post_jetty_recv_wr,
         (void* jetty, void* wr, const void** bad_wr),
         (jetty, wr, bad_wr))

WRAP_INT(urma_post_jfr_wr,
         (void* jfr, void* wr, const void** bad_wr),
         (jfr, wr, bad_wr))

// urma_poll_jfc returns uint32_t (= unsigned int in practice)
static unsigned int (*real_urma_poll_jfc)(void**, unsigned int*, unsigned int) = NULL;
unsigned int urma_poll_jfc(void* jfc[], unsigned int nevents[], unsigned int jfc_cnt) {
    if (!real_urma_poll_jfc)
        real_urma_poll_jfc = (unsigned int(*)(void**, unsigned int*, unsigned int))
                             dlsym(RTLD_NEXT, "urma_poll_jfc");
    if (g_in_trace) return real_urma_poll_jfc(jfc, nevents, jfc_cnt);
    g_in_trace = 1;
    int fid = REG(urma_poll_jfc);
    uint64_t t0 = now_ns();
    unsigned int r = real_urma_poll_jfc(jfc, nevents, jfc_cnt);
    record(fid, now_ns() - t0);
    g_in_trace = 0;
    return r;
}

WRAP_INT(urma_rearm_jfc,
         (void** jfc, unsigned int jfc_cnt),
         (jfc, jfc_cnt))

// urma_wait_jfc has int32_t timeout as 4th arg
WRAP_INT(urma_wait_jfc,
         (void** jfc, unsigned int* nevents, unsigned int jfc_cnt, int timeout),
         (jfc, nevents, jfc_cnt, timeout))

WRAP_VOID(urma_ack_jfc,
          (void** jfc, unsigned int* nevents, unsigned int jfc_cnt),
          (jfc, nevents, jfc_cnt))

// ===========================================================================
// URMA control-plane wrappers (liburma.so) — warm-up critical path
// ===========================================================================

WRAP_INT(urma_init, (void* conf), (conf))
WRAP_INT(urma_uninit, (void), ())
WRAP_INT(urma_create_jetty,
         (void* jetty, const void* attr, void* ctx, void* alloc_param),
         (jetty, attr, ctx, alloc_param))
WRAP_INT(urma_import_jetty,
         (void* ctx, const void* remote, void* token),
         (ctx, remote, token))
WRAP_INT(urma_import_seg,
         (void* ctx, const void* seg, void* token, int flags, void* flag_struct),
         (ctx, seg, token, flags, flag_struct))
WRAP_INT(urma_register_seg,
         (void* ctx, void* seg, void* attr),
         (ctx, seg, attr))
WRAP_INT(urma_create_context,
         (void* dev, unsigned int eid_index),
         (dev, eid_index))
WRAP_INT(urma_user_ctl,
         (void* ctx, void* in, void* out),
         (ctx, in, out))

// ===========================================================================
// UB shared memory wrappers (libubsm_sdk.so)
// ===========================================================================

WRAP_INT(ubsmem_initialize, (const void* opts), (opts))
WRAP_INT(ubsmem_finalize, (void), ())
WRAP_INT(ubsmem_create_region,
         (const char* name, size_t size, const void* attr),
         (name, size, attr))
WRAP_INT(ubsmem_shmem_allocate,
         (const char* region, const char* nm, size_t sz, int mode, void** ptr, size_t* off),
         (region, nm, sz, mode, ptr, off))
WRAP_INT(ubsmem_shmem_map,
         (void* addr, size_t len, int prot, int flags, const char* nm, long off, void** ptr),
         (addr, len, prot, flags, nm, off, ptr))

// ===========================================================================
// Socket syscall wrappers (libc)
// ===========================================================================

WRAP_SSIZE(readv,
           (int fd, const struct iovec* iov, int iovcnt),
           (fd, iov, iovcnt))

WRAP_SSIZE(writev,
           (int fd, const struct iovec* iov, int iovcnt),
           (fd, iov, iovcnt))

WRAP_INT(epoll_wait,
         (int epfd, struct epoll_event* events, int maxevents, int timeout),
         (epfd, events, maxevents, timeout))

// ===========================================================================
// Destructor: dump stats to file
// ===========================================================================

__attribute__((destructor))
static void dump_stats(void) {
    if (g_in_trace) return;
    g_in_trace = 1;

    const char* path = getenv("URMA_TRACE_FILE");
    if (!path) path = "/tmp/urma_ldpreload_trace.log";

    FILE* f = fopen(path, "w");
    if (!f) {
        // Fallback to stderr
        f = stderr;
    }

    fprintf(f, "==================== LD_PRELOAD TRACE ====================\n");
    fprintf(f, "%-32s %12s %14s %12s %12s\n",
            "Function", "Calls", "Total(ms)", "Avg(us)", "Max(us)");
    fprintf(f, "-----------------------------------------------------------------\n");

    for (int i = 0; i < g_next_id && i < MAX_FUNCS; i++) {
        uint64_t cnt = atomic_load(&g_stats[i].count);
        if (cnt == 0) continue;
        uint64_t total = atomic_load(&g_stats[i].total_ns);
        uint64_t mx = atomic_load(&g_stats[i].max_ns);
        double avg_us = (double)(total / cnt) / 1000.0;
        fprintf(f, "%-32s %12llu %14.3f %12.2f %12.2f\n",
                g_names[i] ? g_names[i] : "(unknown)",
                (unsigned long long)cnt,
                (double)total / 1e6,
                avg_us,
                (double)mx / 1000.0);
    }

    fprintf(f, "===========================================================\n");
    if (f != stderr) fclose(f);

    g_in_trace = 0;
}
