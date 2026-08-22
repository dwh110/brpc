// SPDX-License-Identifier: Apache-2.0
//
// urma_ldpreload_wrap.c — LD_PRELOAD intercept for URMA transport layer
// (liburma.so) only.
//
// Architecture: brpc → ubsocket(LD_PRELOAD) → umq → URMA(liburma.so)
// - ubsocket has built-in profiling (UBSOCKET_SPLIT_TRACE_ENABLE) covering
//   brpc + ubsocket + umq layers.
// - This library adds tracing for the URMA hardware layer, which is the one
//   layer NOT covered by ubsocket's built-in profiling.
//
// Loading order (this lib BEFORE libubsocket.so):
//   LD_PRELOAD=./liburma_trace_wrap.so:/path/to/libubsocket.so ./server
//
// When umq internally calls urma_post_jetty_send_wr() etc., this library
// intercepts (if umq resolves through dynamic symbol table) or misses
// (if umq uses dlsym function pointers — in that case use uprobe instead).
//
// When brpc directly calls urma_* functions (e.g. UrmaEndpoint), this
// library intercepts them reliably.
//
// Usage:
//   gcc -shared -fPIC -O2 -o liburma_trace_wrap.so tools/urma_ldpreload_wrap.c -ldl -lpthread
//   LD_PRELOAD=./liburma_trace_wrap.so:./libubsocket.so \
//     UBSOCKET_SPLIT_TRACE_ENABLE=true UBSOCKET_SPLIT_TRACE_LEVEL=all \
//     ./server

#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <stdint.h>
#include <unistd.h>

#define MAX_FUNCS 64

typedef struct {
    _Atomic uint64_t count;
    _Atomic uint64_t total_ns;
    _Atomic uint64_t max_ns;
} func_stats_t;

static func_stats_t g_stats[MAX_FUNCS];
static const char* g_names[MAX_FUNCS];
static _Atomic int g_next_id = 0;
static __thread int g_in_trace = 0;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int reg_fn(const char* name) {
    int id = atomic_fetch_add(&g_next_id, 1);
    if (id < MAX_FUNCS) g_names[id] = name;
    return id;
}

#define REG(name) ({ static _Atomic int _id = -1; int _v = atomic_load(&_id); \
    if (_v < 0) { _v = reg_fn(#name); atomic_store(&_id, _v); } _v; })

static inline void record(int fid, uint64_t d) {
    atomic_fetch_add(&g_stats[fid].count, 1);
    atomic_fetch_add(&g_stats[fid].total_ns, d);
    uint64_t o = atomic_load(&g_stats[fid].max_ns);
    while (d > o) { if (atomic_compare_exchange_weak(&g_stats[fid].max_ns, &o, d)) break; }
    if (d > 1000000ULL && !g_in_trace) {
        g_in_trace = 1;
        fprintf(stderr, "[SLOW-URMA] %s lat=%llu us\n", g_names[fid], (unsigned long long)(d/1000));
        g_in_trace = 0;
    }
}

#define WRAP_INT(name, sig, call) \
    static int (*real_##name)sig = NULL; \
    int name sig { \
        if (!real_##name) real_##name = (int(*)sig)dlsym(RTLD_NEXT, #name); \
        if (g_in_trace) return real_##name call; \
        g_in_trace = 1; int fid = REG(name); uint64_t t0 = now_ns(); \
        int r = real_##name call; record(fid, now_ns()-t0); g_in_trace = 0; return r; }

#define WRAP_VOID(name, sig, call) \
    static void (*real_##name)sig = NULL; \
    void name sig { \
        if (!real_##name) real_##name = (void(*)sig)dlsym(RTLD_NEXT, #name); \
        if (g_in_trace) { real_##name call; return; } \
        g_in_trace = 1; int fid = REG(name); uint64_t t0 = now_ns(); \
        real_##name call; record(fid, now_ns()-t0); g_in_trace = 0; }

// === URMA data-plane ===
WRAP_INT(urma_post_jetty_send_wr, (void* a, void* b, const void** c), (a, b, c))
WRAP_INT(urma_post_jetty_recv_wr, (void* a, void* b, const void** c), (a, b, c))
WRAP_INT(urma_post_jfr_wr, (void* a, void* b, const void** c), (a, b, c))

static unsigned int (*real_urma_poll_jfc)(void**, unsigned int*, unsigned int) = NULL;
unsigned int urma_poll_jfc(void* a[], unsigned int b[], unsigned int c) {
    if (!real_urma_poll_jfc)
        real_urma_poll_jfc = (unsigned int(*)(void**, unsigned int*, unsigned int))dlsym(RTLD_NEXT, "urma_poll_jfc");
    if (g_in_trace) return real_urma_poll_jfc(a, b, c);
    g_in_trace = 1; int fid = REG(urma_poll_jfc); uint64_t t0 = now_ns();
    unsigned int r = real_urma_poll_jfc(a, b, c); record(fid, now_ns()-t0); g_in_trace = 0; return r;
}

WRAP_INT(urma_rearm_jfc, (void** a, unsigned int b), (a, b))
WRAP_INT(urma_wait_jfc, (void** a, unsigned int* b, unsigned int c, int d), (a, b, c, d))
WRAP_VOID(urma_ack_jfc, (void** a, unsigned int* b, unsigned int c), (a, b, c))

// === URMA control-plane ===
WRAP_INT(urma_init, (void* a), (a))
WRAP_INT(urma_uninit, (void), ())
WRAP_INT(urma_create_jetty, (void* a, const void* b, void* c, void* d), (a, b, c, d))
WRAP_INT(urma_import_jetty, (void* a, const void* b, void* c), (a, b, c))
WRAP_INT(urma_import_seg, (void* a, const void* b, void* c, int d, void* e), (a, b, c, d, e))
WRAP_INT(urma_register_seg, (void* a, void* b, void* c), (a, b, c))
WRAP_INT(urma_create_context, (void* a, unsigned int b), (a, b))
WRAP_INT(urma_user_ctl, (void* a, void* b, void* c), (a, b, c))

// === Destructor ===
__attribute__((destructor))
static void dump_stats(void) {
    if (g_in_trace) return;
    g_in_trace = 1;
    const char* path = getenv("URMA_TRACE_FILE");
    if (!path) path = "/tmp/urma_ldpreload_trace.log";
    FILE* f = fopen(path, "w");
    if (!f) f = stderr;
    fprintf(f, "==================== URMA LD_PRELOAD TRACE ====================\n");
    fprintf(f, "%-32s %12s %14s %12s %12s\n", "Function", "Calls", "Total(ms)", "Avg(us)", "Max(us)");
    fprintf(f, "-----------------------------------------------------------------\n");
    for (int i = 0; i < g_next_id && i < MAX_FUNCS; i++) {
        uint64_t cnt = atomic_load(&g_stats[i].count);
        if (cnt == 0) continue;
        uint64_t total = atomic_load(&g_stats[i].total_ns);
        uint64_t mx = atomic_load(&g_stats[i].max_ns);
        fprintf(f, "%-32s %12llu %14.3f %12.2f %12.2f\n",
                g_names[i] ? g_names[i] : "(unknown)",
                (unsigned long long)cnt, (double)total/1e6,
                (double)(total/cnt)/1000.0, (double)mx/1000.0);
    }
    fprintf(f, "================================================================\n");
    if (f != stderr) fclose(f);
    g_in_trace = 0;
}
