// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#if BRPC_WITH_URMA

#include <pthread.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <vector>
#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/containers/flat_map.h"
#include "butil/fd_guard.h"
#include "butil/fd_utility.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_helper.h"

DEFINE_bool(urma_use_polling, false,
            "Use polling mode for JFC instead of event mode");
DEFINE_int32(urma_poller_num, 1,
             "Number of pollers per bthread tag in polling mode");
DEFINE_bool(urma_disable_bthread, false,
            "Process messages inline without spawning bthread");
DEFINE_int32(urma_sq_size, 128,
             "Depth of local Jetty Send Queue [16, 4096]");
DEFINE_int32(urma_rq_size, 128,
             "Depth of local Jetty Recv Queue [16, 4096]");
DEFINE_int32(urma_cqe_poll_once, 32,
             "Max CQEs fetched per urma_poll_jfc call");
DEFINE_bool(urma_recv_zerocopy, true,
            "Use zero-copy recv for messages larger than urma_zerocopy_min_size");
DEFINE_int32(urma_zerocopy_min_size, 512,
             "Messages smaller than this are copied instead of zero-copy");
DEFINE_string(urma_device, "",
              "The name of the URMA device to use "
              "(empty means using the first active device)");
DEFINE_int32(urma_max_sge, 0,
             "Max SGE num in a WR (0 means device limit)");
DEFINE_int32(urma_bonding_mode, 0,
             "Bonding mode: 0=standalone, 1=active-backup, 2=balance");
DEFINE_int32(urma_bonding_level, 0,
             "Bonding level: 0=IODIE, 1=port");
DEFINE_int32(urma_prepared_jetty_cnt, 8,
             "Number of pre-created Jetty+CQ requests; "
             "automatically limited by RLIMIT_NOFILE");
DEFINE_int32(urma_buffer_size, 8192,
             "Size of each buffer in the URMA memory pool (bytes)");
DEFINE_int32(urma_buffer_count, 65536,
             "Number of buffers in the URMA memory pool");
DEFINE_bool(urma_poller_yield, false,
            "Actively yield bthread in busy-polling loop");
DEFINE_int32(urma_client_handshake_version, 2,
             "Client handshake version: 2=binary, 3=protobuf");

namespace butil {
namespace iobuf {
extern void* (*blockmem_allocate)(size_t);
extern void  (*blockmem_deallocate)(void*);
}
}

namespace brpc {
namespace urma {

class UrmaEndpoint;

void* g_handle_urma = NULL;
bool g_skip_urma_init = false;

static int g_max_sge = 0;

butil::atomic<bool> g_urma_available(false);

static urma_device_t** g_devices = NULL;
static urma_context_t* g_context = NULL;
static SocketId g_async_socket;
static urma_target_seg_t* g_pool_seg = NULL;

static butil::FlatMap<void*, urma_target_seg_t*>* g_user_segs = NULL;
static butil::Mutex* g_user_segs_lock = NULL;

static void* (*g_mem_alloc)(size_t) = NULL;
static void (*g_mem_dealloc)(void*) = NULL;

struct UrmaPool {
    void* base;
    size_t size;
    size_t block_size;
    size_t block_count;
    butil::Mutex lock;
    std::vector<void*> free_list;
};

static UrmaPool* g_pool = NULL;

uint32_t GetRegionId(const void* buf) {
    if (!g_pool || !buf) {
        return 0;
    }
    uintptr_t addr = (uintptr_t)buf;
    if (addr >= (uintptr_t)g_pool->base &&
        addr < (uintptr_t)g_pool->base + g_pool->size) {
        return 1;
    }
    return 0;
}

void* AllocBlock(size_t size) {
    if (!g_pool || size == 0 || size > g_pool->block_size) {
        errno = EINVAL;
        return NULL;
    }
    BAIDU_SCOPED_LOCK(g_pool->lock);
    if (g_pool->free_list.empty()) {
        errno = ENOMEM;
        return NULL;
    }
    void* ptr = g_pool->free_list.back();
    g_pool->free_list.pop_back();
    return ptr;
}

void DeallocBlock(void* buf) {
    if (!buf || !g_pool) {
        return;
    }
    if (GetRegionId(buf) == 0) {
        errno = ERANGE;
        return;
    }
    BAIDU_SCOPED_LOCK(g_pool->lock);
    g_pool->free_list.push_back(buf);
}

static void* BlockAllocate(size_t len) {
    if (len == 0) {
        errno = EINVAL;
        return NULL;
    }
    void* ptr = AllocBlock(len);
    if (!ptr) {
        if (g_mem_alloc) {
            return g_mem_alloc(len);
        }
        LOG(ERROR) << "Fail to get block from URMA memory pool";
    }
    return ptr;
}

static void BlockDeallocate(void* buf) {
    if (!buf) {
        errno = EINVAL;
        return;
    }
    if (GetRegionId(buf) != 0) {
        DeallocBlock(buf);
        return;
    }
    if (g_mem_dealloc) {
        g_mem_dealloc(buf);
    }
}

static void OnUrmaAsyncEvent(Socket* m) {
    int progress = Socket::PROGRESS_INIT;
    do {
        urma_async_event_t event;
        if (urma_get_async_event(g_context, &event) != 0) {
            break;
        }
        LOG(WARNING) << "urma async event received";
        urma_ack_async_event(&event);
        if (!m->MoreReadEvents(&progress)) {
            break;
        }
    } while (true);
}

static void GlobalRelease() {
    g_urma_available.store(false, butil::memory_order_release);
    usleep(100000);

    if (g_mem_alloc && g_mem_dealloc) {
        butil::iobuf::blockmem_allocate = g_mem_alloc;
        butil::iobuf::blockmem_deallocate = g_mem_dealloc;
        g_mem_alloc = NULL;
        g_mem_dealloc = NULL;
    }

    if (g_pool) {
        if (g_pool_seg) {
            urma_unregister_seg(g_pool_seg);
            g_pool_seg = NULL;
        }
        if (g_pool->base) {
            munmap(g_pool->base, g_pool->size);
        }
        delete g_pool;
        g_pool = NULL;
    }

    if (g_user_segs_lock) {
        BAIDU_SCOPED_LOCK(*g_user_segs_lock);
        if (g_user_segs) {
            for (butil::FlatMap<void*, urma_target_seg_t*>::iterator it =
                     g_user_segs->begin();
                 it != g_user_segs->end(); ++it) {
                urma_unregister_seg(it->second);
            }
            g_user_segs->clear();
            delete g_user_segs;
            g_user_segs = NULL;
        }
    }
    delete g_user_segs_lock;
    g_user_segs_lock = NULL;

    if (g_context) {
        urma_destroy_context(g_context);
        g_context = NULL;
    }

    if (g_devices) {
        urma_free_device_list(g_devices);
        g_devices = NULL;
    }
}

static inline void ExitWithError() {
    GlobalRelease();
    exit(1);
}

static void GlobalUrmaInitializeOrDieImpl() {
    if (BAIDU_UNLIKELY(g_skip_urma_init)) {
        return;
    }

    if (urma_init() != 0) {
        PLOG(ERROR) << "Fail to urma_init";
        ExitWithError();
    }

    int num = 0;
    g_devices = urma_get_device_list(&num);
    if (num == 0 || !g_devices) {
        LOG(ERROR) << "Fail to find urma device";
        ExitWithError();
    }

    urma_device_t* chosen_dev = NULL;
    int available_devices = 0;
    for (int i = 0; i < num; ++i) {
        const char* dev_name = urma_get_device_name(g_devices[i]);
        if (!FLAGS_urma_device.empty()) {
            if (FLAGS_urma_device == dev_name) {
                chosen_dev = g_devices[i];
                ++available_devices;
            } else {
                LOG(INFO) << "Device name not match: " << dev_name
                          << " vs " << FLAGS_urma_device;
            }
        } else {
            chosen_dev = g_devices[i];
            ++available_devices;
            break;
        }
    }

    if (!chosen_dev) {
        LOG(ERROR) << "Fail to find available URMA device " << FLAGS_urma_device;
        ExitWithError();
    }

    g_context = urma_create_context(chosen_dev);
    if (!g_context) {
        PLOG(ERROR) << "Fail to create urma context";
        ExitWithError();
    }

    LOG(INFO) << "URMA device: "
              << urma_get_device_name(chosen_dev);
    if (available_devices > 1 && FLAGS_urma_device.empty()) {
        LOG(INFO) << "This server has more than one available URMA device. "
                  << "Only the first one ("
                  << urma_get_device_name(chosen_dev)
                  << ") will be used. If you want to use other device, "
                  << "please specify it with --urma_device.";
    }

#if BRPC_WITH_URMA_BONDING
    if (FLAGS_urma_bonding_mode > 0) {
        LOG(INFO) << "URMA bonding mode=" << FLAGS_urma_bonding_mode
                  << " level=" << FLAGS_urma_bonding_level;
    }
#endif

    urma_device_attr_t dev_attr;
    if (urma_query_device(g_context, &dev_attr) != 0) {
        PLOG(ERROR) << "Fail to query urma device";
        ExitWithError();
    }
    if (FLAGS_urma_max_sge > 0) {
        g_max_sge = dev_attr.max_sge < FLAGS_urma_max_sge
                        ? dev_attr.max_sge
                        : FLAGS_urma_max_sge;
    } else {
        g_max_sge = dev_attr.max_sge;
    }

    g_user_segs_lock = new (std::nothrow) butil::Mutex;
    if (!g_user_segs_lock) {
        PLOG(WARNING) << "Fail to construct g_user_segs_lock";
        ExitWithError();
    }

    g_user_segs = new (std::nothrow) butil::FlatMap<void*, urma_target_seg_t*>();
    if (!g_user_segs) {
        PLOG(WARNING) << "Fail to construct g_user_segs";
        ExitWithError();
    }

    if (g_user_segs->init(65536) < 0) {
        PLOG(WARNING) << "Fail to initialize g_user_segs";
        ExitWithError();
    }

    size_t pool_size = (size_t)FLAGS_urma_buffer_size *
                       (size_t)FLAGS_urma_buffer_count;
    void* pool_base = mmap(NULL, pool_size, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (pool_base == MAP_FAILED) {
        PLOG(ERROR) << "Fail to mmap URMA memory pool";
        ExitWithError();
    }

    urma_seg_register_attr_t reg_attr = {};
    reg_attr.addr = pool_base;
    reg_attr.len = pool_size;
    reg_attr.access = URMA_ACCESS_LOCAL_ONLY;
    reg_attr.token_policy = URMA_TOKEN_NONE;
    g_pool_seg = urma_register_seg(g_context, &reg_attr);
    if (!g_pool_seg) {
        PLOG(ERROR) << "Fail to register URMA memory pool";
        munmap(pool_base, pool_size);
        ExitWithError();
    }

    g_pool = new (std::nothrow) UrmaPool;
    if (!g_pool) {
        PLOG(ERROR) << "Fail to allocate UrmaPool";
        ExitWithError();
    }
    g_pool->base = pool_base;
    g_pool->size = pool_size;
    g_pool->block_size = FLAGS_urma_buffer_size;
    g_pool->block_count = FLAGS_urma_buffer_count;
    g_pool->free_list.reserve(FLAGS_urma_buffer_count);
    for (size_t i = 0; i < g_pool->block_count; ++i) {
        g_pool->free_list.push_back(
            (char*)pool_base + i * g_pool->block_size);
    }

    LOG(INFO) << "URMA memory pool: " << pool_size / (1024 * 1024)
              << "MB (" << FLAGS_urma_buffer_count << " blocks x "
              << FLAGS_urma_buffer_size << " bytes)";

    atexit(GlobalRelease);

    butil::SetDefaultBlockSize(FLAGS_urma_buffer_size);

    SocketOptions opt;
    opt.fd = g_context->event_fd;
    butil::make_close_on_exec(opt.fd);
    if (butil::make_non_blocking(opt.fd) < 0) {
        PLOG(WARNING) << "Fail to set event_fd to nonblocking";
        ExitWithError();
    }
    opt.on_edge_triggered_events = OnUrmaAsyncEvent;
    if (Socket::Create(opt, &g_async_socket) < 0) {
        LOG(WARNING) << "Fail to create socket to get async event of URMA";
        ExitWithError();
    }

    g_mem_alloc = butil::iobuf::blockmem_allocate;
    g_mem_dealloc = butil::iobuf::blockmem_deallocate;
    butil::iobuf::blockmem_allocate = BlockAllocate;
    butil::iobuf::blockmem_deallocate = BlockDeallocate;
    g_urma_available.store(true, butil::memory_order_relaxed);
}

static pthread_once_t initialize_urma_once = PTHREAD_ONCE_INIT;

void GlobalUrmaInitializeOrDie() {
    if (pthread_once(&initialize_urma_once,
                     GlobalUrmaInitializeOrDieImpl) != 0) {
        LOG(FATAL) << "Fail to pthread_once GlobalUrmaInitializeOrDie";
        exit(1);
    }
}

uint32_t RegisterMemoryForUrma(void* buf, size_t len) {
    urma_seg_register_attr_t attr = {};
    attr.addr = buf;
    attr.len = len;
    attr.access = URMA_ACCESS_LOCAL_ONLY;
    attr.token_policy = URMA_TOKEN_NONE;
    urma_target_seg_t* seg = urma_register_seg(g_context, &attr);
    if (!seg) {
        PLOG(ERROR) << "Fail to register memory for urma";
        return 0;
    }
    {
        BAIDU_SCOPED_LOCK(*g_user_segs_lock);
        if (!g_user_segs->insert(buf, seg)) {
            LOG(WARNING) << "Fail to insert to user seg maps (now there are "
                         << g_user_segs->size() << " segs already";
            urma_unregister_seg(seg);
            return 0;
        }
    }
    return 1;
}

void DeregisterMemoryForUrma(void* buf) {
    urma_target_seg_t* seg = NULL;
    {
        BAIDU_SCOPED_LOCK(*g_user_segs_lock);
        urma_target_seg_t** seg_ptr = g_user_segs->seek(buf);
        if (seg_ptr) {
            seg = *seg_ptr;
            g_user_segs->erase(buf);
        }
    }
    if (seg) {
        if (urma_unregister_seg(seg) != 0) {
            PLOG(ERROR) << "Failed to deregister memory at: " << buf;
        }
    } else {
        LOG(WARNING) << "Try to deregister a buffer which is not registered";
    }
}

urma_context_t* GetUrmaContext() {
    return g_context;
}

urma_target_seg_t* GetUrmaSeg(void* buf) {
    if (!buf) {
        return NULL;
    }
    if (GetRegionId(buf) != 0 && g_pool_seg) {
        return g_pool_seg;
    }
    BAIDU_SCOPED_LOCK(*g_user_segs_lock);
    urma_target_seg_t** seg_ptr = g_user_segs->seek(buf);
    if (seg_ptr) {
        return *seg_ptr;
    }
    return NULL;
}

int GetUrmaMaxSge() {
    return g_max_sge;
}

bool IsUrmaAvailable() {
    return g_urma_available.load(butil::memory_order_acquire);
}

void GlobalDisableUrma() {
    if (g_urma_available.exchange(false, butil::memory_order_acquire)) {
        LOG(FATAL) << "URMA is disabled due to some unrecoverable problem";
    }
}

bool SupportedByUrma(std::string protocol) {
    if (protocol.compare("baidu_std") == 0) {
        return true;
    }
    return false;
}

bool InitPollingModeWithTag(bthread_tag_t tag,
                            std::function<void()> callback,
                            std::function<void()> init_fn,
                            std::function<void()> release_fn) {
    if (UrmaEndpoint::PollingModeInitialize(tag, callback, init_fn,
                                            release_fn) == 0) {
        return true;
    }
    return false;
}

void ReleasePollingModeWithTag(bthread_tag_t tag) {
    UrmaEndpoint::PollingModeRelease(tag);
}

}  // namespace urma
}  // namespace brpc

#else

#include <stdlib.h>
#include "butil/logging.h"

namespace brpc {
namespace urma {
void GlobalUrmaInitializeOrDie() {
    LOG(ERROR) << "brpc is not compiled with urma. To enable it, please refer to "
               << "https://github.com/apache/brpc/blob/master/docs/en/urma.md";
    exit(1);
}
}
}

#endif  // if BRPC_WITH_URMA
