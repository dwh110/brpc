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


#include <gflags/gflags.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/time.h"
#include "brpc/server.h"
#include "brpc/rpc_pb_message_factory.h"
#include "bvar/variable.h"
#include "press.pb.h"

DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_bool(use_urma, true, "Use URMA transport (true) or TCP (false)");

DEFINE_bool(server_ignore_oc, false, "Server ignore eovercrowded, false by default");
DEFINE_int32(num_threads, 5, "The max number of threads are used");
DEFINE_int32(max_concurrency, 128, "max concurrency");
DEFINE_int32(server_bthread_concurrency, 5, "server bthread concurrency");
DEFINE_int32(stats_timeout_seconds, 20, "Timeout in seconds before collecting statistics. (default 20)");
DEFINE_bool(echo_attachment, false, "Echo request attachment to response. Set on server side directly.");
DEFINE_bool(sort, false, "sort");

butil::atomic<uint64_t> g_total_cnt(0);
#if BRPC_ENABLE_TRACE_SCOPE
static const int64_t kMaxRpcIoNum = BRPC_TRACE_MAX_RPC_IO_NUM;
int64_t g_step_capacity = 0;

static int64_t EstimateStepCapacity() {
    if (kMaxRpcIoNum <= 0) {
        return 0;
    }
    const int64_t split_factor = 16;
    const int64_t guard = 2048;
    const int64_t max_mul = (std::numeric_limits<int64_t>::max() - guard) / split_factor;
    if (kMaxRpcIoNum > max_mul) {
        return kMaxRpcIoNum;
    }
    return kMaxRpcIoNum * split_factor + guard;
}
#endif

static void* StatsPrinter(void* arg) {
    std::cout << "timing for " << FLAGS_stats_timeout_seconds << " seconds" << std::endl;
    bthread_usleep(FLAGS_stats_timeout_seconds * 1000000UL);
    std::cout << "total requests: " << g_total_cnt.load(butil::memory_order_relaxed) << std::endl;
    return nullptr;
}

namespace press {
class PerfTestServiceImpl : public PerfTestService {
public:
    PerfTestServiceImpl() {}

    ~PerfTestServiceImpl() {}

    void Test(google::protobuf::RpcController* cntl_base,
              const Request* request,
              Request* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        response->Swap(const_cast<Request*>(request));
        if (FLAGS_echo_attachment) {
            brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);
            cntl->response_attachment().append(cntl->request_attachment());
        }
        if (g_total_cnt.fetch_add(1, butil::memory_order_relaxed) == 0) {
            bthread_t tid;
            bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, StatsPrinter, nullptr);
        }
    }
};
}

namespace bthread {
    DECLARE_int32(bthread_concurrency);
}

int main(int argc, char* argv[]) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_ENABLE_TRACE_SCOPE
    g_step_capacity = EstimateStepCapacity();
    std::cout << "Step record capacity: " << g_step_capacity << std::endl;
    g_brpc_step_latency = (int64_t **)malloc(BRPC_STEP_COUNT * sizeof(int64_t *));
    if (g_brpc_step_latency == nullptr) {
        fprintf(stderr, "malloc g_brpc_step_latency rows failed\n");
        return 1;
    }
    for (uint32_t i = 0; i < BRPC_STEP_COUNT; ++i) {
        g_brpc_step_latency[i] = (int64_t *)calloc(g_step_capacity, sizeof(int64_t));
        if (g_brpc_step_latency[i] == nullptr) {
            fprintf(stderr, "malloc g_brpc_step_latency cols failed\n");
            return 1;
        }
    }
    BrpcTraceInitMarkerStorage(g_step_capacity);
    for (uint32_t i = 0; i < BRPC_NOCNTL_STEP_COUNT; ++i) {
        for (uint32_t j = 0; j < BRPC_IDX_COUNT; ++j) {
            g_brpc_step_latency_nocntl[i][j].store(0);
        }
    }
#endif
    bthread::FLAGS_bthread_concurrency = FLAGS_server_bthread_concurrency;

    brpc::Server server;
    press::PerfTestServiceImpl perf_test_service_impl;

    g_total_cnt.store(0, butil::memory_order_relaxed);

    if (server.AddService(&perf_test_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    brpc::ServerOptions options;
    options.socket_mode = FLAGS_use_urma ? brpc::SOCKET_MODE_URMA
                                       : brpc::SOCKET_MODE_TCP;
    options.max_concurrency = FLAGS_max_concurrency;
    options.num_threads = FLAGS_num_threads;
    options.ignore_eovercrowded = FLAGS_server_ignore_oc;
    options.rpc_pb_message_factory = brpc::GetArenaRpcPBMessageFactory();
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
#if BRPC_ENABLE_TRACE_SCOPE
    printf("=========== start ===========\n");
    std::cout << "IONum: " << g_total_cnt.load(butil::memory_order_relaxed) << std::endl;
    if (FLAGS_sort) {
        std::cout << "No use to use FLAGS_sort because data is sorted before percentile output." << std::endl;
    }
    BrpcTracePrintFrameworkStepStats(g_step_capacity);
    std::cout << "BRPC_STEP_COUNT: " << BRPC_STEP_COUNT << std::endl;
    BrpcTracePrintAllStepStats(g_step_capacity);
    printf("=========== end ===========\n");
#else
    std::cout << "BRPC_ENABLE_TRACE_SCOPE is off; trace stats are disabled." << std::endl;
#endif
    return 0;
}
