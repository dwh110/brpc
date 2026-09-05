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

#ifndef BRPC_URMA_HELPER_H
#define BRPC_URMA_HELPER_H

#if BRPC_WITH_URMA

#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_types.h>
#include <ub/umdk/urma/urma_opcode.h>
#include <string>
#include <functional>
#include <gflags/gflags.h>
#include "bthread/types.h"

#if BRPC_WITH_URMA_BONDING
#include <ub/umdk/urma/urma_ubagg.h>
#endif

namespace brpc {
namespace urma {

void GlobalUrmaInitializeOrDie();

bool InitPollingModeWithTag(bthread_tag_t tag,
                           std::function<void()> callback = nullptr,
                           std::function<void()> init_fn = nullptr,
                           std::function<void()> release_fn = nullptr);

void ReleasePollingModeWithTag(bthread_tag_t tag);

uint32_t RegisterMemoryForUrma(void* buf, size_t len);

void DeregisterMemoryForUrma(void* buf);

urma_context_t* GetUrmaContext();

urma_target_seg_t* GetUrmaSeg(void* buf);

int GetUrmaMaxSge();

bool IsUrmaAvailable();

void GlobalDisableUrma();

bool SupportedByUrma(std::string protocol);

DECLARE_bool(urma_use_polling);
DECLARE_int32(urma_poller_num);
DECLARE_bool(urma_disable_bthread);
DECLARE_int32(urma_sq_size);
DECLARE_int32(urma_rq_size);
DECLARE_int32(urma_cqe_poll_once);
DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);
DECLARE_string(urma_device);
DECLARE_int32(urma_max_sge);
DECLARE_bool(urma_poller_yield);
DECLARE_int32(urma_prepared_jetty_cnt);
DECLARE_int32(urma_client_handshake_version);

}  // namespace urma
}  // namespace brpc

#else

namespace brpc {
namespace urma {

void GlobalUrmaInitializeOrDie();

}  // namespace urma
}  // namespace brpc

#endif  // if BRPC_WITH_URMA

#endif  // BRPC_URMA_HELPER_H
