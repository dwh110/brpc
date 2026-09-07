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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "bthread/types.h"
#include "butil/atomicops.h"

#if BRPC_WITH_URMA

#include "urma_api.h"
#include "urma_types.h"

namespace brpc {
DECLARE_bool(usercode_in_coroutine);
DECLARE_bool(usercode_in_pthread);
namespace urma {

// Initialize the URMA environment. Failure disables URMA globally so
// individual sockets transparently fall back to TCP.
void GlobalUrmaInitializeOrDie();

// Initialize URMA polling mode for a given bthread tag.
// Returns false on failure.
bool InitPollingModeWithTag(bthread_tag_t tag,
                            std::function<void()> callback = nullptr,
                            std::function<void()> init_fn = nullptr,
                            std::function<void()> release_fn = nullptr);

void ReleasePollingModeWithTag(bthread_tag_t tag);

// Register the given user buffer for URMA access.
// Returns the (opaque, non-zero) target segment handle stored in the user-mr
// table; 0 on failure. To use the memory in an IOBuf, append it via
// append_user_data_with_meta and pass the returned handle as the data meta.
uint64_t RegisterMemoryForUrma(void* buf, size_t len);

// Deregister a previously registered user buffer.
void DeregisterMemoryForUrma(void* buf);

// Return the target segment for a buffer-pool address. Passing nullptr returns
// the segment backing the whole pool. Returns nullptr for any other address.
urma_target_seg_t* GetPoolSegFor(void* buf);

// Get the global URMA context (the urma_context_t created on the selected
// device / EID). Returns nullptr if URMA is not initialized.
urma_context_t* GetUrmaContext();

// Get the EID selected when the global context was created. This is the
// device EID that must be advertised to peers, especially for bonding
// devices where a created jetty may expose a provider-specific physical EID.
// Returns nullptr if URMA is not initialized.
const urma_eid_t* GetUrmaLocalEid();

// Get the process-level uasid assigned by urma_init (or urma_get_uasid
// fallback). This is the uasid that must be advertised to peers during
// handshake — NOT g_context->uasid which is always 0 on bonding devices.
// Returns 0 if URMA is not initialized or uasid was not assigned.
uint32_t GetUrmaLocalUasid();

// Return true when the selected URMA device is a bonding provider device.
bool IsUrmaBondingDevice();

// Find the priority whose advertised transport-path capability exactly
// matches @tp_type. Returns -1 when the device does not report one.
int FindUrmaPriorityForTpType(const urma_device_attr_t& attr,
                              urma_tp_type_t tp_type);

// Return the priority selected for the CTP jettys created by brpc.
uint8_t GetUrmaJettyPriority();

// If the URMA environment is available.
bool IsUrmaAvailable();

// Disable URMA for the remaining lifetime of the process.
void GlobalDisableUrma();

// If the given protocol is supported by UrmaTransport.
// Currently only "baidu_std" is supported.
bool SupportedByUrma(const std::string& protocol);

// Return the configured recv buffer size (one URMA recv WR's payload size).
size_t GetUrmaRecvBlockSize();

// Return max_sge supported by the device (for JFS / send path).
int GetUrmaMaxSge();

// Return max_jfr_sge supported by the device (for JFR / receive path).
int GetUrmaMaxJfrSge();

// Return the effective RQ depth to use. On bonding devices the JFR depth is
// capped to avoid URMA_CR_REM_ACCESS_ABORT_ERR (status=8) under concurrent
// load — the bonding provider fails when JFR depth >= 16 with multiple
// threads. Non-bonding devices return FLAGS_urma_rq_size unchanged.
uint16_t GetUrmaEffectiveRqSize();

// Return the maximum send window size on bonding devices. The bonding
// provider returns status=8 when too many sends are posted concurrently.
// Returns 0 on non-bonding devices (no cap).
uint16_t GetUrmaBondingMaxSendWindow();

// Return the maximum payload bytes per SGE on the current device.
// The bonding provider silently drops SEND WRs whose SGE length exceeds
// 4096 bytes, causing the jetty to flush (status=11) on all subsequent
// WRs.  Non-bonding devices return 0 (no per-SGE limit).
uint32_t GetUrmaMaxSgeLen();

}  // namespace urma
}  // namespace brpc

#else  // BRPC_WITH_URMA

namespace brpc {
namespace urma {

// Initialize the URMA environment.
// Exit the process if initialization fails.
void GlobalUrmaInitializeOrDie();

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA

#endif  // BRPC_URMA_HELPER_H
