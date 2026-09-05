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

#ifndef BRPC_URMA_URMA_HANDSHAKE_CONSTANTS_H
#define BRPC_URMA_URMA_HANDSHAKE_CONSTANTS_H

namespace brpc {
namespace urma {

constexpr const char* HELLO_MAGIC = "URMA";
constexpr const char* HELLO_MAGIC_V3 = "URM3";
constexpr size_t HELLO_MAGIC_LEN = 4;

constexpr uint16_t HELLO_V2_VERSION = 2;
constexpr uint16_t IMPL_V2_VERSION = 1;

constexpr size_t HELLO_V2_MSG_LEN_MIN = 38;
constexpr size_t HELLO_V2_MSG_LEN_MAX = 4096;

constexpr size_t HELLO_V3_PB_SIZE_LEN = 4;
constexpr size_t HELLO_V3_MAX_PB_SIZE = 8192;

constexpr size_t HELLO_ACK_LEN = 4;
constexpr uint32_t HELLO_ACK_URMA_OK = 0x1;

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_URMA_URMA_HANDSHAKE_CONSTANTS_H
