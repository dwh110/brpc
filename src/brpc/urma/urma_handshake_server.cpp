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

#include "brpc/urma/urma_handshake_server.h"

#include <limits>
#include <string>
#include <string.h>
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/object_pool.h"
#include "butil/raw_pack.h"
#include "butil/sys_byteorder.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_handshake_constants.h"
#if BRPC_WITH_URMA
#include "brpc/urma/urma_endpoint.h"
#endif

namespace brpc {
namespace urma {

ServerHandshakeContext* ServerHandshakeContext::Create() {
    return butil::get_object<ServerHandshakeContext>();
}

void ServerHandshakeContext::Destroy() {
    butil::return_object(this);
}

static constexpr uint16_t V2_HELLO_VERSION_INVALID = std::numeric_limits<uint16_t>::max();

static int DrainClientHelloV2(butil::IOBuf* source) {
    constexpr size_t HDR_LEN = HELLO_MAGIC_LEN + 2;
    if (source->size() < HDR_LEN) {
        return 0;
    }

    uint8_t hdr[HDR_LEN];
    CHECK_EQ(source->copy_to(hdr, sizeof(hdr)), sizeof(hdr));

    uint16_t msg_len = 0;
    butil::RawUnpacker(hdr + HELLO_MAGIC_LEN).unpack16(msg_len);
    if (msg_len < HELLO_V2_MSG_LEN_MIN || msg_len > HELLO_V2_MSG_LEN_MAX) {
        return -1;
    }

    if (source->size() < msg_len) {
        return 0;
    }

    CHECK_EQ(source->pop_front(msg_len), msg_len);
    return 1;
}

static int DrainClientHelloV3(butil::IOBuf* source) {
    constexpr size_t HDR_LEN = HELLO_MAGIC_LEN + HELLO_V3_PB_SIZE_LEN;
    if (source->size() < HDR_LEN) {
        return 0;
    }

    uint8_t hdr[HDR_LEN];
    CHECK_EQ(source->copy_to(hdr, sizeof(hdr)), sizeof(hdr));

    uint32_t pb_size = butil::NetToHost32(
        *reinterpret_cast<const uint32_t*>(hdr + HELLO_MAGIC_LEN));
    if (pb_size == 0 || pb_size > HELLO_V3_MAX_PB_SIZE) {
        return -1;
    }

    const size_t total = HDR_LEN + pb_size;
    if (source->size() < total) {
        return 0;
    }

    CHECK_EQ(source->pop_front(total), total);
    return 1;
}

static int SendUnnegotiableHello(Socket* socket, int version) {
    butil::IOBuf packet;
    if (version == 2) {
        packet.append(HELLO_MAGIC, HELLO_MAGIC_LEN);
        char reply[HELLO_V2_MSG_LEN_MIN - HELLO_MAGIC_LEN]{};
        butil::RawPacker(reply).pack16(HELLO_V2_MSG_LEN_MIN)
                               .pack16(V2_HELLO_VERSION_INVALID);
        packet.append(reply, sizeof(reply));
    } else {
        packet.append(HELLO_MAGIC_V3, HELLO_MAGIC_LEN);
        uint32_t pb_size_be = butil::HostToNet32(1);
        packet.append(&pb_size_be, sizeof(pb_size_be));
        char body = 0;
        packet.append(&body, 1);
    }

    if (socket->Write(&packet) != 0) {
        PLOG(WARNING) << "Fail to send URMA fallback hello to " << socket->description();
        return -1;
    }
    return 0;
}

static ParseResult FallbackServerHandshake(butil::IOBuf* source, Socket* socket) {
    if (socket->parsing_context() == NULL) {
        if (source->size() < HELLO_MAGIC_LEN) {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }
        uint8_t magic[HELLO_MAGIC_LEN];
        CHECK_EQ(source->copy_to(magic, HELLO_MAGIC_LEN), HELLO_MAGIC_LEN);

        int version;
        if (memcmp(magic, HELLO_MAGIC, HELLO_MAGIC_LEN) == 0) {
            version = 2;
        } else if (memcmp(magic, HELLO_MAGIC_V3, HELLO_MAGIC_LEN) == 0) {
            version = 3;
        } else {
            return MakeParseError(PARSE_ERROR_TRY_OTHERS);
        }

        const int r = version == 2 ? DrainClientHelloV2(source) : DrainClientHelloV3(source);
        if (r == 0) {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }
        if (r < 0) {
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
        }
        if (SendUnnegotiableHello(socket, version) < 0) {
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
        }
        socket->reset_parsing_context(ServerHandshakeContext::Create());
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    if (source->size() < HELLO_ACK_LEN) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    CHECK_EQ(source->pop_front(HELLO_ACK_LEN), HELLO_ACK_LEN);
    socket->reset_parsing_context(NULL);
    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}

ParseResult ExecuteServerHandshake(butil::IOBuf* source, Socket* socket) {
#if BRPC_WITH_URMA
    if (socket->socket_mode() == SOCKET_MODE_URMA) {
        return UrmaEndpoint::ExecuteServerHandshake(source, socket);
    }
#endif
    return FallbackServerHandshake(source, socket);
}

}  // namespace urma
}  // namespace brpc
