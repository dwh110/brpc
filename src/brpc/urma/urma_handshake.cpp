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

#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake_constants.h"

#include <string.h>
#include <algorithm>
#include <string>
#include <limits>
#include <gflags/gflags.h>
#include "butil/iobuf.h"
#include "butil/sys_byteorder.h"
#include "butil/raw_pack.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"

namespace brpc {
namespace urma {

DECLARE_int32(urma_client_handshake_version);
DECLARE_bool(urma_trace_verbose);

extern const uint16_t MIN_JETTY_SIZE;
extern const uint16_t MIN_BLOCK_SIZE;
extern uint32_t g_urma_recv_block_size;

namespace v2_wire {

void HelloMessage::Serialize(void* data) const {
    butil::RawPacker(data)
        .pack16(msg_len)
        .pack16(hello_ver)
        .pack16(impl_ver)
        .pack32(block_size)
        .pack16(sq_size)
        .pack16(rq_size)
        .pack_bytes(&eid, sizeof(eid))
        .pack32(jpn);
}

void HelloMessage::Deserialize(void* data) {
    butil::RawUnpacker(data)
        .unpack16(msg_len)
        .unpack16(hello_ver)
        .unpack16(impl_ver)
        .unpack32(block_size)
        .unpack16(sq_size)
        .unpack16(rq_size)
        .unpack_bytes(&eid, sizeof(eid))
        .unpack32(jpn);
}

static bool ValidHelloMessage(const HelloMessage& msg) {
    return msg.hello_ver == HELLO_V2_VERSION &&
           msg.impl_ver == IMPL_V2_VERSION &&
           msg.block_size >= MIN_BLOCK_SIZE &&
           msg.sq_size >= MIN_JETTY_SIZE &&
           msg.rq_size >= MIN_JETTY_SIZE;
}

static void TranslateV2Hello(const HelloMessage& msg, ParsedHello* out) {
    out->block_size = msg.block_size;
    out->sq_size = msg.sq_size;
    out->rq_size = msg.rq_size;
    out->eid = msg.eid;
    out->jpn = msg.jpn;
    out->uasid = msg.uasid;
}

RemoteHelloResult ReadBodyAndNegotiate(UrmaEndpoint* ep, ParsedHello* remote) {
    uint8_t data[HELLO_V2_MSG_LEN_MIN];
    if (ep->ReadFromFd(data, HELLO_V2_MSG_LEN_MIN - HELLO_MAGIC_LEN) < 0) {
        return RemoteHelloResult::ERROR;
    }
    HelloMessage remote_msg{};
    remote_msg.Deserialize(data);
    if (remote_msg.msg_len < HELLO_V2_MSG_LEN_MIN ||
        remote_msg.msg_len > HELLO_V2_MSG_LEN_MAX) {
        errno = EPROTO;
        return RemoteHelloResult::ERROR;
    }
    if (remote_msg.msg_len > HELLO_V2_MSG_LEN_MIN) {
        size_t ext_len = remote_msg.msg_len - HELLO_V2_MSG_LEN_MIN;
        if (DrainBytes(ep, ext_len) < 0) {
            return RemoteHelloResult::ERROR;
        }
    }
    if (!ValidHelloMessage(remote_msg)) {
        return RemoteHelloResult::FALLBACK;
    }
    TranslateV2Hello(remote_msg, remote);
    return RemoteHelloResult::NEGOTIATED;
}

int DrainBytes(UrmaEndpoint* ep, size_t n) {
    uint8_t scratch[64];
    while (n > 0) {
        size_t chunk = std::min(n, sizeof(scratch));
        if (ep->ReadFromFd(scratch, chunk) < 0) {
            return -1;
        }
        n -= chunk;
    }
    return 0;
}

}  // namespace v2_wire

int UrmaHandshakeClientV2::SendLocalHello() {
    UrmaEndpoint* ep = _ep;
    uint8_t data[HELLO_V2_MSG_LEN_MIN];

    v2_wire::HelloMessage local_msg{};
    local_msg.msg_len = HELLO_V2_MSG_LEN_MIN;
    local_msg.hello_ver = HELLO_V2_VERSION;
    local_msg.impl_ver = IMPL_V2_VERSION;
    local_msg.block_size = g_urma_recv_block_size;
    local_msg.sq_size = ep->_sq_size;
    local_msg.rq_size = ep->_rq_size;
    local_msg.eid = GetUrmaEid();
    if (BAIDU_LIKELY(ep->_resource)) {
        local_msg.jpn = ep->_resource->jetty->jetty_id.jpn;
    } else {
        local_msg.jpn = 0;
    }
    local_msg.uasid = 0;
    fast_memcpy(data, HELLO_MAGIC, 4);
    local_msg.Serialize((char*)data + 4);
    return ep->WriteToFd(data, HELLO_V2_MSG_LEN_MIN);
}

RemoteHelloResult UrmaHandshakeClientV2::ReceiveAndParseRemoteHello(ParsedHello* remote) {
    uint8_t magic[HELLO_MAGIC_LEN];
    if (_ep->ReadFromFd(magic, HELLO_MAGIC_LEN) < 0) {
        return RemoteHelloResult::ERROR;
    }
    if (memcmp(magic, HELLO_MAGIC, HELLO_MAGIC_LEN) != 0) {
        errno = EPROTO;
        return RemoteHelloResult::ERROR;
    }

    return v2_wire::ReadBodyAndNegotiate(_ep, remote);
}

RemoteHelloResult UrmaHandshakeServerV2::ReceiveAndParseRemoteHello(ParsedHello* remote) {
    butil::IOBuf* source = _source;
    constexpr size_t HDR_LEN = HELLO_MAGIC_LEN + 2;
    if (source->size() < HDR_LEN) {
        return RemoteHelloResult::NEED_MORE;
    }

    uint8_t hdr[HDR_LEN];
    CHECK_EQ(source->copy_to(hdr, sizeof(hdr)), sizeof(hdr));

    uint16_t msg_len = 0;
    butil::RawUnpacker(hdr + HELLO_MAGIC_LEN).unpack16(msg_len);
    if (msg_len < HELLO_V2_MSG_LEN_MIN || msg_len > HELLO_V2_MSG_LEN_MAX) {
        errno = EPROTO;
        return RemoteHelloResult::ERROR;
    }
    if (source->size() < msg_len) {
        return RemoteHelloResult::NEED_MORE;
    }

    CHECK_EQ(source->pop_front(HELLO_MAGIC_LEN), HELLO_MAGIC_LEN);
    uint8_t body[HELLO_V2_MSG_LEN_MIN - HELLO_MAGIC_LEN];
    CHECK_EQ(source->cutn(body, sizeof(body)), sizeof(body));
    if (!source->empty()) {
        source->clear();
    }

    v2_wire::HelloMessage remote_msg{};
    remote_msg.Deserialize(body);
    if (!v2_wire::ValidHelloMessage(remote_msg)) {
        return RemoteHelloResult::FALLBACK;
    }
    v2_wire::TranslateV2Hello(remote_msg, remote);
    return RemoteHelloResult::NEGOTIATED;
}

int UrmaHandshakeServerV2::SendLocalHello() {
    uint8_t data[HELLO_V2_MSG_LEN_MIN];
    v2_wire::HelloMessage local_msg{};
    local_msg.msg_len = HELLO_V2_MSG_LEN_MIN;
    auto urma_transport = static_cast<UrmaTransport*>(_ep->_socket->_transport.get());
    if (urma_transport->_urma_state == UrmaTransport::URMA_OFF) {
        local_msg.hello_ver = 0;
        local_msg.impl_ver = 0;
        local_msg.block_size = 0;
        local_msg.sq_size = 0;
        local_msg.rq_size = 0;
        memset(&local_msg.eid, 0, sizeof(local_msg.eid));
        local_msg.jpn = 0;
        local_msg.uasid = 0;
    } else {
        local_msg.hello_ver = HELLO_V2_VERSION;
        local_msg.impl_ver = IMPL_V2_VERSION;
        local_msg.block_size = g_urma_recv_block_size;
        local_msg.sq_size = _ep->_sq_size;
        local_msg.rq_size = _ep->_rq_size;
        local_msg.eid = GetUrmaEid();
        if (BAIDU_LIKELY(_ep->_resource)) {
            local_msg.jpn = _ep->_resource->jetty->jetty_id.jpn;
        } else {
            local_msg.jpn = 0;
        }
        local_msg.uasid = 0;
    }
    fast_memcpy(data, HELLO_MAGIC, 4);
    local_msg.Serialize((char*)data + 4);
    return _ep->WriteToFd(data, HELLO_V2_MSG_LEN_MIN);
}

int UrmaHandshakeClientV3::SendLocalHello() {
    errno = ENOTSUP;
    return -1;
}

RemoteHelloResult UrmaHandshakeClientV3::ReceiveAndParseRemoteHello(ParsedHello*) {
    errno = ENOTSUP;
    return RemoteHelloResult::ERROR;
}

RemoteHelloResult UrmaHandshakeServerV3::ReceiveAndParseRemoteHello(ParsedHello*) {
    constexpr size_t HDR_LEN = HELLO_MAGIC_LEN + HELLO_V3_PB_SIZE_LEN;
    if (_source->size() < HDR_LEN) {
        return RemoteHelloResult::NEED_MORE;
    }

    uint8_t hdr[HDR_LEN];
    CHECK_EQ(_source->copy_to(hdr, sizeof(hdr)), sizeof(hdr));

    uint32_t pb_size = butil::NetToHost32(
        *reinterpret_cast<const uint32_t*>(hdr + HELLO_MAGIC_LEN));
    if (pb_size == 0 || pb_size > HELLO_V3_MAX_PB_SIZE) {
        errno = EPROTO;
        return RemoteHelloResult::ERROR;
    }
    size_t total = HDR_LEN + pb_size;
    if (_source->size() < total) {
        return RemoteHelloResult::NEED_MORE;
    }

    CHECK_EQ(_source->cutn(hdr, HDR_LEN), HDR_LEN);
    butil::IOBuf pb;
    CHECK_EQ(_source->cutn(&pb, pb_size), pb_size);
    errno = ENOTSUP;
    return RemoteHelloResult::ERROR;
}

int UrmaHandshakeServerV3::SendLocalHello() {
    errno = ENOTSUP;
    return -1;
}

std::unique_ptr<UrmaHandshake> CreateClientHandshake(UrmaEndpoint* ep) {
    switch (FLAGS_urma_client_handshake_version) {
    case 3:
        return std::unique_ptr<UrmaHandshake>(new UrmaHandshakeClientV3(ep));
    case 2:
    default:
        return std::unique_ptr<UrmaHandshake>(new UrmaHandshakeClientV2(ep));
    }
}

std::unique_ptr<UrmaHandshake> CreateServerHandshakeByMagic(
    UrmaEndpoint* ep, butil::IOBuf* source, const uint8_t magic[HELLO_MAGIC_LEN]) {
    if (memcmp(magic, HELLO_MAGIC, HELLO_MAGIC_LEN) == 0) {
        return std::unique_ptr<UrmaHandshake>(
                new UrmaHandshakeServerV2(ep, source));
    }
    if (memcmp(magic, HELLO_MAGIC_V3, HELLO_MAGIC_LEN) == 0) {
        return std::unique_ptr<UrmaHandshake>(
                new UrmaHandshakeServerV3(ep, source));
    }
    return NULL;
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
