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

#ifndef BRPC_URMA_HANDSHAKE_H
#define BRPC_URMA_HANDSHAKE_H

#if BRPC_WITH_URMA

#include <memory>
#include <ub/umdk/urma/urma_types.h>
#include "butil/macros.h"
#include "butil/containers/optional.h"
#include "brpc/urma/urma_handshake_constants.h"

namespace butil {
class IOBuf;
}

namespace brpc {
namespace urma {

class UrmaEndpoint;

struct ParsedHello {
    uint32_t block_size;
    uint16_t sq_size;
    uint16_t rq_size;
    urma_eid_t eid;
    uint32_t jpn;
    uint32_t uasid;
};

enum class RemoteHelloResult {
    NEGOTIATED,
    FALLBACK,
    NEED_MORE,
    ERROR,
};

namespace v2_wire {

struct HelloMessage {
    void Serialize(void* data) const;
    void Deserialize(void* data);

    uint16_t msg_len;
    uint16_t hello_ver;
    uint16_t impl_ver;
    uint32_t block_size;
    uint16_t sq_size;
    uint16_t rq_size;
    urma_eid_t eid;
    uint32_t jpn;
    uint32_t uasid;
};

}  // namespace v2_wire

class UrmaHandshake {
public:
    UrmaHandshake(UrmaEndpoint* ep, int version) : _ep(ep), _version(version) {}
    virtual ~UrmaHandshake() = default;

    DISALLOW_COPY_AND_ASSIGN(UrmaHandshake);

    int ProtocolVersion() const { return _version; }

    virtual int SendLocalHello() = 0;
    virtual RemoteHelloResult ReceiveAndParseRemoteHello(ParsedHello* remote) = 0;

protected:
    UrmaEndpoint* _ep;
    int _version;
};

class ServerUrmaHandshake : public UrmaHandshake {
public:
    ServerUrmaHandshake(UrmaEndpoint* ep, butil::IOBuf* source, int version)
        : UrmaHandshake(ep, version), _source(source) {}

protected:
    butil::IOBuf* _source;
};

class UrmaHandshakeClientV2 : public UrmaHandshake {
public:
    explicit UrmaHandshakeClientV2(UrmaEndpoint* ep) : UrmaHandshake(ep, 2) {}
    int SendLocalHello() override;
    RemoteHelloResult ReceiveAndParseRemoteHello(ParsedHello* remote) override;
};

class UrmaHandshakeServerV2 : public ServerUrmaHandshake {
public:
    UrmaHandshakeServerV2(UrmaEndpoint* ep, butil::IOBuf* source)
        : ServerUrmaHandshake(ep, source, 2) {}
    int SendLocalHello() override;
    RemoteHelloResult ReceiveAndParseRemoteHello(ParsedHello* remote) override;
};

class UrmaHandshakeClientV3 : public UrmaHandshake {
public:
    explicit UrmaHandshakeClientV3(UrmaEndpoint* ep) : UrmaHandshake(ep, 3) {}
    int SendLocalHello() override;
    RemoteHelloResult ReceiveAndParseRemoteHello(ParsedHello* remote) override;
};

class UrmaHandshakeServerV3 : public ServerUrmaHandshake {
public:
    UrmaHandshakeServerV3(UrmaEndpoint* ep, butil::IOBuf* source)
        : ServerUrmaHandshake(ep, source, 3) {}
    int SendLocalHello() override;
    RemoteHelloResult ReceiveAndParseRemoteHello(ParsedHello* remote) override;
};

std::unique_ptr<UrmaHandshake> CreateClientHandshake(UrmaEndpoint* ep);

std::unique_ptr<UrmaHandshake> CreateServerHandshakeByMagic(
    UrmaEndpoint* ep, butil::IOBuf* source, const uint8_t magic[HELLO_MAGIC_LEN]);

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
#endif  // BRPC_URMA_HANDSHAKE_H
