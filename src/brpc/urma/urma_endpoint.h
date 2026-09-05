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

#ifndef BRPC_URMA_ENDPOINT_H
#define BRPC_URMA_ENDPOINT_H

#if BRPC_WITH_URMA
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_types.h>
#include <ub/umdk/urma/urma_opcode.h>
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/macros.h"
#include "butil/containers/mpsc_queue.h"
#include "butil/containers/optional.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake_constants.h"


namespace brpc {
class Socket;
namespace urma {

class UrmaHandshakeClientV2;
class UrmaHandshakeServerV2;
class UrmaHandshakeClientV3;
class UrmaHandshakeServerV3;
struct ParsedHello;
enum class RemoteHelloResult;
class UrmaEndpoint;
namespace v2_wire {
    RemoteHelloResult ReadBodyAndNegotiate(UrmaEndpoint* ep, ParsedHello* remote);
    int DrainBytes(UrmaEndpoint* ep, size_t n);
}  // namespace v2_wire

class UrmaConnect : public AppConnect {
public:
    void StartConnect(const Socket* socket,
            void (*done)(int err, void* data), void* data) override;
    void StopConnect(Socket*) override;
    struct RunGuard {
        RunGuard(UrmaConnect* rc) { this_rc = rc; }
        ~RunGuard() { if (this_rc) this_rc->Run(); }
        UrmaConnect* this_rc;
    };

private:
    void Run();
    void (*_done)(int, void*){NULL};
    void* _data{NULL};
};

struct UrmaResource {
    UrmaResource* next{NULL};
    urma_jfc_t* jfc{NULL};
    urma_jfce_t* jfce{NULL};
    urma_jfr_t* jfr{NULL};
    urma_jetty_t* jetty{NULL};
    urma_target_jetty_t* remote_jetty{NULL};
    urma_target_seg_t* remote_seg{NULL};
    UrmaResource() = default;
    ~UrmaResource();
    DISALLOW_COPY_AND_ASSIGN(UrmaResource);
};

urma_eid_t GetUrmaEid();

class BAIDU_CACHELINE_ALIGNMENT UrmaEndpoint : public SocketUser {
friend class UrmaConnect;
friend class Socket;
friend class UrmaHandshakeClientV2;
friend class UrmaHandshakeServerV2;
friend class UrmaHandshakeClientV3;
friend class UrmaHandshakeServerV3;
friend RemoteHelloResult v2_wire::ReadBodyAndNegotiate(UrmaEndpoint*, ParsedHello*);
friend int v2_wire::DrainBytes(UrmaEndpoint*, size_t);
public:
    explicit UrmaEndpoint(Socket* s);
    ~UrmaEndpoint() override;

    static int GlobalInitialize();

    static void GlobalRelease();

    void Reset();

    ssize_t CutFromIOBufList(butil::IOBuf** data, size_t ndata);

    bool IsWritable() const;

    void DebugInfo(std::ostream& os,
                   butil::StringPiece connector = "\n") const;

    static void OnNewDataFromTcp(Socket* m);

    static ParseResult ExecuteServerHandshake(butil::IOBuf* source, Socket* socket);

    static int PollingModeInitialize(bthread_tag_t tag,
                                     std::function<void(void)> callback,
                                     std::function<void(void)> init_fn,
                                     std::function<void(void)> release_fn);

    static void PollingModeRelease(bthread_tag_t tag);

private:
    enum State {
        UNINIT = 0x0,
        C_ALLOC_QPCQ = 0x1,
        C_HELLO_SEND = 0x2,
        C_HELLO_WAIT = 0x3,
        C_BRINGUP_QP = 0x4,
        C_ACK_SEND = 0x5,
        S_HELLO_WAIT = 0x11,
        S_ALLOC_QPCQ = 0x12,
        S_BRINGUP_QP = 0x13,
        S_HELLO_SEND = 0x14,
        S_ACK_WAIT = 0x15,
        ESTABLISHED = 0x100,
        FALLBACK_TCP = 0x200,
        FAILED = 0x300
    };

    static void* ProcessHandshakeAtClient(void* arg);

    int AllocateResources();

    void DeallocateResources();

    int SendImm(uint32_t imm);

    int SendAck(int num);

    ssize_t HandleCompletion(urma_cr_t& cr);

    int PostRecv(uint32_t num, bool zerocopy);

    int DoPostRecv(void* block, size_t block_size);

    int ReadFromFd(void* data, size_t len);
    int ReadFromFd(butil::IOPortal* data, size_t len);

    int WriteToFd(void* data, size_t len);
    int WriteToFd(butil::IOBuf* data);

    void ApplyRemoteHello(const ParsedHello& remote);

    int BringUpQp(const ParsedHello& remote, bool is_server);

    int GetAndAckEvents(SocketUniquePtr& s);

    int ReqNotifyCq(bool send_cq);

    static void PollCq(Socket* m);

    std::string GetStateStr() const;

    void PollerAddCqSid();

    void PollerRemoveCqSid();

    Socket* _socket;

    butil::atomic<State> _state;

    int _handshake_version;

    UrmaResource* _resource;

    unsigned int _jfc_events;

    SocketId _cq_sid;

    uint16_t _sq_size;
    uint16_t _rq_size;

    std::vector<butil::IOBuf> _sbuf;
    std::vector<butil::IOBuf> _rbuf;
    std::vector<void*> _rbuf_data;
    uint32_t _remote_recv_block_size;

    uint16_t _accumulated_ack;
    uint16_t _unsolicited;
    uint32_t _unsolicited_bytes;
    uint16_t _sq_current;
    uint16_t _sq_unsignaled;
    uint16_t _sq_sent;
    uint16_t _rq_received;
    uint16_t _local_window_capacity;
    uint16_t _remote_window_capacity;
    uint16_t _sq_imm_window_size;
    butil::atomic<uint16_t> _remote_rq_window_size;
    butil::atomic<uint16_t> _sq_window_size;
    butil::atomic<uint16_t> _new_rq_wrs;

    butil::atomic<int> *_read_butex;

    DISALLOW_COPY_AND_ASSIGN(UrmaEndpoint);

    struct CqSidOp {
        enum OpType {
            ADD,
            REMOVE,
        };
        SocketId sid;
        OpType type;
    };
    struct BAIDU_CACHELINE_ALIGNMENT Poller {
        bthread_t tid{INVALID_BTHREAD};
        butil::MPSCQueue<CqSidOp, butil::ObjectPoolAllocator<CqSidOp>> op_queue;
        std::function<void()> callback;
        std::function<void()> init_fn;
        std::function<void()> release_fn;
    };
    struct BAIDU_CACHELINE_ALIGNMENT PollerGroup {
        PollerGroup() : pollers(FLAGS_urma_poller_num), running(false) {}
        std::vector<Poller> pollers;
        std::atomic<bool> running;
    };
    static std::vector<PollerGroup> _poller_groups;
};

}  // namespace urma
}  // namespace brpc

#else  // if BRPC_WITH_URMA

class UrmaEndpoint { };

#endif  // if BRPC_WITH_URMA

#endif // BRPC_URMA_ENDPOINT_H
