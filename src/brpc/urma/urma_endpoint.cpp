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

#include "brpc/urma/urma_endpoint.h"

#if BRPC_WITH_URMA

#include <sys/resource.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "butil/sys_byteorder.h"
#include "butil/time.h"
#include "bthread/bthread.h"
#include "bthread/butex.h"

#include "urma_api.h"

#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_bonding.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"

DECLARE_int32(task_group_ntags);

namespace brpc {
namespace urma {

// Flags used here are declared in urma_endpoint.h (urma_use_polling,
// urma_poller_num, urma_disable_bthread). Declare the rest here.
DECLARE_int32(urma_sq_size);
DECLARE_int32(urma_rq_size);
DECLARE_int32(urma_cqe_poll_once);
DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);
DECLARE_int32(urma_prepared_jetty_cnt);
DECLARE_bool(urma_poller_yield);
DECLARE_bool(urma_trace_latency);
DECLARE_int32(urma_io_mode);
DECLARE_int32(urma_inline_threshold);
DECLARE_int32(urma_send_buf_size);
DECLARE_int32(urma_recv_buf_size);

// ---- Constants shared with the handshake module ----
static const int WAIT_TIMEOUT_MS = 50;
static const size_t HELLO_ACK_LEN = 4;
static const uint32_t HELLO_ACK_URMA_OK = 0x1;
static const size_t IOBUF_BLOCK_HEADER_LEN = sizeof(butil::IOBuf::Block);

// ---- Globals: prepared jetty pool + poller groups ----
struct PreparedJetty {
    UrmaResource* res;
};
static butil::Mutex g_prepared_mutex;
static UrmaResource* g_prepared_list = nullptr;  // singly-linked
static int g_prepared_cnt = 0;

static int PreparedJettyCount() {
    const int requested =
        std::max(0, std::min(FLAGS_urma_prepared_jetty_cnt, 1024));
    if (requested == 0) {
        return 0;
    }

    struct rlimit nofile;
    if (getrlimit(RLIMIT_NOFILE, &nofile) != 0 ||
        nofile.rlim_cur == RLIM_INFINITY) {
        return requested;
    }

    // In event mode each prepared JFCE consumes a file descriptor. Keep room
    // for one TCP fd per future URMA connection and for brpc/system internals.
    static const rlim_t kReservedFdCount = 64;
    const rlim_t max_prepared =
        nofile.rlim_cur > kReservedFdCount
            ? (nofile.rlim_cur - kReservedFdCount) / 2
            : 0;
    if (max_prepared >= static_cast<rlim_t>(requested)) {
        return requested;
    }

    LOG(WARNING) << "Cap URMA prepared jetty count from " << requested
                 << " to " << max_prepared
                 << " due to RLIMIT_NOFILE=" << nofile.rlim_cur;
    return static_cast<int>(max_prepared);
}

std::vector<UrmaEndpoint::PollerGroup> UrmaEndpoint::_poller_groups;

// ============================================================================
// UrmaResource lifecycle.
// ============================================================================

UrmaResource::~UrmaResource() {
    if (remote_jetty) {
        urma_unimport_jetty(remote_jetty);
    }
    if (remote_seg) {
        urma_unimport_seg(remote_seg);
    }
    if (remote_recv_buf_seg) {
        urma_unimport_seg(remote_recv_buf_seg);
    }
    if (jetty) {
        urma_delete_jetty(jetty);
    }
    if (jfr) {
        urma_delete_jfr(jfr);
    }
    if (jfc) {
        urma_delete_jfc(jfc);
    }
    if (jfce) {
        urma_delete_jfce(jfce);
    }
}

// ============================================================================
// Constructor / destructor / Reset.
// ============================================================================

UrmaEndpoint::UrmaEndpoint(Socket* s)
    : _socket(s),
      _state(UNINIT),
      _handshake_version(0),
      _resource(nullptr) {
    _sq_size = static_cast<uint16_t>(
        std::max(16, std::min(4096, static_cast<int>(FLAGS_urma_sq_size))));
    _rq_size = GetUrmaEffectiveRqSize();
    _io_mode = static_cast<uint8_t>(
        std::max(0, std::min(2, static_cast<int>(FLAGS_urma_io_mode))));
    _read_butex = bthread::butex_create_checked<butil::atomic<int>>();
    _read_butex->store(0, butil::memory_order_relaxed);
}

UrmaEndpoint::~UrmaEndpoint() {
    DeallocateResources();
    if (_read_butex) {
        bthread::butex_destroy(_read_butex);
        _read_butex = nullptr;
    }
}

void UrmaEndpoint::Reset() {
    DeallocateResources();
    DeallocateOneSidedBuffers();
    _state = UNINIT;
    _handshake_version = 0;
    _remote_recv_block_size = 0;
    _local_window_capacity = 0;
    _remote_window_capacity = 0;
    _remote_rq_window_size.store(0, butil::memory_order_relaxed);
    _sq_window_size.store(0, butil::memory_order_relaxed);
    _new_rq_wrs.store(0, butil::memory_order_relaxed);
    _sq_imm_window_size = 0;
    _sq_current = 0;
    _rq_received = 0;
    _pending_received_bytes.store(0, butil::memory_order_relaxed);
    _sbuf.clear();
    _rbuf.clear();
    _rbuf_data.clear();
    _read_butex->store(0, butil::memory_order_relaxed);
    _io_mode = static_cast<uint8_t>(
        std::max(0, std::min(2, static_cast<int>(FLAGS_urma_io_mode))));
    _remote_recv_buf_va = 0;
    _remote_recv_buf_size = 0;
    _remote_send_buf_va = 0;
    _remote_send_buf_size = 0;
    _one_sided_seq.store(1, butil::memory_order_relaxed);
    _rx_consume_seq.store(0, butil::memory_order_relaxed);
    for (uint32_t i = 0; i < URMA_RX_RING_SIZE; ++i) {
        _rx_slots[i].Reset();
    }
    {
        std::lock_guard<std::mutex> lock(_pending_sends_mutex);
        for (auto& pair : _pending_sends) {
            delete pair.second;
        }
        _pending_sends.clear();
    }
}

// ============================================================================
// Handshake IO helpers (ReadFromFd / WriteToFd / PushBackToReadBuf).
// Modeled on RdmaEndpoint::ReadFromFdLoop / WriteToFdLoop.
// ============================================================================

int UrmaEndpoint::ReadFromFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t received = 0;
    while (received < len) {
        const int expected_val = _read_butex->load(butil::memory_order_acquire);
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nr = read(fd, p + received, len - received);
        if (nr < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                int rc = bthread::butex_wait(_read_butex, expected_val, &duetime);
                if (rc < 0 && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
                    return -1;
                }
                continue;
            }
            return -1;
        }
        if (nr == 0) {
            errno = EEOF;
            return -1;
        }
        received += nr;
    }
    return 0;
}

void UrmaEndpoint::PushBackToReadBuf(const void* data, size_t len) {
    _socket->_read_buf.append(data, len);
}

int UrmaEndpoint::WriteToFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t written = 0;
    while (written < len) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = write(fd, p + written, len - written);
        if (nw >= 0) {
            written += nw;
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

// ============================================================================
// Hello builders / parsers.
// ============================================================================

void UrmaEndpoint::MakeLocalParsedHello(ParsedHello* out) const {
    *out = ParsedHello{};  // value-initialize (avoids memset on non-trivial type)
    out->buffer_size = static_cast<uint32_t>(GetUrmaRecvBlockSize());
    out->recv_buffer_cnt = _rq_size - 1;
    if (_resource && _resource->jetty) {
        out->jetty_id = _resource->jetty->jetty_id.id;
        out->uasid = GetUrmaLocalUasid();
        const urma_eid_t* local_eid = GetUrmaLocalEid();
        const uint8_t* advertised_eid =
            local_eid != nullptr
                ? local_eid->raw
                : _resource->jetty->jetty_id.eid.raw;
        std::memcpy(out->eid, advertised_eid, 16);
    }
    out->tp_type = static_cast<uint8_t>(URMA_CTP);
    // Pool segment: flatten g_pool_seg's seg fields.
    urma_target_seg_t* pool = GetPoolSegFor(nullptr);
    if (pool) {
        std::memcpy(out->seg_eid, pool->seg.ubva.eid.raw, 16);
        out->seg_uasid = GetUrmaLocalUasid();
        out->seg_va = pool->seg.ubva.va;
        out->seg_len = pool->seg.len;
        out->seg_token_id = pool->seg.token_id;
    }
    {
        char eid_str[URMA_EID_STR_LEN + 1] = {};
        char seg_eid_str[URMA_EID_STR_LEN + 1] = {};
        std::snprintf(eid_str, sizeof(eid_str), EID_FMT, EID_RAW_ARGS(out->eid));
        std::snprintf(seg_eid_str, sizeof(seg_eid_str), EID_FMT,
                      EID_RAW_ARGS(out->seg_eid));
        LOG(INFO) << "MakeLocalParsedHello: advertising jetty_id=" << out->jetty_id
                  << " uasid=" << out->uasid
                  << " (0 means kernel-owned, peer import will fail with EPERM)"
                  << " eid=" << eid_str
                  << " tp_type=" << static_cast<int>(out->tp_type)
                  << " seg_uasid=" << out->seg_uasid
                  << " seg_eid=" << seg_eid_str
                  << " seg_va=0x" << std::hex << out->seg_va << std::dec
                  << " seg_len=" << out->seg_len
                  << " on " << _socket->description();
    }
    // One-sided: fill send_buf / recv_buf info if we have allocated them.
    if (_io_mode != 0 && _send_buf_tseg) {
        out->io_mode = _io_mode;
        out->has_one_sided = true;
        const char* base = static_cast<const char*>(_one_sided_buf);
        // recv_buf is at the base of the registered segment.
        out->recv_buf_va = reinterpret_cast<uint64_t>(base);
        out->recv_buf_size = _recv_buf_capacity;
        out->recv_buf_token_id = _send_buf_tseg->seg.token_id;
        std::memcpy(out->recv_buf_seg_eid,
                    _send_buf_tseg->seg.ubva.eid.raw, 16);
        out->recv_buf_seg_uasid = _send_buf_tseg->seg.ubva.uasid;
        // send_buf is at base + recv_buf_size.
        out->send_buf_va = reinterpret_cast<uint64_t>(
            base + _recv_buf_capacity);
        out->send_buf_size = _send_buf_capacity;
        out->send_buf_token_id = _send_buf_tseg->seg.token_id;
        std::memcpy(out->send_buf_seg_eid,
                    _send_buf_tseg->seg.ubva.eid.raw, 16);
        out->send_buf_seg_uasid = _send_buf_tseg->seg.ubva.uasid;
    }
}

void UrmaEndpoint::FillLocalHelloV2(v2_wire::HelloMessage* out) const {
    *out = v2_wire::HelloMessage{};  // value-initialize
    out->msg_len = v2_wire::HELLO_PACKET_LEN;
    out->hello_ver = v2_wire::HELLO_V2_VERSION;
    out->impl_ver = v2_wire::IMPL_V2_VERSION;
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->buffer_size = p.buffer_size;
    out->recv_buffer_cnt = p.recv_buffer_cnt;
    out->jetty_id = p.jetty_id;
    std::memcpy(out->eid, p.eid, 16);
    out->uasid = p.uasid;
    out->tp_type = p.tp_type;
    std::memcpy(out->seg_eid, p.seg_eid, 16);
    out->seg_uasid = p.seg_uasid;
    out->seg_va = p.seg_va;
    out->seg_len = p.seg_len;
    out->seg_token_id = p.seg_token_id;
}

void UrmaEndpoint::FillLocalHelloV3(UrmaHello* out) const {
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->set_buffer_size(p.buffer_size);
    out->set_recv_buffer_cnt(p.recv_buffer_cnt);
    out->set_jetty_id(p.jetty_id);
    out->set_eid(p.eid, 16);
    out->set_uasid(p.uasid);
    out->set_tp_type(p.tp_type);
    out->set_seg_eid(p.seg_eid, 16);
    out->set_seg_uasid(p.seg_uasid);
    out->set_seg_va(p.seg_va);
    out->set_seg_len(p.seg_len);
    out->set_seg_token_id(p.seg_token_id);
    // One-sided parameters: only advertise if io_mode != 0 and we have
    // allocated the send/recv buffers.
    if (p.has_one_sided && _send_buf_tseg) {
        out->set_send_buf_va(p.send_buf_va);
        out->set_send_buf_size(p.send_buf_size);
        out->set_send_buf_token_id(p.send_buf_token_id);
        out->set_send_buf_seg_eid(p.send_buf_seg_eid, 16);
        out->set_send_buf_seg_uasid(p.send_buf_seg_uasid);
        out->set_recv_buf_va(p.recv_buf_va);
        out->set_recv_buf_size(p.recv_buf_size);
        out->set_recv_buf_token_id(p.recv_buf_token_id);
        out->set_recv_buf_seg_eid(p.recv_buf_seg_eid, 16);
        out->set_recv_buf_seg_uasid(p.recv_buf_seg_uasid);
        out->set_io_mode(p.io_mode);
    }
}

int UrmaEndpoint::WriteHelloV3(const UrmaHello& msg) {
    butil::IOBuf packet;
    packet.append("URM3", 4);
    std::string body;
    if (!msg.SerializeToString(&body)) {
        LOG(ERROR) << "Fail to serialize UrmaHello";
        return -1;
    }
    uint32_t pb_size_be = butil::HostToNet32(static_cast<uint32_t>(body.size()));
    packet.append(&pb_size_be, sizeof(pb_size_be));
    packet.append(body);
    return WriteToFd(packet);
}

int UrmaEndpoint::WriteToFd(butil::IOBuf& data) {
    // Write out the IOBuf in a single WriteToFd-style loop.
    while (!data.empty()) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = data.cut_into_file_descriptor(fd);
        if (nw >= 0) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

int UrmaEndpoint::ReadAndParseHelloV3(ParsedHello* out, bool* negotiated) {
    *negotiated = false;
    uint32_t pb_size_be = 0;
    if (ReadFromFd(&pb_size_be, sizeof(pb_size_be)) < 0) {
        return -1;
    }
    const uint32_t pb_size = butil::NetToHost32(pb_size_be);
    if (pb_size == 0 || pb_size > 4096) {
        return 0;
    }
    std::string body(pb_size, '\0');
    if (ReadFromFd(&body[0], pb_size) < 0) {
        return -1;
    }
    UrmaHello msg;
    if (!msg.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
        return 0;
    }
    if (msg.eid().size() != 16 || msg.seg_eid().size() != 16) {
        return 0;
    }
    out->buffer_size = msg.buffer_size();
    out->recv_buffer_cnt = msg.recv_buffer_cnt();
    out->jetty_id = msg.jetty_id();
    std::memcpy(out->eid, msg.eid().data(), 16);
    out->uasid = msg.uasid();
    out->tp_type = static_cast<uint8_t>(msg.tp_type());
    std::memcpy(out->seg_eid, msg.seg_eid().data(), 16);
    out->seg_uasid = msg.seg_uasid();
    out->seg_va = msg.seg_va();
    out->seg_len = msg.seg_len();
    out->seg_token_id = msg.seg_token_id();
    // One-sided parameters (v3 extension): all optional.
    if (msg.has_io_mode()) {
        out->io_mode = msg.io_mode();
        out->has_one_sided = (out->io_mode != 0);
        if (out->has_one_sided) {
            if (msg.send_buf_seg_eid().size() == 16) {
                out->send_buf_va = msg.send_buf_va();
                out->send_buf_size = msg.send_buf_size();
                out->send_buf_token_id = msg.send_buf_token_id();
                std::memcpy(out->send_buf_seg_eid,
                            msg.send_buf_seg_eid().data(), 16);
                out->send_buf_seg_uasid = msg.send_buf_seg_uasid();
            }
            if (msg.recv_buf_seg_eid().size() == 16) {
                out->recv_buf_va = msg.recv_buf_va();
                out->recv_buf_size = msg.recv_buf_size();
                out->recv_buf_token_id = msg.recv_buf_token_id();
                std::memcpy(out->recv_buf_seg_eid,
                            msg.recv_buf_seg_eid().data(), 16);
                out->recv_buf_seg_uasid = msg.recv_buf_seg_uasid();
            }
        }
    }
    if (!ValidHello(*out)) {
        return 0;
    }
    *negotiated = true;
    return 0;
}

// ============================================================================
// Allocate / deallocate per-connection resources.
// ============================================================================

int UrmaEndpoint::AllocateResources() {
    if (_resource) {
        return 0;
    }
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }

    _resource = new (std::nothrow) UrmaResource();
    if (!_resource) {
        return -1;
    }

    // Try the prepared pool first (sized sq/rq match).
    if (_sq_size <= static_cast<uint16_t>(FLAGS_urma_sq_size) &&
        _rq_size <= static_cast<uint16_t>(FLAGS_urma_rq_size)) {
        BAIDU_SCOPED_LOCK(g_prepared_mutex);
        if (g_prepared_list) {
            UrmaResource* next = g_prepared_list->next;
            delete _resource;
            _resource = g_prepared_list;
            g_prepared_list = next;
            _resource->next = nullptr;
            --g_prepared_cnt;
        }
    }

    if (!_resource->jfc) {
        // The SDK requires every JFC to reference a JFCE. Polling mode does
        // not arm or consume it, but still supplies the required object.
        _resource->jfce = urma_create_jfce(ctx);
        if (!_resource->jfce ||
            (!FLAGS_urma_use_polling && _resource->jfce->fd < 0)) {
            LOG(ERROR) << "Fail to create a usable URMA JFCE";
            errno = ENODEV;
            return -1;
        }

        urma_jfc_cfg_t jfc_cfg{};
        jfc_cfg.depth = static_cast<uint32_t>(_sq_size + _rq_size);
        jfc_cfg.jfce = _resource->jfce;
        _resource->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (!_resource->jfc) {
            PLOG(ERROR) << "urma_create_jfc";
            return -1;
        }

        urma_jfr_cfg_t jfr_cfg{};
        jfr_cfg.depth = static_cast<uint32_t>(_rq_size);
        jfr_cfg.trans_mode = URMA_TM_RM;
        jfr_cfg.max_sge =
            static_cast<uint8_t>(GetUrmaMaxJfrSge());
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.jfc = _resource->jfc;
        _resource->jfr = urma_create_jfr(ctx, &jfr_cfg);
        if (!_resource->jfr) {
            PLOG(ERROR) << "urma_create_jfr";
            return -1;
        }

        urma_jetty_cfg_t jetty_cfg{};
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(_sq_size);
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        jetty_cfg.jfs_cfg.priority = GetUrmaJettyPriority();
        jetty_cfg.jfs_cfg.max_sge =
            static_cast<uint8_t>(GetUrmaMaxSge());
        jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jetty_cfg.jfs_cfg.jfc = _resource->jfc;
        jetty_cfg.shared.jfr = _resource->jfr;
        jetty_cfg.shared.jfc = _resource->jfc;
        _resource->jetty = urma_create_jetty(ctx, &jetty_cfg);
        if (!_resource->jetty) {
            PLOG(ERROR) << "urma_create_jetty";
            return -1;
        }
    }

    _sbuf.resize(_sq_size - RESERVED_WR_NUM);
    _rbuf.resize(_rq_size);
    _rbuf_data.resize(_rq_size, nullptr);

    // Wrap the JFCE fd in a brpc Socket so PollCq is driven by epoll.
    if (!FLAGS_urma_use_polling) {
        if (!_resource->jfce || _resource->jfce->fd < 0) {
            LOG(ERROR) << "Prepared URMA resource has no usable JFCE";
            errno = ENODEV;
            return -1;
        }
        if (ReqNotifyCq() != 0) {
            return -1;
        }
        SocketOptions options;
        options.user = this;
        options.keytable_pool = _socket->keytable_pool();
        options.fd = _resource->jfce->fd;
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(ERROR) << "Fail to create CQ socket";
            return -1;
        }
    } else {
        // Polling mode: synthetic carrier socket (no fd).
        SocketOptions options;
        options.user = this;
        options.keytable_pool = _socket->keytable_pool();
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(ERROR) << "Fail to create CQ socket (polling)";
            return -1;
        }
        PollerAddCqSid();
    }
    // Allocate one-sided buffers if io_mode is enabled.
    if (_io_mode != 0) {
        if (AllocateOneSidedBuffers() != 0) {
            LOG(WARNING) << "Failed to allocate one-sided buffers; "
                         << "falling back to SEND_ONLY on "
                         << _socket->description();
            _io_mode = 0;
        }
    }
    return 0;
}

void UrmaEndpoint::DeallocateResources() {
    if (!_resource) {
        return;
    }

    if (FLAGS_urma_use_polling) {
        PollerRemoveCqSid();
    }

    // Tear down the CQ socket so the EventDispatcher stops calling PollCq.
    if (_cq_sid != INVALID_SOCKET_ID) {
        SocketUniquePtr s;
        if (Socket::Address(_cq_sid, &s) == 0) {
            if (s->fd() >= 0) {
                s->_io_event.RemoveConsumer(s->_fd);
            }
            s->_user = nullptr;  // Do not release user (this UrmaEndpoint).
            s->_fd = -1;  // Already removed fd from epoll.
            s->SetFailed();
        }
        _cq_sid = INVALID_SOCKET_ID;
    }

    // Reusing a Jetty requires a driver-supported RESET plus a complete JFC
    // drain. Until that lifecycle is implemented, prepared resources are
    // one-shot: they accelerate connection setup but are destroyed on close.
    delete _resource;
    _resource = nullptr;
}

// ============================================================================
// One-sided buffer allocation and deallocation.
// ============================================================================

int UrmaEndpoint::AllocateOneSidedBuffers() {
    if (_one_sided_buf) {
        return 0;  // already allocated
    }
    _recv_buf_capacity = static_cast<uint32_t>(
        std::max(1, FLAGS_urma_recv_buf_size)) * 1024;
    _send_buf_capacity = static_cast<uint32_t>(
        std::max(1, FLAGS_urma_send_buf_size)) * 1024;
    // Round to allocation unit.
    _recv_buf_capacity = (_recv_buf_capacity + URMA_ONE_SIDED_ALLOC_UNIT - 1) /
                         URMA_ONE_SIDED_ALLOC_UNIT * URMA_ONE_SIDED_ALLOC_UNIT;
    _send_buf_capacity = (_send_buf_capacity + URMA_ONE_SIDED_ALLOC_UNIT - 1) /
                         URMA_ONE_SIDED_ALLOC_UNIT * URMA_ONE_SIDED_ALLOC_UNIT;

    const size_t total = _recv_buf_capacity + _send_buf_capacity;
    _one_sided_buf = mmap(nullptr, total, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (_one_sided_buf == MAP_FAILED) {
        PLOG(ERROR) << "Fail to mmap one-sided buffers";
        _one_sided_buf = nullptr;
        return -1;
    }

    // Register the entire region as a single URMA segment.
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }
    urma_reg_seg_flag_t flag{};
    flag.bs.token_policy = URMA_TOKEN_NONE;
    flag.bs.cacheable = URMA_NON_CACHEABLE;
    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    urma_seg_cfg_t cfg{};
    cfg.va = reinterpret_cast<uint64_t>(_one_sided_buf);
    cfg.len = total;
    cfg.token_id = nullptr;
    cfg.token_value = {};
    cfg.flag = flag;
    cfg.user_ctx = reinterpret_cast<uint64_t>(_one_sided_buf);
    cfg.iova = 0;
    _send_buf_tseg = urma_register_seg(ctx, &cfg);
    if (!_send_buf_tseg) {
        PLOG(ERROR) << "Fail to register one-sided segment";
        munmap(_one_sided_buf, total);
        _one_sided_buf = nullptr;
        return -1;
    }

    // Initialize the send_buf allocator.
    _send_buf_alloc = new UrmaRingBuf();
    _send_buf_alloc->Init(_send_buf_capacity);

    LOG(INFO) << "Allocated one-sided buffers: recv_buf=" << _recv_buf_capacity
              << " send_buf=" << _send_buf_capacity
              << " total=" << total
              << " io_mode=" << static_cast<int>(_io_mode)
              << " on " << _socket->description();
    return 0;
}

void UrmaEndpoint::DeallocateOneSidedBuffers() {
    if (_send_buf_alloc) {
        delete _send_buf_alloc;
        _send_buf_alloc = nullptr;
    }
    if (_send_buf_tseg) {
        urma_unregister_seg(_send_buf_tseg);
        _send_buf_tseg = nullptr;
    }
    if (_one_sided_buf) {
        munmap(_one_sided_buf, _recv_buf_capacity + _send_buf_capacity);
        _one_sided_buf = nullptr;
    }
    _send_buf_capacity = 0;
    _recv_buf_capacity = 0;
    // Clean up any leftover pending send contexts.
    std::lock_guard<std::mutex> lock(_pending_sends_mutex);
    for (auto& pair : _pending_sends) {
        delete pair.second;
    }
    _pending_sends.clear();
}

// Post empty recv WRs: num_sge=1, len=0. These are consumed by incoming
// WRITE_IMM operations (URMA protocol requires a recv WR to be posted
// even for WRITE_IMM, and the completion arrives as WRITE_WITH_IMM).
int UrmaEndpoint::PostEmptyRecvWr(uint32_t count) {
    if (!_resource || !_resource->jfr) {
        errno = ENOTCONN;
        return -1;
    }
    for (uint32_t i = 0; i < count; ++i) {
        urma_jfr_wr_t wr{};
        std::memset(&wr, 0, sizeof(wr));
        urma_sge_t sge{};
        // Point at the recv_buf (valid registered memory, but len=0).
        if (_one_sided_buf) {
            sge.addr = reinterpret_cast<uint64_t>(_one_sided_buf);
            sge.len = 0;
            sge.tseg = _send_buf_tseg;
        } else {
            // Fallback: use pool seg.
            sge.addr = reinterpret_cast<uint64_t>(GetPoolSegFor(nullptr));
            sge.len = 0;
            sge.tseg = GetPoolSegFor(nullptr);
        }
        sge.user_tseg = nullptr;
        wr.src.sge = &sge;
        wr.src.num_sge = 1;
        wr.user_ctx = 0;
        urma_jfr_wr_t* bad = nullptr;
        if (urma_post_jfr_wr(_resource->jfr, &wr, &bad) != URMA_SUCCESS) {
            PLOG(WARNING) << "PostEmptyRecvWr failed";
            return -1;
        }
    }
    return 0;
}

UrmaSendContext* UrmaEndpoint::FindAndRemoveSendContext(uint64_t request_id) {
    std::lock_guard<std::mutex> lock(_pending_sends_mutex);
    auto it = _pending_sends.find(request_id);
    if (it == _pending_sends.end()) {
        return nullptr;
    }
    UrmaSendContext* ctx = it->second;
    _pending_sends.erase(it);
    return ctx;
}

// ============================================================================
// ImportPeer: the critical import_seg-before-import_jetty sequence.
// ============================================================================

int UrmaEndpoint::ImportPeer(const ParsedHello& peer) {
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }

    // 1. urma_import_seg FIRST so the kernel establishes TP routing for the
    //    remote EID. Without this the first SEND is rejected by hardware with
    //    URMA_CR_RNR_RETRY_CNT_EXC_ERR.
    urma_seg_t peer_seg{};
    std::memcpy(peer_seg.ubva.eid.raw, peer.seg_eid, 16);
    peer_seg.ubva.uasid = peer.seg_uasid;
    peer_seg.ubva.va = peer.seg_va;
    peer_seg.len = peer.seg_len;
    peer_seg.token_id = peer.seg_token_id;
    urma_token_t seg_token{};
    urma_import_seg_flag_t seg_flag{};
    seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
    seg_flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    seg_flag.bs.mapping = URMA_SEG_NOMAP;
    _resource->remote_seg = urma_import_seg(ctx, &peer_seg, &seg_token, 0, seg_flag);
    if (!_resource->remote_seg) {
        PLOG(ERROR) << "urma_import_seg failed";
        return -1;
    }

    // 2. urma_import_jetty.
    urma_rjetty_t remote{};
    std::memcpy(remote.jetty_id.eid.raw, peer.eid, 16);
    remote.jetty_id.uasid = peer.uasid;
    remote.jetty_id.id = peer.jetty_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    if (peer.tp_type > static_cast<uint8_t>(URMA_UTP)) {
        errno = EPROTO;
        return -1;
    }
    remote.tp_type = static_cast<urma_tp_type_t>(peer.tp_type);

    urma_token_t token{};
    const bool use_bonding_extension =
        IsUrmaBondingDevice() && remote.trans_mode == URMA_TM_RM;
    {
        char peer_eid_str[URMA_EID_STR_LEN + 1] = {};
        char peer_seg_eid_str[URMA_EID_STR_LEN + 1] = {};
        std::snprintf(peer_eid_str, sizeof(peer_eid_str), EID_FMT,
                      EID_RAW_ARGS(peer.eid));
        std::snprintf(peer_seg_eid_str, sizeof(peer_seg_eid_str), EID_FMT,
                      EID_RAW_ARGS(peer.seg_eid));
        LOG(INFO) << "ImportPeer about to call urma_import_jetty:"
                  << " remote_eid=" << peer_eid_str
                  << " remote_uasid=" << peer.uasid
                  << (peer.uasid == 0 ? " [DANGER] peer uasid=0 means kernel-"
                                       "owned jetty, urma_import_jetty will "
                                       "likely fail with EPERM"
                                     : "")
                  << " remote_jetty_id=" << peer.jetty_id
                  << " trans_mode=" << remote.trans_mode
                  << " tp_type=" << static_cast<int>(remote.tp_type)
                  << " seg_eid=" << peer_seg_eid_str
                  << " seg_uasid=" << peer.seg_uasid
                  << " seg_va=0x" << std::hex << peer.seg_va << std::dec
                  << " seg_len=" << peer.seg_len
                  << " bonding_extension=" << use_bonding_extension
                  << " local_ctx_uasid=" << ctx->uasid
                  << " on " << _socket->description();
    }
    errno = 0;
    if (use_bonding_extension) {
#if BRPC_URMA_HAS_BONDING_EXT
        // The bonding provider needs the local jetty to associate its send
        // path with the imported target. A plain import may return success
        // without setting that association, leaving traffic one-way only.
        bondp_rjetty_t bonding_remote{};
        bonding_remote.base = remote;
        bonding_remote.base.flag.bs.has_drv_ext = 1;
        bonding_remote.jetty = _resource->jetty;
        _resource->remote_jetty =
            urma_import_jetty(ctx, &bonding_remote.base, &token);
#else
        LOG(ERROR) << "Bonding remote jetty import requires provider header "
                      "urma_ubagg.h";
        errno = ENOTSUP;
#endif
    } else {
        _resource->remote_jetty = urma_import_jetty(ctx, &remote, &token);
    }
    if (!_resource->remote_jetty) {
        if (errno == 0) {
            errno = EIO;
        }
        char remote_eid[URMA_EID_STR_LEN + 1] = {};
        std::snprintf(remote_eid, sizeof(remote_eid), EID_FMT,
                      EID_RAW_ARGS(peer.eid));
        PLOG(ERROR) << "urma_import_jetty failed"
                    << " remote_eid=" << remote_eid
                    << " remote_uasid=" << peer.uasid
                    << " remote_jetty_id=" << peer.jetty_id
                    << " trans_mode=" << remote.trans_mode
                    << " tp_type=" << remote.tp_type
                    << " bonding_extension=" << use_bonding_extension;
        return -1;
    }
    return 0;
}

// ============================================================================
// Send / recv data path.
// ============================================================================

// Private IOBuf accessor mirroring RdmaIOBuf: reach into IOBuf block refs to
// build a urma_sge_t directly, without memcpy.
class UrmaIOBuf : private butil::IOBuf {
    friend class ::brpc::urma::UrmaEndpoint;
public:
    using butil::IOBuf::_ref_num;
    using butil::IOBuf::_ref_at;
    using butil::IOBuf::fetch1;
    using butil::IOBuf::get_first_data_meta;
    using butil::IOBuf::cutn;
    // Build the SGE for the current head block.
    // Returns bytes added, or -1 (errno set).
    ssize_t cut_into_sglist(urma_sge_t* sglist, size_t* sge_index,
                            butil::IOBuf* to, size_t max_sge,
                            size_t max_len, size_t max_sge_len) {
        size_t len = 0;
        while (*sge_index < max_sge && len < max_len && _ref_num() != 0) {
            butil::IOBuf::BlockRef const& r = _ref_at(0);
            const void* start = fetch1();
            urma_target_seg_t* tseg =
                GetPoolSegFor(const_cast<void*>(start));
            if (!tseg) {
                // User-registered memory: look up the seg handle.
                uint64_t meta = get_first_data_meta();
                if (meta != 0) {
                    tseg = reinterpret_cast<urma_target_seg_t*>(
                        static_cast<uintptr_t>(meta));
                }
            }
            if (!tseg) {
                errno = ERDMAMEM;
                return -1;
            }
            size_t this_len = r.length;
            if (len + this_len > max_len) {
                this_len = max_len - len;
            }
            // Bonding provider drops SEND WRs whose SGE payload exceeds
            // 4096 bytes (max_sge_len).  Cap this_len so each SGE stays
            // within the limit; the remaining bytes stay in the IOBuf for
            // the next SGE/WR iteration.
            if (max_sge_len > 0 && this_len > max_sge_len) {
                this_len = max_sge_len;
            }
            sglist[*sge_index].addr = reinterpret_cast<uint64_t>(start);
            sglist[*sge_index].len = static_cast<uint32_t>(this_len);
            sglist[*sge_index].tseg = tseg;
            sglist[*sge_index].user_tseg = nullptr;
            cutn(to, this_len);
            len += this_len;
            (*sge_index)++;
        }
        return static_cast<ssize_t>(len);
    }
};

ssize_t UrmaEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    // Dispatch based on IO mode.
    if (_io_mode == 0) {
        return CutFromIOBufList_Send(from, ndata);
    }
    // Calculate total payload size to decide small vs large path.
    size_t total = 0;
    for (size_t i = 0; i < ndata; ++i) {
        total += from[i]->size();
    }
    if (_io_mode == 1) {
        // WRITE_ONLY: all sizes use WriteInline.
        return WriteInline(from, ndata);
    }
    // HYBRID: small IO uses WriteInline, large IO uses WriteZeroCopy.
    if (total <= static_cast<size_t>(FLAGS_urma_inline_threshold)) {
        return WriteInline(from, ndata);
    }
    return WriteZeroCopy(from, ndata);
}

ssize_t UrmaEndpoint::CutFromIOBufList_Send(butil::IOBuf** from, size_t ndata) {
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    // T1: trace send start
    const bool trace_on = FLAGS_urma_trace_latency;
    if (trace_on) {
        _trace.send_start_us = butil::monotonic_time_us();
        _trace.send_first_post_us = 0;
        _trace.send_post_time_us = 0;
        _trace.send_wr_count = 0;
        _trace.send_eagain_count = 0;
        _trace.send_total_bytes = 0;
    }
    // brpc's KeepWrite model guarantees single-writer access to this
    // function per Socket, so no lock is needed for _sq_current or
    // urma_post_jetty_send_wr. Window variables are atomic with CAS.
    int max_sge = GetUrmaMaxSge();
    if (max_sge < 1) {
        max_sge = 1;
    }
    const uint32_t max_sge_len = GetUrmaSendMaxSgeLen();

    urma_sge_t* sglist = static_cast<urma_sge_t*>(
        alloca(sizeof(urma_sge_t) * max_sge));
    if (!sglist) {
        errno = ENOMEM;
        return -1;
    }
    std::memset(sglist, 0, sizeof(urma_sge_t) * max_sge);

    size_t current = 0;
    ssize_t total_len = 0;
    while (current < ndata) {
        uint16_t remote_wnd = _remote_rq_window_size.load(butil::memory_order_relaxed);
        uint16_t sq_wnd = _sq_window_size.load(butil::memory_order_relaxed);
        if (remote_wnd == 0 || sq_wnd == 0) {
            // T4: window blocked
            if (trace_on) {
                _trace.send_eagain_count++;
            }
            if (total_len > 0) {
                break;
            }
            errno = EAGAIN;
            return -1;
        }
        butil::IOBuf* to = &_sbuf[_sq_current];
        size_t sge_index = 0;
        size_t this_len = 0;
        size_t max_len = _remote_recv_block_size > 0
                             ? _remote_recv_block_size
                             : GetUrmaRecvBlockSize();
        while (sge_index < static_cast<size_t>(max_sge) &&
               this_len < max_len && current < ndata) {
            auto* data = reinterpret_cast<UrmaIOBuf*>(from[current]);
            if (data->empty()) {
                ++current;
                continue;
            }
            ssize_t n = data->cut_into_sglist(sglist, &sge_index, to,
                                              max_sge, max_len - this_len,
                                              max_sge_len);
            if (n < 0) {
                return -1;
            }
            this_len += n;
        }
        if (sge_index == 0) {
            break;
        }

        urma_sg_t sg{sglist, static_cast<uint32_t>(sge_index)};
        urma_jfs_wr_t wr{};
        std::memset(&wr, 0, sizeof(wr));
        // Send payload with URMA_OPC_SEND. Receive credits are flushed
        // separately by SendImm() after SendAck() reaches its threshold.
        // Piggybacking credits turns every payload into SEND_IMM and can
        // produce asymmetric completions with the bonding provider.
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.flag.bs.solicited_enable = 1;
        wr.tjetty = _resource->remote_jetty;
        wr.send.src = sg;
        // Use the SQ slot index as user_ctx so that error completions can
        // identify which _sbuf slot to reclaim. umq_ub uses the buffer
        // pointer; we use the slot index for the same purpose.
        wr.user_ctx = static_cast<uint64_t>(_sq_current + 1);
        urma_jfs_wr_t* bad_wr = nullptr;
        const uint16_t sq_slot = _sq_current;
        const uint32_t local_jetty_id = _resource->jetty->jetty_id.id;
        const uint32_t remote_jetty_id = _resource->remote_jetty->id.id;

        // Reserve both credits before making the WR visible to the provider.
        // In polling mode a completion (and even the peer's receive-credit
        // ACK) can be processed by another thread before post_send returns.
        // Decrementing after post therefore creates a transient capacity + 1
        // window and makes the strict credit check tear down a healthy
        // connection.
        _remote_rq_window_size.fetch_sub(1, butil::memory_order_relaxed);
        _sq_window_size.fetch_sub(1, butil::memory_order_relaxed);
        // T2: trace per-WR post time
        const int64_t post_t0 = trace_on ? butil::monotonic_time_us() : 0;
        int rc = urma_post_jetty_send_wr(_resource->jetty, &wr, &bad_wr);
        if (trace_on) {
            const int64_t post_dt = butil::monotonic_time_us() - post_t0;
            _trace.send_post_time_us += post_dt;
            if (_trace.send_first_post_us == 0) {
                _trace.send_first_post_us = post_t0;
            }
            _trace.send_wr_count++;
            _trace.send_total_bytes += this_len;
        }
        if (rc != URMA_SUCCESS) {
            const int provider_errno = errno;
            _remote_rq_window_size.fetch_add(1, butil::memory_order_relaxed);
            _sq_window_size.fetch_add(1, butil::memory_order_relaxed);
            LOG(WARNING) << "urma_post_jetty_send_wr failed: " << rc
                         << ", provider_errno=" << provider_errno
                         << " (" << berror(provider_errno) << ')'
                         << ", bad_wr=" << static_cast<const void*>(bad_wr)
                         << ", bad_is_current=" << (bad_wr == &wr)
                         << ", sq_slot=" << sq_slot
                         << ", local_jetty_id=" << local_jetty_id
                         << ", remote_jetty_id=" << remote_jetty_id
                         << ", state=" << GetStateStr()
                         << ", sq_window=" << sq_wnd
                         << ", remote_rq_window=" << remote_wnd
                         << ", num_sge=" << sge_index
                         << ", configured_max_sge=" << GetUrmaMaxSge()
                         << ", payload_size=" << this_len
                         << " on " << _socket->description();
            errno = rc;
            return -1;
        }
        _sq_current = (_sq_current + 1) % (_sq_size - RESERVED_WR_NUM);
        total_len += static_cast<ssize_t>(this_len);
        VLOG(99) << "URMA post WR: sq_slot=" << sq_slot
                  << " sge_count=" << sge_index
                  << " payload=" << this_len
                  << " total_sent=" << total_len
                  << " sq_wnd=" << _sq_window_size.load(butil::memory_order_relaxed)
                  << " remote_rq_wnd=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
                  << " max_sge=" << max_sge
                  << " max_len=" << max_len
                  << " max_sge_len=" << max_sge_len
                  << " ndata=" << ndata
                  << " current=" << current
                  << " on " << _socket->description();
    }
    // T3: trace send complete summary
    if (trace_on && total_len > 0) {
        const int64_t now_us = butil::monotonic_time_us();
        const int64_t total_us = now_us - _trace.send_start_us;
        LOG(INFO) << "[URMA-TRACE] send: wrs=" << _trace.send_wr_count
                  << " bytes=" << _trace.send_total_bytes
                  << " eagain=" << _trace.send_eagain_count
                  << " post_time=" << _trace.send_post_time_us << "us"
                  << " total=" << total_us << "us"
                  << " sq_wnd=" << _sq_window_size.load(butil::memory_order_relaxed)
                  << " remote_rq_wnd=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
                  << " on " << _socket->description();
    }
    return total_len;
}

bool UrmaEndpoint::IsWritable() const {
    if (_io_mode == 0) {
        return _remote_rq_window_size.load(butil::memory_order_relaxed) > 0 &&
               _sq_window_size.load(butil::memory_order_relaxed) > 0;
    }
    // One-sided: writable if send_buf has at least one allocation unit free.
    return _send_buf_alloc &&
           _send_buf_alloc->Available() >= URMA_ONE_SIDED_ALLOC_UNIT;
}

// ============================================================================
// One-sided send paths.
// ============================================================================

// Small IO path: WRITE_IN_BAND.
// Copy IOBuf data into send_buf, then WRITE_IMM into peer's recv_buf.
ssize_t UrmaEndpoint::WriteInline(butil::IOBuf** from, size_t ndata) {
    if (!_resource || !_resource->jetty || !_resource->remote_jetty ||
        !_send_buf_alloc || !_send_buf_tseg || !_one_sided_buf) {
        errno = ENOTCONN;
        return -1;
    }
    // Calculate total payload size.
    size_t total_payload = 0;
    for (size_t i = 0; i < ndata; ++i) {
        total_payload += from[i]->size();
    }
    if (total_payload == 0) {
        return 0;
    }

    // Allocate send_buf space: UrmaMessageHead + payload.
    const uint32_t alloc_size = static_cast<uint32_t>(
        sizeof(UrmaMessageHead) + total_payload);
    uint32_t offset = 0;
    if (!_send_buf_alloc->Allocate(alloc_size, &offset)) {
        errno = EAGAIN;
        return -1;
    }

    // The send_buf starts at _one_sided_buf + _recv_buf_capacity.
    char* send_buf = static_cast<char*>(_one_sided_buf) + _recv_buf_capacity;
    char* dst = send_buf + offset;

    // Fill UrmaMessageHead.
    UrmaMessageHead* head = reinterpret_cast<UrmaMessageHead*>(dst);
    head->magic = URMA_CTRL_MAGIC;
    head->message_size = alloc_size;
    head->data_count = static_cast<uint32_t>(total_payload);
    head->flags = 0;
    const uint64_t request_id = _one_sided_seq.fetch_add(1,
                                butil::memory_order_relaxed);
    head->request_id = request_id;

    // Copy IOBuf data after the head.
    size_t copied = 0;
    for (size_t i = 0; i < ndata; ++i) {
        if (from[i]->empty()) {
            continue;
        }
        from[i]->copy_to(dst + sizeof(UrmaMessageHead) + copied,
                         from[i]->size());
        copied += from[i]->size();
    }

    // Build WRITE_IMM WR: src=local send_buf, dst=peer recv_buf (mirrored offset).
    urma_sge_t src_sge{};
    src_sge.addr = reinterpret_cast<uint64_t>(dst);
    src_sge.len = alloc_size;
    src_sge.tseg = _send_buf_tseg;
    src_sge.user_tseg = nullptr;

    urma_sge_t dst_sge{};
    dst_sge.addr = _remote_recv_buf_va + offset;
    dst_sge.len = alloc_size;
    dst_sge.tseg = _resource->remote_recv_buf_seg;
    dst_sge.user_tseg = nullptr;

    urma_jfs_wr_t wr{};
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src.sge = &src_sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dst_sge;
    wr.rw.dst.num_sge = 1;

    // Encode imm_data: opcode + buffer_offset.
    UrmaWriteImmData imm{};
    imm.io.opcode = URMA_IO_WRITE_IN_BAND;
    imm.io.buffer_offset = static_cast<uint16_t>(offset / URMA_ONE_SIDED_ALLOC_UNIT);
    wr.rw.notify_data = imm.data;

    // Encode user_ctx for completion identification.
    wr.user_ctx = EncodeUserCtx(CTRL_DATA_REQUEST, request_id);

    // Register the send context.
    auto* ctx = new UrmaSendContext();
    ctx->request_id = request_id;
    ctx->send_buf_offset = offset;
    ctx->send_buf_size = alloc_size;
    ctx->opcode = URMA_IO_WRITE_IN_BAND;
    {
        std::lock_guard<std::mutex> lock(_pending_sends_mutex);
        _pending_sends[request_id] = ctx;
    }

    urma_jfs_wr_t* bad = nullptr;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        const int provider_errno = errno;
        LOG(WARNING) << "WriteInline: urma_post_jetty_send_wr failed: "
                     << status << " provider_errno=" << provider_errno
                     << " on " << _socket->description();
        _send_buf_alloc->Release(offset, alloc_size);
        FindAndRemoveSendContext(request_id);
        delete ctx;
        errno = status;
        return -1;
    }

    // Consume the IOBuf data (it's been copied to send_buf).
    for (size_t i = 0; i < ndata; ++i) {
        from[i]->clear();
    }
    return static_cast<ssize_t>(total_payload);
}

// Large IO path: PRE_WRITE + READ.
// Build a control message listing IOBuf block addresses, send via WRITE_IMM.
// The receiver will READ the data from our pool buffers.
ssize_t UrmaEndpoint::WriteZeroCopy(butil::IOBuf** from, size_t ndata) {
    if (!_resource || !_resource->jetty || !_resource->remote_jetty ||
        !_send_buf_alloc || !_send_buf_tseg || !_one_sided_buf) {
        errno = ENOTCONN;
        return -1;
    }
    // Collect IOBuf blocks: {addr, size, tseg} for each block.
    struct BlockInfo {
        uint64_t addr;
        uint32_t size;
        urma_target_seg_t* tseg;
    };
    std::vector<BlockInfo> blocks;
    size_t total_payload = 0;
    const uint32_t max_sge_len = GetUrmaMaxSgeLen();
    for (size_t i = 0; i < ndata; ++i) {
        butil::IOBuf* buf = from[i];
        const size_t num_blocks = buf->backing_block_num();
        for (size_t j = 0; j < num_blocks; ++j) {
            butil::StringPiece sp = buf->backing_block(j);
            if (sp.empty()) {
                continue;
            }
            const void* start = sp.data();
            urma_target_seg_t* tseg =
                GetPoolSegFor(const_cast<void*>(start));
            if (!tseg) {
                // User-registered memory: look up the seg handle.
                uint64_t meta = buf->get_first_data_meta();
                if (meta != 0) {
                    tseg = reinterpret_cast<urma_target_seg_t*>(
                        static_cast<uintptr_t>(meta));
                }
            }
            if (!tseg) {
                errno = ERDMAMEM;
                return -1;
            }
            // Split blocks larger than max_sge_len.
            uint32_t remaining = static_cast<uint32_t>(sp.size());
            uint64_t addr = reinterpret_cast<uint64_t>(start);
            while (remaining > 0) {
                uint32_t chunk = remaining;
                if (max_sge_len > 0 && chunk > max_sge_len) {
                    chunk = max_sge_len;
                }
                blocks.push_back({addr, chunk, tseg});
                total_payload += chunk;
                addr += chunk;
                remaining -= chunk;
            }
        }
    }
    if (blocks.empty()) {
        return 0;
    }

    // Build control message: UrmaMessageHead + PageBufferInMessage[blocks.size()].
    const uint32_t ctrl_msg_size = static_cast<uint32_t>(
        sizeof(UrmaMessageHead) +
        blocks.size() * sizeof(PageBufferInMessage));
    uint32_t offset = 0;
    if (!_send_buf_alloc->Allocate(ctrl_msg_size, &offset)) {
        errno = EAGAIN;
        return -1;
    }

    char* send_buf = static_cast<char*>(_one_sided_buf) + _recv_buf_capacity;
    char* dst = send_buf + offset;

    UrmaMessageHead* head = reinterpret_cast<UrmaMessageHead*>(dst);
    head->magic = URMA_CTRL_MAGIC;
    head->message_size = ctrl_msg_size;
    head->data_count = static_cast<uint32_t>(blocks.size());
    head->flags = 0;
    const uint64_t request_id = _one_sided_seq.fetch_add(1,
                                butil::memory_order_relaxed);
    head->request_id = request_id;

    auto* entries = reinterpret_cast<PageBufferInMessage*>(
        dst + sizeof(UrmaMessageHead));
    for (size_t i = 0; i < blocks.size(); ++i) {
        entries[i].addr = blocks[i].addr;
        entries[i].size = blocks[i].size;
        entries[i].seg_token_id = 0;  // pool seg already imported by peer
    }

    // Build WRITE_IMM WR for the control message.
    urma_sge_t src_sge{};
    src_sge.addr = reinterpret_cast<uint64_t>(dst);
    src_sge.len = ctrl_msg_size;
    src_sge.tseg = _send_buf_tseg;
    src_sge.user_tseg = nullptr;

    urma_sge_t dst_sge{};
    dst_sge.addr = _remote_recv_buf_va + offset;
    dst_sge.len = ctrl_msg_size;
    dst_sge.tseg = _resource->remote_recv_buf_seg;
    dst_sge.user_tseg = nullptr;

    urma_jfs_wr_t wr{};
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src.sge = &src_sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dst_sge;
    wr.rw.dst.num_sge = 1;

    UrmaWriteImmData imm{};
    imm.io.opcode = URMA_IO_PRE_WRITE;
    imm.io.buffer_offset = static_cast<uint16_t>(offset / URMA_ONE_SIDED_ALLOC_UNIT);
    wr.rw.notify_data = imm.data;
    wr.user_ctx = EncodeUserCtx(CTRL_DATA_REQUEST, request_id);

    // Register the send context and save IOBuf block references.
    auto* ctx = new UrmaSendContext();
    ctx->request_id = request_id;
    ctx->send_buf_offset = offset;
    ctx->send_buf_size = ctrl_msg_size;
    ctx->opcode = URMA_IO_PRE_WRITE;
    for (size_t i = 0; i < ndata; ++i) {
        ctx->saved_blocks.append(*from[i]);
    }
    {
        std::lock_guard<std::mutex> lock(_pending_sends_mutex);
        _pending_sends[request_id] = ctx;
    }

    urma_jfs_wr_t* bad = nullptr;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        const int provider_errno = errno;
        LOG(WARNING) << "WriteZeroCopy: urma_post_jetty_send_wr failed: "
                     << status << " provider_errno=" << provider_errno
                     << " on " << _socket->description();
        _send_buf_alloc->Release(offset, ctrl_msg_size);
        FindAndRemoveSendContext(request_id);
        delete ctx;
        errno = status;
        return -1;
    }

    // Consume the IOBuf data (blocks are kept alive by saved_blocks).
    for (size_t i = 0; i < ndata; ++i) {
        from[i]->clear();
    }
    return static_cast<ssize_t>(total_payload);
}

// Send a control response (WRITE_IN_BAND_ACK or POST_WRITE) back to the
// peer's send_buf at the mirrored offset.
int UrmaEndpoint::ResponseCtrlMessage(uint8_t opcode, uint16_t buffer_offset,
                                       uint64_t request_id,
                                       uint32_t message_size) {
    if (!_resource || !_resource->jetty || !_resource->remote_jetty ||
        !_send_buf_tseg || !_one_sided_buf) {
        errno = ENOTCONN;
        return -1;
    }
    // Write a small control message into our recv_buf (which mirrors the
    // peer's send_buf offset). The peer reads the UrmaMessageHead from
    // its send_buf at the same offset.
    char* recv_buf = static_cast<char*>(_one_sided_buf);
    const uint32_t offset = buffer_offset * URMA_ONE_SIDED_ALLOC_UNIT;
    char* dst = recv_buf + offset;

    // Write the UrmaMessageHead into our recv_buf. The peer will see this
    // in its send_buf (mirrored region).
    UrmaMessageHead* head = reinterpret_cast<UrmaMessageHead*>(dst);
    head->magic = URMA_CTRL_MAGIC;
    head->message_size = message_size;
    head->data_count = 0;
    head->flags = 0;
    head->request_id = request_id;

    // Build WRITE_IMM WR: src=local recv_buf, dst=peer send_buf (mirrored offset).
    urma_sge_t src_sge{};
    src_sge.addr = reinterpret_cast<uint64_t>(dst);
    src_sge.len = static_cast<uint32_t>(sizeof(UrmaMessageHead));
    src_sge.tseg = _send_buf_tseg;
    src_sge.user_tseg = nullptr;

    urma_sge_t dst_sge{};
    dst_sge.addr = _remote_send_buf_va + offset;
    dst_sge.len = static_cast<uint32_t>(sizeof(UrmaMessageHead));
    dst_sge.tseg = _resource->remote_recv_buf_seg;  // same segment
    dst_sge.user_tseg = nullptr;

    urma_jfs_wr_t wr{};
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src.sge = &src_sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dst_sge;
    wr.rw.dst.num_sge = 1;

    UrmaWriteImmData imm{};
    imm.io.opcode = opcode;
    imm.io.buffer_offset = buffer_offset;
    wr.rw.notify_data = imm.data;
    wr.user_ctx = EncodeUserCtx(CTRL_DATA_RESPONSE, request_id);

    urma_jfs_wr_t* bad = nullptr;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        PLOG(WARNING) << "ResponseCtrlMessage: post failed: " << status
                      << " on " << _socket->description();
        errno = status;
        return -1;
    }
    return 0;
}

// ============================================================================
// Recv path.
// ============================================================================

int UrmaEndpoint::DoPostRecv(void* block, size_t block_size) {
    urma_target_seg_t* tseg = GetPoolSegFor(block);
    if (!tseg) {
        errno = ERDMAMEM;
        return -1;
    }
    urma_sge_t sge{reinterpret_cast<uint64_t>(block),
                   static_cast<uint32_t>(block_size), tseg, nullptr};
    urma_sg_t sg{&sge, 1};
    urma_jfr_wr_t wr{sg, 0, nullptr};
    urma_jfr_wr_t* bad = nullptr;
    // Use the shared-JFR path on every device, including bonding. The bonding
    // provider owns physical receive scheduling for the JFR; a local
    // jetty-to-target association is not part of the RM receive API.
    const urma_status_t status =
        urma_post_jfr_wr(_resource->jfr, &wr, &bad);
    if (status != URMA_SUCCESS) {
        LOG(WARNING) << "Failed to post URMA receive WR: status=" << status
                     << " bonding=" << IsUrmaBondingDevice()
                     << " bad_wr=" << static_cast<const void*>(bad)
                     << " bad_is_current=" << (bad == &wr)
                     << " local_jetty_id=" << _resource->jetty->jetty_id.id
                     << " provider_associated_remote="
                     << static_cast<const void*>(
                            _resource->jetty->remote_jetty)
                     << " state=" << GetStateStr()
                     << " on " << _socket->description();
        errno = status;
        return -1;
    }
    return 0;
}

int UrmaEndpoint::PostRecv(uint32_t num, bool zerocopy) {
    for (uint32_t i = 0; i < num; ++i) {
        size_t block_size = GetUrmaRecvBlockSize();
        if (zerocopy) {
            _rbuf[_rq_received].clear();
            butil::IOBufAsZeroCopyOutputStream zcis(
                &_rbuf[_rq_received], block_size + IOBUF_BLOCK_HEADER_LEN);
            void* data = nullptr;
            int size = 0;
            if (!zcis.Next(&data, &size) || !data ||
                size < static_cast<int>(block_size)) {
                errno = ENOMEM;
                return -1;
            }
            _rbuf_data[_rq_received] = data;
            if (DoPostRecv(data, block_size) < 0) {
                return -1;
            }
        } else {
            if (_rbuf_data[_rq_received] == nullptr) {
                _rbuf[_rq_received].clear();
                butil::IOBufAsZeroCopyOutputStream zcos(
                    &_rbuf[_rq_received],
                    block_size + IOBUF_BLOCK_HEADER_LEN);
                void* data = nullptr;
                int size = 0;
                if (!zcos.Next(&data, &size) || !data ||
                    size < static_cast<int>(block_size)) {
                    errno = ENOMEM;
                    return -1;
                }
                _rbuf_data[_rq_received] = data;
            }
            if (DoPostRecv(_rbuf_data[_rq_received], block_size) < 0) {
                return -1;
            }
        }
        _rq_received = (_rq_received + 1) % _rq_size;
    }
    return 0;
}

int UrmaEndpoint::SendImm(uint32_t imm) {
    if (imm == 0) {
        return 0;
    }
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    if (_sq_imm_window_size == 0) {
        errno = EAGAIN;
        return -1;
    }
    // Empty-payload SEND_IMM flushes peer-side receive credit. Connection
    // lifetime is owned by the TCP fd, so this is not an EOF marker.
    urma_jfs_wr_t wr{};
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.flag.bs.solicited_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.send.imm_data = imm;
    wr.user_ctx = 0;  // 0 == pure ack (HandleCompletion reuses budget).
    urma_jfs_wr_t* bad = nullptr;
    // Reserve the ACK-only SQ slot before posting for the same reason as the
    // data windows in CutFromIOBufList: polling may observe its completion as
    // soon as the provider accepts the WR.
    --_sq_imm_window_size;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        const int provider_errno = errno;
        ++_sq_imm_window_size;
        _new_rq_wrs.fetch_add(imm, butil::memory_order_relaxed);
        LOG(WARNING) << "Failed to post URMA credit ACK: status=" << status
                     << " provider_errno=" << provider_errno
                     << " (" << berror(provider_errno) << ')'
                     << " bad_wr=" << static_cast<const void*>(bad)
                     << " bad_is_current=" << (bad == &wr)
                     << " imm=" << imm
                     << " local_jetty_id="
                     << _resource->jetty->jetty_id.id
                     << " remote_jetty_id="
                     << _resource->remote_jetty->id.id
                     << " state=" << GetStateStr()
                     << " on " << _socket->description();
        errno = status;
        return -1;
    }
    return 0;
}

int UrmaEndpoint::SendAck(int num) {
    const uint16_t old =
        _new_rq_wrs.fetch_add(num, butil::memory_order_relaxed);
    if (old + num > _remote_window_capacity / 2 &&
        _sq_imm_window_size > 0) {
        return SendImm(_new_rq_wrs.exchange(0, butil::memory_order_relaxed));
    }
    return 0;
}

ssize_t UrmaEndpoint::HandleCompletion(const urma_cr_t& cr) {
    bool zerocopy = FLAGS_urma_recv_zerocopy;
    if (cr.status != URMA_CR_SUCCESS) {
        // Distinguish fatal vs non-fatal completion errors.
        // FLUSH/UNHANDLED: jetty torn down — fatal.
        // REM_ACCESS_ABORT_ERR (status=8) on bonding devices: TP dead — fatal.
        // Other errors (RNR_RETRY, ACK_TIMEOUT, LOC_ACCESS): non-fatal,
        // the SQ/RQ windows are replenished and the loop continues.
        if (cr.status == URMA_CR_WR_FLUSH_ERR ||
            cr.status == URMA_CR_WR_UNHANDLED) {
            // Jetty is being torn down — this IS fatal.
            LOG(ERROR) << "URMA fatal completion error (jetty flush): status="
                       << cr.status << " s_r=" << cr.flag.bs.s_r
                       << " user_ctx=" << cr.user_ctx
                       << " sq_wnd=" << _sq_window_size.load(butil::memory_order_relaxed)
                       << " remote_rq_wnd=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
                       << " state=" << GetStateStr()
                       << " on " << _socket->description();
            errno = EIO;
            return -1;
        }
        if (cr.status == URMA_CR_WR_SUSPEND_DONE ||
            cr.status == URMA_CR_WR_FLUSH_ERR_DONE) {
            // Hardware-generated fake CQE; user_ctx is invalid.
            // Just log and skip — do NOT touch user_ctx.
            LOG(WARNING) << "URMA fake CQE: status=" << cr.status
                         << " on " << _socket->description();
            return 0;
        }
        // Distinguish REM_ACCESS_ABORT_ERR (status=8) on bonding devices from
        // other transient errors. The bonding provider TP enters a dead state
        // after returning status=8: no further completions are generated for
        // the jetty, so continuing is futile. The connection must be rebuilt.
        const bool fatal_tp_error =
            (cr.status == URMA_CR_REM_ACCESS_ABORT_ERR);
        if (cr.flag.bs.s_r == 0) {
            // --- TX (send) completion error ---
            LOG(WARNING) << "URMA TX completion error: status=" << cr.status
                         << " user_ctx=" << cr.user_ctx
                         << " sq_wnd=" << _sq_window_size.load(butil::memory_order_relaxed)
                         << " remote_rq_wnd=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
                         << " sq_capacity=" << _local_window_capacity
                         << " fatal=" << fatal_tp_error
                         << " on " << _socket->description();
            if (cr.user_ctx == 0) {
                // Pure-ack WR error: replenish imm budget only.
                if (_sq_imm_window_size < RESERVED_WR_NUM) {
                    _sq_imm_window_size += 1;
                }
            } else {
                // Data WR error: reclaim the send buffer and SQ window.
                // user_ctx is the SQ slot index + 1 (0 reserved for ack).
                uint16_t slot = static_cast<uint16_t>(cr.user_ctx - 1);
                if (slot < (_sq_size - RESERVED_WR_NUM)) {
                    _sbuf[slot].clear();
                }
                // Use the same CAS loop as the success path to avoid racing
                // with the send path's fetch_sub on _sq_window_size.
                uint16_t old =
                    _sq_window_size.load(butil::memory_order_relaxed);
                while (true) {
                    if (old >= _local_window_capacity) {
                        LOG(WARNING)
                            << "URMA TX error: sq_window overflow: old="
                            << old << " capacity=" << _local_window_capacity
                            << " on " << _socket->description();
                        break;
                    }
                    if (_sq_window_size.compare_exchange_weak(
                            old, static_cast<uint16_t>(old + 1),
                            butil::memory_order_relaxed)) {
                        break;
                    }
                }
                // The remote RQE was not consumed — return the credit.
                _remote_rq_window_size.fetch_add(
                    1, butil::memory_order_relaxed);
            }
            butil::subtle::MemoryBarrier();
            _socket->WakeAsEpollOut();
            if (fatal_tp_error) {
                // Bonding provider TP is dead after status=8 — no more
                // completions will arrive. Mark the connection failed so
                // brpc retries on a new connection.
                LOG(ERROR) << "URMA TX fatal TP error (status=8): bonding "
                           << "provider TP dead, failing connection "
                           << _socket->description();
                errno = EIO;
                return -1;
            }
            return 0;  // Non-fatal: continue processing CQEs.
        } else {
            // --- RX (recv) completion error ---
            LOG(WARNING) << "URMA RX completion error: status=" << cr.status
                         << " on " << _socket->description();
            if (fatal_tp_error) {
                // Same as TX: bonding provider TP is dead after status=8.
                LOG(ERROR) << "URMA RX fatal TP error (status=8): bonding "
                           << "provider TP dead, failing connection "
                           << _socket->description();
                errno = EIO;
                return -1;
            }
            // Replenish the recv buffer that was consumed by this failed
            // completion, so subsequent receives can proceed.
            if (PostRecv(1, zerocopy) < 0) {
                LOG(ERROR) << "URMA RX error: PostRecv failed after RX error";
                errno = EIO;
                return -1;
            }
            SendAck(1);
            return 0;  // Non-fatal: continue processing CQEs.
        }
    }
    if (cr.flag.bs.s_r == 0) {
        // Send completion: reclaim SQ window and wake the writer.
        // T6: trace TX completion
        if (FLAGS_urma_trace_latency) {
            _trace.recv_tx_complete++;
        }
        // One-sided send completion: check user_ctx encoding.
        if (cr.user_ctx != 0) {
            const OneSideSenderType type = DecodeUserCtxType(cr.user_ctx);
            if (type == READ_DATA_REQUEST) {
                return HandleReadCompletion(cr);
            }
            if (type == CTRL_DATA_REQUEST || type == CTRL_DATA_RESPONSE) {
                // Send completion for our WRITE_IMM (data or control response).
                // The send_buf slot is released when we receive the ACK
                // (WRITE_IN_BAND_ACK or POST_WRITE), not here. For
                // CTRL_DATA_RESPONSE, there's nothing to do — the response
                // is fire-and-forget.
                return 0;
            }
            // Fall through to existing SEND path for user_ctx != 0
            // that is not one-sided encoded.
        }
        if (cr.user_ctx == 0) {
            // Pure-ack WR: just replenish the imm budget.
            if (_sq_imm_window_size >= RESERVED_WR_NUM) {
                LOG(WARNING)
                    << "URMA credit-ACK completion exceeds reserved SQ "
                       "window: current="
                    << _sq_imm_window_size
                    << " capacity=" << RESERVED_WR_NUM
                    << " on " << _socket->description();
                errno = EPROTO;
                return -1;
            }
            _sq_imm_window_size += 1;
            SendAck(0);
            return 0;
        }
        uint16_t wnd = 1;  // We signal every WR (complete_enable=1).
        // Use user_ctx (SQ slot index + 1) to locate the completed buffer.
        // URMA does not guarantee in-order completions, so _sq_sent-based
        // clearing would free the wrong slot on out-of-order completions.
        const uint16_t slot = static_cast<uint16_t>(cr.user_ctx - 1);
        if (slot >= (_sq_size - RESERVED_WR_NUM)) {
            LOG(WARNING) << "URMA send completion has invalid user_ctx="
                         << cr.user_ctx << " on " << _socket->description();
            errno = EPROTO;
            return -1;
        }
        _sbuf[slot].clear();
        uint16_t old =
            _sq_window_size.load(butil::memory_order_relaxed);
        while (true) {
            if (old >= _local_window_capacity) {
                LOG(WARNING)
                    << "URMA send completion exceeds SQ window: old=" << old
                    << " increment=" << wnd
                    << " capacity=" << _local_window_capacity
                    << " user_ctx=" << cr.user_ctx
                    << " on " << _socket->description();
                errno = EPROTO;
                return -1;
            }
            if (_sq_window_size.compare_exchange_weak(
                    old, static_cast<uint16_t>(old + wnd),
                    butil::memory_order_relaxed)) {
                break;
            }
        }
        butil::subtle::MemoryBarrier();
        // Wake the send path unconditionally: the SQ slot is reclaimed
        // and _remote_rq_window_size may have been replenished by an
        // incoming SEND_WITH_IMM since the last check.
        _socket->WakeAsEpollOut();
        return 0;
    }
    // Recv completion.
    // One-sided: WRITE_IMM completions arrive as URMA_CR_OPC_WRITE_WITH_IMM.
    if (cr.opcode == URMA_CR_OPC_WRITE_WITH_IMM) {
        // Decode the immediate data to determine the one-sided opcode.
        const UrmaWriteImmData imm{cr.imm_data};
        const uint32_t offset = imm.io.buffer_offset *
                                URMA_ONE_SIDED_ALLOC_UNIT;
        if (imm.io.opcode == URMA_IO_WRITE_IN_BAND ||
            imm.io.opcode == URMA_IO_PRE_WRITE) {
            return HandleWriteImmCompletion(cr);
        }
        if (imm.io.opcode == URMA_IO_WRITE_IN_BAND_ACK) {
            HandleWriteInBandAck(cr);
            return 0;
        }
        if (imm.io.opcode == URMA_IO_POST_WRITE) {
            HandlePostWrite(cr);
            return 0;
        }
        LOG(WARNING) << "Unknown one-sided opcode: "
                     << static_cast<int>(imm.io.opcode)
                     << " on " << _socket->description();
        errno = EPROTO;
        return -1;
    }
    if (cr.opcode == URMA_CR_OPC_SEND_WITH_IMM && cr.imm_data > 0) {
        if (cr.imm_data > _local_window_capacity) {
            LOG(WARNING) << "Invalid URMA receive credit: " << cr.imm_data;
            errno = EPROTO;
            return -1;
        }
        const uint16_t acks = static_cast<uint16_t>(cr.imm_data);
        uint16_t old =
            _remote_rq_window_size.load(butil::memory_order_relaxed);
        while (true) {
            if (old > _local_window_capacity - acks) {
                LOG(WARNING)
                    << "URMA receive credit exceeds window: old=" << old
                    << " credit=" << acks
                    << " capacity=" << _local_window_capacity
                    << " imm=" << cr.imm_data
                    << " remote_window_capacity="
                    << _remote_window_capacity
                    << " on " << _socket->description();
                errno = EPROTO;
                return -1;
            }
            if (_remote_rq_window_size.compare_exchange_weak(
                    old, static_cast<uint16_t>(old + acks),
                    butil::memory_order_relaxed)) {
                break;
            }
        }
        // Always wake: credits have been returned and the send path
        // may be blocked waiting for remote_rq_window_size.
        _socket->WakeAsEpollOut();
    } else if (cr.completion_len == 0) {
        LOG(WARNING) << "Zero-length URMA receive without immediate credit";
        errno = EPROTO;
        return -1;
    }
    if (cr.completion_len > GetUrmaRecvBlockSize()) {
        LOG(WARNING) << "URMA completion exceeds receive buffer: "
                     << cr.completion_len
                     << " vs block_size=" << GetUrmaRecvBlockSize()
                     << " on " << _socket->description();
        errno = EPROTO;
        return -1;
    }
    if (cr.completion_len < static_cast<uint32_t>(FLAGS_urma_zerocopy_min_size)) {
        zerocopy = false;
    }
    if (zerocopy) {
        _rbuf[_rq_received].cutn(&_socket->_read_buf, cr.completion_len);
    } else {
        _socket->_read_buf.append(_rbuf_data[_rq_received], cr.completion_len);
    }
    // T7: trace RX completion
    if (FLAGS_urma_trace_latency) {
        if (_trace.recv_first_rx_us == 0) {
            _trace.recv_first_rx_us = butil::monotonic_time_us();
        }
        _trace.recv_wr_count++;
        _trace.recv_total_bytes += cr.completion_len;
    }
    // T8: trace PostRecv
    if (PostRecv(1, zerocopy) < 0) {
        return -1;
    }
    if (FLAGS_urma_trace_latency) {
        _trace.recv_postrecv_count++;
    }
    // T9: trace SendAck
    if (cr.completion_len > 0) {
        SendAck(1);
    }
    if (FLAGS_urma_trace_latency && cr.completion_len > 0) {
        _trace.recv_sendack_count++;
    }
    return static_cast<ssize_t>(cr.completion_len);
}

// ============================================================================
// One-sided completion handlers.
// ============================================================================

// Handle a WRITE_IMM receive completion (WRITE_IN_BAND or PRE_WRITE).
// The imm_data encodes the opcode and buffer_offset. The data was written
// into our recv_buf at the specified offset.
ssize_t UrmaEndpoint::HandleWriteImmCompletion(const urma_cr_t& cr) {
    const UrmaWriteImmData imm{cr.imm_data};
    const uint32_t offset = imm.io.buffer_offset * URMA_ONE_SIDED_ALLOC_UNIT;
    char* recv_buf = static_cast<char*>(_one_sided_buf);
    const UrmaMessageHead* head =
        reinterpret_cast<const UrmaMessageHead*>(recv_buf + offset);

    if (head->magic != URMA_CTRL_MAGIC) {
        LOG(ERROR) << "HandleWriteImmCompletion: bad magic=0x" << std::hex
                   << head->magic << std::dec
                   << " offset=" << offset
                   << " on " << _socket->description();
        errno = EPROTO;
        return -1;
    }

    if (imm.io.opcode == URMA_IO_WRITE_IN_BAND) {
        // Small IO: data is inline in recv_buf after the head.
        const uint32_t data_count = head->data_count;
        const char* data = recv_buf + offset + sizeof(UrmaMessageHead);
        _socket->_read_buf.append(data, data_count);

        // Send WRITE_IN_BAND_ACK back to the peer's send_buf.
        ResponseCtrlMessage(URMA_IO_WRITE_IN_BAND_ACK,
                            imm.io.buffer_offset,
                            head->request_id,
                            head->message_size);

        // Repost the empty recv WR that was consumed by this WRITE_IMM.
        if (_io_mode != 0) {
            PostEmptyRecvWr(1);
        }
        return static_cast<ssize_t>(data_count);
    }

    if (imm.io.opcode == URMA_IO_PRE_WRITE) {
        // Large IO: control message with PageBufferInMessage entries.
        // The receiver should issue READ WRs to pull data from the sender.
        const uint32_t block_count = head->data_count;
        const auto* entries = reinterpret_cast<const PageBufferInMessage*>(
            recv_buf + offset + sizeof(UrmaMessageHead));

        // Allocate an RX slot.
        const uint64_t seq = _rx_consume_seq.fetch_add(
            1, butil::memory_order_relaxed);
        const uint32_t idx = static_cast<uint32_t>(
            seq % URMA_RX_RING_SIZE);
        UrmaRxSlot& slot = _rx_slots[idx];
        slot.Reset();
        slot.state.store(UrmaRxSlot::READING, butil::memory_order_relaxed);
        slot.write_imm = imm.data;
        slot.request_id = head->request_id;

        // Build READ WR chain: src=remote block, dst=local pool buffer.
        uint32_t total_bytes = 0;
        const size_t recv_block_size = GetUrmaRecvBlockSize();
        // We need to post READ WRs. Each READ pulls one block from the
        // sender's pool into a local pool buffer.
        urma_jfs_wr_t* wr_head = nullptr;
        urma_jfs_wr_t* wr_tail = nullptr;
        // Use alloca for WR and SGE arrays (freed on return).
        urma_jfs_wr_t* wrs = static_cast<urma_jfs_wr_t*>(
            alloca(sizeof(urma_jfs_wr_t) * block_count));
        urma_sge_t* src_sges = static_cast<urma_sge_t*>(
            alloca(sizeof(urma_sge_t) * block_count));
        urma_sge_t* dst_sges = static_cast<urma_sge_t*>(
            alloca(sizeof(urma_sge_t) * block_count));

        for (uint32_t i = 0; i < block_count; ++i) {
            // Allocate a local pool buffer as READ destination.
            butil::IOBuf iobuf;
            butil::IOBufAsZeroCopyOutputStream zcis(
                &iobuf, entries[i].size + IOBUF_BLOCK_HEADER_LEN);
            void* data = nullptr;
            int size = 0;
            if (!zcis.Next(&data, &size) || !data ||
                size < static_cast<int>(entries[i].size)) {
                LOG(ERROR) << "HandlePreWrite: failed to alloc READ target";
                errno = ENOMEM;
                return -1;
            }
            slot.local_bufs.push_back(data);
            slot.read_targets.push_back({data, entries[i].size});
            total_bytes += entries[i].size;

            // Build READ WR: src=remote block (sender's pool), dst=local buffer.
            src_sges[i].addr = entries[i].addr;
            src_sges[i].len = entries[i].size;
            src_sges[i].tseg = _resource->remote_seg;  // sender's pool seg
            src_sges[i].user_tseg = nullptr;

            dst_sges[i].addr = reinterpret_cast<uint64_t>(data);
            dst_sges[i].len = entries[i].size;
            dst_sges[i].tseg = GetPoolSegFor(data);
            dst_sges[i].user_tseg = nullptr;

            std::memset(&wrs[i], 0, sizeof(urma_jfs_wr_t));
            wrs[i].opcode = URMA_OPC_READ;
            wrs[i].flag.bs.complete_enable =
                (i == block_count - 1) ? 1 : 0;  // signal on last only
            wrs[i].tjetty = _resource->remote_jetty;
            wrs[i].rw.src.sge = &src_sges[i];
            wrs[i].rw.src.num_sge = 1;
            wrs[i].rw.dst.sge = &dst_sges[i];
            wrs[i].rw.dst.num_sge = 1;
            wrs[i].user_ctx = EncodeUserCtx(READ_DATA_REQUEST, seq);
            wrs[i].next = (i + 1 < block_count) ? &wrs[i + 1] : nullptr;
        }
        slot.total_bytes = total_bytes;

        urma_jfs_wr_t* bad = nullptr;
        const urma_status_t status =
            urma_post_jetty_send_wr(_resource->jetty, wrs, &bad);
        if (status != URMA_SUCCESS) {
            LOG(WARNING) << "HandlePreWrite: READ post failed: " << status
                         << " on " << _socket->description();
            slot.state.store(UrmaRxSlot::IDLE, butil::memory_order_relaxed);
            errno = status;
            return -1;
        }

        // Repost the empty recv WR.
        if (_io_mode != 0) {
            PostEmptyRecvWr(1);
        }
        return 0;  // Data will be delivered when READs complete.
    }

    LOG(WARNING) << "HandleWriteImmCompletion: unexpected opcode="
                 << static_cast<int>(imm.io.opcode)
                 << " on " << _socket->description();
    errno = EPROTO;
    return -1;
}

// Handle a READ completion (large IO path). All READ WRs for this seq
// have been posted; the last one has complete_enable=1 so we get one
// completion. Copy data from local pool buffers into _socket->_read_buf.
ssize_t UrmaEndpoint::HandleReadCompletion(const urma_cr_t& cr) {
    const uint64_t seq = DecodeUserCtxSeq(cr.user_ctx);
    const uint32_t idx = static_cast<uint32_t>(seq % URMA_RX_RING_SIZE);
    UrmaRxSlot& slot = _rx_slots[idx];

    if (slot.state.load(butil::memory_order_relaxed) != UrmaRxSlot::READING) {
        LOG(WARNING) << "HandleReadCompletion: slot not READING, seq=" << seq
                     << " on " << _socket->description();
        return 0;
    }

    // Copy data from local pool buffers into _socket->_read_buf.
    ssize_t total_bytes = 0;
    for (const auto& target : slot.read_targets) {
        _socket->_read_buf.append(target.first, target.second);
        total_bytes += static_cast<ssize_t>(target.second);
    }

    slot.state.store(UrmaRxSlot::DATA_READY, butil::memory_order_relaxed);

    // Send POST_WRITE ack to the peer so it can release send_buf and
    // saved IOBuf block references.
    const UrmaWriteImmData imm{slot.write_imm};
    ResponseCtrlMessage(URMA_IO_POST_WRITE,
                        imm.io.buffer_offset,
                        slot.request_id,
                        0);

    // Clean up the slot.
    slot.Reset();

    return total_bytes;
}

// Handle WRITE_IN_BAND_ACK: the peer has consumed our inline data.
// Release the send_buf slot and the pending send context.
void UrmaEndpoint::HandleWriteInBandAck(const urma_cr_t& cr) {
    const UrmaWriteImmData imm{cr.imm_data};
    const uint32_t offset = imm.io.buffer_offset * URMA_ONE_SIDED_ALLOC_UNIT;

    // Find the send context by request_id from the UrmaMessageHead in
    // our send_buf. But we don't know the request_id from the ACK alone.
    // Instead, we search pending_sends by offset.
    std::lock_guard<std::mutex> lock(_pending_sends_mutex);
    for (auto it = _pending_sends.begin(); it != _pending_sends.end(); ++it) {
        if (it->second->send_buf_offset == offset &&
            it->second->opcode == URMA_IO_WRITE_IN_BAND) {
            _send_buf_alloc->Release(offset, it->second->send_buf_size);
            delete it->second;
            _pending_sends.erase(it);
            _socket->WakeAsEpollOut();
            return;
        }
    }
    LOG(WARNING) << "HandleWriteInBandAck: no pending send for offset="
                 << offset << " on " << _socket->description();
}

// Handle POST_WRITE: the peer has finished READing our data.
// Release the send_buf slot and the saved IOBuf block references.
void UrmaEndpoint::HandlePostWrite(const urma_cr_t& cr) {
    const UrmaWriteImmData imm{cr.imm_data};
    const uint32_t offset = imm.io.buffer_offset * URMA_ONE_SIDED_ALLOC_UNIT;

    // Read the UrmaMessageHead from our send_buf to get the request_id
    // and message_size.
    char* send_buf = static_cast<char*>(_one_sided_buf) + _recv_buf_capacity;
    const UrmaMessageHead* head =
        reinterpret_cast<const UrmaMessageHead*>(send_buf + offset);
    if (head->magic != URMA_CTRL_MAGIC) {
        LOG(ERROR) << "HandlePostWrite: bad magic on " << _socket->description();
        return;
    }

    UrmaSendContext* ctx = FindAndRemoveSendContext(head->request_id);
    if (ctx) {
        _send_buf_alloc->Release(ctx->send_buf_offset, ctx->send_buf_size);
        delete ctx;  // releases saved_blocks
    }
    _socket->WakeAsEpollOut();
}

void UrmaEndpoint::DispatchReceivedBytes(SocketUniquePtr& s, ssize_t bytes) {
    int64_t pending = _pending_received_bytes.load(butil::memory_order_relaxed);
    if (bytes > 0) {
        pending = _pending_received_bytes.fetch_add(
                      bytes, butil::memory_order_acq_rel) + bytes;
    }

    const State state = _state.load(butil::memory_order_acquire);
    if (state != ESTABLISHED) {
        return;
    }

    // PollCq and the handshake bthread can both reach this method when the
    // state changes to ESTABLISHED. Serialize them so each byte added to
    // _socket->_read_buf is reported to InputMessenger exactly once.
    std::unique_lock<butil::Mutex> dispatch_lock(_dispatch_mutex);
    if (_state.load(butil::memory_order_acquire) != ESTABLISHED) {
        return;
    }
    pending = _pending_received_bytes.exchange(
        0, butil::memory_order_acq_rel);
    if (pending <= 0 || s->Failed()) {
        return;
    }

    auto* messenger = static_cast<InputMessenger*>(s->user());
    if (!messenger) {
        LOG(ERROR) << "URMA socket has no InputMessenger: "
                   << s->description();
        return;
    }

    const int64_t received_us = butil::cpuwide_time_us();
    const int64_t base_realtime = butil::gettimeofday_us() - received_us;
    // T10: trace recv complete summary
    if (FLAGS_urma_trace_latency && _trace.recv_total_bytes > 0) {
        const int64_t now_us = butil::monotonic_time_us();
        const int64_t recv_total_us = now_us - _trace.recv_first_rx_us;
        LOG(INFO) << "[URMA-TRACE] recv: rx_wrs=" << _trace.recv_wr_count
                  << " bytes=" << _trace.recv_total_bytes
                  << " tx_completions=" << _trace.recv_tx_complete
                  << " polls=" << _trace.recv_poll_count
                  << " postrecv=" << _trace.recv_postrecv_count
                  << " sendack=" << _trace.recv_sendack_count
                  << " recv_time=" << recv_total_us << "us"
                  << " pending=" << pending
                  << " on " << _socket->description();
        // Reset recv trace for next batch
        _trace.recv_first_rx_us = 0;
        _trace.recv_wr_count = 0;
        _trace.recv_total_bytes = 0;
        _trace.recv_postrecv_count = 0;
        _trace.recv_sendack_count = 0;
        _trace.recv_poll_count = 0;
        _trace.recv_tx_complete = 0;
    }
    InputMessageClosure last_msg;
    messenger->ProcessNewMessage(s.get(), static_cast<ssize_t>(pending),
                                 false, received_us, base_realtime, last_msg);
}

void UrmaEndpoint::PollCq(Socket* m) {
    auto* ep = static_cast<UrmaEndpoint*>(m->user());
    if (!ep || !ep->_resource || !ep->_resource->jfc) {
        return;
    }
    SocketUniquePtr s;
    if (Socket::Address(ep->_socket->id(), &s) != 0) {
        return;
    }
    if (s->Failed()) {
        return;
    }

    const bool event_mode = !FLAGS_urma_use_polling;
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        urma_jfc_t* event_jfc = nullptr;
        if (event_mode) {
            const int event_count = ep->WaitCqEvent(s, &event_jfc);
            if (event_count < 0) {
                return;
            }
            if (event_count == 0) {
                if (!m->MoreReadEvents(&progress)) {
                    return;
                }
                continue;
            }
        }

        ssize_t bytes = 0;
        int total_cqes = 0;
        int tx_ok = 0;
        int tx_err = 0;
        int rx_ok = 0;
        auto drain_cq = [&]() -> int {
            while (true) {
                const int n =
                    std::max(1, std::min<int>(FLAGS_urma_cqe_poll_once, 32));
                urma_cr_t crs[32];
                // T5: trace poll count
                if (FLAGS_urma_trace_latency) {
                    ep->_trace.recv_poll_count++;
                }
                const int cnt =
                    urma_poll_jfc(ep->_resource->jfc, n, crs);
                if (cnt < 0) {
                    return EIO;
                }
                if (cnt == 0) {
                    return 0;
                }
                total_cqes += cnt;
                for (int i = 0; i < cnt; ++i) {
                    if (s->Failed()) {
                        return ECANCELED;
                    }
                    const ssize_t nr = ep->HandleCompletion(crs[i]);
                    if (nr < 0) {
                        return errno ? errno : EIO;
                    }
                    if (crs[i].status == URMA_CR_SUCCESS) {
                        if (crs[i].flag.bs.s_r == 0) {
                            ++tx_ok;
                        } else {
                            ++rx_ok;
                        }
                    } else {
                        ++tx_err;
                    }
                    bytes += nr;
                }
            }
        };

        int completion_error = drain_cq();
        if (tx_err > 0) {
            LOG(WARNING) << "URMA CQ drain: total=" << total_cqes
                      << " tx_ok=" << tx_ok << " tx_err=" << tx_err
                      << " rx_ok=" << rx_ok << " bytes=" << bytes
                      << " sq_wnd=" << ep->_sq_window_size.load(butil::memory_order_relaxed)
                      << " remote_rq_wnd=" << ep->_remote_rq_window_size.load(butil::memory_order_relaxed)
                      << " on " << ep->_socket->description();
        }
        if (event_mode) {
            // The bonding provider records which physical JFCs produced CRs
            // while bondp_poll_jfc drains the virtual JFC.
            // bondp_rearm_jfc consumes that mask, so rearming before the drain
            // leaves those physical JFCs unarmed.
            uint32_t nevents = 1;
            urma_ack_jfc(&event_jfc, &nevents, 1);
            if (completion_error == 0) {
                if (ep->ReqNotifyCq() != 0) {
                    return;
                }

                // Close the drain/rearm race. A completion that arrived while
                // the JFC was unarmed may not produce an edge on every
                // provider. The JFC is armed now, so a final nonblocking drain
                // is safe.
                completion_error = drain_cq();
            }
        }

        if (completion_error != 0) {
            if (!s->Failed()) {
                s->SetFailed(completion_error, "URMA completion error");
            }
            return;
        }
        ep->DispatchReceivedBytes(s, bytes);

        if (!event_mode) {
            return;
        }
        // The bonding JFCE fd is itself an epoll fd aggregating physical
        // JFCEs, while brpc watches it with EPOLLET. urma_wait_jfc(..., 1, ...)
        // consumes only one aggregated event. Keep draining the inner JFCE
        // until it reports no event; otherwise another physical event can
        // leave the fd continuously readable and never create a new outer
        // edge. The event_count == 0 branch above resets _nevent only after
        // the inner queue is empty.
    }
}

// ============================================================================
// ApplyRemoteHello: size the send/recv windows from the peer's hello.
// ============================================================================

void UrmaEndpoint::ApplyRemoteHello(const ParsedHello& remote) {
    _remote_recv_block_size = remote.buffer_size;
    const uint32_t peer_rq_size = remote.recv_buffer_cnt + 1;
    uint32_t local_capacity =
        std::min<uint32_t>(_sq_size, peer_rq_size);
    // Cap the send window on bonding devices to avoid status=8 errors.
    // The bonding provider fails when too many sends are in-flight
    // concurrently with larger messages.
    const uint16_t bonding_max_wnd = GetUrmaBondingMaxSendWindow();
    if (bonding_max_wnd > 0 && local_capacity > bonding_max_wnd) {
        LOG(INFO) << "Bonding device: capping send window from "
                  << local_capacity << " to " << bonding_max_wnd
                  << " (--urma_bonding_max_send_window) on "
                  << _socket->description();
        local_capacity = bonding_max_wnd;
    }
    _local_window_capacity = static_cast<uint16_t>(
        local_capacity > RESERVED_WR_NUM
            ? local_capacity - RESERVED_WR_NUM
            : 0);
    _remote_window_capacity =
        _rq_size > RESERVED_WR_NUM ? _rq_size - RESERVED_WR_NUM : 0;
    _sq_imm_window_size = RESERVED_WR_NUM;
    _remote_rq_window_size.store(_local_window_capacity,
                                 butil::memory_order_relaxed);
    _sq_window_size.store(_local_window_capacity, butil::memory_order_relaxed);

    // Negotiate one-sided IO mode: min(local, remote). v2 peers or peers
    // without one-sided support will have io_mode=0, so we fall back to
    // SEND_ONLY.
    const uint8_t negotiated_mode = static_cast<uint8_t>(
        std::min(static_cast<uint32_t>(_io_mode), remote.io_mode));
    if (negotiated_mode != 0 && remote.has_one_sided &&
        remote.recv_buf_size > 0 && remote.send_buf_size > 0) {
        _io_mode = negotiated_mode;
        _remote_recv_buf_va = remote.recv_buf_va;
        _remote_recv_buf_size = remote.recv_buf_size;
        _remote_send_buf_va = remote.send_buf_va;
        _remote_send_buf_size = remote.send_buf_size;
        LOG(INFO) << "Negotiated one-sided io_mode=" << static_cast<int>(_io_mode)
                  << " remote_recv_buf_va=0x" << std::hex
                  << _remote_recv_buf_va << std::dec
                  << " size=" << _remote_recv_buf_size
                  << " on " << _socket->description();
        // Import the peer's recv_buf segment so we can WRITE_IMM into it.
        urma_context_t* ctx = GetUrmaContext();
        if (ctx) {
            urma_seg_t peer_recv_seg{};
            std::memcpy(peer_recv_seg.ubva.eid.raw,
                        remote.recv_buf_seg_eid, 16);
            peer_recv_seg.ubva.uasid = remote.recv_buf_seg_uasid;
            peer_recv_seg.ubva.va = remote.recv_buf_va;
            peer_recv_seg.len = remote.recv_buf_size;
            peer_recv_seg.token_id = remote.recv_buf_token_id;
            urma_token_t seg_token{};
            urma_import_seg_flag_t seg_flag{};
            seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
            seg_flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
            seg_flag.bs.mapping = URMA_SEG_NOMAP;
            _resource->remote_recv_buf_seg =
                urma_import_seg(ctx, &peer_recv_seg, &seg_token, 0, seg_flag);
            if (!_resource->remote_recv_buf_seg) {
                PLOG(WARNING) << "Failed to import peer recv_buf seg; "
                              << "falling back to SEND_ONLY";
                _io_mode = 0;
            }
        }
    } else {
        _io_mode = 0;
    }
}
// ============================================================================

static void TryReadOnTcpDuringUrmaEst(Socket* socket);

void UrmaEndpoint::OnNewDataFromTcp(Socket* m) {
    auto* tp = static_cast<UrmaTransport*>(m->_transport.get());
    if (!tp) {
        return;
    }
    // Access _urma_ep directly (OnNewDataFromTcp is a friend of UrmaTransport);
    // GetUrmaEp() CHECKs non-null which would crash on TCP-fallback sockets.
    UrmaEndpoint* ep = tp->_urma_ep;
    if (!ep) {
        // No URMA endpoint: pure TCP path.
        InputMessenger::OnNewMessages(m);
        return;
    }
    int progress = 0;
    while (true) {
        const State state =
            ep->_state.load(butil::memory_order_acquire);
        if (state == UNINIT) {
            if (!m->CreatedByConnect()) {
                // Server side: kick off the handshake bthread.
                if (!IsUrmaAvailable()) {
                    ep->_state = FALLBACK_TCP;
                    tp->_urma_state = UrmaTransport::URMA_OFF;
                    InputMessenger::OnNewMessages(m);
                    return;
                }
                SocketUniquePtr s;
                m->ReAddress(&s);
                ep->_state = S_HELLO_WAIT;
                bthread_t tid;
                bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
                bthread_attr_set_name(&attr, "UrmaServerHandshake");
                if (bthread_start_background(&tid, &attr,
                        ProcessHandshakeAtServer, ep) != 0) {
                    ep->_state = UNINIT;
                    LOG(FATAL) << "Fail to start UrmaServerHandshake bthread";
                } else {
                    s.release();
                }
                return;
            }
            // Client side: handled by ProcessHandshakeAtClient.
            return;
        } else if (state < ESTABLISHED) {
            // During handshake: wake the handshake bthread parked in ReadFromFd.
            ep->_read_butex->fetch_add(1, butil::memory_order_release);
            bthread::butex_wake(ep->_read_butex);
            return;
        } else if (state == FALLBACK_TCP) {
            InputMessenger::OnNewMessages(m);
            return;
        } else if (state == ESTABLISHED) {
            TryReadOnTcpDuringUrmaEst(m);
            return;
        }
        if (!m->MoreReadEvents(&progress)) {
            break;
        }
    }
}

inline void UrmaEndpoint::TryReadOnTcp() {
    if (_state.load(butil::memory_order_acquire) == FALLBACK_TCP) {
        InputMessenger::OnNewMessages(_socket);
    }
}

static void TryReadOnTcpDuringUrmaEst(Socket* socket) {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        uint8_t byte = 0;
        const ssize_t nr = read(socket->fd(), &byte, 1);
        if (nr < 0) {
            if (errno != EAGAIN) {
                const int saved_errno = errno;
                socket->SetFailed(saved_errno, "Fail to read URMA TCP fd: %s",
                                  berror(saved_errno));
                return;
            }
            if (!socket->MoreReadEvents(&progress)) {
                return;
            }
        } else if (nr == 0) {
            socket->SetEOF();
            return;
        } else {
            socket->SetFailed(
                EPROTO, "Unexpected TCP data after URMA was established");
            return;
        }
    }
}

void UrmaEndpoint::FallbackToTcp(UrmaTransport* transport, bool process_tcp) {
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FALLBACK_TCP, butil::memory_order_release);
    DeallocateResources();
    if (process_tcp) {
        TryReadOnTcp();
    }
}

void UrmaEndpoint::FailHandshake(UrmaTransport* transport, int error,
                                 const char* reason) {
    LOG(ERROR) << "URMA handshake failed in state=" << GetStateStr()
               << " on " << _socket->description()
               << ": " << reason << ", error=" << error
               << " (" << berror(error) << ')';
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FAILED, butil::memory_order_release);
    DeallocateResources();
    auto* connect =
        static_cast<UrmaConnect*>(_socket->_app_connect.get());
    if (connect) {
        connect->_error = error;
    }
    _socket->SetFailed(error, "URMA handshake failed: %s", reason);
}

// ============================================================================
// Handshake state machines (client / server). Run in a background bthread.
// ============================================================================

void* UrmaEndpoint::ProcessHandshakeAtClient(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    if (!IsUrmaAvailable()) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_ALLOC_RES;
    if (ep->AllocateResources() < 0) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    // Prepost the shared JFR before sending the client hello so the peer sees
    // a ready receive queue as soon as its import completes.
    if (ep->PostRecv(ep->_rq_size, FLAGS_urma_recv_zerocopy) < 0) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_HELLO_SEND;
    std::unique_ptr<UrmaHandshake> hs(CreateClientHandshake(ep));
    ep->_handshake_version = hs->ProtocolVersion();
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client hello");
        return nullptr;
    }
    ep->_state = C_HELLO_WAIT;
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read server hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->ApplyRemoteHello(remote);
    ep->_state = C_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import server resources");
        return nullptr;
    }
    ep->_state = C_ACK_SEND;
    uint32_t flags = HELLO_ACK_URMA_OK;
    uint32_t flags_be = butil::HostToNet32(flags);
    if (ep->WriteToFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client ack");
        return nullptr;
    }
    tp->_urma_state = UrmaTransport::URMA_ON;
    ep->_state = ESTABLISHED;
    // Post empty recv WRs for one-sided WRITE_IMM operations.
    if (ep->_io_mode != 0) {
        ep->PostEmptyRecvWr(ep->_rq_size);
    }
    ep->DispatchReceivedBytes(s, 0);
    return nullptr;
}

void* UrmaEndpoint::ProcessHandshakeAtServer(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    ep->_state = S_HELLO_WAIT;
    uint8_t magic[v2_wire::MAGIC_STR_LEN];
    if (ep->ReadFromFd(magic, v2_wire::MAGIC_STR_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client magic");
        return nullptr;
    }
    std::unique_ptr<UrmaHandshake> hs(CreateServerHandshakeByMagic(ep, magic));
    if (!hs) {
        // Not an URMA peer: push the magic back and fall back to TCP.
        ep->PushBackToReadBuf(magic, v2_wire::MAGIC_STR_LEN);
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_handshake_version = hs->ProtocolVersion();
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FailHandshake(tp, EPROTO, "invalid client hello");
        return nullptr;
    }
    ep->_state = S_ALLOC_RES;
    if (ep->AllocateResources() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "allocate server resources");
        return nullptr;
    }
    const bool bonding = IsUrmaBondingDevice();
    if (!bonding &&
        ep->PostRecv(ep->_rq_size, FLAGS_urma_recv_zerocopy) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "post server receives");
        return nullptr;
    }
    ep->ApplyRemoteHello(remote);
    ep->_state = S_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import client resources");
        return nullptr;
    }
    if (bonding &&
        ep->PostRecv(ep->_rq_size, FLAGS_urma_recv_zerocopy) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "post server receives");
        return nullptr;
    }
    ep->_state = S_HELLO_SEND;
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send server hello");
        return nullptr;
    }
    ep->_state = S_ACK_WAIT;
    uint32_t flags_be = 0;
    if (ep->ReadFromFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client ack");
        return nullptr;
    }
    uint32_t flags = butil::NetToHost32(flags_be);
    bool client_ack_ok = (flags & HELLO_ACK_URMA_OK) != 0;
    if (client_ack_ok) {
        if (tp->_urma_state.load(butil::memory_order_acquire) ==
            UrmaTransport::URMA_OFF) {
            // Protocol breakdown: client wants URMA but we already fell back.
            ep->FailHandshake(tp, EPROTO, "client ack mismatch");
            return nullptr;
        }
        tp->_urma_state = UrmaTransport::URMA_ON;
        ep->_state = ESTABLISHED;
        // Post empty recv WRs for one-sided WRITE_IMM operations.
        if (ep->_io_mode != 0) {
            ep->PostEmptyRecvWr(ep->_rq_size);
        }
        ep->DispatchReceivedBytes(s, 0);
    } else {
        ep->FallbackToTcp(tp, true);
    }
    return nullptr;
}

// ============================================================================
// UrmaConnect: drives the client handshake bthread.
// ============================================================================

void UrmaConnect::StartConnect(const Socket* socket,
                               void (*done)(int, void*), void* data) {
    SocketUniquePtr s;
    if (Socket::Address(socket->id(), &s) != 0) {
        return;
    }
    _done = done;
    _data = data;
    _error = 0;
    auto* tp = static_cast<UrmaTransport*>(socket->_transport.get());
    if (!tp) {
        Run();
        return;
    }
    if (!tp->_urma_ep || !IsUrmaAvailable()) {
        // Fall back to TCP immediately.
        if (tp->_urma_ep) {
            tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        }
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
        return;
    }
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UrmaClientHandshake");
    if (bthread_start_background(&tid, &attr,
            UrmaEndpoint::ProcessHandshakeAtClient,
            tp->_urma_ep) != 0) {
        tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
    } else {
        // ProcessHandshakeAtClient adopts this reference in its
        // SocketUniquePtr constructor.
        s.release();
    }
}

void UrmaConnect::StopConnect(Socket*) {}

void UrmaConnect::Run() {
    if (_done) {
        auto cb = _done;
        _done = nullptr;
        cb(_error, _data);
    }
}

// ============================================================================
// Debug / polling-mode stubs.
// ============================================================================

std::string UrmaEndpoint::GetStateStr() const {
    switch (_state.load(butil::memory_order_acquire)) {
    case UNINIT:        return "UNINIT";
    case C_ALLOC_RES:   return "C_ALLOC_RES";
    case C_HELLO_SEND:  return "C_HELLO_SEND";
    case C_HELLO_WAIT:  return "C_HELLO_WAIT";
    case C_IMPORT_PEER: return "C_IMPORT_PEER";
    case C_ACK_SEND:    return "C_ACK_SEND";
    case S_HELLO_WAIT:  return "S_HELLO_WAIT";
    case S_ALLOC_RES:   return "S_ALLOC_RES";
    case S_IMPORT_PEER: return "S_IMPORT_PEER";
    case S_HELLO_SEND:  return "S_HELLO_SEND";
    case S_ACK_WAIT:    return "S_ACK_WAIT";
    case ESTABLISHED:   return "ESTABLISHED";
    case FALLBACK_TCP:  return "FALLBACK_TCP";
    case FAILED:        return "FAILED";
    }
    return "UNKNOWN";
}

void UrmaEndpoint::DebugInfo(std::ostream& os, butil::StringPiece) const {
    os << "state=" << GetStateStr()
       << " sq_size=" << _sq_size << " rq_size=" << _rq_size
       << " remote_recv_block_size=" << _remote_recv_block_size
       << " sq_window=" << _sq_window_size.load(butil::memory_order_relaxed)
       << " remote_rq_window=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
       << " handshake_version=" << _handshake_version;
}

int UrmaEndpoint::WaitCqEvent(SocketUniquePtr& s,
                              urma_jfc_t** event_jfc) {
    if (!_resource || !_resource->jfce || !_resource->jfc) {
        errno = ENODEV;
        return -1;
    }
    *event_jfc = nullptr;
    int count = urma_wait_jfc(_resource->jfce, 1, 0, event_jfc);
    if (count < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return 0;
        }
        const int saved_errno = errno;
        PLOG(ERROR) << "Fail to wait URMA JFC event from "
                    << s->description();
        s->SetFailed(saved_errno, "Fail to wait URMA JFC event: %s",
                     berror(saved_errno));
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    if (*event_jfc != _resource->jfc) {
        LOG(ERROR) << "Unexpected URMA JFC event on " << s->description();
        errno = EPROTO;
        s->SetFailed(EPROTO, "Unexpected URMA JFC event");
        return -1;
    }
    return 1;
}

int UrmaEndpoint::ReqNotifyCq() {
    if (!_resource || !_resource->jfc) {
        errno = ENODEV;
        return -1;
    }
    const int rc = urma_rearm_jfc(_resource->jfc, false);
    if (rc != URMA_SUCCESS) {
        errno = rc;
        PLOG(WARNING) << "Fail to rearm URMA JFC";
        _socket->SetFailed(rc, "Fail to rearm URMA JFC: %s", berror(rc));
        return -1;
    }
    return 0;
}

int UrmaEndpoint::PollingModeInitialize(
        bthread_tag_t tag, std::function<void()> callback,
        std::function<void()> init_fn, std::function<void()> release_fn) {
    if (!FLAGS_urma_use_polling) {
        return 0;
    }
    if (tag >= _poller_groups.size() ||
        _poller_groups[tag].pollers.empty()) {
        errno = EINVAL;
        return -1;
    }
    auto& group = _poller_groups[tag];
    bool expected = false;
    if (!group.running.compare_exchange_strong(expected, true)) {
        return 0;
    }
    struct FnArgs {
        Poller* poller;
        butil::atomic<bool>* running;
    };
    auto fn = [](void* p) -> void* {
        std::unique_ptr<FnArgs> args(static_cast<FnArgs*>(p));
        Poller* poller = args->poller;
        butil::atomic<bool>* running = args->running;
        std::unordered_set<SocketId> cq_sids;
        CqSidOp op;

        if (poller->init_fn) {
            poller->init_fn();
        }
        while (running->load(butil::memory_order_relaxed)) {
            while (poller->op_queue.Dequeue(op)) {
                if (op.type == CqSidOp::ADD) {
                    cq_sids.emplace(op.sid);
                } else {
                    cq_sids.erase(op.sid);
                }
            }
            for (SocketId sid : cq_sids) {
                SocketUniquePtr s;
                if (Socket::Address(sid, &s) == 0) {
                    PollCq(s.get());
                }
            }
            if (poller->callback) {
                poller->callback();
            }
            if (FLAGS_urma_poller_yield || cq_sids.empty()) {
                bthread_yield();
            }
        }
        if (poller->release_fn) {
            poller->release_fn();
        }
        return nullptr;
    };

    auto& pollers = group.pollers;
    for (size_t i = 0; i < pollers.size(); ++i) {
        pollers[i].callback = callback;
        pollers[i].init_fn = init_fn;
        pollers[i].release_fn = release_fn;
        std::unique_ptr<FnArgs> args(new (std::nothrow)
                                        FnArgs{&pollers[i], &group.running});
        if (!args) {
            group.running.store(false, butil::memory_order_relaxed);
            for (size_t j = 0; j < i; ++j) {
                bthread_join(pollers[j].tid, nullptr);
                pollers[j].tid = INVALID_BTHREAD;
            }
            errno = ENOMEM;
            return -1;
        }
        bthread_attr_t attr = FLAGS_urma_disable_bthread
                                  ? BTHREAD_ATTR_PTHREAD
                                  : BTHREAD_ATTR_NORMAL;
        attr.tag = tag;
        bthread_attr_set_name(&attr, "UrmaPolling");
        const int rc = bthread_start_background(
            &pollers[i].tid, &attr, fn, args.get());
        if (rc != 0) {
            group.running.store(false, butil::memory_order_relaxed);
            for (size_t j = 0; j < i; ++j) {
                bthread_join(pollers[j].tid, nullptr);
                pollers[j].tid = INVALID_BTHREAD;
            }
            errno = rc;
            return -1;
        }
        args.release();
    }
    return 0;
}

void UrmaEndpoint::PollingModeRelease(bthread_tag_t tag) {
    if (!FLAGS_urma_use_polling || tag >= _poller_groups.size()) {
        return;
    }
    auto& group = _poller_groups[tag];
    group.running.store(false, butil::memory_order_relaxed);
    for (auto& poller : group.pollers) {
        if (poller.tid != INVALID_BTHREAD) {
            bthread_join(poller.tid, nullptr);
            poller.tid = INVALID_BTHREAD;
        }
    }
}

void UrmaEndpoint::PollerAddCqSid() {
    if (_cq_sid == INVALID_SOCKET_ID || _poller_groups.empty()) {
        return;
    }
    _poller_tag = bthread_self_tag();
    if (_poller_tag >= _poller_groups.size()) {
        return;
    }
    auto& pollers = _poller_groups[_poller_tag].pollers;
    if (pollers.empty()) {
        return;
    }
    const size_t index =
        butil::fmix32(_cq_sid) % pollers.size();
    pollers[index].op_queue.Enqueue(
        CqSidOp{CqSidOp::ADD, _cq_sid});
}

void UrmaEndpoint::PollerRemoveCqSid() {
    if (_cq_sid == INVALID_SOCKET_ID || _poller_groups.empty() ||
        _poller_tag >= _poller_groups.size()) {
        return;
    }
    auto& pollers = _poller_groups[_poller_tag].pollers;
    if (pollers.empty()) {
        return;
    }
    const size_t index =
        butil::fmix32(_cq_sid) % pollers.size();
    pollers[index].op_queue.Enqueue(
        CqSidOp{CqSidOp::REMOVE, _cq_sid});
}

int UrmaEndpoint::GlobalInitialize() {
    // Pre-allocate the prepared jetty pool. Skipped if URMA init is skipped
    // (unit-test mode).
    if (FLAGS_urma_use_polling && _poller_groups.empty()) {
        if (FLAGS_urma_poller_num <= 0) {
            LOG(ERROR) << "urma_poller_num must be positive";
            errno = EINVAL;
            return -1;
        }
        size_t ntags = static_cast<size_t>(FLAGS_task_group_ntags);
        if (ntags == 0) {
            ntags = 1;
        }
        _poller_groups = std::vector<PollerGroup>(ntags);
    }
    if (g_prepared_cnt > 0) {
        return 0;
    }
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        return 0;
    }
    const int prepared_jetty_count = PreparedJettyCount();
    for (int i = 0; i < prepared_jetty_count; ++i) {
        auto* r = new (std::nothrow) UrmaResource();
        if (!r) {
            break;
        }
        r->jfce = urma_create_jfce(ctx);
        if (!r->jfce ||
            (!FLAGS_urma_use_polling && r->jfce->fd < 0)) {
            delete r;
            break;
        }
        urma_jfc_cfg_t jfc_cfg{};
        const uint16_t effective_rq = GetUrmaEffectiveRqSize();
        jfc_cfg.depth = static_cast<uint32_t>(FLAGS_urma_sq_size + effective_rq);
        jfc_cfg.jfce = r->jfce;
        r->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (!r->jfc) {
            delete r;
            break;
        }
        urma_jfr_cfg_t jfr_cfg{};
        jfr_cfg.depth = static_cast<uint32_t>(effective_rq);
        jfr_cfg.trans_mode = URMA_TM_RM;
        jfr_cfg.max_sge =
            static_cast<uint8_t>(GetUrmaMaxJfrSge());
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.jfc = r->jfc;
        r->jfr = urma_create_jfr(ctx, &jfr_cfg);
        if (!r->jfr) {
            delete r;
            break;
        }
        urma_jetty_cfg_t jetty_cfg{};
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(FLAGS_urma_sq_size);
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        jetty_cfg.jfs_cfg.priority = GetUrmaJettyPriority();
        jetty_cfg.jfs_cfg.max_sge =
            static_cast<uint8_t>(GetUrmaMaxSge());
        jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jetty_cfg.jfs_cfg.jfc = r->jfc;
        jetty_cfg.shared.jfr = r->jfr;
        jetty_cfg.shared.jfc = r->jfc;
        r->jetty = urma_create_jetty(ctx, &jetty_cfg);
        if (!r->jetty) {
            delete r;
            break;
        }
        r->next = g_prepared_list;
        g_prepared_list = r;
        ++g_prepared_cnt;
    }
    return 0;
}

void UrmaEndpoint::GlobalRelease() {
    {
        BAIDU_SCOPED_LOCK(g_prepared_mutex);
        while (g_prepared_list) {
            UrmaResource* next = g_prepared_list->next;
            delete g_prepared_list;
            g_prepared_list = next;
        }
        g_prepared_cnt = 0;
    }
    for (size_t tag = 0; tag < _poller_groups.size(); ++tag) {
        PollingModeRelease(static_cast<bthread_tag_t>(tag));
    }
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
