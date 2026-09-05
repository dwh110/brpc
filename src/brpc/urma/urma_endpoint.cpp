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

#include <gflags/gflags.h>
#include "butil/fd_utility.h"
#include "butil/logging.h"
#include "butil/sys_byteorder.h"
#include "bthread/bthread.h"
#include "brpc/errno.pb.h"
#include "brpc/event_dispatcher.h"
#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "brpc/reloadable_flags.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma_transport.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake_constants.h"

DECLARE_int32(task_group_ntags);

DECLARE_bool(urma_use_polling);
DECLARE_int32(urma_poller_num);
DECLARE_bool(urma_disable_bthread);
DECLARE_int32(urma_sq_size);
DECLARE_int32(urma_rq_size);
DECLARE_int32(urma_cqe_poll_once);
DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);
DECLARE_bool(urma_poller_yield);
DECLARE_int32(urma_prepared_jetty_cnt);

DEFINE_bool(urma_trace_verbose, false, "Print log message verbosely");
BRPC_VALIDATE_GFLAG(urma_trace_verbose, brpc::PassValidate);
DEFINE_int32(urma_prepared_qp_size, 128, "SQ and RQ size for prepared Jetty.");

static const size_t IOBUF_BLOCK_HEADER_LEN = 32;

extern const size_t RESERVED_WR_NUM = 3;

uint32_t g_urma_recv_block_size = 0;

static const uint16_t MIN_JETTY_SIZE = 16;
static const uint16_t MAX_JETTY_SIZE = 4096;
extern const uint16_t MIN_BLOCK_SIZE = 1024;

static butil::Mutex* g_urma_resource_mutex = NULL;
static UrmaResource* g_urma_resource_list = NULL;

extern bool g_skip_urma_init;

UrmaResource::~UrmaResource() {
    if (NULL != remote_jetty) {
        urma_unimport_jetty(remote_jetty);
    }
    if (NULL != remote_seg) {
        urma_unimport_seg(remote_seg);
    }
    if (NULL != jetty) {
        urma_delete_jetty(jetty);
    }
    if (NULL != jfr) {
        urma_delete_jfr(jfr);
    }
    if (NULL != jfc) {
        urma_delete_jfc(jfc);
    }
    if (NULL != jfce) {
        urma_delete_jfce(jfce);
    }
}

UrmaEndpoint::UrmaEndpoint(Socket* s)
    : _socket(s)
    , _state(UNINIT)
    , _handshake_version(0)
    , _resource(NULL)
    , _jfc_events(0)
    , _cq_sid(INVALID_SOCKET_ID)
    , _sq_size(FLAGS_urma_sq_size)
    , _rq_size(FLAGS_urma_rq_size)
    , _remote_recv_block_size(0)
    , _accumulated_ack(0)
    , _unsolicited(0)
    , _unsolicited_bytes(0)
    , _sq_current(0)
    , _sq_unsignaled(0)
    , _sq_sent(0)
    , _rq_received(0)
    , _local_window_capacity(0)
    , _remote_window_capacity(0)
    , _sq_imm_window_size(0)
    , _remote_rq_window_size(0)
    , _sq_window_size(0)
    , _new_rq_wrs(0)
{
    if (_sq_size < MIN_JETTY_SIZE) {
        _sq_size = MIN_JETTY_SIZE;
    }
    if (_sq_size > MAX_JETTY_SIZE) {
        _sq_size = MAX_JETTY_SIZE;
    }
    if (_rq_size < MIN_JETTY_SIZE) {
        _rq_size = MIN_JETTY_SIZE;
    }
    if (_rq_size > MAX_JETTY_SIZE) {
        _rq_size = MAX_JETTY_SIZE;
    }
    _read_butex = bthread::butex_create_checked<butil::atomic<int> >();
}

UrmaEndpoint::~UrmaEndpoint() {
    Reset();
    bthread::butex_destroy(_read_butex);
}

void UrmaEndpoint::Reset() {
    DeallocateResources();

    _state.store(UNINIT, butil::memory_order_relaxed);
    _handshake_version = 0;
    _resource = NULL;
    _jfc_events = 0;
    _cq_sid = INVALID_SOCKET_ID;
    _sbuf.clear();
    _rbuf.clear();
    _rbuf_data.clear();
    _remote_recv_block_size = 0;
    _accumulated_ack = 0;
    _unsolicited = 0;
    _unsolicited_bytes = 0;
    _sq_current = 0;
    _sq_unsignaled = 0;
    _sq_sent = 0;
    _rq_received = 0;
    _local_window_capacity = 0;
    _remote_window_capacity = 0;
    _sq_imm_window_size = 0;
    _remote_rq_window_size.store(0, butil::memory_order_relaxed);
    _sq_window_size.store(0, butil::memory_order_relaxed);
    _new_rq_wrs.store(0, butil::memory_order_relaxed);
}

void UrmaConnect::StartConnect(const Socket* socket,
                               void (*done)(int err, void* data),
                               void* data) {
    auto* urma_transport = static_cast<UrmaTransport*>(socket->_transport.get());
    CHECK(urma_transport->_urma_ep != NULL);
    SocketUniquePtr s;
    if (Socket::Address(socket->id(), &s) != 0) {
        return;
    }
    if (!IsUrmaAvailable()) {
        urma_transport->_urma_state = UrmaTransport::URMA_OFF;
        urma_transport->_urma_ep->_state.store(
            UrmaEndpoint::FALLBACK_TCP, butil::memory_order_release);
        done(0, data);
        return;
    }
    _done = done;
    _data = data;
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UrmaProcessHandshakeAtClient");
    if (bthread_start_background(&tid, &attr,
                                 UrmaEndpoint::ProcessHandshakeAtClient,
                                 urma_transport->_urma_ep) < 0) {
        LOG(FATAL) << "Fail to start handshake bthread";
        Run();
    } else {
        s.release();
    }
}

void UrmaConnect::StopConnect(Socket* socket) { }

void UrmaConnect::Run() {
    _done(errno, _data);
}

void UrmaEndpoint::OnNewDataFromTcp(Socket* m) {
    auto* urma_transport = static_cast<UrmaTransport*>(m->_transport.get());
    UrmaEndpoint* ep = urma_transport->GetUrmaEp();
    CHECK(ep != NULL);

    int progress = Socket::PROGRESS_INIT;
    while (true) {
        const State state = ep->_state.load(butil::memory_order_acquire);
        if (state == UNINIT) {
        } else if (state < ESTABLISHED) {
            ep->_read_butex->fetch_add(1, butil::memory_order_release);
            bthread::butex_wake(ep->_read_butex);
        } else if (state == FALLBACK_TCP){
            InputMessenger::OnNewMessages(m);
            return;
        } else if (state == ESTABLISHED) {
            uint8_t tmp;
            ssize_t nr = read(ep->_socket->fd(), &tmp, 1);
            if (nr == 0) {
                ep->_socket->SetEOF();
                return;
            }
            if (nr > 0) {
                LOG(WARNING) << "Read unexpected data from " << ep->_socket;
                ep->_socket->SetFailed(EPROTO, "Read unexpected data from %s",
                                       ep->_socket->description().c_str());
                return;
            }

            if (errno != EAGAIN) {
                const int saved_errno = errno;
                PLOG(WARNING) << "Fail to read from " << ep->_socket;
                ep->_socket->SetFailed(saved_errno, "Fail to read from %s: %s",
                                       ep->_socket->description().c_str(),
                                       berror(saved_errno));
            }
        }
        if (!m->MoreReadEvents(&progress)) {
            break;
        }
    }
}

static const int WAIT_TIMEOUT_MS = 50;

template <class ReadOnce>
static int ReadFromFdLoop(butil::atomic<int>* read_butex,
                          size_t len, ReadOnce&& read_once) {
    size_t received = 0;
    while (received < len) {
        const int expected_val = read_butex->load(butil::memory_order_acquire);
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        ssize_t nr = read_once(received, len - received);
        if (nr < 0) {
            if (errno == EAGAIN) {
                if (bthread::butex_wait(read_butex, expected_val, &duetime) < 0) {
                    if (errno != EWOULDBLOCK && errno != ETIMEDOUT) {
                        return -1;
                    }
                }
            } else {
                return -1;
            }
        } else if (nr == 0) {
            errno = EEOF;
            return -1;
        } else {
            received += nr;
        }
    }
    return 0;
}

int UrmaEndpoint::ReadFromFd(void* data, size_t len) {
    CHECK(data != NULL);
    const int fd = _socket->fd();
    return ReadFromFdLoop(_read_butex, len,
        [data, fd](size_t offset, size_t remaining) {
            return read(fd, (uint8_t*)data + offset, remaining);
        });
}

int UrmaEndpoint::ReadFromFd(butil::IOPortal* data, size_t len) {
    CHECK(data != NULL);
    const int fd = _socket->fd();
    return ReadFromFdLoop(_read_butex, len,
        [data, fd](size_t /*offset*/, size_t remaining) {
            return data->append_from_file_descriptor(fd, remaining);
        });
}

template <class WriteOnce, class WaitWritable>
static int WriteToFdLoop(size_t len, WriteOnce&& write_once, WaitWritable&& wait_writable) {
    size_t written = 0;
    while (written < len) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        ssize_t nw = write_once(written, len - written);
        if (nw >= 0) {
            written += nw;
            continue;
        }

        if (errno != EAGAIN) {
            return -1;
        }
        if (!wait_writable(&duetime)) {
            return -1;
        }
    }
    return 0;
}

int UrmaEndpoint::WriteToFd(void* data, size_t len) {
    CHECK(data != NULL);
    Socket* s = _socket;
    const int fd = s->fd();
    return WriteToFdLoop(len,
        [data, fd](size_t offset, size_t remaining) {
            return write(fd, (uint8_t*)data + offset, remaining);
        },
        [s, fd](const timespec* duetime) {
            return s->WaitEpollOut(fd, true, duetime) == 0 || errno == ETIMEDOUT;
        });
}

int UrmaEndpoint::WriteToFd(butil::IOBuf* data) {
    CHECK(data != NULL);
    Socket* s = _socket;
    const int fd = s->fd();
    return WriteToFdLoop(data->size(),
        [data, fd](size_t /*offset*/, size_t /*remaining*/) {
            return data->cut_into_file_descriptor(fd);
        },
        [s, fd](const timespec* duetime) {
            return s->WaitEpollOut(fd, true, duetime) == 0 || errno == ETIMEDOUT;
        });
}

void UrmaEndpoint::ApplyRemoteHello(const ParsedHello& remote) {
    _remote_recv_block_size = remote.block_size;
    _local_window_capacity = std::min(_sq_size, remote.rq_size) - RESERVED_WR_NUM;
    _remote_window_capacity = std::min(_rq_size, remote.sq_size) - RESERVED_WR_NUM;
    _sq_imm_window_size = RESERVED_WR_NUM;
    _remote_rq_window_size.store(_local_window_capacity, butil::memory_order_relaxed);
    _sq_window_size.store(_local_window_capacity, butil::memory_order_relaxed);
}

void* UrmaEndpoint::ProcessHandshakeAtClient(void* arg) {
    auto ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    UrmaConnect::RunGuard rg((UrmaConnect*)s->_app_connect.get());
    auto urma_transport = static_cast<UrmaTransport*>(s->_transport.get());

    LOG_IF(INFO, FLAGS_urma_trace_verbose)
        << "Start handshake on " << s->description();

    std::unique_ptr<UrmaHandshake> handshake = CreateClientHandshake(ep);
    CHECK(handshake != NULL);
    ep->_handshake_version = handshake->ProtocolVersion();

    ep->_state.store(C_ALLOC_QPCQ, butil::memory_order_relaxed);
    if (ep->AllocateResources() < 0) {
        LOG(WARNING) << "Fallback to tcp:" << s->description();
        urma_transport->_urma_state = UrmaTransport::URMA_OFF;
        ep->_state.store(FALLBACK_TCP, butil::memory_order_release);
        return NULL;
    }

    ep->_state.store(C_HELLO_SEND, butil::memory_order_relaxed);
    if (handshake->SendLocalHello() < 0) {
        int saved_errno = errno;
        PLOG(WARNING) << "Fail to send hello message to server:"
                      << s->description();
        s->SetFailed(saved_errno, "Fail to complete urma handshake from %s: %s",
                     s->description().c_str(), berror(saved_errno));
        ep->_state.store(FAILED, butil::memory_order_relaxed);
        return NULL;
    }

    ep->_state.store(C_HELLO_WAIT, butil::memory_order_relaxed);
    ParsedHello remote{};
    const RemoteHelloResult r = handshake->ReceiveAndParseRemoteHello(&remote);
    if (r == RemoteHelloResult::ERROR) {
        int saved_errno = errno;
        PLOG(WARNING) << "Fail to receive hello from server:"
                      << s->description();
        s->SetFailed(saved_errno, "Fail to complete urma handshake from %s: %s",
                     s->description().c_str(), berror(saved_errno));
        ep->_state.store(FAILED, butil::memory_order_relaxed);
        return NULL;
    }

    if (r != RemoteHelloResult::NEGOTIATED) {
        LOG(WARNING) << "Fail to negotiate with server, fallback to tcp:"
                     << s->description();
        urma_transport->_urma_state = UrmaTransport::URMA_OFF;
    } else {
        ep->ApplyRemoteHello(remote);
        ep->_state.store(C_BRINGUP_QP, butil::memory_order_relaxed);
        if (ep->BringUpQp(remote, /*is_server=*/false) < 0) {
            LOG(WARNING) << "Fail to bringup QP, fallback to tcp:"
                         << s->description();
            urma_transport->_urma_state = UrmaTransport::URMA_OFF;
        } else {
            urma_transport->_urma_state = UrmaTransport::URMA_ON;
        }
    }

    ep->_state.store(C_ACK_SEND, butil::memory_order_relaxed);
    bool urma_on = urma_transport->_urma_state == UrmaTransport::URMA_ON;
    uint32_t flags = urma_on ? HELLO_ACK_URMA_OK : 0;
    uint32_t flags_be = butil::HostToNet32(flags);
    if (ep->WriteToFd(&flags_be, HELLO_ACK_LEN) < 0) {
        int saved_errno = errno;
        PLOG(WARNING) << "Fail to send Ack Message to server:"
                      << s->description();
        s->SetFailed(saved_errno, "Fail to complete urma handshake from %s: %s",
                     s->description().c_str(), berror(saved_errno));
        ep->_state.store(FAILED, butil::memory_order_relaxed);
        return NULL;
    }

    if (urma_transport->_urma_state == UrmaTransport::URMA_ON) {
        ep->_state.store(ESTABLISHED, butil::memory_order_relaxed);
        LOG_IF(INFO, FLAGS_urma_trace_verbose)
            << "Client handshake ends (use urma v" << ep->_handshake_version
            << ") on " << s->description();
    } else {
        ep->_state.store(FALLBACK_TCP, butil::memory_order_release);
        LOG_IF(INFO, FLAGS_urma_trace_verbose)
            << "Client handshake ends (use tcp) on " << s->description();
    }

    errno = 0;

    return NULL;
}

ParseResult UrmaEndpoint::ExecuteServerHandshake(butil::IOBuf* source, Socket* s) {
    UrmaTransport* urma_transport = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaEndpoint* ep = urma_transport->_urma_ep;
    CHECK(ep != NULL);

    if (s->parsing_context() == NULL) {
        if (source->size() < HELLO_MAGIC_LEN) {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }
        uint8_t magic[HELLO_MAGIC_LEN];
        CHECK_EQ(source->copy_to(magic, HELLO_MAGIC_LEN), HELLO_MAGIC_LEN);

        std::unique_ptr<UrmaHandshake> hs = CreateServerHandshakeByMagic(ep, source, magic);
        if (hs == NULL) {
            return MakeParseError(PARSE_ERROR_TRY_OTHERS);
        }
        ep->_handshake_version = hs->ProtocolVersion();
        ep->_state.store(S_HELLO_WAIT, butil::memory_order_relaxed);

        ParsedHello remote{};
        const RemoteHelloResult r = hs->ReceiveAndParseRemoteHello(&remote);
        if (r == RemoteHelloResult::NEED_MORE) {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }
        if (r == RemoteHelloResult::ERROR) {
            ep->_state.store(FAILED, butil::memory_order_relaxed);
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
        }

        bool negotiated = r == RemoteHelloResult::NEGOTIATED;
        if (negotiated) {
            ep->ApplyRemoteHello(remote);
            ep->_state.store(S_ALLOC_QPCQ, butil::memory_order_relaxed);
            if (ep->AllocateResources() < 0) {
                LOG(WARNING) << "Fail to allocate urma resources, fallback to tcp:"
                             << s->description();
                negotiated = false;
            } else {
                ep->_state.store(S_BRINGUP_QP, butil::memory_order_relaxed);
                if (ep->BringUpQp(remote, /*is_server=*/true) < 0) {
                    LOG(WARNING) << "Fail to bringup QP, fallback to tcp:"
                                 << s->description();
                    negotiated = false;
                }
            }
        }
        if (!negotiated) {
            urma_transport->_urma_state = UrmaTransport::URMA_OFF;
        }

        ep->_state.store(S_HELLO_SEND, butil::memory_order_relaxed);
        if (hs->SendLocalHello() < 0) {
            PLOG(WARNING) << "Fail to send server hello to " << s->description();
            ep->_state.store(FAILED, butil::memory_order_relaxed);
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
        }

        s->reset_parsing_context(ServerHandshakeContext::Create());
        ep->_state.store(S_ACK_WAIT, butil::memory_order_relaxed);
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    if (source->size() < HELLO_ACK_LEN) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    if (source->size() > HELLO_ACK_LEN) {
        LOG(WARNING) << "Too many bytes in handshake ACK, drop connection: "
                     << s->description();
        ep->_state.store(FAILED, butil::memory_order_relaxed);
        s->reset_parsing_context(NULL);
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    uint32_t flags_be = 0;
    CHECK_EQ(source->cutn(&flags_be, HELLO_ACK_LEN), HELLO_ACK_LEN);
    uint32_t flags = butil::NetToHost32(flags_be);
    bool client_ack_ok = (flags & HELLO_ACK_URMA_OK) != 0;
    if (!client_ack_ok) {
        LOG_IF(INFO, FLAGS_urma_trace_verbose)
            << "Server handshake ends (use tcp) on " << s->description();
        urma_transport->_urma_state = UrmaTransport::URMA_OFF;
        ep->_state.store(FALLBACK_TCP, butil::memory_order_release);
        s->reset_parsing_context(NULL);
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }

    if (urma_transport->_urma_state == UrmaTransport::URMA_OFF) {
        LOG(WARNING) << "Client wants URMA in ACK but server fell back: "
                     << s->description();
        ep->_state.store(FAILED, butil::memory_order_relaxed);
        s->reset_parsing_context(NULL);
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    LOG_IF(INFO, FLAGS_urma_trace_verbose)
        << "Server handshake ends (use urma v" << ep->_handshake_version
        << ") on " << s->description();
    urma_transport->_urma_state = UrmaTransport::URMA_ON;
    ep->_state.store(ESTABLISHED, butil::memory_order_relaxed);
    s->reset_parsing_context(NULL);
    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}

bool UrmaEndpoint::IsWritable() const {
    if (BAIDU_UNLIKELY(g_skip_urma_init)) {
        return false;
    }

    return _remote_rq_window_size.load(butil::memory_order_relaxed) > 0 &&
           _sq_window_size.load(butil::memory_order_relaxed) > 0;
}

class UrmaIOBuf : public butil::IOBuf {
friend class UrmaEndpoint;
private:
    ssize_t cut_into_sglist_and_iobuf(urma_sge_t* sglist, size_t* sge_index,
                                      butil::IOBuf* to, size_t max_sge,
                                      size_t max_len) {
        size_t len = 0;
        while (*sge_index < max_sge) {
            if (len == max_len || _ref_num() == 0) {
                break;
            }
            butil::IOBuf::BlockRef const& r = _ref_at(0);
            CHECK(r.length > 0);
            const void* start = fetch1();
            urma_target_seg_t* tseg = GetUrmaSeg((void*)start);
            if (BAIDU_UNLIKELY(tseg == NULL)) {
                LOG(WARNING) << "Memory not registered for urma. "
                             << "Is this iobuf allocated before calling "
                             << "GlobalUrmaInitializeOrDie? Or just forget to "
                             << "call RegisterMemoryForUrma for your own buffer?";
                errno = ERDMAMEM;
                return -1;
            }
            size_t i = *sge_index;
            if (len + r.length > max_len) {
                sglist[i].len = max_len - len;
                len = max_len;
            } else {
                sglist[i].len = r.length;
                len += r.length;
            }
            sglist[i].addr = (uint64_t)start;
            sglist[i].tseg = tseg;
            cutn(to, sglist[i].len);
            (*sge_index)++;
        }
        return len;
    }
};

ssize_t UrmaEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    if (BAIDU_UNLIKELY(g_skip_urma_init)) {
        errno = EAGAIN;
        return -1;
    }

    CHECK(from != NULL);
    CHECK(ndata > 0);

    size_t total_len = 0;
    size_t current = 0;
    uint32_t remote_rq_window_size =
        _remote_rq_window_size.load(butil::memory_order_relaxed);
    uint32_t sq_window_size =
        _sq_window_size.load(butil::memory_order_relaxed);
    urma_jfs_wr_t wr;
    int max_sge = GetUrmaMaxSge();
    urma_sge_t sglist[max_sge];
    while (current < ndata) {
        if (remote_rq_window_size == 0 || sq_window_size == 0) {
            if (total_len > 0) {
                break;
            } else {
                errno = EAGAIN;
                return -1;
            }
        }
        butil::IOBuf* to = &_sbuf[_sq_current];
        size_t this_len = 0;

        memset(&wr, 0, sizeof(wr));
        wr.opcode = URMA_OPC_SEND_IMM;
        wr.send.src.sge = sglist;
        wr.tjetty = _resource->remote_jetty;

        UrmaIOBuf* data = (UrmaIOBuf*)from[current];
        size_t sge_index = 0;
        while (sge_index < (uint32_t)max_sge &&
                this_len < _remote_recv_block_size) {
            if (data->empty()) {
                ++current;
                if (current == ndata) {
                    break;
                }
                data = (UrmaIOBuf*)from[current];
                continue;
            }

            ssize_t len = data->cut_into_sglist_and_iobuf(
                sglist, &sge_index, to, max_sge, _remote_recv_block_size - this_len);
            if (len < 0) {
                return -1;
            }
            CHECK(len > 0);
            this_len += len;
            total_len += len;
        }
        if (this_len == 0) {
            continue;
        }

        wr.send.src.num_sge = sge_index;

        uint32_t imm = _new_rq_wrs.exchange(0, butil::memory_order_relaxed);
        wr.send.imm_data = butil::HostToNet32(imm);
        bool solicited = false;
        if (remote_rq_window_size == 1 || sq_window_size == 1 || current + 1 >= ndata) {
            solicited = true;
        } else {
            if (_unsolicited > _local_window_capacity / 4) {
                solicited = true;
            } else if (_accumulated_ack > _remote_window_capacity / 4) {
                solicited = true;
            } else if (_unsolicited_bytes > 1048576) {
                solicited = true;
            } else {
                ++_unsolicited;
                _unsolicited_bytes += this_len;
                _accumulated_ack += imm;
            }
        }
        if (solicited) {
            wr.flag.bs.solicited_enable = 1;
            _unsolicited = 0;
            _unsolicited_bytes = 0;
            _accumulated_ack = 0;
        }

        ++_sq_unsignaled;
        if (_sq_unsignaled >= _local_window_capacity / 4) {
            wr.flag.bs.complete_enable = 1;
            wr.user_ctx = _sq_unsignaled;
            _sq_unsignaled = 0;
        }

        urma_jfs_wr_t* bad = NULL;
        int err = urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
        if (err != 0) {
            std::ostringstream oss;
            DebugInfo(oss, ", ");
            LOG(WARNING) << "Fail to urma_post_jetty_send_wr: " << berror(err) << " " << oss.str();
            errno = err;
            return -1;
        }

        ++_sq_current;
        if (_sq_current == _sq_size - RESERVED_WR_NUM) {
            _sq_current = 0;
        }

        remote_rq_window_size =
            _remote_rq_window_size.fetch_sub(1, butil::memory_order_relaxed) - 1;
        sq_window_size = _sq_window_size.fetch_sub(1, butil::memory_order_relaxed) - 1;
    }

    return total_len;
}

int UrmaEndpoint::SendAck(int num) {
    if (_new_rq_wrs.fetch_add(num, butil::memory_order_relaxed) > _remote_window_capacity / 2 &&
        _sq_imm_window_size > 0) {
        return SendImm(_new_rq_wrs.exchange(0, butil::memory_order_relaxed));
    }
    return 0;
}

int UrmaEndpoint::SendImm(uint32_t imm) {
    if (imm == 0) {
        return 0;
    }

    urma_jfs_wr_t wr;
    memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND_IMM;
    wr.send.imm_data = butil::HostToNet32(imm);
    wr.flag.bs.solicited_enable = 1;
    wr.flag.bs.complete_enable = 1;
    wr.user_ctx = 0;
    wr.tjetty = _resource->remote_jetty;

    urma_jfs_wr_t* bad = NULL;
    int err = urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (err != 0) {
        std::ostringstream oss;
        DebugInfo(oss, ", ");
        LOG(WARNING) << "Fail to urma_post_jetty_send_wr: " << berror(err) << " " << oss.str();
        return -1;
    }

    _sq_imm_window_size -= 1;
    return 0;
}

ssize_t UrmaEndpoint::HandleCompletion(urma_cr_t& cr) {
    bool zerocopy = FLAGS_urma_recv_zerocopy;
    if (cr.flag.bs.s_r == 0) {
        if (0 == (uintptr_t)cr.user_ctx) {
            _sq_imm_window_size += 1;
            SendAck(0);
            return 0;
        }
        uint16_t wnd_to_update = (uint16_t)(uintptr_t)cr.user_ctx;
        for (uint16_t i = 0; i < wnd_to_update; ++i) {
            _sbuf[_sq_sent++].clear();
            if (_sq_sent == _sq_size - RESERVED_WR_NUM) {
                _sq_sent = 0;
            }
        }
        butil::subtle::MemoryBarrier();

        _sq_window_size.fetch_add(wnd_to_update, butil::memory_order_relaxed);
        if (_remote_rq_window_size.load(butil::memory_order_relaxed) >=
            _local_window_capacity / 8) {
            _socket->WakeAsEpollOut();
        }
        return 0;
    } else {
        if (cr.completion_len > 0) {
            if (cr.completion_len < (uint32_t)FLAGS_urma_zerocopy_min_size) {
                zerocopy = false;
            }
            CHECK_NE(_state.load(butil::memory_order_relaxed), FALLBACK_TCP);
            if (zerocopy) {
                _rbuf[_rq_received].cutn(&_socket->_read_buf, cr.completion_len);
            } else {
                _socket->_read_buf.append(_rbuf_data[_rq_received], cr.completion_len);
            }
        }
        if (cr.imm_data > 0) {
            uint32_t acks = butil::NetToHost32(cr.imm_data);
            uint32_t wnd_thresh = _local_window_capacity / 8;
            uint32_t remote_rq_window_size =
                _remote_rq_window_size.fetch_add(acks, butil::memory_order_relaxed);
            if (_sq_window_size.load(butil::memory_order_relaxed) > 0 &&
                (remote_rq_window_size >= wnd_thresh || acks >= wnd_thresh)) {
                _socket->WakeAsEpollOut();
            }
        }
        if (PostRecv(1, zerocopy) < 0) {
            return -1;
        }
        if (cr.completion_len > 0) {
            SendAck(1);
        }
        return cr.completion_len;
    }
}

int UrmaEndpoint::DoPostRecv(void* block, size_t block_size) {
    urma_jfr_wr_t wr;
    memset(&wr, 0, sizeof(wr));
    urma_sge_t sge;
    sge.addr = (uint64_t)block;
    sge.len = block_size;
    sge.tseg = GetUrmaSeg(block);
    wr.src.num_sge = 1;
    wr.src.sge = &sge;

    urma_jfr_wr_t* bad = NULL;
    int err = urma_post_jetty_recv_wr(_resource->jetty, &wr, &bad);
    if (err != 0) {
        LOG(WARNING) << "Fail to urma_post_jetty_recv_wr: " << berror(err);
        return -1;
    }
    return 0;
}

int UrmaEndpoint::PostRecv(uint32_t num, bool zerocopy) {
    while (num > 0) {
        if (zerocopy) {
            _rbuf[_rq_received].clear();
            butil::IOBufAsZeroCopyOutputStream os(&_rbuf[_rq_received],
                    g_urma_recv_block_size + IOBUF_BLOCK_HEADER_LEN);
            int size = 0;
            if (!os.Next(&_rbuf_data[_rq_received], &size)) {
                PLOG(WARNING) << "Fail to allocate rbuf";
                return -1;
            } else {
                CHECK_EQ(static_cast<uint32_t>(size), g_urma_recv_block_size);
            }
        }
        if (DoPostRecv(_rbuf_data[_rq_received], g_urma_recv_block_size) < 0) {
            _rbuf[_rq_received].clear();
            return -1;
        }
        --num;
        ++_rq_received;
        if (_rq_received == _rq_size) {
            _rq_received = 0;
        }
    };
    return 0;
}

static UrmaResource* AllocateJettyCq(uint16_t sq_size, uint16_t rq_size) {
    std::unique_ptr<UrmaResource> resource(new UrmaResource);
    urma_context_t* ctx = GetUrmaContext();

    if (!FLAGS_urma_use_polling) {
        resource->jfce = urma_create_jfce(ctx);
        if (NULL == resource->jfce) {
            PLOG(WARNING) << "Fail to create jfce";
            return NULL;
        }

        if (butil::make_close_on_exec(resource->jfce->fd) < 0) {
            PLOG(WARNING) << "Fail to set jfce close-on-exec";
            return NULL;
        }
        if (butil::make_non_blocking(resource->jfce->fd) < 0) {
            PLOG(WARNING) << "Fail to set jfce nonblocking";
            return NULL;
        }

        urma_jfc_cfg_t jfc_cfg;
        memset(&jfc_cfg, 0, sizeof(jfc_cfg));
        jfc_cfg.depth = sq_size + rq_size;
        jfc_cfg.jfce = resource->jfce;
        resource->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (NULL == resource->jfc) {
            PLOG(WARNING) << "Fail to create jfc";
            return NULL;
        }
    } else {
        urma_jfc_cfg_t jfc_cfg;
        memset(&jfc_cfg, 0, sizeof(jfc_cfg));
        jfc_cfg.depth = sq_size + rq_size;
        jfc_cfg.jfce = NULL;
        resource->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (NULL == resource->jfc) {
            PLOG(WARNING) << "Fail to create polling jfc";
            return NULL;
        }
    }

    urma_jfr_cfg_t jfr_cfg;
    memset(&jfr_cfg, 0, sizeof(jfr_cfg));
    jfr_cfg.depth = rq_size;
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.jfc = resource->jfc;
    jfr_cfg.token_policy = URMA_TOKEN_NONE;
    resource->jfr = urma_create_jfr(ctx, &jfr_cfg);
    if (NULL == resource->jfr) {
        PLOG(WARNING) << "Fail to create jfr";
        return NULL;
    }

    urma_jetty_cfg_t jetty_cfg;
    memset(&jetty_cfg, 0, sizeof(jetty_cfg));
    jetty_cfg.share_jfr = URMA_SHARE_JFR;
    jetty_cfg.shared.jfr = resource->jfr;
    jetty_cfg.jfs_cfg.depth = sq_size;
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.jfc = resource->jfc;
    jetty_cfg.jfs_cfg.max_sge = GetUrmaMaxSge();
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    resource->jetty = urma_create_jetty(ctx, &jetty_cfg);
    if (NULL == resource->jetty) {
        PLOG(WARNING) << "Fail to create jetty";
        return NULL;
    }

    return resource.release();
}

int UrmaEndpoint::AllocateResources() {
    if (BAIDU_UNLIKELY(g_skip_urma_init)) {
        return 0;
    }

    CHECK(_resource == NULL);

    if (_sq_size <= FLAGS_urma_prepared_qp_size &&
        _rq_size <= FLAGS_urma_prepared_qp_size) {
        BAIDU_SCOPED_LOCK(*g_urma_resource_mutex);
        if (g_urma_resource_list) {
            _resource = g_urma_resource_list;
            g_urma_resource_list = g_urma_resource_list->next;
        }
    }
    if (!_resource) {
        _resource = AllocateJettyCq(_sq_size, _rq_size);
    } else {
        _resource->next = NULL;
    }
    if (!_resource) {
        return -1;
    }

    if (!FLAGS_urma_use_polling) {
        if (0 != ReqNotifyCq(true)) {
            return -1;
        }

        SocketOptions options;
        options.user = this;
        options.keytable_pool = _socket->_keytable_pool;
        options.fd = _resource->jfce->fd;
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(WARNING) << "Fail to create socket for jfc";
            return -1;
        }
    } else {
        SocketOptions options;
        options.user = this;
        options.keytable_pool = _socket->_keytable_pool;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(WARNING) << "Fail to create socket for jfc";
            return -1;
        }
        PollerAddCqSid();
    }

    _sbuf.resize(_sq_size - RESERVED_WR_NUM);
    if (_sbuf.size() != _sq_size - RESERVED_WR_NUM) {
        return -1;
    }
    _rbuf.resize(_rq_size);
    if (_rbuf.size() != _rq_size) {
        return -1;
    }
    _rbuf_data.resize(_rq_size, NULL);
    if (_rbuf_data.size() != _rq_size) {
        return -1;
    }

    return 0;
}

int UrmaEndpoint::BringUpQp(const ParsedHello& remote, bool is_server) {
    if (BAIDU_UNLIKELY(g_skip_urma_init)) {
        return 0;
    }

    urma_context_t* ctx = GetUrmaContext();

    urma_seg_import_attr_t rseg_attr;
    memset(&rseg_attr, 0, sizeof(rseg_attr));
    rseg_attr.eid = remote.eid;
    _resource->remote_seg = urma_import_seg(ctx, &rseg_attr);
    if (NULL == _resource->remote_seg) {
        PLOG(WARNING) << "Fail to import remote seg";
        return -1;
    }

    urma_jetty_import_attr_t rjetty_attr;
    memset(&rjetty_attr, 0, sizeof(rjetty_attr));
    rjetty_attr.eid = remote.eid;
    rjetty_attr.jpn = remote.jpn;
    rjetty_attr.trans_mode = URMA_TM_RM;
    rjetty_attr.tp_type = URMA_CTP;
    rjetty_attr.type = URMA_JETTY;
    _resource->remote_jetty = urma_import_jetty(ctx, &rjetty_attr);
    if (NULL == _resource->remote_jetty) {
        PLOG(WARNING) << "Fail to import remote jetty";
        return -1;
    }

    if (PostRecv(_rq_size, true) < 0) {
        PLOG(WARNING) << "Fail to post recv wr";
        return -1;
    }

    return 0;
}

static void DeallocateJfc(urma_jfc_t* jfc) {
    if (NULL == jfc) {
        return;
    }

    int err = urma_delete_jfc(jfc);
    LOG_IF(WARNING, 0 != err) << "Fail to destroy JFC: " << berror(err);
}

static int DrainJfc(urma_jfc_t* jfc) {
    if (NULL == jfc) {
        return 0;
    }

    urma_cr_t cr;
    int ret;
    do {
        ret = urma_poll_jfc(jfc, 1, &cr);
    } while (ret > 0);

    LOG_IF(ERROR, ret < 0) << "drain JFC failed: " << ret;
    return ret;
}

void UrmaEndpoint::DeallocateResources() {
    if (!_resource) {
        return;
    }
    if (FLAGS_urma_use_polling) {
        PollerRemoveCqSid();
    }
    bool move_to_urma_resource_list = false;
    if (_sq_size <= FLAGS_urma_prepared_qp_size &&
        _rq_size <= FLAGS_urma_prepared_qp_size &&
        FLAGS_urma_prepared_jetty_cnt > 0) {
        if (_resource->remote_jetty) {
            if (urma_unimport_jetty(_resource->remote_jetty) == 0) {
                _resource->remote_jetty = NULL;
                move_to_urma_resource_list = true;
            }
        } else {
            move_to_urma_resource_list = true;
        }
    }

    if (NULL != _resource->jfc) {
        uint32_t ack_cnt = _jfc_events;
        if (ack_cnt > 0) {
            urma_ack_jfc(&_resource->jfc, &ack_cnt, 1);
        }
    }
    _jfc_events = 0;

    bool remove_consumer = true;
_reclaim:
    if (!move_to_urma_resource_list) {
        if (NULL != _resource->remote_jetty) {
            urma_unimport_jetty(_resource->remote_jetty);
            _resource->remote_jetty = NULL;
        }
        if (NULL != _resource->remote_seg) {
            urma_unimport_seg(_resource->remote_seg);
            _resource->remote_seg = NULL;
        }
        if (NULL != _resource->jetty) {
            int err = urma_delete_jetty(_resource->jetty);
            LOG_IF(WARNING, 0 != err) << "Fail to destroy jetty: " << berror(err);
            _resource->jetty = NULL;
        }
        if (NULL != _resource->jfr) {
            int err = urma_delete_jfr(_resource->jfr);
            LOG_IF(WARNING, 0 != err) << "Fail to destroy jfr: " << berror(err);
            _resource->jfr = NULL;
        }

        DeallocateJfc(_resource->jfc);

        if (NULL != _resource->jfce) {
            int fd = _resource->jfce->fd;
            GetGlobalEventDispatcher(fd, _socket->_io_event.bthread_tag()).RemoveConsumer(fd);
            remove_consumer = false;
            int err = urma_delete_jfce(_resource->jfce);
            LOG_IF(WARNING, 0 != err) << "Fail to destroy jfce: " << berror(err);
        }

        _resource->jfc = NULL;
        _resource->jfce = NULL;
        _resource->jfr = NULL;
        _resource->jetty = NULL;
        delete _resource;
        _resource = NULL;
    }

    if (INVALID_SOCKET_ID != _cq_sid) {
        SocketUniquePtr s;
        if (Socket::Address(_cq_sid, &s) == 0) {
            if (remove_consumer) {
                s->_io_event.RemoveConsumer(s->_fd);
            }
            s->_user = NULL;
            s->_fd = -1;
            s->SetFailed();
        }
    }

    if (move_to_urma_resource_list) {
        int ret = DrainJfc(_resource->jfc);
        if (ret < 0) {
            move_to_urma_resource_list = false;
            goto _reclaim;
        }

        BAIDU_SCOPED_LOCK(*g_urma_resource_mutex);
        _resource->next = g_urma_resource_list;
        g_urma_resource_list = _resource;
    }
}

static const int MAX_CQ_EVENTS = 128;

int UrmaEndpoint::GetAndAckEvents(SocketUniquePtr& s) {
    while (true) {
        urma_jfc_t* ev_jfc = NULL;
        int ret = urma_wait_jfc(_resource->jfce, 1, 0, &ev_jfc);
        if (ret <= 0) {
            if (ret < 0 && errno != EAGAIN && errno != ETIMEDOUT) {
                const int saved_errno = errno;
                PLOG(ERROR) << "Fail to get jfc event from " << s->description();
                s->SetFailed(saved_errno, "Fail to get jfc event from %s: %s",
                             s->description().c_str(), berror(saved_errno));
                return -1;
            }
            break;
        }
        ++_jfc_events;
    }
    if (_jfc_events >= MAX_CQ_EVENTS) {
        uint32_t ack_cnt = _jfc_events;
        urma_ack_jfc(&_resource->jfc, &ack_cnt, 1);
        _jfc_events = 0;
    }
    return 0;
}

int UrmaEndpoint::ReqNotifyCq(bool send_cq) {
    if (urma_rearm_jfc(_resource->jfc, false) < 0) {
        const int saved_errno = errno;
        PLOG(WARNING) << "Fail to arm JFC comp channel from "
                      << _socket->description();
        _socket->SetFailed(saved_errno, "Fail to arm JFC channel from %s: %s",
                           _socket->description().c_str(),
                           berror(saved_errno));
        return -1;
    }

    return 0;
}

void UrmaEndpoint::PollCq(Socket* m) {
    UrmaEndpoint* ep = static_cast<UrmaEndpoint*>(m->user());
    if (!ep) {
        return;
    }

    SocketUniquePtr s;
    if (Socket::Address(ep->_socket->id(), &s) < 0) {
        return;
    }
    auto* urma_transport = static_cast<UrmaTransport*>(s->_transport.get());
    CHECK(ep == urma_transport->_urma_ep);

    urma_jfc_t* jfc = ep->_resource->jfc;

    if (!FLAGS_urma_use_polling) {
        if (ep->GetAndAckEvents(s) < 0) {
            return;
        }
    }

    int progress = Socket::PROGRESS_INIT;
    bool notified = false;
    InputMessageClosure last_msg;
    urma_cr_t cr[FLAGS_urma_cqe_poll_once];
    while (true) {
        int cnt = urma_poll_jfc(jfc, FLAGS_urma_cqe_poll_once, cr);
        if (cnt < 0) {
            const int saved_errno = errno;
            PLOG(WARNING) << "Fail to poll jfc: " << s->description();
            s->SetFailed(saved_errno, "Fail to poll jfc from %s: %s",
                    s->description().c_str(), berror(saved_errno));
            return;
        }
        if (cnt == 0) {
            if (FLAGS_urma_use_polling) {
                return;
            }

            if (!notified) {
                if (0 != ep->ReqNotifyCq(true)) {
                    return;
                }
                notified = true;
                continue;
            }
            if (!m->MoreReadEvents(&progress)) {
                break;
            }

            if (0 != ep->GetAndAckEvents(s)) {
                return;
            }

            notified = false;
            continue;
        }
        notified = false;

        ssize_t bytes = 0;
        for (int i = 0; i < cnt; ++i) {
            if (s->Failed()) {
                return;
            }

            if (cr[i].status != URMA_CR_SUCCESS) {
                PLOG(WARNING) << "Fail to handle URMA completion, error status("
                              << cr[i].status << "): " << s->description();
                s->SetFailed(ERDMA, "URMA completion error(%d) from %s: %s",
                             cr[i].status, s->description().c_str(), berror(ERDMA));
                continue;
            }

            ssize_t nr = ep->HandleCompletion(cr[i]);
            if (nr < 0) {
                const int saved_errno = errno;
                PLOG(WARNING) << "Fail to handle URMA completion: " << s->description();
                s->SetFailed(saved_errno, "Fail to handle urma completion from %s: %s",
                             s->description().c_str(), berror(saved_errno));
            } else if (nr > 0) {
                bytes += nr;
            }
        }

        const int64_t received_us = butil::cpuwide_time_us();
        const int64_t base_realtime = butil::gettimeofday_us() - received_us;
        InputMessenger* messenger = static_cast<InputMessenger*>(s->user());
        if (messenger->ProcessNewMessage(
                    s.get(), bytes, false, received_us, base_realtime, last_msg) < 0) {
            return;
        }
    }
}

std::string UrmaEndpoint::GetStateStr() const {
    switch (_state.load(butil::memory_order_relaxed)) {
    case UNINIT: return "UNINIT";
    case C_ALLOC_QPCQ: return "C_ALLOC_QPCQ";
    case C_HELLO_SEND: return "C_HELLO_SEND";
    case C_HELLO_WAIT: return "C_HELLO_WAIT";
    case C_BRINGUP_QP: return "C_BRINGUP_QP";
    case C_ACK_SEND: return "C_ACK_SEND";
    case S_HELLO_WAIT: return "S_HELLO_WAIT";
    case S_ALLOC_QPCQ: return "S_ALLOC_QPCQ";
    case S_BRINGUP_QP: return "S_BRINGUP_QP";
    case S_HELLO_SEND: return "S_HELLO_SEND";
    case S_ACK_WAIT: return "S_ACK_WAIT";
    case ESTABLISHED: return "ESTABLISHED";
    case FALLBACK_TCP: return "FALLBACK_TCP";
    case FAILED: return "FAILED";
    default: return "UNKNOWN";
    }
}

void UrmaEndpoint::DebugInfo(std::ostream& os, butil::StringPiece connector) const {
    os << "urma_state=ON"
       << connector << "handshake_state=" << GetStateStr()
       << connector << "handshake_version=" << static_cast<int>(_handshake_version)
       << connector << "urma_sq_imm_window_size=" << _sq_imm_window_size
       << connector << "urma_remote_rq_window_size=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
       << connector << "urma_sq_window_size=" << _sq_window_size.load(butil::memory_order_relaxed)
       << connector << "urma_local_window_capacity=" << _local_window_capacity
       << connector << "urma_remote_window_capacity=" << _remote_window_capacity
       << connector << "urma_sbuf_head=" << _sq_current
       << connector << "urma_sbuf_tail=" << _sq_sent
       << connector << "urma_rbuf_head=" << _rq_received
       << connector << "urma_unacked_rq_wr=" << _new_rq_wrs.load(butil::memory_order_relaxed)
       << connector << "urma_received_ack=" << _accumulated_ack
       << connector << "urma_unsolicited_sent=" << _unsolicited
       << connector << "urma_unsignaled_sq_wr=" << _sq_unsignaled;
}

int UrmaEndpoint::GlobalInitialize() {
    g_urma_recv_block_size = FLAGS_urma_buffer_size - IOBUF_BLOCK_HEADER_LEN;
    if (g_urma_recv_block_size <= 0) {
        LOG(ERROR) << "urma_recv_block_size incorrect "
                   << "(valid value: urma_buffer_size must be > "
                   << IOBUF_BLOCK_HEADER_LEN << ")";
        errno = EINVAL;
        return -1;
    }

    g_urma_resource_mutex = new butil::Mutex;
    for (int i = 0; i < FLAGS_urma_prepared_jetty_cnt; ++i) {
        UrmaResource* res = AllocateJettyCq(FLAGS_urma_prepared_qp_size,
                                            FLAGS_urma_prepared_qp_size);
        if (!res) {
            return -1;
        }
        res->next = g_urma_resource_list;
        g_urma_resource_list = res;
    }

    if (FLAGS_urma_use_polling) {
        _poller_groups = std::vector<PollerGroup>(FLAGS_task_group_ntags);
    }

    return 0;
}

void UrmaEndpoint::GlobalRelease() {
    if (g_urma_resource_mutex) {
        BAIDU_SCOPED_LOCK(*g_urma_resource_mutex);
        while (g_urma_resource_list) {
            UrmaResource* res = g_urma_resource_list;
            g_urma_resource_list = g_urma_resource_list->next;
            delete res;
        }
    }
    if (FLAGS_urma_use_polling) {
        for (int i = 0; i < FLAGS_task_group_ntags; ++i) {
            PollingModeRelease(i);
        }
    }
}

std::vector<UrmaEndpoint::PollerGroup> UrmaEndpoint::_poller_groups;

int UrmaEndpoint::PollingModeInitialize(bthread_tag_t tag,
                                        std::function<void()> callback,
                                        std::function<void()> init_fn,
                                        std::function<void()> release_fn) {
    if (!FLAGS_urma_use_polling) {
        return 0;
    }
    auto& group = _poller_groups[tag];
    auto& pollers = group.pollers;
    auto& running = group.running;
    bool expected = false;
    if (!running.compare_exchange_strong(expected, true)) {
        return 0;
    }
    struct FnArgs {
        Poller* poller;
        std::atomic<bool>* running;
    };
    auto fn = [](void* p) -> void* {
        std::unique_ptr<FnArgs> args(static_cast<FnArgs*>(p));
        auto poller = args->poller;
        auto running = args->running;
        std::unordered_set<SocketId> cq_sids;
        CqSidOp op;

        if (poller->init_fn) {
            poller->init_fn();
        }

        while (running->load(std::memory_order_relaxed)) {
            while (poller->op_queue.Dequeue(op)) {
                if (op.type == CqSidOp::ADD) {
                    cq_sids.emplace(op.sid);
                } else if (op.type == CqSidOp::REMOVE) {
                    cq_sids.erase(op.sid);
                }
            }
            for (auto sid : cq_sids) {
                SocketUniquePtr s;
                if (Socket::Address(sid, &s) < 0) {
                    continue;
                }
                PollCq(s.get());
            }
            if (poller->callback) {
                poller->callback();
            }
            if (FLAGS_urma_poller_yield) {
                bthread_yield();
            }
        }

        if (poller->release_fn) {
            poller->release_fn();
        }

        return nullptr;
    };
    for (int i = 0; i < FLAGS_urma_poller_num; ++i) {
        auto args = new FnArgs{&pollers[i], &running};
        auto attr = FLAGS_urma_disable_bthread ? BTHREAD_ATTR_PTHREAD
                                               : BTHREAD_ATTR_NORMAL;
        attr.tag = tag;
        bthread_attr_set_name(&attr, "UrmaPolling");
        pollers[i].callback = callback;
        pollers[i].init_fn = init_fn;
        pollers[i].release_fn = release_fn;
        auto rc = bthread_start_background(&pollers[i].tid, &attr, fn, args);
        if (rc != 0) {
            LOG(ERROR) << "Fail to start urma polling bthread";
            return -1;
        }
    }
    return 0;
}

void UrmaEndpoint::PollingModeRelease(bthread_tag_t tag) {
    if (!FLAGS_urma_use_polling) {
        return;
    }
    auto& group = _poller_groups[tag];
    auto& pollers = group.pollers;
    auto& running = group.running;
    running.store(false, std::memory_order_relaxed);
    for (int i = 0; i < FLAGS_urma_poller_num; ++i) {
        bthread_join(pollers[i].tid, NULL);
    }
}

void UrmaEndpoint::PollerAddCqSid() {
    auto index = butil::fmix32(_cq_sid) % FLAGS_urma_poller_num;
    auto& group = _poller_groups[bthread_self_tag()];
    auto& pollers = group.pollers;
    auto& poller = pollers[index];
    if (INVALID_SOCKET_ID != _cq_sid) {
        poller.op_queue.Enqueue(CqSidOp{_cq_sid, CqSidOp::ADD});
    }
}

void UrmaEndpoint::PollerRemoveCqSid() {
    auto index = butil::fmix32(_cq_sid) % FLAGS_urma_poller_num;
    auto& group = _poller_groups[bthread_self_tag()];
    auto& pollers = group.pollers;
    auto& poller = pollers[index];
    if (INVALID_SOCKET_ID != _cq_sid) {
        poller.op_queue.Enqueue(CqSidOp{_cq_sid, CqSidOp::REMOVE});
    }
}

urma_eid_t GetUrmaEid() {
    urma_context_t* ctx = GetUrmaContext();
    if (ctx) {
        return ctx->eid;
    }
    urma_eid_t eid;
    memset(&eid, 0, sizeof(eid));
    return eid;
}

}  // namespace urma
}  // namespace brpc

#endif  // if BRPC_WITH_URMA
