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

#include "brpc/urma_transport.h"
#include "brpc/event_dispatcher.h"
#include "brpc/tcp_transport.h"
#include "brpc/input_messenger.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_helper.h"

namespace brpc {
DECLARE_bool(usercode_in_coroutine);
DECLARE_bool(usercode_in_pthread);

extern SocketVarsCollector *g_vars;

void UrmaTransport::Init(Socket *socket, const SocketOptions &options) {
    CHECK(_urma_ep == NULL);
    if (options.socket_mode == SOCKET_MODE_URMA) {
        _urma_ep = new(std::nothrow)urma::UrmaEndpoint(socket);
        if (!_urma_ep) {
            const int saved_errno = errno;
            PLOG(ERROR) << "Fail to create UrmaEndpoint";
            socket->SetFailed(
                saved_errno, "Fail to create UrmaEndpoint: %s", berror(saved_errno));
        }
        _urma_state = URMA_UNKNOWN;
    } else {
        _urma_state = URMA_OFF;
        socket->_socket_mode = SOCKET_MODE_TCP;
    }
    _socket = socket;
    _default_connect = options.app_connect;
    _on_edge_trigger = options.on_edge_triggered_events;
    if (options.need_on_edge_trigger && _on_edge_trigger == NULL) {
        // Server-side URMA sockets drive the handshake through the standard
        // InputMessenger path (ParseUrmaHandshake), so they use OnNewMessages
        // just like TCP sockets. Only client-side sockets, whose handshake
        // (ProcessHandshakeAtClient) is an active blocking bthread relying on
        // _read_butex woken by OnNewDataFromTcp, still need OnNewDataFromTcp.
        if (options.user == static_cast<SocketUser*>(get_client_side_messenger())) {
            _on_edge_trigger = urma::UrmaEndpoint::OnNewDataFromTcp;
        } else {
            _on_edge_trigger = InputMessenger::OnNewMessages;
        }
    }
    _tcp_transport = std::make_shared<TcpTransport>();
    _tcp_transport->Init(socket, options);
}

void UrmaTransport::Release() {
    if (_urma_ep) {
        delete _urma_ep;
        _urma_ep = NULL;
        _urma_state = URMA_UNKNOWN;
    }
}

int UrmaTransport::Reset(int32_t expected_nref) {
    if (_urma_ep) {
        _urma_ep->Reset();
        _urma_state = URMA_UNKNOWN;
    }
    return 0;
}

std::shared_ptr<AppConnect> UrmaTransport::Connect() {
    if (_default_connect == nullptr) {
        return  std::make_shared<urma::UrmaConnect>();
    }
    return _default_connect;
}

int UrmaTransport::CutFromIOBuf(butil::IOBuf *buf) {
    // Only send over the URMA channel once the handshake has NEGOTIATED it
    // (URMA_ON). While the state is still URMA_UNKNOWN (handshake in progress,
    // or a server connection that turned out to be plain TCP and never
    // handshook) or URMA_OFF (fell back), the Jetty is not usable and
    // everything must go over the TCP fd. Mirrors the URMA_ON check in
    // WaitEpollOut().
    if (_urma_ep && _urma_state == URMA_ON) {
        butil::IOBuf *data_arr[1] = {buf};
        return _urma_ep->CutFromIOBufList(data_arr, 1);
    } else {
        return _tcp_transport->CutFromIOBuf(buf);
    }
}

ssize_t UrmaTransport::CutFromIOBufList(butil::IOBuf **buf, size_t ndata) {
    if (_urma_ep && _urma_state == URMA_ON) {
        return _urma_ep->CutFromIOBufList(buf, ndata);
    }
    return _tcp_transport->CutFromIOBufList(buf, ndata);
}

int UrmaTransport::WaitEpollOut(butil::atomic<int> *_epollout_butex,
                                    bool pollin, const timespec duetime) {
    if (_urma_state == URMA_ON) {
        const int expected_val = _epollout_butex->load(butil::memory_order_acquire);
        CHECK(_urma_ep != NULL);
        if (!_urma_ep->IsWritable()) {
            g_vars->nwaitepollout << 1;
            if (bthread::butex_wait(_epollout_butex, expected_val, &duetime) < 0) {
                if (errno != EAGAIN && errno != ETIMEDOUT) {
                    const int saved_errno = errno;
                    PLOG(WARNING) << "Fail to wait urma window of " << _socket;
                    _socket->SetFailed(saved_errno,
                                       "Fail to wait urma window of %s: %s",
                                       _socket->description().c_str(),
                                       berror(saved_errno));
                }
                if (_socket->Failed()) {
                    return 1;
                }
            }
        }
    } else {
        return _tcp_transport->WaitEpollOut(_epollout_butex, pollin, duetime);
    }
    return 0;
}

void UrmaTransport::ProcessEvent(bthread_attr_t attr) {
    bthread_t tid;
    if (FLAGS_usercode_in_coroutine) {
        OnEdge(_socket);
    } else if (!EventDispatcherUnsched()) {
        auto rc = bthread_start_urgent(&tid, &attr, OnEdge, _socket);
        if (rc != 0) {
            LOG(FATAL) << "Fail to start ProcessEvent";
            OnEdge(_socket);
        }
    } else if (bthread_start_background(&tid, &attr, OnEdge, _socket) != 0) {
        LOG(FATAL) << "Fail to start ProcessEvent";
        OnEdge(_socket);
    }
}

void UrmaTransport::QueueMessage(InputMessageClosure& input_msg,
                                 int* num_bthread_created, bool last_msg) {
    if (last_msg && !urma::FLAGS_urma_use_polling) {
        return;
    }
    InputMessageBase* to_run_msg = input_msg.release();
    if (!to_run_msg) {
        return;
    }

    if (urma::FLAGS_urma_disable_bthread) {
        ProcessInputMessage(to_run_msg);
        return;
    }
    // Create bthread for last_msg. The bthread is not scheduled
    // until bthread_flush() is called (in the worse case).

    bthread_t th;
    bthread_attr_t tmp = (FLAGS_usercode_in_pthread ?
                                      BTHREAD_ATTR_PTHREAD :
                                                                    BTHREAD_ATTR_NORMAL) | BTHREAD_NOSIGNAL;
    tmp.keytable_pool = _socket->keytable_pool();
    tmp.tag = bthread_self_tag();
    bthread_attr_set_name(&tmp, "ProcessInputMessage");

    if (!FLAGS_usercode_in_coroutine && bthread_start_background(
            &th, &tmp, ProcessInputMessage, to_run_msg) == 0) {
        ++*num_bthread_created;
    } else {
        ProcessInputMessage(to_run_msg);
    }
}

void UrmaTransport::Debug(std::ostream &os) {
    if (_urma_state == URMA_ON && _urma_ep) {
        _urma_ep->DebugInfo(os);
    }
}

int UrmaTransport::ContextInitOrDie(bool serverOrNot, const void* _options) {
    if (serverOrNot) {
        if (!OptionsAvailableOverUrma(static_cast<const ServerOptions *>(_options))) {
            return -1;
        }
        urma::GlobalUrmaInitializeOrDie();
        if (!urma::InitPollingModeWithTag(static_cast<const ServerOptions *>(_options)->bthread_tag)) {
            return -1;
        }
    } else {
        if (!OptionsAvailableForUrma(static_cast<const ChannelOptions *>(_options))) {
            return -1;
        }
        urma::GlobalUrmaInitializeOrDie();
        if (!urma::InitPollingModeWithTag(bthread_self_tag())) {
            return -1;
        }
        return 0;
    }

    return 0;
}

bool UrmaTransport::OptionsAvailableForUrma(const ChannelOptions* opt) {
    if (opt->has_ssl_options()) {
        LOG(WARNING) << "Cannot use SSL and URMA at the same time";
        return false;
    }
    if (!urma::SupportedByUrma(opt->protocol.name())) {
        LOG(WARNING) << "Cannot use " << opt->protocol.name()
                     << " over URMA";
        return false;
    }
    return true;
}

bool UrmaTransport::OptionsAvailableOverUrma(const ServerOptions* opt) {
    if (opt->rtmp_service) {
        LOG(WARNING) << "RTMP is not supported by URMA";
        return false;
    }
    if (opt->has_ssl_options()) {
        LOG(WARNING) << "SSL is not supported by URMA";
        return false;
    }
    if (opt->nshead_service) {
        LOG(WARNING) << "NSHEAD is not supported by URMA";
        return false;
    }
    if (opt->mongo_service_adaptor) {
        LOG(WARNING) << "MONGO is not supported by URMA";
        return false;
    }
    return true;
}
} // namespace brpc
#endif
