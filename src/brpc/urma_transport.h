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

#ifndef BRPC_URMA_TRANSPORT_H
#define BRPC_URMA_TRANSPORT_H

#if BRPC_WITH_URMA
#include "brpc/socket.h"
#include "brpc/channel.h"
#include "brpc/transport.h"

namespace brpc {
class UrmaTransport : public Transport {
friend class TransportFactory;
friend class urma::UrmaEndpoint;
friend class urma::UrmaConnect;
public:
    void Init(Socket* socket, const SocketOptions& options) override;
    void Release() override;
    int Reset(int32_t expected_nref) override;
    std::shared_ptr<AppConnect> Connect() override;
    int CutFromIOBuf(butil::IOBuf* buf) override;
    ssize_t CutFromIOBufList(butil::IOBuf** buf, size_t ndata) override;
    int WaitEpollOut(butil::atomic<int>* _epollout_butex, bool pollin, const timespec duetime) override;
    void ProcessEvent(bthread_attr_t attr) override;
    void QueueMessage(InputMessageClosure& inputMsg, int* num_bthread_created, bool last_msg) override;
    void Debug(std::ostream &os) override;
    urma::UrmaEndpoint* GetUrmaEp() {
        CHECK(_urma_ep != NULL);
        return _urma_ep;
    }
    static int ContextInitOrDie(bool serverOrNot, const void* _options);
private:
    static bool OptionsAvailableForUrma(const ChannelOptions* opt);
    static bool OptionsAvailableOverUrma(const ServerOptions* opt);

    // The on/off state of URMA
    enum UrmaState {
        URMA_ON,
        URMA_OFF,
        URMA_UNKNOWN
    };
    // The UrmaEndpoint
    urma::UrmaEndpoint* _urma_ep = NULL;
    // Should use URMA or not
    UrmaState _urma_state;
    std::shared_ptr<TcpTransport>  _tcp_transport;
};
} // namespace brpc
#endif // BRPC_WITH_URMA
#endif //BRPC_URMA_TRANSPORT_H
