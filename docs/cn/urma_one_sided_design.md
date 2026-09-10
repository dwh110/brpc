<!-- SPDX-License-Identifier: Apache-2.0 -->

# brpc URMA 单边操作（READ/WRITE）实现设计方案

基于 `ubs-comm` 中 ubsocket-over-URMA 的单边读写方案，结合 brpc 当前 URMA
实现（仅支持双边 SEND/RECV）的架构特点，设计 brpc 的 URMA 单边操作支持方案。

## 目录

- [1. 背景与动机](#1-背景与动机)
- [2. 现状分析](#2-现状分析)
- [3. ubs-comm 方案要点](#3-ubs-comm-方案要点)
- [4. 总体设计](#4-总体设计)
- [5. 核心数据结构](#5-核心数据结构)
- [6. 握手协议扩展](#6-握手协议扩展)
- [7. 发送路径设计](#7-发送路径设计)
- [8. 接收路径设计](#8-接收路径设计)
- [9. 完成队列处理](#9-完成队列处理)
- [10. 流控机制](#10-流控机制)
- [11. 内存管理](#11-内存管理)
- [12. 与现有双边路径的共存](#12-与现有双边路径的共存)
- [13. 实现步骤](#13-实现步骤)

---

## 1. 背景与动机

### 1.1 单边操作的优势

brpc 当前 URMA 实现 100% 基于双边 SEND/RECV 操作（`urma_endpoint.cpp:788`
使用 `URMA_OPC_SEND`，`:951` 使用 `URMA_OPC_SEND_IMM`）。双边操作要求收发
双方各持有发送/接收队列，存在以下限制：

| 限制 | 影响 |
|------|------|
| 接收方必须预投递 recv WR | 增加内存占用和延迟 |
| 信用量流控（credit） | 每次发送消耗对端一个 recv WR，需 ACK 回报 |
| 数据拷贝到预投递 buffer | 无法充分利用零拷贝 |
| RNR 重试风险 | 对端 recv WR 不足时硬件重试 |

单边操作（WRITE_IMM / READ）的优势：

| 优势 | 说明 |
|------|------|
| 无需对端 recv WR | WRITE 直接写入对端预注册内存，READ 直接拉取对端内存 |
| 无信用量流控 | 基于 buffer 可用性流控，更低延迟 |
| 零拷贝 | 发送方 IOBuf block 地址直接作为 SGE，数据不经过中间 buffer |
| 小 IO 1 RTT | WRITE_IN_BAND 直接写入对端 recv_buf |
| 大 IO 零拷贝 | PRE_WRITE + READ 模式，发送方 block 直接被对端拉取 |

### 1.2 目标

在 brpc 现有 URMA 双边路径基础上，新增单边 READ/WRITE 数据路径，使 brpc
能根据消息大小自动选择最优传输模式：

- **小 IO（≤ 阈值）**：单边 WRITE_IMM 直接写入对端 recv_buf（1 RTT）
- **大 IO（> 阈值）**：PRE_WRITE 通知 + 单边 READ 拉取（2 RTT，零拷贝）
- **兼容**：保留现有双边 SEND/RECV 路径，通过 flag 切换

---

## 2. 现状分析

### 2.1 brpc 当前 URMA 实现

**数据路径**（仅双边）：

```
发送: CutFromIOBufList → urma_post_jetty_send_wr(URMA_OPC_SEND)
      预扣 _remote_rq_window_size + _sq_window_size
      SEND 完成 → 回收 SQ 窗口 → WakeAsEpollOut

接收: urma_poll_jfc → HandleCompletion(RECV完成)
      _rbuf[slot].cutn(_socket->_read_buf)  // 零拷贝切出
      PostRecv(1)  // 补充 recv WR
      SendAck(1)   // 信用回报
```

**关键限制**：

| 组件 | 当前状态 | 单边操作所需改造 |
|------|----------|-----------------|
| `wr.opcode` | 仅 `SEND` / `SEND_IMM` | 新增 `WRITE_IMM` / `READ` |
| `wr.send.src` | SGE 指向本地 IOBuf block | WRITE 需 `rw.src`(本地) + `rw.dst`(远端) |
| `HandleCompletion` | 仅处理 SEND/RECV 完成 | 新增 WRITE_IMM 完成 + READ 完成处理 |
| 窗口机制 | 双窗口信用流控 | 新增 send_buf 可用性流控 |
| 内存注册 | 全局 pool segment | 新增 per-connection send_buf/recv_buf 注册 |
| 握手 | 交换 pool segment 信息 | 新增交换 send_buf/recv_buf 地址 |

### 2.2 关键源码位置

| 组件 | 文件 | 行号 |
|------|------|------|
| 发送入口 | `src/brpc/urma/urma_endpoint.cpp` | 722 (`CutFromIOBufList`) |
| SGE 构建 | `src/brpc/urma/urma_endpoint.cpp` | 668 (`UrmaIOBuf::cut_into_sglist`) |
| SEND WR 构造 | `src/brpc/urma/urma_endpoint.cpp` | 788 (`wr.opcode = URMA_OPC_SEND`) |
| ACK WR 构造 | `src/brpc/urma/urma_endpoint.cpp` | 951 (`wr.opcode = URMA_OPC_SEND_IMM`) |
| 完成处理 | `src/brpc/urma/urma_endpoint.cpp` | 996 (`HandleCompletion`) |
| RECV 完成处理 | `src/brpc/urma/urma_endpoint.cpp` | 1166-1225 |
| PostRecv | `src/brpc/urma/urma_endpoint.cpp` | 860-933 |
| 窗口变量 | `src/brpc/urma/urma_endpoint.h` | 302-305 |
| 内存注册 | `src/brpc/urma/urma_helper.cpp` | 384-405 (`urma_register_seg`) |
| 对端内存导入 | `src/brpc/urma/urma_endpoint.cpp` | 565-580 (`ImportPeer`) |
| 握手信息 | `src/brpc/urma/urma_handshake.h` | 37-51 (`ParsedHello`) |
| Transport 分发 | `src/brpc/urma_transport.cpp` | 99-105 |

---

## 3. ubs-comm 方案要点

### 3.1 核心设计

ubs-comm 的 ubsocket-over-URMA 方案完全基于单边操作，不使用 SEND/RECV：

**四种 IOOpcode 控制消息**（通过 WRITE_IMM 的 imm_data 携带）：

| IOOpcode | 值 | 方向 | 含义 |
|----------|---|------|------|
| `PRE_WRITE` | 0 | 发送方→接收方 | 通知接收方：数据已就绪，发起 RDMA READ 拉取 |
| `POST_WRITE` | 1 | 接收方→发送方 | 通知发送方：数据已拉取完毕，释放 send_buf |
| `WRITE_IN_BAND` | 2 | 发送方→接收方 | 小 IO：数据直接写入对端 recv_buf |
| `WRITE_IN_BAND_ACK` | 3 | 接收方→发送方 | 小 IO 确认：通知发送方释放 send_buf |

**立即数编码**（64-bit imm_data）：

```
  ubsocket view:
    [0:3]   opcode (4 bit)         — IOOpcode
    [4:19]  buffer_offset (16 bit) — ring buffer 偏移，128B 粒度
    [20:63] reserved (44 bit)
```

### 3.2 小 IO 流程（WRITE_IN_BAND，1 RTT）

```
发送方                                    接收方
─────                                    ─────
1. allocate send_buf (ring buffer)
2. copy header+data → send_buf
3. PostWriteImm(WRITE_IN_BAND, offset)
   URMA_OPC_WRITE_IMM                ────WRITE_IMM────►
   src=本地send_buf[offset]               4. CQE: WRITE_WITH_IMM
   dst=对端recv_buf[offset]                  解析 imm_data.opcode=WRITE_IN_BAND
   imm_data=opcode+offset                    从 recv_buf[offset] 读取数据
                                          5. PostWriteImm(WRITE_IN_BAND_ACK)
                                             ←────WRITE_IMM────
6. CQE: WRITE_WITH_IMM                    ←────WRITE_IMM────
   解析 imm_data.opcode=WRITE_IN_BAND_ACK
   release send_buf[offset]
```

### 3.3 大 IO 流程（PRE_WRITE + READ，2 RTT，零拷贝）

```
发送方                                    接收方
─────                                    ─────
1. allocate send_buf (控制消息)
2. build PageBufferInMessage[]
   (发送方 IOBuf block 的远端地址+大小)
3. IncRef IOBuf blocks
4. PostWriteImm(PRE_WRITE, offset)
   URMA_OPC_WRITE_IMM                ────WRITE_IMM────►
   仅写入控制消息                         5. CQE: WRITE_WITH_IMM
                                          解析 imm_data.opcode=PRE_WRITE
                                          从 recv_buf[offset] 读取控制消息
                                          6. import 远端内存段
                                          7. allocate 本地 blocks
                                          8. PostRead(URMA_OPC_READ)
                                             src=远端block地址
                                             dst=本地block地址
                                             ←────RDMA READ────
   (NIC 服务 READ 请求)                  ←────RDMA READ────
                                          9. READ CQE: 数据已在本地 blocks
                                         10. PostWriteImm(POST_WRITE)
                                             ←────WRITE_IMM────
11. CQE: WRITE_WITH_IMM                ←────WRITE_IMM────
    解析 imm_data.opcode=POST_WRITE
    release send_buf[offset]
    DecRef IOBuf blocks
```

### 3.4 send_buf / recv_buf 管理

- **per-connection 分配**：每条连接在握手时分配 send_buf 和 recv_buf（默认各 128KB）
- **ring buffer 分配器**：`UrmaSockContinuousBuf` 保证物理连续分配，64B 粒度
- **镜像偏移**：send_buf 和 recv_buf 使用相同 offset，简化地址计算
- **可用性流控**：send_buf 可用空间不足时返回 EAGAIN，由 POST_WRITE/ACK 释放后唤醒

### 3.5 内存注册

- **send_buf/recv_buf**：在握手时通过 `urma_register_seg` 注册，地址通过 TCP 握手交换
- **IOBuf block pool**：全局 pool 通过 `urma_register_seg` 注册，block 地址可直接作为 SGE
- **远端内存导入**：接收方通过 `urma_import_seg` 导入发送方的 pool segment，用于 RDMA READ

---

## 4. 总体设计

### 4.1 架构选择

采用**共存方案**：在现有双边 SEND/RECV 路径基础上，新增单边 WRITE/READ 路径，
通过 gflag 控制切换。

```
                    ┌─────────────────────────┐
                    │   CutFromIOBufList      │
                    │   (发送入口)             │
                    └────────┬────────────────┘
                             │
                    ┌────────▼────────────────┐
                    │  消息大小 + flag 判断     │
                    └───┬─────────┬───────────┘
                        │         │
          小IO ≤ 阈值    │         │  大IO > 阈值
     ┌────▼──────────┐   │         │   ┌───▼──────────────┐
     │ WriteInline    │   │         │   │ WriteZeroCopy     │
     │ URMA_OPC_      │   │         │   │ PRE_WRITE通知     │
     │ WRITE_IMM      │   │         │   │ + 对端READ拉取    │
     └────────────────┘   │         │   └───────────────────┘
                          │
               双边模式    │
     ┌──────────────────▼──┐
     │ SendSend            │
     │ URMA_OPC_SEND       │  ← 现有路径，保持不变
     │ (现有实现)           │
     └─────────────────────┘
```

### 4.2 模式选择

新增 gflag `urma_io_mode`：

| 值 | 模式 | 说明 |
|----|------|------|
| 0 | `SEND_ONLY` | 仅双边 SEND/RECV（当前行为，默认） |
| 1 | `WRITE_ONLY` | 仅单边 WRITE/READ |
| 2 | `HYBRID` | 混合模式：小 IO 用 WRITE_IN_BAND，大 IO 用 PRE_WRITE+READ |

新增 gflag `urma_inline_threshold`：小 IO 阈值，默认 2048 字节。

### 4.3 对 brpc IOBuf 的适配

brpc 的 `butil::IOBuf` 使用 block 链表管理数据。现有 `UrmaIOBuf::cut_into_sglist`
（`urma_endpoint.cpp:668-720`）已能从 IOBuf block 直接构建 SGE，且通过
`GetPoolSegFor()` 查找 block 所属的注册内存段。这正是单边操作所需的：

- **WRITE_IN_BAND**：将 IOBuf 数据拷贝到 send_buf，然后 WRITE_IMM 到对端
- **PRE_WRITE**：将 IOBuf block 的地址 + tseg 封装为 `PageBufferInMessage`，
  通过 WRITE_IMM 发送控制消息，对端用 READ 拉取 block 数据

---

## 5. 核心数据结构

### 5.1 IOOpcode 枚举

```c++
// src/brpc/urma/urma_endpoint.h

// Single-sided IO operation opcode, carried in WRITE_IMM imm_data.
enum UrmaIoOpcode : uint8_t {
    URMA_IO_SEND       = 0,   // 双边 SEND（现有路径）
    URMA_IO_PRE_WRITE  = 1,   // 大IO: 通知对端发起READ拉取
    URMA_IO_POST_WRITE = 2,   // 大IO完成: 通知发送方释放buffer
    URMA_IO_WRITE_IN_BAND = 3,      // 小IO: 数据直接写入对端recv_buf
    URMA_IO_WRITE_IN_BAND_ACK = 4,  // 小IO确认: 通知发送方释放send_buf
};
```

### 5.2 imm_data 编码

```c++
// src/brpc/urma/urma_endpoint.h

// 64-bit immediate data encoding, shared between WRITE_IMM sender and receiver.
union UrmaWriteImmData {
    uint64_t data{0};
    struct {
        uint64_t opcode        : 4;   // UrmaIoOpcode
        uint64_t buffer_offset : 16;  // send_buf/recv_buf offset (128B granularity)
        uint64_t reserved      : 44;
    } io;
    struct {
        uint64_t user_data     : 4;
        uint64_t reserved      : 60;
    } raw;
};
```

### 5.3 控制消息格式

PRE_WRITE 消息体（写入对端 recv_buf 的控制消息）：

```c++
// src/brpc/urma/urma_endpoint.h

// Control message written to peer's recv_buf via WRITE_IMM.
// Carries the sender's IOBuf block addresses for the receiver to RDMA READ.
struct UrmaCtrlMessage {
    uint64_t request_id;        // correlation_id for matching
    uint32_t total_bytes;       // total payload bytes
    uint32_t buffer_count;      // number of PageBufferInMessage entries
    // Followed by buffer_count PageBufferInMessage entries
};

// One entry per IOBuf block segment.
struct PageBufferInMessage {
    uint64_t addr;              // sender's block virtual address
    uint32_t size;              // block data size
    uint32_t seg_token_id;      // sender's segment token (for urma_import_seg)
};
```

### 5.4 接收槽状态机

```c++
// src/brpc/urma/urma_endpoint.h

// Per-sequence-number receive slot for single-sided operations.
struct UrmaRxSlot {
    enum State : uint8_t {
        IDLE = 0,
        READING,     // RDMA READ issued, waiting for completion
        DATA_READY,  // data available for consumption
    };

    butil::atomic<State> state{IDLE};
    uint64_t write_imm{0};      // imm_data from the PRE_WRITE/WRITE_IN_BAND
    uint64_t request_id{0};
    butil::IOBuf data;          // received data (for WRITE_IN_BAND) or
                                // block chain (for READ completion)
    int64_t start_read_time_us{0};
};
```

### 5.5 发送上下文

```c++
// src/brpc/urma/urma_endpoint.h

// Context for tracking an in-flight single-sided send operation.
struct UrmaSendContext {
    uint64_t request_id;
    uint64_t send_buf_offset;   // offset in local send_buf
    uint32_t send_buf_size;     // allocated size in send_buf
    butil::IOBuf saved_blocks;  // IOBuf blocks kept alive until POST_WRITE
    // For CQE correlation
    static UrmaSendContext* s_pending[MAX_SQ_SIZE];
    static size_t s_next_request_id;
};
```

### 5.6 per-connection buffer 新增字段

在 `UrmaEndpoint` 类中新增：

```c++
// src/brpc/urma/urma_endpoint.h — class UrmaEndpoint 新增成员

// ---- Single-sided operation buffers ----
void* _send_buf_addr{nullptr};         // local send_buf base address
uint32_t _send_buf_size{0};            // send_buf capacity
void* _recv_buf_addr{nullptr};         // local recv_buf base address
uint32_t _recv_buf_size{0};            // recv_buf capacity

// Peer's buffer addresses (exchanged during handshake)
uint64_t _remote_send_buf_addr{0};
uint64_t _remote_recv_buf_addr{0};

// Registered segment for send_buf/recv_buf
urma_target_seg_t* _send_buf_tseg{nullptr};
urma_target_seg_t* _recv_buf_tseg{nullptr};
urma_target_seg_t* _remote_buf_seg{nullptr};  // imported peer's recv_buf segment

// send_buf allocator (ring buffer)
std::unique_ptr<UrmaRingBuf> _send_buf_alloc;

// Receive slots for single-sided operations
std::vector<UrmaRxSlot> _rx_slots;
butil::atomic<uint32_t> _rx_consume_seq{0};
static constexpr uint32_t RX_RING_SIZE = 256;
```

---

## 6. 握手协议扩展

### 6.1 ParsedHello 扩展

在 `src/brpc/urma/urma_handshake.h` 的 `ParsedHello` 结构体中新增字段：

```c++
struct ParsedHello {
    // --- 现有字段（保持不变）---
    uint32_t buffer_size;        // peer recv buffer size per WR
    uint32_t recv_buffer_cnt;    // peer RQ depth
    uint32_t jetty_id;
    uint8_t eid[16];
    uint32_t uasid;
    uint8_t tp_type;
    uint8_t seg_eid[16];
    uint32_t seg_uasid;
    uint64_t seg_va;
    uint64_t seg_len;
    uint32_t seg_token_id;

    // --- 新增：单边操作所需字段 ---
    uint64_t send_buf_va;        // peer send_buf base address
    uint64_t recv_buf_va;        // peer recv_buf base address
    uint32_t send_buf_size;      // peer send_buf capacity
    uint32_t recv_buf_size;      // peer recv_buf capacity
    uint32_t send_buf_token_id;  // peer send_buf segment token
    uint32_t recv_buf_token_id;  // peer recv_buf segment token
    uint8_t  send_buf_seg_eid[16];
    uint32_t send_buf_seg_uasid;
    uint8_t  recv_buf_seg_eid[16];
    uint32_t recv_buf_seg_uasid;
    uint8_t  io_mode;            // 0=SEND_ONLY, 1=WRITE_ONLY, 2=HYBRID
};
```

### 6.2 握手流程扩展

在 `ProcessHandshakeAtClient` / `ProcessHandshakeAtServer` 中新增：

```c++
// 1. 分配 send_buf/recv_buf（连续内存）
const uint32_t buf_size = FLAGS_urma_send_buf_size * 1024;  // default 128KB
void* buf = mmap(nullptr, buf_size * 2, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

_send_buf_addr = buf;
_send_buf_size = buf_size;
_recv_buf_addr = (char*)buf + buf_size;
_recv_buf_size = buf_size;

// 2. 注册内存段
urma_seg_cfg_t seg_cfg{};
seg_cfg.va = reinterpret_cast<uint64_t>(buf);
seg_cfg.len = buf_size * 2;
urma_reg_seg_flag_t seg_flag{};
seg_flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
seg_flag.bs.token_policy = URMA_TOKEN_NONE;
_send_buf_tseg = urma_register_seg(g_context, &seg_cfg);  // 整块注册

// 3. 初始化 ring buffer 分配器
_send_buf_alloc = std::make_unique<UrmaRingBuf>(_send_buf_size);

// 4. 填入 ParsedHello
MakeLocalParsedHello(&hello);
hello.send_buf_va = reinterpret_cast<uint64_t>(_send_buf_addr);
hello.recv_buf_va = reinterpret_cast<uint64_t>(_recv_buf_addr);
hello.send_buf_size = _send_buf_size;
hello.recv_buf_size = _recv_buf_size;
hello.send_buf_token_id = /* from _send_buf_tseg */;
hello.io_mode = FLAGS_urma_io_mode;

// 5. 握手完成后，导入对端 recv_buf segment
ApplyRemoteHello(remote);
// 在 ApplyRemoteHello 中新增：
if (remote.io_mode != 0) {
    // 导入对端的 buffer segment（用于 WRITE_IMM 的 dst 地址）
    urma_seg_t peer_seg{};
    std::memcpy(peer_seg.ubva.eid.raw, remote.recv_buf_seg_eid, 16);
    peer_seg.ubva.uasid = remote.recv_buf_seg_uasid;
    peer_seg.ubva.va = remote.recv_buf_va;
    peer_seg.len = remote.recv_buf_size;
    peer_seg.token_id = remote.recv_buf_token_id;
    urma_import_seg_flag_t flag{};
    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    flag.bs.mapping = URMA_SEG_NOMAP;
    _remote_buf_seg = urma_import_seg(g_context, &peer_seg, nullptr, 0, flag);
    _remote_recv_buf_addr = remote.recv_buf_va;
}
```

---

## 7. 发送路径设计

### 7.1 入口分发

在 `CutFromIOBufList` 中新增模式分发：

```c++
// src/brpc/urma/urma_endpoint.cpp

ssize_t UrmaEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }

    // 计算总数据量
    size_t total_len = 0;
    for (size_t i = 0; i < ndata; ++i) {
        total_len += from[i]->length();
    }

    // 根据模式选择路径
    switch (FLAGS_urma_io_mode) {
        case 0:  // SEND_ONLY（现有路径）
            return CutFromIOBufList_Send(from, ndata);

        case 1:  // WRITE_ONLY
            if (total_len <= (size_t)FLAGS_urma_inline_threshold) {
                return WriteInline(from, ndata);
            } else {
                return WriteZeroCopy(from, ndata);
            }

        case 2:  // HYBRID
            // 小IO用WRITE_IN_BAND，大IO用PRE_WRITE+READ
            if (total_len <= (size_t)FLAGS_urma_inline_threshold) {
                return WriteInline(from, ndata);
            } else {
                return WriteZeroCopy(from, ndata);
            }

        default:
            return CutFromIOBufList_Send(from, ndata);
    }
}
```

### 7.2 WriteInline（小 IO）

```c++
// src/brpc/urma/urma_endpoint.cpp

ssize_t UrmaEndpoint::WriteInline(butil::IOBuf** from, size_t ndata) {
    // 1. 计算 send_buf 需求
    size_t total_len = 0;
    for (size_t i = 0; i < ndata; ++i) {
        total_len += from[i]->length();
    }

    // 2. 分配 send_buf（ring buffer，128B 对齐）
    uint64_t buf_offset;
    if (!_send_buf_alloc->Allocate(total_len, buf_offset)) {
        errno = EAGAIN;
        return -1;
    }

    // 3. 拷贝数据到 send_buf
    char* dst = (char*)_send_buf_addr + buf_offset;
    size_t written = 0;
    for (size_t i = 0; i < ndata; ++i) {
        size_t n = from[i]->cutn(dst + written, from[i]->length());
        // 注意：IOBuf::cutn 到裸指针不是标准接口，实际实现需要
        // 使用 IOBuf::append_to或手动遍历 block
        written += n;
    }

    // 4. 构建 SGE
    urma_sge_t src_sge;
    src_sge.addr = reinterpret_cast<uint64_t>(_send_buf_addr) + buf_offset;
    src_sge.len = static_cast<uint32_t>(total_len);
    src_sge.tseg = _send_buf_tseg;

    urma_sge_t dst_sge;
    dst_sge.addr = _remote_recv_buf_addr + buf_offset;  // 镜像偏移
    dst_sge.len = static_cast<uint32_t>(total_len);
    dst_sge.tseg = _remote_buf_seg;

    // 5. 构建 imm_data
    UrmaWriteImmData imm;
    imm.io.opcode = URMA_IO_WRITE_IN_BAND;
    imm.io.buffer_offset = buf_offset / 128;  // 128B 粒度

    // 6. 构建 WR
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src.sge = &src_sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dst_sge;
    wr.rw.dst.num_sge = 1;
    wr.rw.notify_data = imm.data;
    wr.user_ctx = 0;  // WriteInline 不需要 SQ 窗口管理

    // 7. 提交
    urma_jfs_wr_t* bad_wr = nullptr;
    if (urma_post_jetty_send_wr(_resource->jetty, &wr, &bad_wr) != URMA_SUCCESS) {
        _send_buf_alloc->Release(buf_offset, total_len);
        errno = EAGAIN;
        return -1;
    }

    // 保存 send_buf 偏移以便 WRITE_IN_BAND_ACK 时释放
    // （通过 request_id 关联）

    return total_len;
}
```

### 7.3 WriteZeroCopy（大 IO）

```c++
// src/brpc/urma/urma_endpoint.cpp

ssize_t UrmaEndpoint::WriteZeroCopy(butil::IOBuf** from, size_t ndata) {
    // 1. 遍历 IOBuf，收集 block 地址信息
    struct BlockInfo {
        const void* addr;
        size_t size;
        urma_target_seg_t* tseg;
    };
    std::vector<BlockInfo> blocks;

    for (size_t i = 0; i < ndata; ++i) {
        auto* uio = reinterpret_cast<UrmaIOBuf*>(from[i]);
        // 遍历 IOBuf 的 block refs
        size_t ref_count = uio->_ref_num();
        for (size_t j = 0; j < ref_count; ++j) {
            butil::IOBuf::BlockRef const& r = uio->_ref_at(j);
            const void* start = /* block data pointer */;
            urma_target_seg_t* tseg = GetPoolSegFor(const_cast<void*>(start));
            if (!tseg) {
                uint64_t meta = uio->get_first_data_meta();
                tseg = reinterpret_cast<urma_target_seg_t*>(
                    static_cast<uintptr_t>(meta));
            }
            blocks.push_back({start, r.length, tseg});
        }
    }

    // 2. 构建控制消息
    uint32_t ctrl_msg_size = sizeof(UrmaCtrlMessage) +
        blocks.size() * sizeof(PageBufferInMessage);
    // 对齐到 128B
    ctrl_msg_size = (ctrl_msg_size + 127) & ~127;

    // 3. 分配 send_buf 给控制消息
    uint64_t buf_offset;
    if (!_send_buf_alloc->Allocate(ctrl_msg_size, buf_offset)) {
        errno = EAGAIN;
        return -1;
    }

    // 4. 填充控制消息
    auto* msg = reinterpret_cast<UrmaCtrlMessage*>(
        (char*)_send_buf_addr + buf_offset);
    msg->request_id = GenerateRequestId();
    msg->total_bytes = 0;
    msg->buffer_count = blocks.size();

    auto* bufs = reinterpret_cast<PageBufferInMessage*>(msg + 1);
    for (size_t i = 0; i < blocks.size(); ++i) {
        bufs[i].addr = reinterpret_cast<uint64_t>(blocks[i].addr);
        bufs[i].size = static_cast<uint32_t>(blocks[i].size);
        bufs[i].seg_token_id = GetSegTokenId(blocks[i].tseg);
        msg->total_bytes += blocks[i].size;
    }

    // 5. 引用计数管理：阻止 IOBuf block 在 POST_WRITE 前被释放
    //    brpc 的 IOBuf block 已有引用计数（_ref_num），这里需要
    //    通过 IOBuf 持有引用，保存到 UrmaSendContext
    auto* ctx = new UrmaSendContext();
    ctx->request_id = msg->request_id;
    ctx->send_buf_offset = buf_offset;
    ctx->send_buf_size = ctrl_msg_size;
    for (size_t i = 0; i < ndata; ++i) {
        ctx->saved_blocks.append(*from[i]);  // 持有 block 引用
    }

    // 6. 构建 SGE
    urma_sge_t src_sge;
    src_sge.addr = reinterpret_cast<uint64_t>(_send_buf_addr) + buf_offset;
    src_sge.len = ctrl_msg_size;
    src_sge.tseg = _send_buf_tseg;

    urma_sge_t dst_sge;
    dst_sge.addr = _remote_recv_buf_addr + buf_offset;
    dst_sge.len = ctrl_msg_size;
    dst_sge.tseg = _remote_buf_seg;

    UrmaWriteImmData imm;
    imm.io.opcode = URMA_IO_PRE_WRITE;
    imm.io.buffer_offset = buf_offset / 128;

    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src.sge = &src_sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dst_sge;
    wr.rw.dst.num_sge = 1;
    wr.rw.notify_data = imm.data;
    wr.user_ctx = reinterpret_cast<uint64_t>(ctx);

    urma_jfs_wr_t* bad_wr = nullptr;
    if (urma_post_jetty_send_wr(_resource->jetty, &wr, &bad_wr) != URMA_SUCCESS) {
        _send_buf_alloc->Release(buf_offset, ctrl_msg_size);
        delete ctx;
        errno = EAGAIN;
        return -1;
    }

    return msg->total_bytes;  // 返回实际数据量（不含控制消息）
}
```

### 7.4 Ring Buffer 分配器

```c++
// src/brpc/urma/urma_endpoint.h

// Simple SPSC ring buffer allocator for send_buf management.
// 128-byte granularity, lock-free single-producer.
class UrmaRingBuf {
public:
    explicit UrmaRingBuf(uint32_t capacity)
        : _capacity(capacity)
        , _mask(capacity - 1)  // capacity must be power of 2
        , _alloc_tail(0)
        , _reclaim_tail(0) {}

    bool Allocate(uint32_t size, uint64_t& offset) {
        uint32_t aligned = (size + 127) & ~127;  // 128B align
        uint32_t reclaimed = _reclaim_tail.load(butil::memory_order_acquire);
        if (_alloc_tail - reclaimed + aligned > _capacity) {
            return false;  // no space
        }
        offset = _alloc_tail & _mask;
        _alloc_tail += aligned;
        _alloc_tail_atomic.store(_alloc_tail, butil::memory_order_release);
        return true;
    }

    void Release(uint64_t offset, uint32_t size) {
        // 简化实现：标记释放，由后续 Allocate 扫描回收
        // 生产实现可用 bitmap 标记 + reclaim_tail 推进
        uint32_t aligned = (size + 127) & ~127;
        _reclaim_tail.fetch_add(aligned, butil::memory_order_release);
    }

    uint32_t Available() const {
        return _capacity - (_alloc_tail.load() -
                            _reclaim_tail.load(butil::memory_order_acquire));
    }

private:
    const uint32_t _capacity;
    const uint32_t _mask;
    uint32_t _alloc_tail;
    butil::atomic<uint32_t> _alloc_tail_atomic{0};
    butil::atomic<uint32_t> _reclaim_tail{0};
};
```

---

## 8. 接收路径设计

### 8.1 HandleCompletion 扩展

在 `HandleCompletion` 中新增 WRITE_IMM 完成处理：

```c++
// src/brpc/urma/urma_endpoint.cpp

ssize_t UrmaEndpoint::HandleCompletion(const urma_cr_t& cr) {
    // --- 现有 SEND/RECV 完成处理保持不变 ---

    // --- 新增：WRITE_IMM 接收完成处理 ---
    // 当对端用 WRITE_IMM 写入我们的 recv_buf 时，我们收到一个
    // URMA_CR_OPC_WRITE_WITH_IMM 完成事件
    if (cr.flag.bs.s_r == 1 &&
        cr.opcode == URMA_CR_OPC_WRITE_WITH_IMM) {
        return HandleWriteImmCompletion(cr);
    }

    // --- 新增：READ 发起方完成处理 ---
    // 当我们发起的 RDMA READ 完成时（s_r == 0, opcode == READ）
    if (cr.flag.bs.s_r == 0 &&
        cr.opcode == URMA_CR_OPC_READ) {
        return HandleReadCompletion(cr);
    }

    // ... 原有逻辑 ...
}
```

### 8.2 HandleWriteImmCompletion

处理对端 WRITE_IMM 写入本地 recv_buf 的完成事件：

```c++
ssize_t UrmaEndpoint::HandleWriteImmCompletion(const urma_cr_t& cr) {
    UrmaWriteImmData imm;
    imm.data = cr.imm_data;

    switch (imm.io.opcode) {
        case URMA_IO_WRITE_IN_BAND:
            return HandleWriteInBand(imm);

        case URMA_IO_PRE_WRITE:
            return HandlePreWrite(imm);

        case URMA_IO_POST_WRITE:
            return HandlePostWrite(imm);

        case URMA_IO_WRITE_IN_BAND_ACK:
            return HandleWriteInBandAck(imm);

        default:
            LOG(ERROR) << "Unknown UrmaIoOpcode: " << (int)imm.io.opcode;
            return -1;
    }
}
```

### 8.3 HandleWriteInBand（小 IO 接收）

```c++
ssize_t UrmaEndpoint::HandleWriteInBand(const UrmaWriteImmData& imm) {
    uint64_t offset = imm.io.buffer_offset * 128;

    // 数据已在 recv_buf[offset]，直接读取
    // 注意：WRITE_IMM 的数据直接落在 recv_buf 中，不需要 cutn
    char* data = (char*)_recv_buf_addr + offset;

    // 获取请求序列号用于接收槽管理
    uint32_t seq = _rx_consume_seq.fetch_add(1, butil::memory_order_relaxed);
    uint32_t idx = seq % RX_RING_SIZE;
    auto& slot = _rx_slots[idx];

    // 将数据从 recv_buf 拷贝到 _socket->_read_buf
    // （小 IO 数据量小，拷贝开销可接受）
    // 也可零拷贝：将 recv_buf 区域包装为 IOBuf block
    size_t data_len = /* 从 CQE completion_len 获取 */;
    _socket->_read_buf.append(data, data_len);

    slot.state.store(UrmaRxSlot::DATA_READY, butil::memory_order_release);

    // 发送 WRITE_IN_BAND_ACK 回报发送方
    ResponseCtrlMessage(URMA_IO_WRITE_IN_BAND_ACK, imm.io.buffer_offset);

    // 触发 InputMessenger 处理
    // （由 DispatchReceivedBytes 统一处理）

    return data_len;
}
```

### 8.4 HandlePreWrite（大 IO 接收，触发 READ）

```c++
ssize_t UrmaEndpoint::HandlePreWrite(const UrmaWriteImmData& imm) {
    uint64_t offset = imm.io.buffer_offset * 128;

    // 1. 从 recv_buf 读取控制消息
    auto* msg = reinterpret_cast<UrmaCtrlMessage*>(
        (char*)_recv_buf_addr + offset);

    uint32_t seq = _rx_consume_seq.fetch_add(1, butil::memory_order_relaxed);
    uint32_t idx = seq % RX_RING_SIZE;
    auto& slot = _rx_slots[idx];

    slot.request_id = msg->request_id;
    slot.write_imm = imm.data;
    slot.state.store(UrmaRxSlot::READING, butil::memory_order_release);
    slot.start_read_time_us = butil::cpuwide_time_us();

    // 2. 导入远端内存段（如果尚未导入）
    auto* bufs = reinterpret_cast<PageBufferInMessage*>(msg + 1);
    for (uint32_t i = 0; i < msg->buffer_count; ++i) {
        // 检查是否已导入该远端 segment
        // 如果远端使用全局 pool，segment 已在握手时导入
        // 否则需要动态导入
        if (!GetPoolSegFor(reinterpret_cast<void*>(bufs[i].addr))) {
            // 动态导入远端内存段
            ImportRemoteSegment(bufs[i]);
        }
    }

    // 3. 构建并提交 RDMA READ WR 链
    std::vector<urma_jfs_wr_t> wrs;
    std::vector<urma_sge_t> src_sges;
    std::vector<urma_sge_t> dst_sges;

    // 为每个远端 buffer 构建一个 READ WR
    // 目标地址：本地 IOBuf pool 中分配的 block
    size_t total_bytes = 0;
    for (uint32_t i = 0; i < msg->buffer_count; ++i) {
        urma_sge_t src_sge;
        src_sge.addr = bufs[i].addr;            // 远端地址（源）
        src_sge.len = bufs[i].size;
        src_sge.tseg = _remote_buf_seg;          // 远端 segment
        src_sges.push_back(src_sge);

        // 分配本地 block 作为 READ 目标
        void* local_buf = AllocateFromPool(bufs[i].size);
        urma_sge_t dst_sge;
        dst_sge.addr = reinterpret_cast<uint64_t>(local_buf);
        dst_sge.len = bufs[i].size;
        dst_sge.tseg = GetPoolSegFor(local_buf);  // 本地 segment
        dst_sges.push_back(dst_sge);

        urma_jfs_wr_t wr{};
        wr.opcode = URMA_OPC_READ;
        wr.flag.bs.complete_enable = 0;  // 仅最后一个 WR 完成时通知
        wr.tjetty = _resource->remote_jetty;
        wr.rw.src.sge = &src_sges[i];
        wr.rw.src.num_sge = 1;
        wr.rw.dst.sge = &dst_sges[i];
        wr.rw.dst.num_sge = 1;
        wrs.push_back(wr);

        total_bytes += bufs[i].size;
    }

    // 最后一个 WR 设置 complete_enable=1 并携带 user_ctx
    if (!wrs.empty()) {
        wrs.back().flag.bs.complete_enable = 1;
        wrs.back().user_ctx = EncodeReadCtx(seq, idx);
        // 链接 WR
        for (size_t i = 0; i + 1 < wrs.size(); ++i) {
            wrs[i].next = &wrs[i + 1];
        }
    }

    // 4. 提交 READ WR 链
    urma_jfs_wr_t* bad_wr = nullptr;
    urma_status_t rc = urma_post_jetty_send_wr(
        _resource->jetty, &wrs[0], &bad_wr);
    if (rc != URMA_SUCCESS) {
        slot.state.store(UrmaRxSlot::IDLE, butil::memory_order_release);
        LOG(ERROR) << "RDMA READ post failed: " << rc;
        return -1;
    }

    return 0;  // 数据尚未就绪，READ 完成后再处理
}
```

### 8.5 HandleReadCompletion（READ 完成）

```c++
ssize_t UrmaEndpoint::HandleReadCompletion(const urma_cr_t& cr) {
    // 解码 user_ctx 获取接收槽索引
    uint32_t idx = DecodeReadCtxIdx(cr.user_ctx);
    uint32_t seq = DecodeReadCtxSeq(cr.user_ctx);
    auto& slot = _rx_slots[idx];

    if (slot.state.load() != UrmaRxSlot::READING) {
        LOG(ERROR) << "Unexpected READ completion for slot " << idx;
        return -1;
    }

    // 数据已在本地 block 中（READ 目标地址）
    // 将 block 数据加入 _socket->_read_buf
    // 零拷贝：将 block 包装为 IOBuf block ref
    // ...
    // 此处需要保存 READ 的目标地址列表，以便组装 IOBuf

    slot.state.store(UrmaRxSlot::DATA_READY, butil::memory_order_release);

    // 发送 POST_WRITE 通知发送方释放资源
    ResponseCtrlMessage(URMA_IO_POST_WRITE, slot.write_imm_offset);

    // 触发 DispatchReceivedBytes
    return /* total_bytes */;
}
```

### 8.6 HandlePostWrite（发送方收到接收方的释放通知）

```c++
ssize_t UrmaEndpoint::HandlePostWrite(const UrmaWriteImmData& imm) {
    uint64_t offset = imm.io.buffer_offset * 128;

    // 释放 send_buf
    auto* msg = reinterpret_cast<UrmaCtrlMessage*>(
        (char*)_send_buf_addr + offset);
    uint32_t msg_size = /* 保存的分配大小 */;
    _send_buf_alloc->Release(offset, msg_size);

    // 释放 IOBuf block 引用（DecRef）
    // 通过 request_id 找到对应的 UrmaSendContext
    auto* ctx = FindAndRemoveSendContext(msg->request_id);
    if (ctx) {
        // IOBuf block 引用随 ctx->saved_blocks 析构自动释放
        delete ctx;
    }

    // 唤醒发送路径（可能有等待 send_buf 空间的写者）
    _socket->WakeAsEpollOut();

    return 0;
}
```

### 8.7 HandleWriteInBandAck

```c++
ssize_t UrmaEndpoint::HandleWriteInBandAck(const UrmaWriteImmData& imm) {
    uint64_t offset = imm.io.buffer_offset * 128;

    // 释放 send_buf
    uint32_t size = /* 保存的 WriteInline 分配大小 */;
    _send_buf_alloc->Release(offset, size);

    // 唤醒发送路径
    _socket->WakeAsEpollOut();

    return 0;
}
```

### 8.8 ResponseCtrlMessage（发送控制消息回报）

```c++
void UrmaEndpoint::ResponseCtrlMessage(UrmaIoOpcode opcode,
                                        uint16_t buffer_offset_units) {
    // 用 WRITE_IMM 发送一个空数据控制消息
    // src = 本地 recv_buf（或 send_buf 的一个小区域）
    // dst = 对端 send_buf（镜像偏移）
    // imm_data 携带 opcode + offset

    UrmaWriteImmData imm;
    imm.io.opcode = opcode;
    imm.io.buffer_offset = buffer_offset_units;

    // 构建一个最小 SGE（1 字节占位数据）
    urma_sge_t src_sge;
    src_sge.addr = reinterpret_cast<uint64_t>(_recv_buf_addr);
    src_sge.len = 1;  // 最小占位
    src_sge.tseg = _recv_buf_tseg;

    urma_sge_t dst_sge;
    dst_sge.addr = _remote_send_buf_addr;
    dst_sge.len = 1;
    dst_sge.tseg = _remote_buf_seg;

    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src.sge = &src_sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dst_sge;
    wr.rw.dst.num_sge = 1;
    wr.rw.notify_data = imm.data;
    wr.user_ctx = 0;

    urma_jfs_wr_t* bad_wr = nullptr;
    urma_post_jetty_send_wr(_resource->jetty, &wr, &bad_wr);
}
```

---

## 9. 完成队列处理

### 9.1 HandleCompletion 分发扩展

现有 `HandleCompletion` 通过 `cr.flag.bs.s_r` 区分 SEND（s_r=0）和 RECV（s_r=1）。
新增 WRITE_IMM 和 READ 完成处理：

```c++
ssize_t UrmaEndpoint::HandleCompletion(const urma_cr_t& cr) {
    // 错误处理（保持现有逻辑）
    if (cr.status != URMA_CR_SUCCESS) {
        return HandleErrorCompletion(cr);
    }

    if (cr.flag.bs.s_r == 0) {
        // === 发送方完成 ===

        // READ 完成（我方发起的 RDMA READ）
        if (cr.opcode == URMA_CR_OPC_READ) {
            return HandleReadCompletion(cr);
        }

        // WRITE_IMM 完成（我方发起的 WRITE_IMM 已写入对端）
        // 对于 WriteInline/WriteZeroCopy，WRITE_IMM 完成只表示
        // 数据已到达对端 recv_buf，真正完成需等待对端的 ACK
        // 可用于统计但不触发数据释放
        if (cr.opcode == URMA_CR_OPC_WRITE ||
            cr.opcode == URMA_CR_OPC_WRITE_IMM) {
            // WRITE_IMM 发送完成：数据已写入对端
            // 不释放 send_buf（等 POST_WRITE/ACK）
            // 可记录 TRACEPRINTF
            TRACEPRINTF("UrmaWriteImmComplete: user_ctx=%lu", cr.user_ctx);
            return 0;
        }

        // SEND 完成（现有逻辑）
        return HandleSendCompletion(cr);
    } else {
        // === 接收方完成 ===

        // WRITE_WITH_IMM 完成（对端写入我方 recv_buf）
        if (cr.opcode == URMA_CR_OPC_WRITE_WITH_IMM) {
            return HandleWriteImmCompletion(cr);
        }

        // RECV 完成（现有逻辑）
        return HandleRecvCompletion(cr);
    }
}
```

### 9.2 完成类型对照

| 完成类型 | s_r | cr.opcode | 含义 | 处理函数 |
|----------|-----|-----------|------|---------|
| SEND 完成 | 0 | SEND | 双边发送完成 | `HandleSendCompletion`（现有） |
| SEND_IMM 完成 | 0 | SEND_IMM | ACK 发送完成 | `HandleSendCompletion`（现有） |
| READ 完成 | 0 | READ | 我方 READ 拉取完成 | `HandleReadCompletion`（新增） |
| WRITE_IMM 完成 | 0 | WRITE_IMM | 我方 WRITE_IMM 写入完成 | 统计记录（新增） |
| RECV 完成 | 1 | SEND/SEND_IMM | 双边接收完成 | `HandleRecvCompletion`（现有） |
| WRITE_WITH_IMM 完成 | 1 | WRITE_WITH_IMM | 对端写入我方 recv_buf | `HandleWriteImmCompletion`（新增） |

---

## 10. 流控机制

### 10.1 单边操作的流控

单边操作**不使用**现有双边信用流控（`_remote_rq_window_size` / `_sq_window_size`），
而是基于 **send_buf 可用性**流控：

| 操作 | 流控条件 | 背压方式 |
|------|---------|---------|
| WRITE_IN_BAND | send_buf 有可用空间 | EAGAIN → KeepWrite 重试 |
| PRE_WRITE | send_buf 有可用空间 | EAGAIN → KeepWrite 重试 |
| READ | JFS 队列深度 | RNR 重试（硬件） |
| POST_WRITE/ACK | JFS 队列深度 | 无（小消息） |

### 10.2 可写性判断

```c++
bool UrmaEndpoint::IsWritable() const {
    if (FLAGS_urma_io_mode == 0) {
        // 双边模式：检查双窗口
        return _remote_rq_window_size.load() > 0 &&
               _sq_window_size.load() > 0;
    }
    // 单边模式：检查 send_buf 可用空间
    return _send_buf_alloc->Available() >= 128;  // 至少一个 128B 槽位
}
```

### 10.3 空闲 recv WR 投递

单边 WRITE_IMM 操作虽然不通过 JFR 接收数据（数据直接写入 recv_buf），但 URMA
硬件协议要求 JFR 中有可用的 recv WR 才能接受 WRITE_IMM 操作。因此需要投递空
recv WR：

```c++
// 投递空 recv WR（与 ubs-comm 的 PostEmptyRecvWorkerRecords 类似）
int UrmaEndpoint::PostEmptyRecvWr(uint32_t count) {
    urma_sge_t empty_sge{};  // addr=0, len=0
    urma_sg_t empty_sg{&empty_sge, 0};
    urma_jfr_wr_t wr{empty_sg, 0, nullptr};

    for (uint32_t i = 0; i < count; ++i) {
        urma_jfr_wr_t* bad = nullptr;
        if (urma_post_jfr_wr(_resource->jfr, &wr, &bad) != URMA_SUCCESS) {
            return -1;
        }
    }
    return 0;
}
```

在握手完成时投递一批空 recv WR（如 128 个），并在每次 WRITE_WITH_IMM 完成后
补充一个。

---

## 11. 内存管理

### 11.1 内存段关系

```
                  发送方                                  接收方
                  ──────                                  ──────

  ┌─────────────────────────────┐         ┌─────────────────────────────┐
  │ send_buf (注册段 A)          │         │ send_buf (注册段 C)          │
  │  · 控制消息缓冲              │         │  · 控制消息缓冲              │
  │  · WRITE_IN_BAND 数据缓冲    │         │  · WRITE_IN_BAND 数据缓冲    │
  ├─────────────────────────────┤         ├─────────────────────────────┤
  │ recv_buf (注册段 B)          │         │ recv_buf (注册段 D)          │
  │  · 接收对端 WRITE_IMM 数据   │         │  · 接收对端 WRITE_IMM 数据   │
  │  · 接收对端控制消息          │         │  · 接收对端控制消息          │
  └─────────────────────────────┘         └─────────────────────────────┘

  ┌─────────────────────────────┐         ┌─────────────────────────────┐
  │ IOBuf pool (全局注册段 E)    │         │ IOBuf pool (全局注册段 F)    │
  │  · block 数据缓冲            │         │  · block 数据缓冲            │
  │  · 被 READ 的源数据          │         │  · READ 目标缓冲             │
  └─────────────────────────────┘         └─────────────────────────────┘

  导入关系:
    发送方导入: 段 D（对端 recv_buf）、段 F（对端 pool，用于 READ）
    接收方导入: 段 A（对端 send_buf）、段 E（对端 pool，用于 READ）
```

### 11.2 关键区别

| 内存段 | 双边模式 | 单边模式 |
|--------|---------|---------|
| 全局 pool | recv WR 的 buffer 来源 | READ 目标 + PRE_WRITE 的 block 来源 |
| send_buf | 不存在 | 控制消息 + WRITE_IN_BAND 数据 |
| recv_buf | 不存在 | 接收 WRITE_IMM 数据 + 控制消息 |
| 对端段导入 | 仅 pool segment | pool + send_buf + recv_buf |

### 11.3 IOBuf block 引用管理

大 IO 零拷贝模式下，发送方的 IOBuf block 需要保持存活直到接收方完成 READ。
管理机制：

1. `WriteZeroCopy` 时：将 IOBuf append 到 `UrmaSendContext::saved_blocks`，
   持有 block 引用
2. `HandlePostWrite` 时：删除 `UrmaSendContext`，block 引用自动释放
3. brpc IOBuf block 已有引用计数（`IOBuf::Block::ref_count`），append 操作
   会递增引用计数，析构会递减

---

## 12. 与现有双边路径的共存

### 12.1 模式切换

通过 gflag `urma_io_mode` 在连接级别控制：

- **握手协商**：双方在 `ParsedHello` 中交换 `io_mode`，取较小值
  （如果一方仅支持 SEND，则该连接用双边模式）
- **运行时不可切换**：模式在握手时确定，连接生命周期内不变

### 12.2 PollCq 统一处理

`PollCq` 不需要修改——它调用 `HandleCompletion` 处理所有 CQE，
`HandleCompletion` 内部分发到对应的处理函数。双边和单边完成事件在同一
CQ 上混合到达。

### 12.3 DispatchReceivedBytes 统一处理

单边操作接收到的数据最终也写入 `_socket->_read_buf`，由
`DispatchReceivedBytes` → `InputMessenger::ProcessNewMessage` 统一处理，
与双边路径完全一致。上层（协议解析、用户回调）无需感知传输模式。

### 12.4 回退 TCP

现有 fallback TCP 机制保持不变。如果握手失败，回退到 TCP 传输，单边/双边
模式都不适用。

---

## 13. 实现步骤

### 第一阶段：基础框架（最小可用）

1. **新增数据结构**：`UrmaIoOpcode`、`UrmaWriteImmData`、`UrmaCtrlMessage`、
   `PageBufferInMessage`、`UrmaRxSlot`、`UrmaSendContext`、`UrmaRingBuf`
2. **握手扩展**：`ParsedHello` 新增 send_buf/recv_buf 字段，握手时分配和
   注册 buffer，交换地址，导入对端 segment
3. **空 recv WR 投递**：实现 `PostEmptyRecvWr`，握手完成后投递
4. **HandleCompletion 扩展**：新增 `URMA_CR_OPC_WRITE_WITH_IMM` 和
   `URMA_CR_OPC_READ` 完成处理分支

### 第二阶段：小 IO 路径（WRITE_IN_BAND）

5. **WriteInline 实现**：send_buf 分配 → 数据拷贝 → WRITE_IMM 提交
6. **HandleWriteInBand 实现**：从 recv_buf 读取数据 → 写入 read_buf →
   发送 WRITE_IN_BAND_ACK
7. **HandleWriteInBandAck 实现**：释放 send_buf → 唤醒发送路径
8. **测试**：小 IO echo 验证端到端正确性

### 第三阶段：大 IO 路径（PRE_WRITE + READ）

9. **WriteZeroCopy 实现**：收集 block 地址 → 构建控制消息 → IncRef blocks →
   WRITE_IMM(PRE_WRITE)
10. **HandlePreWrite 实现**：解析控制消息 → 导入远端段 → 分配本地 block →
    构建 READ WR 链 → 提交
11. **HandleReadCompletion 实现**：组装数据到 read_buf → 发送 POST_WRITE
12. **HandlePostWrite 实现**：释放 send_buf → DecRef blocks → 唤醒发送路径
13. **测试**：大 IO echo 验证零拷贝正确性

### 第四阶段：优化和完善

14. **send_buf ring buffer 优化**：实现 bitmap 标记回收，避免伪回收
15. **接收槽管理优化**：seq 索引 ring，处理乱序和溢出
16. **bvar 监控**：新增 `urma_write_latency`、`urma_read_latency`、
    `urma_send_buf_usage` 等 bvar
17. **TRACEPRINTF 打点**：在单边路径关键节点插入注解
18. **性能测试**：与双边模式对比延迟和吞吐
19. **gflag 默认值调优**：`urma_inline_threshold`、`urma_send_buf_size`

---

## 附录：文件改动清单

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `src/brpc/urma/urma_endpoint.h` | 修改 | 新增数据结构、成员变量、方法声明 |
| `src/brpc/urma/urma_endpoint.cpp` | 修改 | 新增 WriteInline/WriteZeroCopy/HandlePreWrite 等方法 |
| `src/brpc/urma/urma_handshake.h` | 修改 | ParsedHello 新增字段 |
| `src/brpc/urma/urma_handshake.cpp` | 修改 | v2/v3 握手序列化/反序列化新增字段 |
| `src/brpc/urma/urma_handshake.proto` | 修改 | v3 protobuf 新增字段 |
| `src/brpc/urma/urma_helper.cpp` | 修改 | 新增 send_buf/recv_buf 内存注册辅助函数 |
| `src/brpc/urma/urma_helper.h` | 修改 | 新增 gflag 声明 |
| `test/brpc_urma_unittest.cpp` | 修改 | 新增单边操作单元测试 |
| `docs/cn/urma.md` | 修改 | 文档更新 |
| `docs/cn/urma_tracing.md` | 修改 | 打点方案更新 |
