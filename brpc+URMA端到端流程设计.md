# brpc + URMA 端到端流程设计

## 代码仓路径与版本信息

| 项 | 值 |
|---|---|
| **本地路径** | `D:\kunpeng\bpc_urma\brpc` |
| **远程仓库** | `baize: https://szv-open.codehub.huawei.com/OpenBaize/CPlusPlus/bRPC.git` |
| | `origin: https://github.com/LinQuickDev/brpc.git` |
| **当前分支** | `local_urma_transport` |
| **最新 commit** | `23848076` — Stage4: dual-jetty 下移除 write jetty 冗余 mutex |
| **commit hash** | `23848076...`（工作区另有 segfault 修复 + Stage3 CQE 合并计划的未提交改动） |
| **URMA 源码目录** | `src/brpc/urma/` |
| **核心文件** | `urma_endpoint.h` / `urma_endpoint.cpp` / `urma_one_sided.h` / `urma_helper.cpp` / `urma_transport.cpp` / `urma_handshake.cpp` |
| **构建方式** | Bazel: `bazel build -c opt //example:ub_test_server //example:ub_test_client --define BRPC_WITH_URMA=true` |
| **测试环境** | ARM64 servers (141.61.17.202 server / 141.61.17.204 client), RDMA device=rocep42s0f0 (bonding) / udmac0d1e2 (物理 UDMA, 非 bonding) |
| **性能基准** | Stage4 矩阵（2026-09-27, io_mode=2, dual-jetty + zerocopy_read, inline_threshold=16384, SQ=1024, read_jetty_sq=1024, send/recv_buf=8MB, chunk=2MB） |

---

## 一、架构总览

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        User Application                             │
│              Channel::CallMethod / Server::Start                    │
├─────────────────────────────────────────────────────────────────────┤
│                     brpc Core (src/brpc/)                           │
│   Channel ──► Socket ──► Transport ──► CutFromIOBufList            │
│                                  │                                   │
│   InputMessenger ◄─ DispatchReceivedBytes ◄─ PollCq ◄─ HandleCompletion │
├─────────────────────────────────────────────────────────────────────┤
│                  URMA Transport (src/brpc/urma/)                     │
│   UrmaTransport ──► UrmaEndpoint                                    │
│       ├─ io_mode=0: CutFromIOBufList_Send (双边 SEND/RECV、FC URMA_OPC_SEND_IMM)          │
│       ├─ io_mode=1: WriteInline (单边 WRITE_IN_BAND)                │
│       └─ io_mode=2: WriteInline / WriteInlineChunked (混合模式)     │
│              └─ urma_use_zerocopy_read=true: WriteZeroCopy (PRE_WRITE+READ 拉模式) │
│                                                                      │
│   UrmaRingBuf (bitmap 分配器 + _alloc_cursor 游标)  PendingSendSlot (无锁 MPSC slot) │
│   UrmaSendContext (对象池)     UrmaRxSlot (接收 slot, 支持分批 READ) │
│   UrmaReasmCtx (大包重组)      UrmaMessageHead (24B 控制头)         │
│   Dual-Jetty: jetty (WRITE_IMM) + jetty_read (READ) 物理隔离        │
│   _resource_refcnt + _shutdown_butex: PollCq/DeallocateResources 竞态保护 │
├─────────────────────────────────────────────────────────────────────┤
│                  URMA Kernel API (liburma)                           │
│   urma_post_jetty_send_wr  urma_poll_jfc  urma_import_seg/jetty     │
├─────────────────────────────────────────────────────────────────────┤
│                  Hardware (RoCE / bonding_dev_0)                     │
│   JFS (Send Queue)  JFR (Recv Queue)  JFC (Completion Queue)        │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心类关系

```
brpc::Channel
  └─ _options.socket_mode = SOCKET_MODE_URMA
      └─ TransportFactory::CreateTransport() → new UrmaTransport()

brpc::Socket (每个连接一个)
  ├─ _transport: UrmaTransport
  │    ├─ _urma_ep: UrmaEndpoint*        // URMA 端点
  │    ├─ _tcp_transport: TcpTransport   // 握手阶段 fallback
  │    └─ _urma_state: URMA_OFF/ON/UNKNOWN
  ├─ _read_buf: butil::IOBuf            // 接收数据暂存
  └─ user(): SocketUser* → InputMessenger

brpc::urma::UrmaEndpoint
  ├─ _socket: Socket*                   // 反向引用（不拥有）
  ├─ _resource: UrmaResource*           // JFC/JFR/JFS/JFCE 句柄
  │    ├─ jfc: urma_jfc_t*              // Completion Queue
  │    ├─ jfr: urma_jfr_t*              // Receive Queue
  │    ├─ jetty: urma_jetty_t*          // Send Queue (WRITE_IMM/SEND)
  │    ├─ jetty_read: urma_jetty_t*     // [dual-jetty] READ 专用 Send Queue
  │    ├─ jfce: urma_jfce_t*           // CQ 事件 fd
  │    ├─ remote_jetty: urma_target_jetty_t*   // 对端 write Jetty
  │    ├─ remote_jetty_read: urma_target_jetty_t* // [dual-jetty] 对端 read Jetty
  │    ├─ remote_seg: urma_target_seg_t*       // 对端 segment
  │    └─ remote_recv_buf_seg: urma_target_seg_t*  // 对端 recv_buf segment
  ├─ _resource_refcnt: atomic<int32_t>  // [segfault-fix] 0=空闲/>0=PollCq 持有/INT32_MIN=shutdown
  ├─ _shutdown_butex: atomic<int>*      // [segfault-fix] DeallocateResources 等待 PollCq 退出
  ├─ _one_sided_buf: void*             // mmap 内存（recv_buf + send_buf）
  │    ├─ [0, _recv_buf_capacity):       recv_buf
  │    └─ [_recv_buf_capacity, total):   send_buf
  ├─ _send_buf_alloc: UrmaRingBuf*      // send_buf bitmap 分配器（含 _alloc_cursor 游标）
  ├─ _send_buf_tseg: urma_target_seg_t* // send_buf MR token
  ├─ _pending_sends[4096]: PendingSendSlot  // 无锁 MPSC slot 数组（_insert_hint 游标）
  ├─ _rx_slots[128]: UrmaRxSlot         // PRE_WRITE+READ 接收 slot（含分批 READ 状态）
  ├─ _reasm_ctxs: unordered_map<rid, UrmaReasmCtx*>  // chunk 重组
  ├─ _io_mode: uint8_t                  // 0=SEND / 1=WRITE / 2=HYBRID
  ├─ _sq_window_size: atomic<uint16_t>  // write jetty SQ 可用窗口
  ├─ _sq_window_size_read: atomic<uint16_t> // [dual-jetty] read jetty SQ 可用窗口
  ├─ _remote_rq_window_size: atomic<uint16_t>  // 对端 RQ credit
  ├─ _pending_retry_count: atomic<uint32_t> // [Stage2] RetryPendingReads 空 guard 计数
  ├─ _zerocopy_post_mutex: butil::Mutex // [Stage4] dual-jetty 时跳过的 post 串行锁
  └─ _state: atomic<State>              // UNINIT→CONNECTING→ESTABLISHED

brpc::urma::UrmaRingBuf
  ├─ _mutex: pthread_spinlock_t         // MPSC 自旋锁
  ├─ _bitmap: vector<uint8_t>           // 1KB 粒度 bitmap
  ├─ _alloc_cursor: uint32_t            // [Stage2] first-fit 搜索起始游标
  └─ Allocate(): 从 _alloc_cursor 起 first-fit 搜索
```

---

## 二、连接建立与握手

### 2.1 握手时序图

```
Client                                          Server
  │                                               │
  │── TCP connect ──────────────────────────────►│
  │                                               │
  │  ProcessHandshakeAtClient                     │  ProcessHandshakeAtServer
  │  ├─ AllocateResources()                       │  ├─ ReadFromFd("URM3")
  │  │  ├─ 创建 JFCE/JFC/JFR/Jetty (write)       │  ├─ AllocateResources()
  │  │  └─ [dual-jetty] 创建 jetty_read           │  │  ├─ 创建 JFCE/JFC/JFR/Jetty
  │  │     (depth = read_jetty_sq_size)           │  │  └─ [dual-jetty] 创建 jetty_read
  │  ├─ CreateClientHandshake(v2/v3)              │  ├─ ReceiveAndParseRemoteHello()
  │  │  └─ v3 protobuf: 含 read_jetty_id/        │  ├─ ApplyRemoteHello()
  │  │     has_read_jetty 字段                    │  │  └─ import peer recv_buf seg
  │  ├─ SendLocalHello() ──► TCP ──────────────►│  ├─ PostRecv(rq_size)
  │  │  (含 seg_eid/uasid/va/len/io_mode/        │  ├─ [dual-jetty] SendLocalHello
  │  │   send_buf_size/recv_buf_size/            │  │  携带 read_jetty_id
  │  │   read_jetty_id/has_read_jetty)           │  ├─ 等待 ACK
  │  ├─ ReceiveAndParseRemoteHello() ◄──────────│  └─ _state = ESTABLISHED
  │  ├─ ApplyRemoteHello()                       │
  │  │  ├─ 协商 io_mode = min(local, remote)     │
  │  │  ├─ import peer recv_buf seg              │
  │  │  └─ import peer jetty + seg               │
  │  ├─ ImportPeer()                             │
  │  │  └─ [dual-jetty] import peer jetty_read   │
  │  │     (对端 has_read_jetty=true 时)         │
  │  ├─ WriteToFd(ACK) ────────────────────────►│
  │  └─ _state = ESTABLISHED                     │
  │                                               │
  │◄════════ URMA 数据通道建立 ═══════════════►│
```

**dual-jetty 协商要点**（`urma_endpoint.cpp:398-401, 1059-1065`）：
- 仅当 `FLAGS_urma_dual_jetty=true` 且本地 `jetty_read` 创建成功时，才在握手消息中置 `has_read_jetty=true` 并携带 `read_jetty_id`
- 对端 `ImportPeer` 时若 `has_read_jetty=true` 才 `urma_import_jetty(remote_jetty_read)`，否则 READ 回退到 write jetty
- `jetty_read` 与 `jetty` 共享同一 JFC/JFR，仅 JFS 深度不同（`read_jetty_sq_size` 默认 1024）
- `UrmaResource::~UrmaResource` 析构顺序：先 unimport/destroy `jetty_read`，再处理 `jetty`

### 2.2 io_mode 协商

```cpp
// urma_endpoint.cpp ApplyRemoteHello
const uint8_t negotiated_mode = std::min(_io_mode, remote.io_mode);
if (negotiated_mode != 0 && remote.has_one_sided &&
    remote.recv_buf_size > 0 && remote.send_buf_size > 0) {
    _io_mode = negotiated_mode;  // 取双方最小值
    _remote_recv_buf_va = remote.recv_buf_va;  // 对端 recv_buf 地址
    // import 对端 recv_buf segment，用于 WRITE_IMM
} else {
    _io_mode = 0;  // 降级到双边 SEND/RECV
}
```

**io_mode 含义**：

| io_mode | 名称 | 发送路径 | 适用场景 |
|---------|------|---------|---------|
| 0 | SEND_ONLY | CutFromIOBufList_Send (URMA_OPC_SEND) | 兼容模式，需对端 PostRecv |
| 1 | WRITE_ONLY | WriteInline (WRITE_IN_BAND) | 所有尺寸都走单边 WRITE |
| 2 | HYBRID | WriteInline (≤`urma_inline_threshold`) / WriteInlineChunked (>threshold) | 小包低延迟+大包分块 |
| 2 + zerocopy_read | HYBRID+RNDV | WriteInline (小包) / WriteZeroCopy (大包, PRE_WRITE+READ) | 大包零拷贝拉模式 |

**关键 flag 联动**：
- `--urma_inline_threshold`（默认 **16384**）：io_mode=2 小包/大包分割阈值
- `--urma_use_zerocopy_read`（默认 **false**）：true 时 io_mode=2 大包改走 WriteZeroCopy
- `--urma_dual_jetty`（默认 **false**）：true 时创建 READ 专用 jetty，消除 READ+WRITE_IMM 并发 status=8
- `--urma_read_jetty_sq_size`（默认 **1024**）：dual-jetty 时 read jetty 的 SQ 深度
- `--urma_client_handshake_version`（默认 **2**）：2=binary, 3=protobuf（v3 才携带 read_jetty_id）

---

## 三、数据发送流程

### 3.1 brpc → URMA 调用链

```
Channel::CallMethod(channel.cpp:474)
  └─ cntl->IssueRPC(channel.cpp:659)
       └─ Socket::Write(socket.cpp)  // 获取或创建 Socket
            └─ Socket::KeepWrite(socket.cpp:1805)
                 └─ Socket::DoWrite(socket.cpp:1890)
                      ├─ 收集 WriteRequest 链表 → data_list[DATA_LIST_MAX]
                      └─ _transport->CutFromIOBufList(data_list, ndata)
                           └─ UrmaTransport::CutFromIOBufList(urma_transport.cpp:99)
                                ├─ if _urma_state == URMA_OFF:
                                │    └─ _tcp_transport->CutFromIOBufList()  // TCP fallback
                                └─ if _urma_state == URMA_ON:
                                     └─ _urma_ep->CutFromIOBufList(buf, ndata)
                                          └─ UrmaEndpoint::CutFromIOBufList(urma_endpoint.cpp:1024)
                                               ├─ io_mode==0: CutFromIOBufList_Send()
                                               ├─ io_mode==1: WriteInline()
                                               └─ io_mode==2:
                                                    ├─ total ≤ urma_inline_threshold (默认 2048): WriteInline()
                                                    └─ total > urma_inline_threshold: WriteInlineChunked()
```

**关键保证**：brpc Socket 的 `_write_head.exchange(req)` 原子操作保证同一 Socket 任意时刻只有一个 KeepWrite bthread 执行写入（`socket.cpp:1704`），因此 UrmaEndpoint 发送路径无需加锁。

**[Stage4] dual-jetty 无锁 post**：`_zerocopy_post_mutex` 原为 bonding 设备 READ+WRITE_IMM 并发导致的 status=8 而加。dual-jetty 激活（`jetty_read && remote_jetty_read` 均非空）时，READ 在 `jetty_read`、WRITE_IMM 在 `jetty`，物理隔离消除并发冲突，所有 `urma_post_jetty_send_wr` 调用点跳过该锁：
```cpp
// 5 处调用点统一模式（WriteInline / WriteZeroCopy / chunked KeepWrite / ResponseCtrlMessage / SendImm）
const bool need_lock =
    FLAGS_urma_use_zerocopy_read &&
    !(_resource->jetty_read && _resource->remote_jetty_read);
std::unique_lock<butil::Mutex> lock(_zerocopy_post_mutex, std::defer_lock);
if (need_lock) lock.lock();
urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
// PostReadBatch 额外用 use_read_jetty 选择 jetty_read（无锁）或 jetty（加锁）
```
KeepWrite 主路径（`CutFromIOBufList_Send` 的 SEND WR）单写者模型，本身无锁。

### 3.2 io_mode=0：双边 SEND/RECV

```
CutFromIOBufList_Send(urma_endpoint.cpp:1049)
```

**数据流**：
```
IOBuf data_list[] ──► cut_into_sglist() ──► _sbuf[_sq_current] (IOBuf 备份)
                                    │
                                    └──► urma_sge_t sglist[] (SGE 列表)
                                              │
                                              ▼
                                    urma_post_jetty_send_wr()
                                    opcode = URMA_OPC_SEND
                                    tjetty = remote_jetty
                                    user_ctx = _sq_current + 1
```

**流控**：
- 发送前检查 `_remote_rq_window_size > 0 && _sq_window_size > 0`
- post 前 `fetch_sub(1)` 预扣双窗口
- 对端通过 `SEND_WITH_IMM` (URMA_OPC_SEND_IMM)返还 RQ credit（imm_data = repost 数量）
- TX completion 时恢复 `_sq_window_size`

**特点**：
- 数据零拷贝：IOBuf block 地址直接作为 SGE，无 memcpy
- 需要 `_sbuf[_sq_current]` 保存 IOBuf 引用（直到 TX completion）
- 每次发送消耗对端 1 个 RQ slot

### 3.3 io_mode=1/2 小包：WriteInline (WRITE_IN_BAND)

```
WriteInline(urma_endpoint.cpp:1420 附近)
```

**数据流**：
```
IOBuf data_list[]
    │
    ├──► 计算总大小 total
    │
    ├──► UrmaRingBuf::Allocate(24 + total)  ──► send_buf[offset]
    │                                         (bitmap first-fit, 1KB 粒度)
    │
    ├──► 构造 UrmaMessageHead (24B)
    │    magic=0x55524D31, flags=(1<<16)|0  // total_chunks=1, chunk_idx=0
    │    request_id = _one_sided_seq.fetch_add(1)
    │
    ├──► IOBuf::copy_to()  ──► send_buf[offset + 24]  (memcpy)
    │
    ├──► 构造 WRITE_IMM WR
    │    opcode = URMA_OPC_WRITE_IMM
    │    src = local send_buf[offset, 24+total]
    │    dst = peer recv_buf[offset, 24+total]  // 相同 offset
    │    imm = {opcode=WRITE_IN_BAND, buffer_offset=offset/1024}
    │    user_ctx = EncodeUserCtx(CTRL_DATA_REQUEST, request_id)
    │
    ├──► InsertPendingSend(key=(rid<<20)|0, ctx)  ──► _pending_sends[slot]
    │    (CAS 0→key 抢占空闲 slot)
    │
    ├──► _sq_window_size.fetch_sub(1)
    │
    └──► urma_post_jetty_send_wr()
```

**与 io_mode=0 的关键差异**：
- 数据 **memcpy 到 send_buf**（io_mode=0 零拷贝 SGE）
- 写入对端 recv_buf 的相同 offset（双方 buffer 布局对称）
- 需要分配/释放 send_buf slot（UrmaRingBuf）
- 需要注册 UrmaSendContext 等待 ACK
- 对端不需要预 PostRecv（WRITE_IMM 自带接收通知）

### 3.4 io_mode=2 大包：WriteInlineChunked

```
WriteInlineChunked(urma_endpoint.cpp:1700 附近)
```

**数据流**：
```
IOBuf from[0] (如 8MB)
    │
    ├──► 首次调用时初始化:
    │    _chunked_total_chunks = ceil(8MB / 127KB) = 66
    │    _chunked_chunk_idx = 0
    │    _chunked_request_id = _one_sided_seq.fetch_add(1)
    │
    └──► 循环发送 chunk (受 SQ 窗口限制，可能跨多次 KeepWrite 调用):
         │
         ├── chunk_payload = min(remaining, 127KB)
         ├── UrmaRingBuf::Allocate(24 + chunk_payload) → send_buf[offset]
         ├── 构造 UrmaMessageHead:
         │   flags = (total_chunks << 16) | chunk_idx
         ├── IOBuf::cutn() → send_buf[offset + 24]  // 从 IOBuf 切出数据
         ├── 构造 WRITE_IMM WR (同 WriteInline)
         ├── InsertPendingSend(key=(rid<<20)|chunk_idx, ctx)
         ├── _sq_window_size.fetch_sub(1)
         ├── urma_post_jetty_send_wr()
         ├── total_sent += chunk_payload
         └── ++_chunked_chunk_idx
         
         (SQ 窗口耗尽时返回 EAGAIN，brpc KeepWrite 稍后重试)
         
         所有 chunk 发送完毕 → _chunked_total_chunks = 0
```

**跨 KeepWrite 重入**：
- `_chunked_total_chunks != 0` 表示上一条消息的 chunk 尚未发完
- 下次 KeepWrite 调用时从 `_chunked_chunk_idx` 继续
- `_chunked_request_id` 保持不变，确保所有 chunk 用同一 request_id

### 3.5 WriteZeroCopy (PRE_WRITE + RDMA READ) — [zerocopy_read] 激活路径

```
WriteZeroCopy(urma_endpoint.cpp:1620 附近)
```

> **激活条件**：`FLAGS_urma_use_zerocopy_read=true`（默认 false）时，io_mode=2 大包改走此路径。否则走 WriteInlineChunked 推模式。
> **dual-jetty 联动**：zerocopy_read 通常配合 `--urma_dual_jetty=true` 使用，READ WR 走 `jetty_read`，与 WRITE_IMM 物理隔离。

**数据流**：
```
IOBuf data_list[] (8MB)
    │
    ├──► 枚举所有 IOBuf backing_block
    │    每个超大的 block 按 max_sge_len 切分
    │    → blocks[{addr, size, tseg}, ...]
    │
    ├──► UrmaRingBuf::Allocate(24 + N * sizeof(PageBufferInMessage))
    │    → send_buf[offset] (仅控制消息，不含数据)
    │
    ├──► 填充控制消息:
    │    head->data_count = N (block 数量)
    │    entries[i] = {block_addr, block_size, token_id}
    │
    ├──► ctx->saved_blocks.append(*from[i])  // 保存 IOBuf 引用
    │    (保持 block 存活直到对端 READ 完成)
    │
    ├──► InsertPendingSend(key=request_id, ctx)
    │
    ├──► 构造 WRITE_IMM WR:
    │    imm = {opcode=PRE_WRITE, buffer_offset=offset/1024}
    │    src = send_buf[offset, 24 + N*16]  (仅控制消息)
    │    dst = peer recv_buf[offset]
    │
    └──► urma_post_jetty_send_wr()
         │
         ▼
    对端收到 PRE_WRITE → 分配 UrmaRxSlot → PostReadBatch()
    对端 RDMA READ 拉取数据 → 全部完成后 POST_WRITE ACK
    本端收到 POST_WRITE → Release send_buf + 释放 saved_blocks
```

**与 Chunked 的对比**：

| 维度 | WriteInlineChunked | WriteZeroCopy |
|------|-------------------|----------------|
| 数据传输 | N 次 WRITE_IMM（推送） | 1 次 PRE_WRITE + N 次 RDMA READ（拉取） |
| 内存拷贝 | 每 chunk memcpy 到 send_buf | 零拷贝（READ 直接读 IOBuf block） |
| SQ 消耗 | N 次 WR (write jetty) | 1 WR (write jetty) + N WR (read jetty, dual-jetty 时独立 SQ) |
| send_buf 占用 | N × chunk_size（实测 8MB 消息需 8MB send_buf） | 仅控制消息大小（~2KB） |
| 对端缓冲 | recv_buf（相同 offset） | 本地 pool buffer（动态分配） |
| status=8 风险 | 无（纯 WRITE_IMM） | 有（READ+WRITE_IMM 并发，需 dual-jetty 消除） |

---

## 四、数据接收流程

### 4.1 PollCq 线程模型与资源生命周期

```
┌──────────────────────────────────────────────────────────────┐
│  Event mode (FLAGS_urma_use_polling=false, 默认)              │
│                                                                │
│  EventDispatcher (epoll)                                       │
│    └─ JFCE fd 可读                                             │
│         └─ PollCq(Socket*)  [urma_endpoint.cpp:3120]          │
│              ├─ [segfault-fix] _resource_refcnt.fetch_add(1)  │
│              │   prev < 0 (shutdown 中) → 回滚 return         │
│              ├─ MakeScopeGuard(ReleaseResourceRef) RAII       │
│              ├─ WaitCqEvent() → epoll_wait(jfce_fd)           │
│              └─ drain_cq():                                    │
│                   urma_poll_jfc(jfc, 32, crs)                  │
│                   for each cr: HandleCompletion(cr)            │
│              └─ urma_ack_jfc() + ReqNotifyCq() (rearm)        │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  Busy-poll mode (FLAGS_urma_use_polling=true)                  │
│                                                                │
│  PollerGroup (per bthread tag)                                 │
│    └─ Poller bthread × N                                       │
│         loop:                                                  │
│           dequeue CqSidOp from MPSCQueue                       │
│           for each cq_sid:                                     │
│             Socket::Address(sid, &s) → PollCq(s.get())        │
│               └─ drain_cq() 直接轮询（无 WaitCqEvent）        │
│           bthread_yield() or spin                              │
└──────────────────────────────────────────────────────────────┘
```

**[segfault-fix] PollCq 与 DeallocateResources 竞态保护**：

竞态场景（修复前 ~70% 退出 segfault）：
```
析构线程                              Poller bthread
====================================  =================
DeallocateResources()
  PollerRemoveCqSid() (MPSC 入队)     for (sid : cq_sids) {
  s->SetFailed() (CQ socket)            Address(sid, &s) 成功
  delete _resource  ←─ race ──→        PollCq(s.get())
                                          urma_poll_jfc(ep->_resource->jfc)
                                          → USE-AFTER-FREE → SEGFAULT
```

修复方案：`_resource_refcnt` 原子变量（`urma_endpoint.h:316`）+ `_shutdown_butex`（`urma_endpoint.h:318`）：
- `0` = 空闲，无 PollCq 在内
- `>0` = PollCq 持有计数
- `INT32_MIN` = shutdown sentinel（DeallocateResources 设置）

```
PollCq 入口 (urma_endpoint.cpp:3125):
  prev = _resource_refcnt.fetch_add(1, acq_rel)
  if (prev < 0):  // shutdown 中
    _resource_refcnt.fetch_sub(1)  // 回滚
    return
  auto guard = MakeScopeGuard(ReleaseResourceRef)  // RAII 保证所有 return 释放

DeallocateResources (urma_endpoint.cpp:755):
  while (!_resource_refcnt.CAS(expected=0 → INT32_MIN)):
    if (expected < 0) break  // 已 shutdown
    // expected > 0: PollCq 在内, butex_wait 等待
    _shutdown_butex.store(0)
    bthread::butex_wait(_shutdown_butex, 0, &timeout)
  delete _resource  // 此时无 PollCq 访问
  _resource = nullptr
  _resource_refcnt.store(0)  // 支持 Reset 后重新分配

ReleaseResourceRef (urma_endpoint.cpp:777):
  prev = _resource_refcnt.fetch_sub(1, acq_rel)
  if (prev == INT32_MIN + 1):  // 最后一个持有者
    _shutdown_butex.store(1, release)
    bthread::butex_wake(_shutdown_butex)  // 唤醒 DeallocateResources
```

### 4.2 HandleCompletion 分发逻辑

```
HandleCompletion(cr)  [urma_endpoint.cpp:2240 附近]
  │
  ├── cr.s_r == 0 (TX completion, 发送完成)
  │    ├── user_ctx == READ_DATA_REQUEST → HandleReadCompletion()
  │    │    └─ [dual-jetty] 回收到 _sq_window_size_read 否则 _sq_window_size
  │    │       (每个 READ WR complete_enable=1, 每 CQE 回收 1 个 slot)
  │    ├── user_ctx == CTRL_DATA_REQUEST/RESPONSE → 恢复 SQ 窗口
  │    │    └─ _sq_window_size CAS +1 (complete_enable=1, 每 WR 1 CQE)
  │    └── user_ctx == 0 (pure ACK) → 恢复 _sq_imm_window_size
  │
  └── cr.s_r == 1 (RX completion, 接收完成)
       ├── cr.opcode == URMA_CR_OPC_WRITE_WITH_IMM
       │    └── 解析 imm_data → UrmaWriteImmData
       │         ├── imm.opcode == WRITE_IN_BAND / PRE_WRITE
       │         │    → HandleWriteImmCompletion()
       │         ├── imm.opcode == WRITE_IN_BAND_ACK
       │         │    → HandleWriteInBandAck()
       │         └── imm.opcode == POST_WRITE
       │              → HandlePostWrite()
       │
       ├── cr.opcode == URMA_CR_OPC_SEND_WITH_IMM && imm > 0
       │    └── 恢复 _remote_rq_window_size (credit return)
       │
       └── 普通 SEND 接收 (io_mode=0)
            └─ _rbuf[_rq_received].cutn(&_socket->_read_buf, len)
               PostRecv(1) + SendAck(1)
```

**complete_enable 策略**（`urma_endpoint.cpp:2565-2569`）：所有 WR（SEND/WRITE_IMM/READ）统一设 `complete_enable=1`，即每个 WR 产生 1 个 CQE。注释说明 bonding provider 要求每个 WR 都设 `complete_enable=1`，否则中间 WR 可能被静默丢弃或触发 TP error（status=8）。UBS 在 bonding 设备上也采用相同策略。**未实施非 bonding 设备 CQE 合并优化**（见 §10.8.3 待办）。

### 4.3 接收时序图 — io_mode=0 (双边 SEND/RECV)

```
Sender                                    Receiver (PollCq)
  │                                          │
  │── URMA_OPC_SEND ──────────────────────►│
  │   (payload data)                         │
  │                                          ├─ HandleCompletion (RX)
  │                                          │  └─ _rbuf[slot].cutn(&_read_buf)
  │                                          ├─ PostRecv(1)  // 补充 RQ
  │                                          ├─ SendAck(1)   // 累积 ACK
  │                                          └─ DispatchReceivedBytes()
  │                                               └─ InputMessenger::ProcessNewMessage()
  │                                                    └─ 用户 handler
  │                                          │
  │◄── SEND_WITH_IMM (credit) ─────────────│
  │   imm_data = repost_count                │
  │                                          │
  ├─ HandleCompletion (TX)                   │
  │  └─ _sq_window_size.fetch_add(1)         │
  │  └─ _remote_rq_window_size.fetch_add(n)  │
  └─ _sbuf[slot].clear()  // 释放 IOBuf 引用 │
```

### 4.4 接收时序图 — io_mode=1/2 (WRITE_IN_BAND)

#### 4.4.1 小包（单 chunk）

```
Sender                                    Receiver (PollCq)
  │                                          │
  │  1. UrmaRingBuf::Allocate()              │
  │  2. copy_to send_buf[offset]             │
  │  3. InsertPendingSend(key)               │
  │                                          │
  │── WRITE_IMM ──────────────────────────►│
  │   src=send_buf[offset]                   │
  │   dst=recv_buf[offset]                   │
  │   imm={WRITE_IN_BAND, offset/1024}       │
  │                                          │
  │                          HandleWriteImmCompletion()
  │                          ├─ 读 recv_buf[offset] → UrmaMessageHead
  │                          ├─ total_chunks=1:
  │                          │   recv_buf[offset+24] → append to _read_buf
  │                          ├─ ResponseCtrlMessage(WRITE_IN_BAND_ACK)
  │                          │   写 head 到 recv_buf[offset]
  │                          │   WRITE_IMM → peer send_buf[offset]
  │                          └─ PostEmptyRecvWr(1)
  │                               │
  │                               ▼
  │                          DispatchReceivedBytes()
  │                          └─ InputMessenger::ProcessNewMessage()
  │                               └─ 用户 handler
  │                                          │
  │◄── WRITE_IMM (ACK) ────────────────────│
  │   src=recv_buf[offset]                   │
  │   dst=send_buf[offset]                   │
  │   imm={WRITE_IN_BAND_ACK, offset/1024}   │
  │                                          │
  ├─ HandleWriteInBandAck()                  │
  │  ├─ 读 send_buf[offset] → head           │
  │  ├─ FindAndRemoveSendContext(key)        │
  │  ├─ Release(send_buf offset)             │
  │  ├─ return_object(ctx)                   │
  │  └─ WakeAsEpollOut()  // 唤醒发送端      │
  └─                                         │
```

#### 4.4.2 大包（多 chunk）

```
Sender                                    Receiver (PollCq)
  │                                          │
  │  chunk 0: WRITE_IMM ──────────────────►│
  │  (head.flags = (66<<16)|0)               │
  │                                          ├─ HandleWriteImmCompletion()
  │                                          │  ├─ total_chunks=66, chunk_idx=0
  │                                          │  ├─ _reasm_ctxs[rid] = new UrmaReasmCtx
  │                                          │  ├─ chunks[0] = recv_buf data
  │                                          │  └─ ResponseCtrlMessage(ACK, chunk_idx=0)
  │                                          │
  │  chunk 1: WRITE_IMM ──────────────────►│
  │  (head.flags = (66<<16)|1)               │
  │                                          ├─ chunks[1] = recv_buf data
  │                                          └─ ResponseCtrlMessage(ACK, chunk_idx=1)
  │                                          │
  │  ... (chunk 2~64) ...                    │
  │                                          │
  │  chunk 65: WRITE_IMM ─────────────────►│
  │  (head.flags = (66<<16)|65)              │
  │                                          ├─ chunks[65] = recv_buf data
  │                                          ├─ received_chunks == total_chunks:
  │                                          │   按序拼接 chunks → append to _read_buf
  │                                          │   delete _reasm_ctxs[rid]
  │                                          └─ DispatchReceivedBytes()
  │                                               └─ 用户 handler
  │                                          │
  │  每个 chunk 各收到一个 ACK:               │
  │  ←── WRITE_IN_BAND_ACK × 66 ─────────────│
  │  每个 ACK: Release send_buf slot +        │
  │  return_object(ctx) + WakeAsEpollOut()    │
```

### 4.5 接收时序图 — WriteZeroCopy (PRE_WRITE + READ)

```
Sender                                    Receiver (PollCq)
  │                                          │
  │  1. 枚举 IOBuf blocks                    │
  │  2. 构造控制消息(entries)                 │
  │  3. saved_blocks 保存引用                 │
  │                                          │
  │── WRITE_IMM (PRE_WRITE) ──────────────►│
  │   imm={PRE_WRITE, offset/1024}           │
  │   payload = 控制消息 (block 描述符列表)    │
  │                                          │
  │                          HandleWriteImmCompletion()
  │                          ├─ 读 recv_buf[offset] → head
  │                          ├─ imm.opcode == PRE_WRITE:
  │                          │   分配 UrmaRxSlot[idx]
  │                          │   解析 entries → slot.read_targets
  │                          │   slot.total_blocks = N
  │                          └─ PostReadBatch(idx)
  │                               │
  │                               ▼
  │◄── RDMA READ (batch=64) ──────────────│
  │   src=sender IOBuf block                │
  │   dst=receiver pool buffer              │
  │                                          │
  ├─ HandleCompletion (TX, READ)            │
  │  (READ 完成通知发送端)                    │
  │                                          │
  │                          HandleReadCompletion()  [endpoint.cpp:2875]
  │                          ├─ 每个 READ WR complete_enable=1 → 每 CQE 1 block
  │                          ├─ --slot.sq_slots_used (batch 内剩余计数)
  │                          ├─ sq_slots_used > 0 → return 0 (等更多 CQE)
  │                          ├─ next_read_idx < total_blocks?
  │                          │   └─ PostReadBatch(idx)  // 续发下一批
  │                          └─ 全部完成:
  │                               ResponseCtrlMessage(POST_WRITE)
  │                               └─ DispatchReceivedBytes()
  │                                    └─ 用户 handler
  │                                          │
  │◄── WRITE_IMM (POST_WRITE) ────────────│
  │   imm={POST_WRITE, offset/1024}          │
  │                                          │
  ├─ HandlePostWrite()                       │
  │  ├─ FindAndRemoveSendContext(rid)        │
  │  ├─ Release(send_buf offset)             │
  │  ├─ ctx->Reset()  // 释放 saved_blocks   │
  │  └─ WakeAsEpollOut()                     │
```

---

## 五、内存管理

### 5.1 内存布局

```
_one_sided_buf (mmap, MAP_PRIVATE | MAP_ANONYMOUS)
┌──────────────────────────────────────────────────────────┐
│                    recv_buf                              │  send_buf
│  ◄──── _recv_buf_capacity ────►  ◄── _send_buf_capacity ──►│
│                                  │                        │
│  [slot 0][slot 1]...[slot N-1]   │  [slot 0][slot 1]...   │
│  每个 slot = 1024B (ALLOC_UNIT)   │  bitmap 分配器管理      │
│  对端 WRITE_IMM 的写入目标        │  本端 WRITE_IMM 的源    │
│  本端读取数据的地方               │  ACK 写入的位置(镜像)   │
└──────────────────────────────────────────────────────────┘
  │                                 │
  └─ recv_buf 偏移 = 0              └─ send_buf 偏移 = _recv_buf_capacity

MR 注册: urma_register_seg(_one_sided_buf, total, R/W, NON_CACHEABLE)
         → _send_buf_tseg (整个区域的 token)

对端 import: urma_import_seg(peer_recv_buf_seg, R/W, NON_CACHEABLE)
             → _resource->remote_recv_buf_seg
```

**关键设计**：双方 buffer 布局对称（recv_buf 在前，send_buf 在后），同一 offset 在双方 buffer 中位置一致。发送方写 `send_buf[offset]`，接收方在 `recv_buf[offset]` 读取。ACK 时接收方写 `recv_buf[offset]`（镜像区域），发送方在 `send_buf[offset]` 读取。

### 5.2 UrmaRingBuf 分配器

```
┌─────────────────────────────────────────────┐
│  UrmaRingBuf (bitmap first-fit, 1KB 粒度)    │
│  [Stage2] 新增 _alloc_cursor 游标            │
│                                              │
│  _bitmap: [0,0,1,1,1,0,0,1,0,0,0,1,...]      │
│           0=free, 1=allocated                │
│  _alloc_cursor: 下次搜索起始 unit            │
│                                              │
│  Allocate(3KB):                              │
│    units = ceil(3KB/1KB) = 3                 │
│    从 _alloc_cursor 起 first-fit 扫描        │
│    找到后标记为 1, _alloc_cursor = start+units│
│    返回 start*1024 as offset                 │
│                                              │
│  Release(offset=2048, size=3KB):             │
│    清除 bitmap[2..4] = 0                     │
│                                              │
│  线程安全: pthread_spinlock_t                 │
│  MPSC: Allocate(KeepWrite) vs Release(PollCq)│
└─────────────────────────────────────────────┘
```

**[Stage2] _alloc_cursor 收益**：小包路径省去从头扫描已分配区域的开销，c_post -0.1~0.2us。注意 bitmap 是平面数组非环形，分配必须落在连续 `[start, start+units)` 范围内，扫描 wrap 到 0 时重置 streak。

### 5.3 PendingSendSlot 无锁数组

```
┌───────────────────────────────────────────────────────┐
│  _pending_sends[4096]  (PendingSendSlot)               │
│  [Stage2] _insert_hint 游标辅助插入                     │
│                                                         │
│  slot 0: [state=0, ctx=null]     ← free               │
│  slot 1: [state=KEY1, ctx=ptr]   ← occupied           │
│  slot 2: [state=0, ctx=null]     ← free               │
│  slot 3: [state=KEY2, ctx=ptr]   ← occupied           │
│  ...                                                    │
│  slot 4095: [state=0, ctx=null]  ← free               │
│                                                         │
│  Writer (KeepWrite, 单线程):                           │
│    InsertPendingSend(key, ctx):                         │
│      从 _insert_hint 起线性扫描找 state==0             │
│      CAS(0 → key) 抢占                                 │
│      设置 ctx, 更新 _insert_hint                       │
│                                                         │
│  Reader (PollCq, 单线程):                              │
│    FindAndRemoveSendContext(key):                       │
│      线性扫描找 state==key 的 slot                     │
│      读取 ctx                                          │
│      state.store(0)  // 释放                           │
│                                                         │
│  key 格式:                                              │
│    WriteInline:        (request_id << 20) | 0           │
│    WriteInlineChunked: (request_id << 20) | chunk_idx   │
│    WriteZeroCopy:      request_id                       │
└───────────────────────────────────────────────────────┘
```

**[Stage2] _insert_hint 收益**：插入时跳过已占用前缀，小包 -0.1us。`kMaxPendingSends=4096`（`urma_endpoint.h:56`）。

### 5.3.1 RetryPendingReads 空 guard

[Stage2] 新增 `_pending_retry_count` 原子计数（`urma_endpoint.h:447`）。`RetryPendingReads` 入口检查（`urma_endpoint.cpp:2634`）：
```cpp
if (_pending_retry_count.load(butil::memory_order_relaxed) == 0) {
    return;  // 无 pending slot, 跳过 128-slot 扫描
}
```
收益：1M 消息 255-batch READ 完成时，避免 255×128=32K 次空扫描，修复 +13us 回归。

### 5.4 UrmaSendContext 对象池

```
butil::get_object<UrmaSendContext>()   // TLS free list, 24-67ns
    │
    ▼
┌─────────────────────────┐
│  UrmaSendContext         │
│  ├─ request_id           │
│  ├─ send_buf_offset      │
│  ├─ send_buf_size        │
│  ├─ opcode               │
│  └─ saved_blocks: IOBuf  │  ← WriteZeroCopy 持有 block 引用
└─────────────────────────┘
    │
    │  ACK/POST_WRITE 到达:
    │  ctx->Reset()  // clear saved_blocks
    │  butil::return_object(ctx)  // 归还 TLS free list, 4-6ns
    ▼
  (复用)
```

### 5.5 SQ/RQ 窗口管理

```
┌─────────────────────────────────────────────────────────────┐
│  SQ (Send Queue) 窗口 — write jetty                          │
│                                                               │
│  _sq_size: 硬件 JFS 深度 (FLAGS_urma_sq_size, 默认 128)      │
│  _sq_window_size (atomic): 当前可用 slot 数                   │
│  RESERVED_WR_NUM = 3: 为 ACK/控制消息预留                     │
│  有效深度 = _sq_size - 3                                      │
│                                                               │
│  发送: _sq_window_size.fetch_sub(1)  // 预扣                  │
│  TX completion: _sq_window_size CAS +1  // 恢复              │
│  窗口=0 时: 返回 EAGAIN, brpc KeepWrite 稍后重试             │
├─────────────────────────────────────────────────────────────┤
│  [dual-jetty] SQ Read 窗口 — read jetty                      │
│                                                               │
│  _sq_window_size_read (atomic): read jetty 可用 slot 数      │
│  深度 = FLAGS_urma_read_jetty_sq_size (默认 1024)            │
│  仅 WriteZeroCopy READ WR 消耗/恢复                           │
│  与 write SQ 物理隔离, 消除 READ+WRITE_IMM status=8          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  RQ (Recv Queue) 窗口 — 仅 io_mode=0                         │
│                                                               │
│  _remote_rq_window_size (atomic): 对端 RQ 可用 credit        │
│                                                               │
│  发送: fetch_sub(1)  // 消耗对端一个 RQ slot                 │
│  对端 SEND_WITH_IMM: fetch_add(imm_data)  // 返还 credit     │
│  credit=0 时: 返回 EAGAIN                                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  SQ IMM 窗口 — ACK/控制消息预算                               │
│                                                               │
│  _sq_imm_window_size: 纯 ACK WR 的预算 (上限 3)              │
│  用于 ResponseCtrlMessage / SendAck / SendImm                │
│  与数据 WR 共享 SQ 但独立计数                                 │
└─────────────────────────────────────────────────────────────┘
```

### 5.6 IOBuf 内存池

```
GlobalUrmaInitializeImpl (urma_helper.cpp:549)
  └─ g_pool = new BufferPool()
  └─ butil::iobuf::blockmem_allocate = PoolAllocate
      │
      ▼
  所有 IOBuf block 分配走 BufferPool
  (URMA 要求 block 地址可注册为 MR，PoolAllocate 保证)
```

### 5.7 URMA 消息内存数据流 — SGE / BufferPool / IOBuf / block 完整链路

#### 5.7.1 两套独立的 MR 体系

```
┌─────────────────────────────────────────────────────────────────────┐
│                      全局 BufferPool MR (g_pool_seg)                 │
│                                                                      │
│  初始化: GlobalUrmaInitializeImpl → InitPool()                       │
│    mmap(65536 × 8KB = 512MB)  [urma_helper.cpp:415]                 │
│    urma_register_seg → g_pool_seg  [urma_helper.cpp:438]            │
│                                                                      │
│  用途: 所有 IOBuf block 的 backing memory                           │
│    butil::iobuf::blockmem_allocate = PoolAllocate  [helper.cpp:804] │
│    butil::SetDefaultBlockSize(8192)                  [helper.cpp:806]│
│                                                                      │
│  分配: PoolAllocate() 从 64-shard free_list 取 8KB buffer            │
│    [urma_helper.cpp:333-363]                                         │
│  查找: GetPoolSegFor(addr) → 地址在 pool 范围内则返回 g_pool_seg     │
│    [urma_helper.cpp:459-472]                                         │
│                                                                      │
│  适用: io_mode=0 (SEND 零拷贝 SGE)                                   │
│        io_mode=0 接收 (PostRecv 的 recv buffer)                      │
├─────────────────────────────────────────────────────────────────────┤
│                   per-Connection One-Sided MR (_send_buf_tseg)       │
│                                                                      │
│  初始化: AllocateOneSidedBuffers()  [urma_endpoint.cpp:703]         │
│    mmap(recv_buf_cap + send_buf_cap)  [endpoint.cpp:718]            │
│    urma_register_seg → _send_buf_tseg  [endpoint.cpp:744]           │
│                                                                      │
│  布局: [recv_buf][send_buf] 连续区域, 共用一个 tseg                  │
│    recv_buf: [0, _recv_buf_capacity)                                 │
│    send_buf: [_recv_buf_capacity, total)                             │
│                                                                      │
│  用途: io_mode=1/2 (WRITE_IMM 的 src/dst buffer)                    │
│    src SGE.tseg = _send_buf_tseg (本地 send_buf)                     │
│    dst SGE.tseg = remote_recv_buf_seg (对端 recv_buf, import 得到)   │
│                                                                      │
│  分配: UrmaRingBuf::Allocate() bitmap first-fit, 1KB 粒度            │
│    [urma_one_sided.h:178-208]                                        │
│  释放: UrmaRingBuf::Release()  [urma_one_sided.h:211-220]           │
└─────────────────────────────────────────────────────────────────────┘
```

#### 5.7.2 max_sge / max_sge_len / max_jfr_sge 配置链路

```
┌─────────────────────────────────────────────────────────────────────┐
│  max_sge (Send Queue 最大 SGE 数)                                   │
│                                                                      │
│  设备能力: urma_query_device → dev_cap.max_jfs_sge                   │
│    [urma_helper.cpp:736]                                             │
│                                                                      │
│  Bonding/UDMA 设备 cap: g_max_sge = 1  (多 SGE 返回 status=8/错误)  │
│    [urma_helper.cpp:782-799]                                         │
│    日志: "Capping JFS max_sge from N to 1"                           │
│    注: 所有 URMA 设备（bonding 和 raw UDMA）均 cap 到 1             │
│                                                                      │
│  命令行覆盖: --urma_max_sge (仅能 ≤ 设备值, 实际 bonding 下无效)    │
│    [urma_helper.cpp:755-762]                                         │
│                                                                      │
│  使用: CutFromIOBufList_Send                                         │
│    max_sge = GetUrmaMaxSge()  // 通常 = 1                            │
│    alloca(sizeof(urma_sge_t) * max_sge)  // 栈上分配 SGE 数组        │
│    [urma_endpoint.cpp:1067,1073]                                     │
├─────────────────────────────────────────────────────────────────────┤
│  max_sge_len (单 SGE 最大字节数)                                    │
│                                                                      │
│  SEND 路径: GetUrmaSendMaxSgeLen() → 硬编码 4096                    │
│    [urma_helper.cpp:934-939]                                         │
│    Bonding 设备 SEND WR SGE > 4096 时静默丢弃                        │
│                                                                      │
│  WRITE 路径: GetUrmaMaxSgeLen() → FLAGS_urma_max_sge_len (默认 4096)│
│    [urma_helper.cpp:925-932]                                         │
│    --urma_max_sge_len=65536 可覆盖 (用于大包 chunk)                  │
│                                                                      │
│  使用: cut_into_sglist 中 cap this_len ≤ max_sge_len                │
│    [urma_endpoint.cpp:1009-1010]                                     │
│    超出部分留在 IOBuf, 下次迭代填充下一个 SGE                        │
├─────────────────────────────────────────────────────────────────────┤
│  max_jfr_sge (Receive Queue 最大 SGE 数)                            │
│                                                                      │
│  设备能力: dev_cap.max_jfr_sge  [urma_helper.cpp:763-770]           │
│  无 bonding cap, 无命令行覆盖                                        │
│                                                                      │
│  使用: DoPostRecv 固定 1 个 SGE                                      │
│    urma_sge_t sge{addr, block_size, tseg, nullptr}                   │
│    urma_sg_t sg{&sge, 1}  // num_sge = 1                             │
│    [urma_endpoint.cpp:1751-1753]                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 5.7.3 IOBuf Block → SGE 映射 (io_mode=0 SEND 路径)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    IOBuf Block 结构与 BufferPool 关系                 │
│                                                                      │
│  butil::IOBuf::Block  [src/butil/iobuf_inl.h:463]                   │
│  ┌──────────────────────────────────┐                                │
│  │ nshared: atomic<int> (ref_count) │                                │
│  │ flags: uint16_t                  │                                │
│  │ size: uint32_t (used bytes)      │                                │
│  │ cap: uint32_t (capacity)         │                                │
│  │ data: char* ─────────────────────┼──► PoolAllocate 返回的 8KB    │
│  │                                 │     buffer (g_pool_base 范围内) │
│  └──────────────────────────────────┘                                │
│                                                                      │
│  Block 分配: butil::iobuf::blockmem_allocate(size)                   │
│    = PoolAllocate(size)  [被 hijack]                                 │
│    → 从 64-shard free_list 取 8KB buffer                             │
│    → 返回地址在 [g_pool_base, g_pool_base+512MB) 范围内             │
│                                                                      │
│  Block 释放: butil::iobuf::blockmem_deallocate(ptr)                  │
│    = PoolDeallocate(ptr)  [被 hijack]                                │
│    → 归还到对应 shard 的 free_list                                   │
└─────────────────────────────────────────────────────────────────────┘

                    io_mode=0 发送时 SGE 构造

  IOBuf (from[current])
    │
    │ _ref_at(0) → BlockRef {block_ptr, offset, length}
    │ fetch1() → start = block.data + offset (数据起始地址)
    │
    ▼
  GetPoolSegFor(start)  [urma_helper.cpp:459]
    │ start 在 [g_pool_base, g_pool_base+g_pool_size) 范围内?
    │ ├── YES → return g_pool_seg  (全局 MR)
    │ └── NO  → get_first_data_meta() → user-registered tseg
    │           (用户手动注册的内存, 如 append_user_data_with_meta)
    │
    ▼
  urma_sge_t  [urma_endpoint.cpp:1012-1015]
    ┌────────────────────────────────┐
    │ addr = start                   │  ← block 数据地址 (pool 内)
    │ len  = min(block_len, 4096)   │  ← max_sge_len cap
    │ tseg = g_pool_seg             │  ← 全局 MR token
    │ user_tseg = nullptr            │
    └────────────────────────────────┘
    │
    │ cutn(to=&_sbuf[_sq_current], this_len)
    │ → 数据所有权从 from 切到 _sbuf (引用计数转移, 零拷贝)
    │
    ▼
  urma_sg_t sg = {sglist, sge_index}
    │
    ▼
  urma_post_jetty_send_wr(jetty, &wr)
    wr.opcode = URMA_OPC_SEND
    wr.send.src = sg
    wr.user_ctx = _sq_current + 1
```

#### 5.7.4 send_buf SGE 映射 (io_mode=1/2 WRITE 路径)

```
                    io_mode=1/2 发送时 SGE 构造

  IOBuf (from[])
    │
    │ copy_to(send_buf + offset + 24, total)  ← memcpy (非零拷贝!)
    │
    ▼
  urma_sge_t src_sge  [urma_endpoint.cpp:1283-1287]
    ┌────────────────────────────────┐
    │ addr = send_buf + offset       │  ← _one_sided_buf + _recv_buf_cap + offset
    │ len  = 24 + total              │  ← UrmaMessageHead + payload
    │ tseg = _send_buf_tseg          │  ← per-connection MR
    │ user_tseg = nullptr            │
    └────────────────────────────────┘

  urma_sge_t dst_sge  [urma_endpoint.cpp:1290-1293]
    ┌────────────────────────────────┐
    │ addr = _remote_recv_buf_va+offset│ ← 对端 recv_buf 内偏移
    │ len  = 24 + total              │
    │ tseg = _resource->             │  ← 对端 import 的 segment
    │        remote_recv_buf_seg     │
    │ user_tseg = nullptr            │
    └────────────────────────────────┘

  urma_post_jetty_send_wr(jetty, &wr)
    wr.opcode = URMA_OPC_WRITE_IMM
    wr.rw.src.sge = &src_sge  (num_sge=1)
    wr.rw.dst.sge = &dst_sge  (num_sge=1)
    wr.rw.notify_data = imm.data
```

#### 5.7.5 _sbuf / _rbuf 与 IOBuf block 的关系

```
┌─────────────────────────────────────────────────────────────────────┐
│  _sbuf: 发送侧 IOBuf 引用池 (io_mode=0 专用)                        │
│                                                                      │
│  结构: vector<butil::IOBuf> _sbuf, size = _sq_size - 3              │
│    [urma_endpoint.h:311, endpoint.cpp:622]                          │
│                                                                      │
│  生命周期:                                                           │
│    1. cut_into_sglist → cutn(&_sbuf[slot], len)                     │
│       IOBuf block 引用从 from 转移到 _sbuf[slot]                    │
│       block 的 nshared++ (引用计数+1)                                │
│    2. urma_post_jetty_send_wr → 硬件 DMA 从 block 地址读取           │
│       (_sbuf 持有引用, block 不会被释放)                             │
│    3. TX completion → _sbuf[slot].clear()                           │
│       block 引用释放, nshared--, 归零则归还 BufferPool               │
│                                                                      │
│  环形索引: _sq_current = (_sq_current + 1) % (_sq_size - 3)         │
│    [urma_endpoint.cpp:1185]                                          │
├─────────────────────────────────────────────────────────────────────┤
│  _rbuf: 接收侧 IOBuf 缓冲池 (io_mode=0 专用)                        │
│                                                                      │
│  结构: vector<butil::IOBuf> _rbuf, size = _rq_size                  │
│         vector<void*> _rbuf_data, size = _rq_size                   │
│    [urma_endpoint.h:313-314, endpoint.cpp:623-624]                  │
│                                                                      │
│  生命周期:                                                           │
│    1. PostRecv → IOBufAsZeroCopyOutputStream(&_rbuf[slot], 8KB)     │
│       从 BufferPool 分配新 block, _rbuf[slot] 引用它                 │
│       _rbuf_data[slot] = block.data 指针                             │
│    2. DoPostRecv(_rbuf_data[slot], block_size)                      │
│       将 block 地址注册为 recv WR 的 SGE                             │
│       SGE.tseg = GetPoolSegFor(block) = g_pool_seg                  │
│    3. RX completion → _rbuf[slot].cutn(&_socket->_read_buf, len)   │
│       数据从 _rbuf 切到 Socket 读取缓冲, block 引用转移              │
│       (零拷贝: block 引用直接追加到 _read_buf)                       │
│    4. PostRecv(1) → 回到步骤 1, 分配新 block                        │
│                                                                      │
│  环形索引: _rq_received = (_rq_received + 1) % _rq_size             │
│    [urma_endpoint.cpp:1815]                                          │
├─────────────────────────────────────────────────────────────────────┤
│  _sbuf/_rbuf 与 io_mode=1/2 的关系:                                 │
│    io_mode=1/2 不使用 _sbuf (数据 copy 到 send_buf, 不需要持有引用) │
│    io_mode=1/2 不使用 _rbuf (数据写入 recv_buf, 从 recv_buf 读取)   │
│    但 PostEmptyRecvWr 仍使用 _rbuf_data (post 空 recv WR)           │
└─────────────────────────────────────────────────────────────────────┘
```

#### 5.7.6 URMA 消息内存数据流 UML 图

```
                          ┌─────────────────┐
                          │  BufferPool     │
                          │  (全局单例)      │
                          │  g_pool_base    │
                          │  g_pool_size    │
                          │  g_pool_seg ────┼───────────────┐
                          │  64-shard       │               │
                          │  free_lists     │               │
                          └───────┬─────────┘               │
                                  │ PoolAllocate(8192)       │
                                  ▼                          │
                          ┌─────────────────┐               │
                          │  IOBuf::Block   │               │
                          │  ─────────────  │               │
                          │  nshared: ref   │               │
                          │  data: ─────────┼──► 8KB buffer │               │
                          │  cap: 8192     │    (pool 内)   │               │
                          └───────┬─────────┘               │               │
                                  │                          │               │
                    ┌─────────────┼─────────────┐           │               │
                    │             │             │           │               │
                    ▼             ▼             ▼           │               │
              ┌──────────┐ ┌──────────┐ ┌──────────┐      │               │
              │ IOBuf    │ │ IOBuf    │ │ IOBuf    │      │               │
              │ (from[0])│ │ (from[1])│ │ _sbuf[i] │      │               │
              │ BlockRef │ │ BlockRef │ │ BlockRef │      │               │
              └────┬─────┘ └────┬─────┘ └────┬─────┘      │               │
                   │            │            │             │               │
                   │  cut_into_sglist        │             │               │
                   │  (io_mode=0)           │             │               │
                   ▼            ▼            │             │               │
              ┌──────────────────────┐      │             │               │
              │  urma_sge_t sglist[] │      │                           │
              │  ──────────────────  │      │             │               │
              │  [0] addr, len,      │──────┼─────────────┼───────────────┘
              │      tseg=g_pool_seg │      │             │ (MR 引用)
              │  [1] addr, len,      │      │             │
              │      tseg=g_pool_seg │      │             │
              └──────────┬───────────┘      │             │
                         │                  │             │
                         ▼                  │             │
              ┌──────────────────────┐      │             │
              │  urma_post_jetty_    │      │             │
              │  send_wr()           │      │             │
              │  opcode = SEND       │      │             │
              │  src = sglist        │      │             │
              │  user_ctx = slot+1   │      │             │
              └──────────┬───────────┘      │             │
                         │                  │             │
                    TX completion           │             │
                         │                  │             │
                         ▼                  │             │
              ┌──────────────────────┐      │             │
              │  _sbuf[slot].clear() │──────┘             │
              │  → block nshared--   │                    │
              │  → 归零则 PoolDealloc│                    │
              └──────────────────────┘                    │
                                                          │
                                                          │
    ┌─────────────────────────────────────────────────────┘
    │
    │  (per-Connection One-Sided MR, 独立体系)
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  UrmaEndpoint (per-connection)                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  _one_sided_buf (mmap, 注册为 _send_buf_tseg)           │    │
│  │  ┌────────────────────┬───────────────────────────┐    │    │
│  │  │    recv_buf        │       send_buf            │    │    │
│  │  │  (对端写入目标)     │  (本端写入源)             │    │    │
│  │  │  [0, recv_cap)     │  [recv_cap, total)       │    │    │
│  │  └────────────────────┴───────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
│  UrmaRingBuf (bitmap first-fit, 1KB 粒度, spinlock)              │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  Allocate(24+payload) → offset                       │       │
│  │  Release(offset, size)                               │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                   │
│  io_mode=1/2 发送:                                               │
│    IOBuf.copy_to(send_buf+offset+24)  ← memcpy                  │
│    src_sge = {send_buf+offset, 24+payload, _send_buf_tseg}      │
│    dst_sge = {remote_recv_buf_va+offset, 24+payload,            │
│               remote_recv_buf_seg}                               │
│    urma_post_jetty_send_wr(WRITE_IMM, src, dst, imm)            │
│                                                                   │
│  io_mode=1/2 接收:                                               │
│    HandleWriteImmCompletion:                                     │
│      recv_buf[offset+24] → append to _socket->_read_buf         │
│      ResponseCtrlMessage(ACK):                                   │
│        写 head 到 recv_buf[offset]                               │
│        WRITE_IMM → peer send_buf[offset]                         │
└─────────────────────────────────────────────────────────────────┘
```

#### 5.7.7 完整内存流转路径对比

```
io_mode=0 (SEND 零拷贝路径):
  BufferPool → IOBuf::Block → IOBuf → cut_into_sglist → urma_sge_t
       │                                              (addr=block.data)
       │                                              (tseg=g_pool_seg)
       │
       └─► _sbuf[slot] 持有引用 ──► TX completion ──► clear() ──► PoolDeallocate

io_mode=1/2 (WRITE 拷贝路径):
  BufferPool → IOBuf::Block → IOBuf → copy_to(send_buf) ──► memcpy
       │                                              │
       │                                    urma_sge_t (addr=send_buf+offset)
       │                                              (tseg=_send_buf_tseg)
       │
       └─► IOBuf 立即 clear() (数据已拷贝, block 可回收)
           └─► PoolDeallocate

  send_buf slot:
    Allocate ← UrmaRingBuf bitmap
    ──► ACK 到达 ←── Release (bitmap 清零)

io_mode=0 接收路径:
  PostRecv → IOBufAsZeroCopyOutputStream → PoolAllocate → IOBuf::Block
       │                                                        │
       └─► _rbuf[slot] 引用 ──► DoPostRecv (SGE.tseg=g_pool_seg)
                                   │
                              RX completion
                                   │
                              _rbuf[slot].cutn(&_read_buf)  ← 零拷贝转移
                                   │
                              PostRecv(1) → 新 block

io_mode=1/2 接收路径:
  PostEmptyRecvWr → 空 recv WR (len=0, num_sge=1)
    SGE.tseg = GetPoolSegFor(_rbuf_data[slot]) = g_pool_seg
  WRITE_IMM 到达 → recv_buf[offset] 直接有数据 (硬件写入)
    HandleWriteImmCompletion:
      recv_buf[offset+24] → append to _socket->_read_buf (memcpy)
      PostEmptyRecvWr(1) → 补充空 recv WR
```

#### 5.7.8 关键配置参数汇总

| 参数 | 默认值 | Stage4 测试值 | 作用 | 影响路径 |
|------|--------|---------------|------|---------|
| `--urma_buffer_size` | 8192 (8KB) | 65536 (64KB) | BufferPool 单 buffer 大小 = IOBuf block 大小 | 全局, 所有 IOBuf block |
| `--urma_buffer_count` | 65536 | 8192 | BufferPool buffer 数量 (总池) | 全局, block 分配上限 |
| `--urma_max_sge` | 0 (自动→1) | — | Send Queue 最大 SGE 数 (所有 URMA 设备 cap 到 1) | io_mode=0 SEND |
| `--urma_max_sge_len` | 4096 | 65536 | 单 SGE 最大字节数 (WRITE/READ 可调, SEND 固定 4096) | io_mode=0/1/2 |
| `--urma_send_buf_size` | 128 (KB) | 8192 (8MB) | send_buf 容量 (per-connection) | io_mode=1/2 |
| `--urma_recv_buf_size` | 128 (KB) | 8192 (8MB) | recv_buf 容量 (per-connection) | io_mode=1/2 |
| `--urma_inline_threshold` | 16384 (B) | 16384 | io_mode=2 小包/大包分割阈值 (Stage1 从 2048 上调) | io_mode=2 |
| `--urma_chunk_payload_size` | 130048 (127KB) | 2095104 (~2MB) | chunk payload 最大值 (mega-chunk) | io_mode=2 Chunked |
| `--urma_sq_size` | 128 | 1024 | write jetty JFS 深度 | 所有 io_mode |
| `--urma_read_jetty_sq_size` | 1024 | 1024 | [dual-jetty] read jetty JFS 深度 | WriteZeroCopy |
| `--urma_use_zerocopy_read` | false | true | 激活 WriteZeroCopy (PRE_WRITE+READ) | io_mode=2 大包 |
| `--urma_dual_jetty` | false | true | 创建 READ 专用 jetty | WriteZeroCopy |
| `--urma_use_polling` | false | true | busy-poll 模式 | 所有 io_mode |
| `--urma_client_handshake_version` | 2 | 3 | 握手版本 (v3 携带 read_jetty_id) | dual-jetty 协商 |
| `g_max_sge` | 1 (所有设备) | 1 | 运行时 max_sge (getter) | io_mode=0 |
| `g_max_jfr_sge` | 设备值 | 设备值 | 运行时 max_jfr_sge (recv WR) | io_mode=0 接收 |

---

## 六、URMA 硬件限制与分片设计

### 6.1 硬件限制总览

所有 URMA 设备（bonding 和物理 UDMA）有三项关键限制，直接驱动了整个分片体系的设计：

```
┌─────────────────────────────────────────────────────────────────────┐
│  限制 1: max_sge = 1 (所有 URMA 设备)                               │
│    来源: urma_query_device → dev_cap.max_jfs_sge                    │
│    代码: urma_helper.cpp:782-799                                    │
│    原因: bonding 和 raw UDMA provider 在 SEND WR 携带 >1 SGE 时     │
│           均返回错误 (bonding status=8, UDMA completion error/      │
│           silent WR drop)。即使 2 SGE + ~4KB payload 也会触发       │
│    影响: 每个 WR 只能携带 1 个 SGE = 1 段连续内存                   │
│           IOBuf 多 block 无法直接用多 SGE 发送                      │
│                                                                      │
│  限制 2: max_sge_len = 4096 (SEND 路径硬编码)                       │
│    来源: 硬编码, 非 URMA API 查询                                   │
│    代码: urma_helper.cpp:972-977 (GetUrmaSendMaxSgeLen → 4096)     │
│    原因: bonding/UDMA 设备静默丢弃 SGE payload > 4096 字节的 SEND WR│
│    影响: io_mode=0 SEND 路径每个 SGE 最多 4096 字节, 不可配         │
│    注意: WRITE/READ 路径用 GetUrmaMaxSgeLen() → FLAGS_urma_max_sge_ │
│           len (默认 4096, 可配 65536), 不受此限                     │
│                                                                      │
│  限制 3: SQ depth 有限                                               │
│    来源: FLAGS_urma_sq_size = 128 (默认) / 1024 (Stage1 测试)       │
│    代码: urma_helper.cpp:74-75                                      │
│    有效深度: sq_size - 3 (RESERVED_WR_NUM)                          │
│    影响: 最多 sq_size-3 个 in-flight WR, 超出返回 EAGAIN            │
│    [dual-jetty] read jetty 独立 SQ, 深度 read_jetty_sq_size (1024) │
└─────────────────────────────────────────────────────────────────────┘
```

### 6.2 io_mode=0 SEND 路径分片

```
CutFromIOBufList_Send (urma_endpoint.cpp:1049)
  双层循环结构:

  外层 while (current < ndata)            [行 1083]
    │  每个 iteration = 1 个 SEND WR
    │  消耗: 1 SQ slot + 1 RQ credit
    │
    ├─ 内层 while (sge_index < max_sge     [行 1103]
    │               && this_len < max_len
    │               && current < ndata)
    │    │  bonding 设备 max_sge=1 → 循环仅执行 1 次
    │    │
    │    ├─ cut_into_sglist:
    │    │   start = IOBuf block 数据地址
    │    │   this_len = min(block_len, max_len, max_sge_len=4096)
    │    │   SGE = {addr=start, len=this_len, tseg=g_pool_seg}
    │    │   cutn(&_sbuf[slot], this_len)  ← 零拷贝引用转移
    │    │
    │    └─ sge_index++ → 退出内层循环 (max_sge=1)
    │
    ├─ urma_post_jetty_send_wr(SEND, sglist, 1 SGE)
    └─ _sq_current = (_sq_current + 1) % 125
```

**8KB 消息分片示例**：

```
消息: 8192 字节, IOBuf 含 1 个 8KB block (来自 BufferPool)

max_sge = 1 (bonding cap)
max_sge_len = 4096 (硬编码)
max_len = recv_block_size = 8192 (对端协商)

WR #1: SGE = {addr=block+0,    len=4096, tseg=g_pool_seg}
       _sbuf[0] 持有 block 引用, _sq_window_size--, _remote_rq_window_size--

WR #2: SGE = {addr=block+4096, len=4096, tseg=g_pool_seg}
       _sbuf[1] 持有 block 引用, _sq_window_size--, _remote_rq_window_size--

→ 8KB 需要 2 个 WR, 2 个 SQ slot, 2 个 RQ credit
```

**8MB 消息分片示例**：

```
8MB / 4096 = 2048 个 WR
SQ 有效窗口 = 125
→ 需要 ceil(2048/125) = 17 轮 KeepWrite
每轮: post 125 个 WR → EAGAIN → 等 TX completion 恢复窗口 → 继续下一轮
```

### 6.3 io_mode=1/2 WRITE_IN_BAND 路径分片

#### 6.3.1 小包 (≤ `urma_inline_threshold`, 默认 16384B) — WriteInline

```
WriteInline (urma_endpoint.cpp:1420 附近)

小包不分片:
  total ≤ FLAGS_urma_inline_threshold (默认 16384B, Stage1 从 2048 上调)
  → 1 次 UrmaRingBuf::Allocate(24 + total)
  → 1 次 copy_to (memcpy 到 send_buf)
  → 1 个 WRITE_IMM WR
  → 1 个 SQ slot
```

#### 6.3.2 大包 (> `urma_inline_threshold`) — WriteInlineChunked

```
WriteInlineChunked (urma_endpoint.cpp:1700 附近)

chunk 大小计算:
  ┌──────────────────────────────────────────────────┐
  │  GetUrmaChunkPayloadMax() → FLAGS_urma_chunk_    │
  │  payload_size (默认 130048B = 127KB)             │
  │  [urma_helper.cpp:979-993]                        │
  │                                                    │
  │  chunk 总大小 = 24B head + 127KB payload           │
  │              = 131072B = 128KB                     │
  │              = 128 个 alloc units (1KB 粒度)       │
  │                                                    │
  │  为什么 127KB 而非 128KB payload?                  │
  │    24B head + 128KB payload = 131096B             │
  │    → 129 alloc units (1KB 对齐后)                  │
  │    24B head + 127KB payload = 131072B             │
  │    → 128 alloc units (恰好 128KB, 无浪费)         │
  └──────────────────────────────────────────────────┘

分片计算:
  _chunked_total_chunks = ceil(remaining / 127KB)
  _chunked_chunk_idx = 0
  _chunked_request_id = _one_sided_seq.fetch_add(1)

  循环发送:
    while (remaining > 0):
      chunk_payload = min(remaining, 127KB)
      alloc_size = 24 + chunk_payload
      UrmaRingBuf::Allocate(alloc_size, &offset)   ← send_buf 分配
      if (失败) → send_buf 空间不足, break/EAGAIN
      if (_sq_window_size == 0) → SQ 耗尽, break/EAGAIN
      IOBuf::cutn → send_buf[offset+24]  ← 从 IOBuf 切出数据
      WRITE_IMM WR → 1 SQ slot
      ++_chunked_chunk_idx
```

**8MB 消息分片示例**：

```
8MB = 8388608B
chunk_payload = 127KB = 130048B
total_chunks = ceil(8388608 / 130048) = 65 个 chunk

每 chunk: 128KB send_buf 空间 + 1 个 SQ slot

SQ 窗口 = 125, 65 < 125
→ 理论上 1 轮 KeepWrite 可 post 全部 65 个 chunk

但 send_buf 容量限制:
  --urma_send_buf_size=8192 (Stage4 测试配置) → send_buf = 8MB
  8MB / 128KB = 64 个并发 chunk slot
  → send_buf 最多同时容纳 64 个 chunk
  → 65 个 chunk 仅需 2 轮, 几乎一轮发完

  --urma_send_buf_size=2048 (早期测试配置) → send_buf = 2MB
  2MB / 128KB = 16 个并发 chunk slot
  → 65 个 chunk 需要 ceil(65/16) ≈ 5 轮, 等待 ACK 释放 send_buf 后继续

  --urma_send_buf_size=128 (默认) → send_buf = 128KB
  128KB / 128KB = 1 个 chunk slot
  → 65 个 chunk 需要 65 轮, 串行等待每个 ACK
```

### 6.4 send_buf 容量与分片的关系

```
┌─────────────────────────────────────────────────────────────────────┐
│  send_buf 布局 (UrmaRingBuf bitmap first-fit, 1KB 粒度)             │
│                                                                      │
│  --urma_send_buf_size=8192 (8MB, Stage4 测试配置):                 │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬──┤│
│  │ch0 │ch1 │ch2 │ch3 │ch4 │ch5 │ch6 │ch7 │ch8 │ch9 │ch10│ch11│..││
│  │128K│128K│128K│128K│128K│128K│128K│128K│128K│128K│128K│128K│  ││
│  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴──┘│
│  8MB / 128KB = 64 个并发 chunk slot                                 │
│  chunk ACK 到达 → UrmaRingBuf::Release → bitmap 清零 → 可复用       │
│                                                                      │
│  --urma_send_buf_size=128 (128KB, 默认):                            │
│  ┌────┐                                                             │
│  │ch0 │                                                             │
│  │128K│                                                             │
│  └────┘                                                             │
│  仅 1 个 chunk slot, 严格串行: 发送→等ACK→释放→下一个               │
└─────────────────────────────────────────────────────────────────────┘

发送流水线 (send_buf=8MB, 8MB 消息 = 65 chunks):

时间 ──►
ch0 ch1 ... ch63 → send_buf 接近满 (64/64 slot)
                   ch0 ACK → Release → ch64 分配
                   全部 65 chunk 发完

实际并发度 = min(SQ窗口, send_buf slots) = min(1021, 64) = 64
(SQ 窗口 = SQ_SIZE 1024 - RESERVED 3 = 1021, Stage1 从 512 上调)
```

### 6.5 SQ 窗口与 RQ credit 的分片约束

```
┌─────────────────────────────────────────────────────────────────────┐
│  SQ 窗口 (所有 io_mode 共享, write jetty)                            │
│                                                                      │
│  FLAGS_urma_sq_size = 128 (默认) / 1024 (Stage1 测试配置)            │
│  RESERVED_WR_NUM = 3 (保留给 ACK/控制消息)                           │
│  有效窗口 = sq_size - 3 (默认 125 / 测试 1021)                       │
│                                                                      │
│  初始化: _sq_window_size = min(128, peer_rq_size) - 3                │
│    [urma_endpoint.cpp:2863-2872]                                     │
│                                                                      │
│  消耗: 每个 WR (SEND/WRITE_IMM/READ) fetch_sub(1)                    │
│  恢复: TX completion fetch_add(1)                                    │
│  耗尽: 返回 EAGAIN → brpc KeepWrite 挂起 → WakeAsEpollOut 唤醒     │
├─────────────────────────────────────────────────────────────────────┤
│  RQ credit (仅 io_mode=0)                                           │
│                                                                      │
│  FLAGS_urma_rq_size = 128                                            │
│  _remote_rq_window_size = 对端可用 RQ slot 数                        │
│                                                                      │
│  消耗: 每个 SEND WR fetch_sub(1)                                     │
│  返还: SendAck 累积到 50% 阈值后 SendImm 批量返还                    │
│    [urma_endpoint.cpp:1871-1878]                                    │
│    阈值 = _remote_window_capacity / 2 = 62                           │
│    返还方式: URMA_OPC_SEND_IMM, imm_data = credit 数量              │
│                                                                      │
│  耗尽: 返回 EAGAIN → 等对端 SendImm 返还 credit                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 6.6 WriteZeroCopy (PRE_WRITE+READ) 的分片

```
WriteZeroCopy (urma_endpoint.cpp:1620 附近)
激活条件: FLAGS_urma_use_zerocopy_read=true (默认 false)

分片层级 1: block 枚举
  IOBuf backing blocks → 按 max_sge_len 切分
  --urma_max_sge_len=65536 (测试配置) → 每 block 最多 64KB
  8MB 数据 / 8KB block = 1024 个 block entries
  (若 --urma_buffer_size=65536 则 8MB/64KB = 128 block entries)

分片层级 2: READ 批次
  PostReadBatch (urma_endpoint.cpp:2540 附近)
  batch = min(remaining_blocks, sq_avail - 1, URMA_READ_BATCH_MAX=256)
                    │                │              │
                    │                │              └── 每批最多 256 个 READ
                    │                └── 保留 1 个 SQ slot 给 POST_WRITE ACK
                    └── 剩余待读 block 数

  128 blocks / 256 per batch = 1 批 (block=64KB 时)
  1024 blocks / 256 per batch = 4 批 (block=8KB 时)
  每批 READ WR → 对应 SQ slot (dual-jetty 时用 _sq_window_size_read)
  read jetty SQ = 1024 (FLAGS_urma_read_jetty_sq_size)

  批次流水线:
  ┌─────────┐  ┌─────────┐
  │batch 0  │→ │batch 1  │ →...→ 完成
  │256 READs│  │256 READs│
  │每 READ  │  │等首批    │
  │1 CQE    │  │completion│
  └─────────┘  └─────────┘
       │
       ▼ 每批 completion → HandleReadCompletion
         → --sq_slots_used, 归零则续发或 POST_WRITE
         → 全部完成 → ResponseCtrlMessage(POST_WRITE)
```

### 6.7 分片设计总结矩阵

```
┌──────────────┬──────────────────┬──────────────────┬──────────────────┐
│   维度       │ io_mode=0 SEND   │ io_mode=1/2      │ WriteZeroCopy    │
│              │                  │ WriteInline/     │ (PRE_WRITE+READ) │
│              │                  │ Chunked          │ zerocopy_read    │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 分片粒度     │ 4096B (max_sge_  │ 127KB (chunk_    │ 8KB (block size) │
│              │   len 硬限)      │   payload_size)  │ / max_sge_len    │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 每片消耗     │ 1 SQ + 1 RQ     │ 1 SQ             │ 1 SQ (PRE_WRITE) │
│              │                  │                  │ + N SQ (READs)   │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 8MB 需要     │ 2048 WR          │ 65 chunks        │ 1 PRE_WRITE      │
│              │                  │                  │ + 1024 READs     │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 8MB 轮次     │ 17 轮 (SQ=125)   │ 5轮 (send_buf=   │ 16 批 (batch=64) │
│ (KeepWrite)  │                  │   2MB) / 65轮    │                  │
│              │                  │   (send_buf=128K)│                  │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 数据拷贝     │ 零拷贝 (SGE 直指 │ memcpy (copy_to  │ 零拷贝 (READ 直读│
│              │  block)          │  send_buf)       │  block)          │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 并发瓶颈     │ SQ 窗口 + RQ     │ SQ 窗口 +        │ SQ 窗口          │
│              │ credit           │ send_buf 空间    │                  │
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 流控机制     │ SendAck 累积     │ WRITE_IN_BAND_   │ POST_WRITE ACK   │
│              │ 50% 批量返还     │ ACK 逐 chunk 释放│ (全部 READ 完成后)│
├──────────────┼──────────────────┼──────────────────┼──────────────────┤
│ 跨调用续传   │ 无 (每 WR 独立) │ 有 (_chunked_    │ 有 (UrmaRxSlot   │
│              │                  │  chunk_idx 状态) │  next_read_idx)  │
└──────────────┴──────────────────┴──────────────────┴──────────────────┘
```

### 6.8 分片相关配置参数

| 参数 | 默认值 | Stage4 测试值 | 作用 |
|------|--------|---------------|------|
| `--urma_sq_size` | 128 | 1024 | write jetty JFS 深度, 有效窗口 = sq_size - 3 |
| `--urma_rq_size` | 128 | — | JFR 深度 (io_mode=0 RQ credit) |
| `--urma_read_jetty_sq_size` | 1024 | 1024 | [dual-jetty] read jetty JFS 深度 |
| `--urma_max_sge` | 0 (自动→1) | — | 每 WR 最大 SGE 数 (所有 URMA 设备 cap 到 1) |
| `--urma_max_sge_len` | 4096 | 65536 | 单 SGE 最大字节 (WRITE/READ 路径可调, SEND 路径固定 4096) |
| `--urma_send_buf_size` | 128 (KB) | 8192 (KB) | send_buf 容量, 决定并发 chunk 数 |
| `--urma_recv_buf_size` | 128 (KB) | 8192 (KB) | recv_buf 容量, 接收端窗口 |
| `--urma_inline_threshold` | 16384 (B) | 16384 | io_mode=2 小包/大包分割阈值 (Stage1 从 2048 上调) |
| `--urma_chunk_payload_size` | 130048 (B) | 2095104 | chunk payload 最大值 (Stage1 mega-chunk 从 127KB 上调到 ~2MB) |
| `--urma_buffer_size` | 8192 (B) | 65536 | BufferPool 单 buffer = IOBuf block 大小 |
| `--urma_buffer_count` | 65536 | 8192 | BufferPool buffer 数量 |
| `--urma_use_zerocopy_read` | false | true | true 时 io_mode=2 大包走 WriteZeroCopy |
| `--urma_dual_jetty` | false | true | true 时创建 READ 专用 jetty |
| `--urma_use_polling` | false | true | true 时 busy-poll 模式 |
| `--urma_client_handshake_version` | 2 | 3 | 2=binary, 3=protobuf (v3 携带 read_jetty_id) |

> **Stage4 测试配置**：`--urma_io_mode=2 --urma_dual_jetty=true --urma_use_zerocopy_read=true --urma_use_polling=true --urma_sq_size=1024 --urma_read_jetty_sq_size=1024 --urma_send_buf_size=8192 --urma_recv_buf_size=8192 --urma_max_sge_len=65536 --urma_buffer_size=65536 --urma_buffer_count=8192 --urma_chunk_payload_size=2095104 --urma_client_handshake_version=3`

---

## 七、io_mode 对比矩阵

| 维度 | io_mode=0 (SEND) | io_mode=1 (WRITE) | io_mode=2 (HYBRID) |
|------|------------------|--------------------|--------------------|
| **发送 opcode** | URMA_OPC_SEND | URMA_OPC_WRITE_IMM | WRITE_IMM |
| **数据拷贝** | 零拷贝 (SGE 直指 IOBuf) | memcpy 到 send_buf | memcpy 到 send_buf |
| **对端 RQ 依赖** | 需要 PostRecv | 不需要 (WRITE_IMM 自带通知) | 不需要 |
| **流控** | SQ + RQ 双窗口 | SQ 窗口 + send_buf 空间 | SQ 窗口 + send_buf 空间 |
| **小包延迟** | 最低 (零拷贝) | 略高 (memcpy) | 略高 (memcpy) |
| **大包支持** | 受 max_sge 限制 | 分 chunk | 分 chunk (Chunked) |
| **send_buf 占用** | 无 | 24 + payload | 24 + payload (每 chunk) |
| **ACK 机制** | SEND_WITH_IMM credit | WRITE_IN_BAND_ACK | WRITE_IN_BAND_ACK |
| **适用场景** | 兼容/小包极致 | 单边操作 | 小包+大包混合 |

---

## 八、关键数据结构

### 8.1 UrmaMessageHead (24 字节)

```cpp
// urma_one_sided.h
struct UrmaMessageHead {
    uint32_t magic;         // 0x55524D31 ("URM1")
    uint32_t message_size;  // head + payload 总大小
    uint32_t data_count;    // WRITE_IN_BAND: payload 字节数
                            // PRE_WRITE: block 数量
    uint32_t flags;         // [0:15]=chunk_idx, [16:31]=total_chunks
    uint64_t request_id;    // 发送端请求 ID
};
```

### 8.2 UrmaWriteImmData (8 字节, 64-bit immediate data)

```cpp
// urma_one_sided.h
union UrmaWriteImmData {
    uint64_t data;
    struct {
        uint64_t opcode       : 4;   // WRITE_IN_BAND / PRE_WRITE / ACK / POST_WRITE
        uint64_t buffer_offset : 16;  // offset / 1024 (对端 buffer 偏移)
        uint64_t reserved     : 44;
    } io;
};
```

### 8.3 PendingSendSlot (无锁 MPSC slot)

```cpp
// urma_endpoint.h:49-52
struct PendingSendSlot {
    std::atomic<uint64_t> state{0};  // 0=free, nonzero=key
    brpc::urma::UrmaSendContext* ctx{nullptr};
};
// kMaxPendingSends = 4096 (urma_endpoint.h:56)
// _insert_hint: atomic<uint32_t> 插入游标 (Stage2)
```

### 8.4 UrmaRxSlot (接收 slot, 支持分批 READ)

```cpp
// urma_one_sided.h:252-292
struct UrmaRxSlot {
    enum State : uint8_t { IDLE=0, READING=1, DATA_READY=2 };
    butil::atomic<State> state{IDLE};
    uint64_t write_imm{0};
    uint64_t request_id{0};
    uint32_t total_bytes{0};
    uint32_t received_bytes{0};
    uint16_t sq_slots_used{0};      // batch 内剩余 READ 计数
    std::vector<std::pair<void*, size_t>> read_targets;
    std::vector<butil::IOBuf> recv_bufs;
    uint32_t total_blocks{0};
    uint32_t next_read_idx{0};      // 下一批 READ 起始 block
    std::vector<PageBufferInMessage> entries;
    bool pending_retry{false};
};
// URMA_RX_RING_SIZE = 128 (urma_one_sided.h:364)
```

### 8.5 opcode 枚举

```
URMA_IO_WRITE_IN_BAND  = 小包/大包 chunk 数据传输
URMA_IO_PRE_WRITE      = ZeroCopy 控制消息（block 描述符）
URMA_IO_WRITE_IN_BAND_ACK = 数据接收确认
URMA_IO_POST_WRITE     = READ 完成，通知发送端释放 block 引用
```

---

## 九、完整端到端时序图（io_mode=2 小包 1KB）

```
Client                                          Server
  │                                               │
  │  Channel::CallMethod()                        │
  │  └─ IssueRPC()                                │
  │     └─ Socket::KeepWrite()                    │
  │        └─ UrmaEndpoint::CutFromIOBufList()    │
  │           └─ WriteInline()                    │
  │              ├─ RingBuf::Allocate(1048B)      │
  │              ├─ copy_to send_buf              │
  │              ├─ InsertPendingSend(key)        │
  │              └─ urma_post_jetty_send_wr()     │
  │                    │                          │
  │                    │ WRITE_IMM                │
  │                    │ ─────────────────────►  │
  │                    │                          │
  │                    │              PollCq → HandleCompletion(RX)
  │                    │              ├─ HandleWriteImmCompletion()
  │                    │              │  ├─ 读 recv_buf → Head
  │                    │              │  ├─ append to _read_buf
  │                    │              │  └─ ResponseCtrlMessage(ACK)
  │                    │              ├─ PostEmptyRecvWr(1)
  │                    │              └─ DispatchReceivedBytes()
  │                    │                   └─ InputMessenger::ProcessNewMessage()
  │                    │                        └─ 用户 Service handler
  │                    │                          │
  │                    │              (服务端处理 + 序列化响应)
  │                    │                          │
  │                    │              Socket::KeepWrite()
  │                    │              └─ WriteInline()
  │                    │                 ├─ RingBuf::Allocate()
  │                    │                 ├─ copy_to send_buf
  │                    │                 ├─ InsertPendingSend()
  │                    │                 └─ urma_post_jetty_send_wr()
  │                    │                          │
  │                    │              WRITE_IMM   │
  │                    │ ◄─────────────────────  │
  │                    │                          │
  │  PollCq → HandleCompletion(RX)                │
  │  ├─ HandleWriteImmCompletion()                │
  │  │  ├─ append to _read_buf                    │
  │  │  └─ ResponseCtrlMessage(ACK)               │
  │  ├─ PostEmptyRecvWr(1)                        │
  │  └─ DispatchReceivedBytes()                   │
  │       └─ ProcessNewMessage()                  │
  │            └─ cntl->OnCallback()              │
  │                                               │
  │  WRITE_IN_BAND_ACK (响应数据的 ACK)           │
  │  ──────────────────────►                      │
  │                    │                          │
  │              HandleWriteInBandAck()            │
  │              ├─ Release send_buf              │
  │              ├─ return_object(ctx)            │
  │              └─ WakeAsEpollOut()              │
  │                                               │
  │  (同时: 服务端收到请求数据的 ACK)              │
  │  HandleWriteInBandAck()                       │
  │  ├─ Release send_buf                          │
  │  ├─ return_object(ctx)                        │
  │  └─ WakeAsEpollOut()                          │
  │                                               │
  └─ RPC 完成                                     │
```

---

## 十、性能优化要点（对比 UBS v2 参考实现，按 io_mode 分别分析）

> 参考实现：`D:\kunpeng\bpc_urma\ubs方案-单边v2`，brpc UBSocket 分支 `br_urma_rw_0728`。
> UBS v2 的 URMA 路径只有两条：`WriteInline`（≤2000B 内联）和 `WriteZeroCopy`（>2000B 零拷贝 + RDMA READ）。
> 本项目设计了三种 io_mode（0/1/2），每种与 UBS v2 的对应关系和差距各不相同，下面逐一对比。

### 10.1 架构层差异总览

| 维度 | UBS v2 (ubsocket 中间层) | 本项目 (brpc 直集成) | 影响 |
|------|--------------------------|----------------------|------|
| **中间层** | brpc → ubsocket_wrapper → UBSocket(DataTx/DataRx) → URMA | brpc → UrmaEndpoint → URMA | 本项目少一层虚函数 dispatch（`tx_ops_->WriteV`），小包路径省 ~10-20ns |
| **后端支持** | UMQ + URMA 双后端，运行时切换 | 仅 URMA | 本项目无 UMQ 路径，但代码更精简 |
| **符号拦截** | `ubsocket_wrapper_*` 拦截 epoll/socket/writev/readv | 无拦截，直接在 Socket 中集成 | 本项目避免全局符号劫持，但需修改 brpc 内部代码 |
| **Block 管理** | 自有 `Block`/`BlockRef`/`BlockCache`（placement new） | hijack `butil::IOBuf` 的 `blockmem_allocate` | 本项目复用 brpc IOBuf 基础设施，但受 butil Block 语义约束 |
| **send_buf 管理** | `UrmaSockContinuousBuf`（128KB，类似 RingBuf） | `UrmaRingBuf`（8MB，bitmap first-fit + `_alloc_cursor` 游标） | 本项目范围大 64×，但 Stage2 游标缓解搜索开销 |
| **pending 管理** | 无独立结构（Block IncRef/DecRef 控制生命周期） | `PendingSendSlot[4096]` 无锁 slot 数组 | 本项目额外结构开销 |

### 10.2 关键参数对比

| 参数 | UBS v2 | 本项目 | 差异分析 |
|------|--------|--------|----------|
| **inline 阈值** | `URMA_INLINE_DATA_THRESHOLD = 2000B`（硬编码） | `FLAGS_urma_inline_threshold = 16384`（Stage1 从 2048 上调，可配），io_mode=2 大于此值走 Chunked 或 WriteZeroCopy | UBS v2 超过 2000B 即切零拷贝；本项目阈值更高，更多小包走 inline |
| **send_buf 容量** | 128KB (`URMA_SEND_BUF_DEFAULT_KB`) | 8MB (`FLAGS_urma_send_buf_size=8192`, Stage4 测试) | 本项目大 64×，支持 8MB 消息一轮发完 64 chunk |
| **SQ depth** | 1024 (`kDefaultQueueDepth`) | 1024 (`FLAGS_urma_sq_size=1024`, Stage1 测试) / 128 (默认) | Stage1 已对齐 UBS v2 |
| **read SQ depth** | — | 1024 (`FLAGS_urma_read_jetty_sq_size`) | [dual-jetty] READ 专用 SQ，物理隔离 |
| **Block size** | 64KB (`UB_BUF_PAGE_SIZE_KB`) | 64KB (`FLAGS_urma_buffer_size=65536`, Stage4 测试) / 8KB (默认) | Stage4 已对齐 UBS v2 |
| **chunk payload** | — | 2MB (`FLAGS_urma_chunk_payload_size=2095104`, Stage4) / 127KB (默认) | [mega-chunk] 8MB 仅 4 chunk，减少 WR 数 |
| **tseg 段大小** | 16MB (`UB_BUF_BLOCK_SIZE_MB`) | 全局 512MB 一次注册 | UBS v2 分段注册（O(1) 地址映射）；本项目一次注册全池 |
| **max_sge** | 1（设计固定） | 1（所有 URMA 设备 cap） | 两者均 1 SGE/WR，UBS v2 设计固定，本项目硬件/provider 限制 |
| **max_sge_len (SEND)** | — | `GetUrmaSendMaxSgeLen()` 硬编码 4096（不可配） | io_mode=0 SEND 路径固定 4KB，所有 URMA 设备硬件 bug |
| **max_sge_len (WRITE/READ)** | 设备 `max_write_size`/`max_read_size` | `FLAGS_urma_max_sge_len`（默认 4096，可配，测试用 65536） | io_mode=2 WriteZeroCopy READ 路径可放宽，减少 READ WR 数 |
| **batch 提交** | `TX_POST_BATCH_MAX=64`（一次 post 64 WR） | 1 WR/post | UBS v2 batch 减少 post 次数 |
| **READ batch** | 链式 WR（无上限，受 queue depth） | `URMA_READ_BATCH_MAX=256` | 本项目分批 READ + 完成驱动续发 |
| **CQE 合并** | 非 bonding 只对最后 WR 设 complete_enable=1 | 所有 WR complete_enable=1 | UBS v2 非 bonding N→1 CQE；本项目未实施（见 §10.8.3） |

### 10.3 io_mode=0 (SEND_ONLY) 对比分析

#### 10.3.1 路径对应关系

UBS v2 **无** io_mode=0 对应路径。UBS v2 的 URMA 路径全部走 WRITE_IMM（单边），不使用双边 SEND/RECV。UMQ 路径虽用 `UMQ_OPC_SEND_IMM`，但底层语义不同（UMQ 环形缓冲区非 RDMA SEND）。

io_mode=0 是本项目独有的传统 RDMA 双边操作路径，用于兼容性验证和小包零拷贝场景。

#### 10.3.2 发送路径对比

| 环节 | UBS v2 (无对应) | 本项目 io_mode=0 (`CutFromIOBufList_Send`) | 分析 |
|------|----------------|-------------------------------------------|------|
| **opcode** | — | `URMA_OPC_SEND` | 双边操作，需对端 PostRecv |
| **数据拷贝** | — | **零拷贝**：IOBuf block 地址直接作为 SGE | io_mode=0 是三种 mode 中唯一零拷贝发送路径 |
| **SGE 构建** | — | `cut_into_sglist` 从 IOBuf 枚举 block → SGE 数组 | 受 max_sge=1 + `GetUrmaSendMaxSgeLen()=4096`（硬编码）双重限制 |
| **分片** | — | 8KB block 按 4096B 切分 → 每 WR 1 SGE 4KB → 8MB 需 2048 WR | **SQ 压力极大**：8MB 占用 2048 SQ slot，SQ=128 根本无法一次发出。SEND 路径 max_sge_len 不可配 |
| **流控** | — | SQ 窗口 + RQ credit 双窗口 | 需对端 SEND_WITH_IMM 返还 credit |
| **ACK 机制** | — | 对端 PostRecv + SendAck (累积 credit) | 额外 RTT 用于 credit 返还 |

#### 10.3.3 接收路径对比

| 环节 | UBS v2 (无对应) | 本项目 io_mode=0 | 分析 |
|------|----------------|------------------|------|
| **接收数据** | — | `_rbuf[slot].cutn(&_read_buf)` → IOBuf 零拷贝切割 | 接收侧零拷贝 |
| **RQ 补充** | — | PostRecv(1) 每次 + SendAck 累积 | 额外控制开销 |
| **消息组装** | — | 直接 cutn 到 `_read_buf`，无需重组 | 比 io_mode=2 Chunked 简单 |

#### 10.3.4 实测性能与瓶颈

| size | avg_lat | qps | err_rate | 瓶颈分析 |
|------|---------|-----|----------|----------|
| 1KB | 19us | 999.7 | 0% | 零拷贝优势，小包最快 |
| 100KB | 79us | 999.8 | 0% | 100KB/4KB = 25 WR，SQ 可承受 |
| 1MB | 999us | 999.6 | 0% | 1MB/4KB = 256 WR，需多轮 SQ 窗口回收 |
| **8MB** | **22590us** | **404.0** | 0% | 8MB/4KB = **2048 WR**，SQ=128 需 16 轮，**严重瓶颈** |

**io_mode=0 核心差距**：
- **优势**：零拷贝（发送 + 接收均无 memcpy），小包延迟最低（19us vs io_mode=2 的 20us）
- **劣势**：大包 SQ 窗口耗尽（8MB 需 2048 WR，SQ=128），导致 QPS 下降（404 vs io_mode=2 的 499）
- **根因**：SEND 路径 `GetUrmaSendMaxSgeLen()` 硬编码 4096 强制按 4KB 分片，8KB block 也需切成 2 个 SGE，且**不可通过 `FLAGS_urma_max_sge_len` 配置**
- **UBS v2 无此问题**：UBS v2 大包走 WriteZeroCopy（只发描述符），1 个 WR 搞定

#### 10.3.5 io_mode=0 优化方向

| 优化 | 预期收益 | 说明 |
|------|---------|------|
| 增大 IOBuf block 到 64KB | 8MB WR 数 2048→256（8× 减少） | `FLAGS_urma_buffer_size=65536`，但 SEND 路径 `GetUrmaSendMaxSgeLen()=4096` 不可配，64KB block 仍需 16 段切分 |
| 增大 SQ depth 到 1024 | 8MB 一轮发出（2048 WR / 1024 SQ = 2 轮） | 需硬件支持 |
| **结论** | io_mode=0 大包无解 | SEND 路径 max_sge_len=4096 硬编码不可配，只能靠加 SQ 深度缓解，不如 io_mode=2 Chunked |

### 10.4 io_mode=1 (WRITE_ONLY) 对比分析

#### 10.4.1 路径对应关系

UBS v2 的 `WriteInline` 路径（≤2000B）对应本项目 io_mode=1 的**小包**路径。
UBS v2 超过 2000B 走 `WriteZeroCopy`，而 io_mode=1 **所有大小**都走 `WriteInline`（无阈值分流，无大包零拷贝路径）。注意：io_mode=1 不使用 `FLAGS_urma_inline_threshold`，代码中 `if (_io_mode == 1)` 直接调用 `WriteInline`。

#### 10.4.2 发送路径对比

| 环节 | UBS v2 WriteInline (≤2000B) | 本项目 io_mode=1 WriteInline (全尺寸) | 差距 |
|------|----------------------------|--------------------------------------|------|
| **阈值** | ≤2000B 走 inline，>2000B 走 ZeroCopy | **无阈值，全走 inline** | 本项目大包也走 inline |
| **数据拷贝** | 1 次 `CopyFromIovToSendBuf` | 1 次 `IOBuf::copy_to` | 一致（≤2KB 范围） |
| **send_buf 分配** | `UrmaSockContinuousBuf::Allocate`（128KB 池） | `UrmaRingBuf::Allocate`（2MB 池） | UBS v2 范围小 16× |
| **SGE** | 1 SGE，addr=send_buf+offset | 1 SGE，addr=send_buf+offset | 一致 |
| **POST** | `urma_post_jetty_send_wr(WRITE_IMM)` | `urma_post_jetty_send_wr(WRITE_IMM)` | 一致 |

**大包问题**：io_mode=1 对 8MB 消息也调用 `WriteInline`，需在 send_buf 中分配 8MB 连续空间。但 send_buf 只有 2MB，**分配失败**。

#### 10.4.3 接收路径对比

| 环节 | UBS v2 (>2000B 走 ZeroCopy) | 本项目 io_mode=1 (全走 WriteInline) | 差距 |
|------|----------------------------|-------------------------------------|------|
| **大包接收** | RDMA READ 拉取到 Block（零拷贝） | recv_buf → memcpy 到 `_read_buf` | 本项目有 memcpy |
| **ACK** | POST_WRITE（1 次） | WRITE_IN_BAND_ACK（1 次） | 一致 |

#### 10.4.4 实测性能与瓶颈

| size | avg_lat | qps | err_rate | 瓶颈分析 |
|------|---------|-----|----------|----------|
| 1KB | 20us | 999.8 | 0% | 正常，与 io_mode=2 小包一致 |
| 100KB | 71us | 999.7 | 0% | 正常（100KB < 2MB send_buf） |
| 204KB | 126us | 999.7 | 0% | 正常 |
| **1MB** | **0** | **944.5** | **99.97%** | **send_buf 2MB 够用但接近上限，疑似 ACK 处理问题** |
| **8MB** | **ERROR** | — | — | **send_buf 2MB 无法分配 8MB 连续空间** |

**io_mode=1 核心差距**：
- **小包（≤204KB）正常**：路径与 UBS v2 WriteInline 基本一致
- **大包（≥1MB）故障**：
  - 8MB：send_buf 2MB < 8MB，`UrmaRingBuf::Allocate` 失败
  - 1MB：send_buf 够用但 err_rate=99.97%，疑似大包 ACK 逻辑或 send_buf 碎片化问题
- **UBS v2 无此问题**：UBS v2 >2000B 即切 WriteZeroCopy，不在 send_buf 中放大包

#### 10.4.5 io_mode=1 优化方向

| 优化 | 预期收益 | 说明 |
|------|---------|------|
| 1MB 误码修复 | 1MB 可用 | 排查 1MB err_rate=99.97% 根因（ACK 丢失？send_buf 碎片？） |
| 大包切到 Chunked | 8MB 可用 | io_mode=1 大包复用 io_mode=2 的 `WriteInlineChunked`（分 chunk 发送） |
| **结论** | io_mode=1 定位为小包模式 | 全 inline 路径与 UBS v2 WriteInline 对齐，但大包应回退到 Chunked |

### 10.5 io_mode=2 (HYBRID) 对比分析

#### 10.5.1 路径对应关系

| 消息大小 | UBS v2 | 本项目 io_mode=2 | 对应关系 |
|---------|--------|-----------------|----------|
| ≤ `urma_inline_threshold`（默认 2048B） | WriteInline (内联) | WriteInline (内联) | **直接对应** |
| > threshold ~ 127KB | WriteZeroCopy (零拷贝 + READ) | WriteInlineChunked (分 chunk 内联) | **路径不同** |
| > 127KB | WriteZeroCopy (零拷贝 + READ) | WriteInlineChunked (分 chunk 内联) | **路径不同** |

#### 10.5.2 小包路径（≤ `urma_inline_threshold`）对比

| 环节 | UBS v2 WriteInline | 本项目 io_mode=2 WriteInline | 差距 |
|------|-------------------|------------------------------|------|
| **brpc 层** | 内联 Write，不启动 KeepWrite | 内联 Write，不启动 KeepWrite | 一致 |
| **分发层** | `DataTx::WriteV → tx_ops_->WriteV`（虚函数） | `UrmaEndpoint::KeepWrite → WriteInline`（直接调用） | 本项目少 1 层虚函数 |
| **内存拷贝** | 1 次 `CopyFromIovToSendBuf`（iov → send_buf） | 1 次 `IOBuf::copy_to`（IOBuf → send_buf） | 一致（≤ threshold 范围），均 ~50-100ns |
| **SGE 构建** | `SetupSgeContext` resize(1) | 手动构建 1 SGE | 一致 |
| **POST** | `urma_post_jetty_send_wr(WRITE_IMM)` | `urma_post_jetty_send_wr(WRITE_IMM)` | 一致 |
| **实测延迟** | ~0.8us (c_post) | ~1.7us (c_post, 早期) / Stage4 端到端 18us (1KB) | **Stage4 差距缩小** |

**Stage4 后小包差距分析**（1KB: 18us vs UBS v2 16.33us = +1.67us）：
- ~0.1~0.2us：UrmaRingBuf bitmap first-fit 搜索（Stage2 `_alloc_cursor` 游标已缓解，8MB→8192 unit）
- ~0.1us：`_pending_sends` slot 数组扫描（Stage2 `_insert_hint` 游标已缓解）
- ~0.2us：`UrmaSendContext` 对象池 get/return（UBS v2 无此结构）
- ~0.3us：butil::IOBuf `copy_to` 开销（UBS v2 直接从 iov 构建，不经过 IOBuf 层）
- 8KB 已反超 UBS v2（24us vs 28.67us），inline_threshold=16384 覆盖 8KB 走单包 WriteInline

#### 10.5.3 大包路径（8MB）对比

| 环节 | UBS v2 WriteZeroCopy | 本项目 io_mode=2 (Stage4) | 差距 |
|------|---------------------|---------------------------|------|
| **数据拷贝** | **0 次**（只发 16B 描述符） | **2 次**（send: copy_to 8MB→send_buf; recv: copy_from recv_buf→read_buf） | 核心差距，但被流水线掩盖 |
| **网络轮次** | 1 RTT (PRE_WRITE) + N RTT (READ) | 8 RTT（4×2MB WRITE_IMM + 4×ACK, mega-chunk） | Stage4 mega-chunk 后轮次大减 |
| **SQ 占用** | 1 WR (PRE_WRITE) + N WR (READ, 链式) | 8 WR (4 WRITE + 4 ACK) | 本项目 SQ 压力已大减 |
| **send_buf 占用** | 仅控制消息 ~2KB | 4 × 2MB = 8MB（send_buf=8MB 恰好容纳） | Stage4 send_buf=8MB 对齐 |
| **带宽利用率** | RDMA READ 满带宽（硬件直接拉取） | WRITE_IMM 推送，逐 chunk 流水线 | UBS v2 略高 |
| **接收端复杂度** | BuildReadWrs + 分配本地 Block + READ 完成 → copy | 直接 memcpy recv_buf → read_buf | 本项目更简单 |
| **实测延迟 (qps=500)** | 3153us | **3241us** | **+88us（+2.8%）** |

**Stage4 后差距缩小的原因**：
- mega-chunk 2MB：8MB 仅 4 chunk（原 65 chunk），WR 数从 132 降到 8
- send_buf 8MB：4 chunk 全部并发，无需多轮等待 ACK
- SQ 1024：4 WR 占用 < 1% 窗口
- 残留 +88us 主要来自 2 次 memcpy（8MB×2=16MB 拷贝）+ 4 RTT 控制延迟
- Stage3 非 bonding CQE 合并可进一步缩小 gap（WriteZeroCopy READ 路径收益）

#### 10.5.4 接收路径对比

| 环节 | UBS v2 | 本项目 io_mode=2 | 差距 |
|------|--------|------------------|------|
| **CQ 轮询** | JFCE 中断 → `epoll_wait` → 内联 `PollAndDispatch` | busy-poll（`FLAGS_urma_use_polling=true`） | 本项目 P0 优化已消除中断延迟 |
| **事件通知** | `RecordReadableSocket` → 环形缓冲 → `epoll_wait` 返回 | PollCq 线程直接 `NotifySocketReadable` → eventfd | 本项目更直接 |
| **小包数据组装** | `BlockCache::Insert`（placement new） + `CutAndInsertAfter` | `HandleWriteImmCompletion` → memcpy 到 `_read_buf` | **UBS v2 零拷贝；本项目有 memcpy** |
| **大包数据组装** | RDMA READ → Block → `CutAndInsertAfter` | 66 chunk 逐个 memcpy → `UrmaReasmCtx` 重组 → append | **UBS v2 零拷贝；本项目 66 次 memcpy** |
| **c_event 延迟** | ~1-2us（中断驱动） | ~1.5us（busy-poll） | 本项目 P0 优化后更优 |

#### 10.5.5 实测性能（Stage4, 2026-09-27, io_mode=2, dual-jetty + zerocopy_read）

| size | qps=1000 avg_lat | qps=500 avg_lat | err_rate | 分析 |
|------|------------------|-----------------|----------|------|
| 1KB | 18us | 19us | 0% | 小包最优路径 |
| 4KB | 21us | 23us | 0% | 小包 |
| 8KB | 24us | 24us | 0% | 小包 |
| 100KB | 54us | 54us | 0% | 100KB < 2MB chunk, 单 chunk WriteInline |
| 200KB | 77us | 80us | 0% | 200KB < 2MB chunk, 单 chunk |
| 1MB | 358us | 366us | 0% | 1MB < 2MB chunk, 单 chunk WriteInline |
| **8MB** | **3900us** | **3241us** | 0% | 8MB / 2MB = 4 chunk (mega-chunk) |

**io_mode=2 核心进展**（Stage4 vs 早期 42 矩阵）：
- **8MB qps=500**：14513us → 3241us（**-78%**），mega-chunk + 8MB send_buf + SQ=1024 联合收益
- **1MB**：761us → 358us（qps=1000），chunk=2MB 后 1MB 单 chunk 发送
- **100KB**：76us → 54us（-29%），inline_threshold=16384 覆盖 100KB 走 WriteInline 单包
- **小包 1KB**：20us → 18us（-10%），Stage2 RingBuf cursor + pending hint + RetryPendingReads guard
- **0% segfault**（segfault-fix 后，修复前 ~70% 退出崩溃）

#### 10.5.6 io_mode=2 优化方向（Stage4 后）

**Stage3: 非 bonding CQE 合并** — 缩小 8M +88us gap

```
当前: 所有 READ WR complete_enable=1 → N CQE (8MB = 128 CQE)
优化后: 非 bonding 只对最后 WR 设 complete_enable=1 → 1 CQE
```

| 维度 | 当前 (Stage4) | Stage3 目标 (非 bonding) | UBS v2 (参考) |
|------|---------------|--------------------------|---------------------------|
| READ CQE 数 | 128 (每 WR 1 CQE) | 1 (只最后 WR signal) | 1 (非 bonding) |
| PollCq 循环 | 128 次 | 1 次 | 1 次 |
| HandleReadCompletion 调用 | 128 次 | 1 次 | 1 次 |
| 预计收益 (8M) | — | -20~50us | 基准 |
| 预计收益 (1M) | — | -3~8us | 基准 |
| bonding 设备 | 保持 N CQE | 不变（provider 要求） | 同本项目当前 |

**WriteZeroCopy 已激活**（`FLAGS_urma_use_zerocopy_read=true`）：
- dual-jetty 物理隔离 READ+WRITE_IMM，消除 status=8
- Stage4 测试中 io_mode=2 大包仍主要走 WriteInlineChunked（流水线推送更优）
- WriteZeroCopy 作为备选路径，在 ≥16MB 超大包时优势显现

### 10.6 三种 io_mode 横向对比（8MB 场景，Stage4 配置）

| 维度 | io_mode=0 (SEND) | io_mode=1 (WRITE) | io_mode=2 (HYBRID, Stage4) | UBS v2 (WriteZeroCopy) |
|------|------------------|-------------------|----------------------------|-----------------------|
| **8MB 延迟 (qps=500)** | 22590us (早期) | ERROR | **3241us** | ~3153us |
| **8MB QPS** | 404 (早期) | — | **499.8** | 499.9 |
| **数据拷贝** | 0 次（零拷贝） | — | 2 次 (WriteInlineChunked) / 0 次 (WriteZeroCopy) | 0 次 |
| **WR 数** | 2048 (SEND) | — | 8 (4 WRITE + 4 ACK, mega-chunk 2MB) | 1 + 128 (1 PRE_WRITE + 128 READ) |
| **SQ 轮次** | 16 (2048/128) | — | 1 (4/1021) | 1 (链式) |
| **瓶颈** | `GetUrmaSendMaxSgeLen()=4096` 不可配 → 2048 WR | send_buf < 8MB | 2 次 memcpy (Chunked) / READ CQE 数 (ZeroCopy) | 2 RTT 控制延迟 |
| **可用性** | 可用但慢 | **不可用** | **可用且最优** | 可用且最优 |
| **vs UBS v2** | 远慢 | — | **+88us (qps=500)** | 基准 |

### 10.7 已实现优化（P0/P1/Stage1/Stage2/Stage4/segfault-fix）

| 优化 | 影响的 io_mode | 位置 | 效果 | 对标 UBS v2 |
|------|---------------|------|------|-------------|
| **P0: CQ busy-poll** | 0/1/2 | `FLAGS_urma_use_polling=true` | c_event 4.8→1.5us | UBS v2 用中断+epoll(~1-2us)；本项目更优但 CPU 100% |
| **P1: 对象池** | 1/2 | `butil::get_object/return_object` | new/delete 200-500ns → 24-67ns | UBS v2 用 BlockCache placement new |
| **P1: 无锁 MPSC slot** | 1/2 | `PendingSendSlot[4096] + CAS` | 消除 mutex 竞争 ~100-300ns | UBS v2 无独立 pending 管理 |
| **P1: Spinlock** | 1/2 | `UrmaRingBuf::_mutex` → `pthread_spinlock_t` | mutex → spinlock | UBS v2 范围更小（128KB vs 8MB） |
| **Chunked WRITE_IN_BAND** | 2 | `WriteInlineChunked` | 8MB 分 N×chunk，避免 SQ 耗尽 | UBS v2 无此路径 |
| **[Stage1] inline_threshold 16384** | 2 | `FLAGS_urma_inline_threshold` | 2048→16384，100K/200K 走单包 WriteInline | UBS v2 2000B 即切 ZeroCopy |
| **[Stage1] SQ 1024 + read_jetty_sq 1024** | 0/2 | `FLAGS_urma_sq_size` | 128→1024，8M qps=1000 SQ 窗口压力降低 | UBS v2 SQ=1024 已对齐 |
| **[Stage1] mega-chunk 2MB** | 2 | `FLAGS_urma_chunk_payload_size` | 127KB→2MB，8MB 仅 4 chunk（原 65 chunk） | UBS v2 无 chunk（直接 ZeroCopy） |
| **[Stage2] RingBuf _alloc_cursor** | 1/2 | `UrmaRingBuf::_alloc_cursor` | first-fit 游标，小包 c_post -0.1~0.2us | UBS v2 128KB 范围小，无需游标 |
| **[Stage2] PendingSend _insert_hint** | 1/2 | `_insert_hint` | 插入游标，小包 -0.1us | UBS v2 无 pending 结构 |
| **[Stage2] RetryPendingReads guard** | 2 | `_pending_retry_count` 空 guard | 1M 修复 +13us 回归（255×128 空扫描） | UBS v2 无此问题 |
| **[Stage2] 栈数组 SGE** | 2 | WriteZeroCopy `alloca` 替代 vector | 减少堆分配 | UBS v2 栈分配 |
| **[Stage4] dual-jetty 无锁 post** | 2 | 5 处 `need_lock` 跳过 `_zerocopy_post_mutex` | 8M qps=500 -47us, qps=1000 200K -3us | UBS v2 无锁（无并发冲突） |
| **[segfault-fix] 引用计数 + butex** | 0/1/2 | `_resource_refcnt` + `_shutdown_butex` | 退出 segfault 70%→0% | UBS v2 无此竞态（生命周期不同） |

### 10.8 残留差距与优化方向（按 io_mode 分类, Stage4 后状态）

#### 10.8.1 io_mode=0 专属差距

| 差距 | 现状 | UBS v2 | 优化方向 | 预期收益 |
|------|------|--------|---------|---------|
| `GetUrmaSendMaxSgeLen()=4096` 不可配导致大包 WR 爆炸 | 8MB → 2048 WR | 无此问题（不走 SEND） | 增大 IOBuf block 到 64KB | 8MB WR 数 2048→256（8× 减少），但仍慢于 io_mode=2 |
| **结论** | io_mode=0 大包无竞争力 | — | 定位为兼容/小包零拷贝模式 | — |

#### 10.8.2 io_mode=1 专属差距

| 差距 | 现状 | UBS v2 | 优化方向 | 预期收益 |
|------|------|--------|---------|---------|
| 8MB send_buf 分配失败 | ERROR | >2000B 走 ZeroCopy | 大包回退到 Chunked | 8MB 可用 |
| **结论** | io_mode=1 定位为小包模式 | — | 大包应回退到 Chunked 或禁止 | — |

#### 10.8.3 io_mode=2 专属差距（Stage4 后）

| 差距 | 现状 (Stage4) | UBS v2 | 优化方向 | 预期收益 |
|------|---------------|--------|---------|---------|
| **8M qps=500 +88us gap** | 3241us | 3153us | Stage3: 非 bonding CQE 合并 | -20~50us (8M), -3~8us (1M) |
| **非 bonding READ CQE 数** | 128 CQE (每 WR 1 CQE) | 1 CQE (只最后 WR signal) | PostReadBatch complete_enable 只对最后 WR 设 1 | 8M 省 127 次 PollCq + HandleReadCompletion |
| **WriteInlineChunked 2 次 memcpy** | 8MB 拷贝 16MB | 0 次 (WriteZeroCopy) | zerocopy_read=true 已激活 WriteZeroCopy | 已实现，但 8M 仍用 Chunked 更优（流水线） |
| **无 batch post** | 4 次单独 post (mega-chunk) | 一次 64 WR batch | P3.5: 累积 WR 批量 post | post 次数减少，但 mega-chunk 后 WR 数已大减 |
| **RingBuf 搜索 8MB** | 最坏 8192 unit 搜索 | 128 unit | [Stage2 已实施] `_alloc_cursor` 游标 | 已缓解，小包 -0.1~0.2us |
| **pending_sends 扫描** | 4096 slot 线性扫描 | 无 pending 结构 | [Stage2 已实施] `_insert_hint` 游标 | 已缓解，小包 -0.1us |

**Stage3 计划**（`shiny-popping-marshmallow.md`，未实施）：
非 bonding 设备 READ batch CQE 合并（N→1）。当前所有 READ WR 设 `complete_enable=1`（N CQE），UBS v2 非 bonding 只对最后 WR 设 1（1 CQE）。需配套改动 4 处：
1. PostReadBatch `complete_enable` 只对最后 WR 设 1（非 bonding）
2. HandleCompletion TX 路径批量回收 `slot.sq_slots_used` 个 SQ slot（非 bonding）
3. HandleReadCompletion 一次性累加整个 batch（非 bonding）
4. 错误路径批量回收（非 bonding）

预计 8M -20~50us，1M -3~8us，100K/200K -0.5~1us。bonding 设备路径保持不变。

### 10.9 优化优先级矩阵（Stage4 后更新）

| 优先级 | 优化项 | 目标 io_mode | 目标场景 | 预期收益 | 难度 | 状态 |
|--------|--------|-------------|---------|---------|------|------|
| ~~P0~~ | CQ busy-poll | 0/1/2 | 全场景 | c_event 4.8→1.5us | 低 | ✅ 已完成 |
| ~~P1~~ | 对象池 + 无锁 MPSC + Spinlock | 1/2 | 小包 | -100~300ns | 低 | ✅ 已完成 |
| ~~Stage1~~ | inline_threshold 16384 + SQ 1024 + mega-chunk 2MB | 2 | 大包 | 8M -78% | 低 | ✅ 已完成 |
| ~~Stage2~~ | RingBuf cursor + pending hint + RetryPendingReads guard + 栈数组 | 1/2 | 全场景 | 1M 修复回归, 小包 -0.1~0.2us | 低 | ✅ 已完成 |
| ~~Stage4~~ | dual-jetty 无锁 post | 2 | zerocopy_read | 8M qps=500 -47us | 中 | ✅ 已完成 |
| ~~segfault-fix~~ | 引用计数 + butex 等待 | 0/1/2 | 退出阶段 | segfault 70%→0% | 中 | ✅ 已完成 |
| **Stage3** | 非 bonding CQE 合并 (N→1) | 2 | ≥100K 大包 | 8M -20~50us, 1M -3~8us | 中 | ⏳ 计划中 |
| P3.5 | batch post | 2 | 大包 | post 次数减少 | 中 | 待定（mega-chunk 后收益降低） |
| P1.5 | io_mode=1 大包回退 Chunked | 1 | ≥1MB | 8MB 可用 | 低 | 待定 |

### 10.10 性能数据基准（Stage4, 2026-09-27, io_mode=2 主推模式）

Stage4 矩阵测试结果（dual-jetty + zerocopy_read, inline_threshold=16384, SQ=1024, read_jetty_sq=1024, send/recv_buf=8MB, chunk=2MB, buffer=64KB）：

| size | qps=1000 avg_lat | qps=500 avg_lat | UBS v2 (qps=500) | vs UBS v2 | err_rate |
|------|------------------|-----------------|------------------|-----------|----------|
| 1KB | 18us | 19us | 16.33us | +1.67us | 0% |
| 4KB | 21us | 23us | 20.00us | +1.0us | 0% |
| 8KB | 24us | 24us | 28.67us | **-4.67us** | 0% |
| 100KB | 54us | 54us | 48.67us | +5.33us | 0% |
| 200KB | 77us | 80us | 75.00us | +2.0us | 0% |
| 1MB | 358us | 366us | 356.67us | +0.33us | 0% |
| **8MB** | **3900us** | **3241us** | **3153.33us** | **+87.67us** | 0% |

**关键结论**：
- **io_mode=2 Stage4 是主推模式**：全尺寸 0% error，0% segfault
- **8KB 已反超 UBS v2**：24us vs 28.67us（-4.67us），inline_threshold=16384 覆盖 8KB 走单包 WriteInline
- **1MB 基本对齐**：366us vs 356.67us（+0.33us），mega-chunk 2MB 后 1MB 单 chunk
- **8M qps=500 残留 +88us gap**：3241us vs 3153us，Stage3 非 bonding CQE 合并可缩小
- **小包 1K/4K 残留 +1~2us**：RingBuf 搜索 + pending 扫描 + IOBuf 开销（Stage2 已部分优化）
- **100K 残留 +5us**：单 chunk WriteInline，memcpy + 控制消息开销
