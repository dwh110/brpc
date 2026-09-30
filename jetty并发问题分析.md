# URMA Jetty 并发问题分析:双 jetty 分离方案的背景与根因

> 本文分析 brpc 原生 URMA 传输层在 `io_mode=2`(HYBRID)大包路径下的 `status=8`(`URMA_CR_REM_ACCESS_ABORT_ERR`)问题,对比 UBS v2 两种方案(hcom service 层 / UBSocket 直连)的并发隔离机制,说明双 jetty 分离方案的必要性。

---

## 一、问题现象

`io_mode=2`(HYBRID)的 **WriteZeroCopy 路径**(PRE_WRITE + READ)在并发负载下出现 `status=8`(`URMA_CR_REM_ACCESS_ABORT_ERR`),TP 进入死状态、不再产生任何完成事件。

实测阈值(2026-09-26 验证,非 bonding 专属):

| 设备 | qd=1 | qd=4 | qd=8 |
|---|---|---|---|
| `bonding_dev_0` | OK | err 2–6% | err >70% |
| `udmac0d1e2`(物理 UDMA) | OK | err 57% | err 99.9% |

错误落在 **TX 完成端**,`user_ctx` 类型是 `CTRL_DATA_REQUEST`(即 WRITE_IMM 的完成),代码将其判定为 fatal 并标记连接失败。

---

## 二、brpc 原生 URMA:根因——同一进程内两个 bthread 并发 post 到同一 jetty SQ

### 2.1 两条并发路径

`status=8` 的并发重叠发生在**单个进程内部**,不是跨进程。

以客户端进程为例(服务端对称):

| 路径 | 线程 | 提交的 WR | post 位置 |
|---|---|---|---|
| `PostReadBatch` | **PollCq bthread**(CQ 完成轮询) | `URMA_OPC_READ` | `urma_endpoint.cpp:2606` |
| `WriteZeroCopy` | **KeepWrite bthread**(socket 写) | `URMA_OPC_WRITE_IMM` | `urma_endpoint.cpp:1679` |

单 jetty 模式下,两者向**同一个** `_resource->jetty` 提交 WR。qd≥4 时两条路径时间上重叠:KeepWrite 在 post 下一请求的 PRE_WRITE(WRITE_IMM),同时 PollCq 在 post 上一请求响应的 READ。

### 2.2 并发重叠图示

```
客户端进程,同一个 _resource->jetty (单 jetty 模式)
┌─────────────────────────┐    ┌─────────────────────────┐
│ KeepWrite bthread       │    │ PollCq bthread          │
│ WriteZeroCopy           │    │ HandleWriteImmCompletion│
│   urma_post_jetty_send_ │    │   PostReadBatch         │
│   wr(JETTY, WRITE_IMM)  │    │     urma_post_jetty_    │
│   (:1679)               │    │     send_wr(JETTY, READ)│
│                         │    │     (:2606)             │
└───────────┬─────────────┘    └───────────┬─────────────┘
            │                              │
            ▼                              ▼
       ┌──────────────────────────────────────┐
       │  同一个 jetty SQ                      │
       │  WRITE_IMM 与 READ WR 交错驻留        │
       │  → URMA TP 拒绝并发处理 → status=8    │
       └──────────────────────────────────────┘
```

### 2.3 与"乱序"的关系

是 **SQ 内部 WR 执行交错** 问题,更精确的描述是**操作类型不兼容的并发**:

- 不是"同一请求内 WR 顺序被打乱"——单请求 WR 链表顺序由 `next` 指针保证
- 是**不同请求的 READ WR 与 WRITE_IMM WR 在同一 SQ 中交错驻留**,URMA jetty SQ 对这两类操作的并发处理存在硬件/driver 限制,触发 `REM_ACCESS_ABORT_ERR`

### 2.4 应用层 mutex 为何治标不治本

`_zerocopy_post_mutex`(`urma_endpoint.h:460`)只串行化 `urma_post_jetty_send_wr` 的**调用瞬间**。但该函数返回后,WR 仍在 provider/硬件内部排队执行;mutex 无法阻止"已提交的 READ"与"已提交的 WRITE_IMM"在 SQ 内部被并发调度。这就是 qd=1/2 能过(基本串行)、qd≥4 失败(真正重叠)的原因——mutex 只压住了 post 调用重叠,没压住 WR 执行重叠。

---

## 三、为什么 brpc 不能通过 bthread 间同步机制解决

### 3.1 brpc 已有 bthread 同步原语

brpc 的 `WriteRequest` 里有 `id_wait`(`socket.cpp:318`),KeepWrite 写完后调 `ReturnSuccessfulWriteRequest` → `bthread_id_error(id_wait, 0)`(`:521`)通知调用方。bthread 侧用 `bthread_id_join` / `butex_wait` 等待。所以"bthread 间同步"在 brpc 里是现成的。

**问题不在于有没有同步原语,而在于同步什么、等什么完成。**

### 3.2 brpc 的"完成"语义和同步 RPC 根本不同

| | UBS v2 的 sem_wait | brpc 的 id_wait |
|---|---|---|
| 等的是什么 | **整个 RPC 完成**(对端收到响应) | **数据写入 SQ 完成**(WR post 成功) |
| 阻塞时长 | 一个 RTT(几十 us) | 几 us(post 完就通知) |
| 通知来源 | worker 线程收到对端响应 CQE | KeepWrite 自己 post 完就通知 |

brpc 的 `id_wait` 在 **KeepWrite 把 WRITE_IMM post 到 SQ 之后就通知调用方**。它不等对端确认,不等 READ 完成。这是**流水线**语义。

### 3.3 同步等待会摧毁 brpc 的流水线

KeepWrite 的循环(`socket.cpp:1814-1883`):

```cpp
do {
    nw = s->DoWrite(req);           // post WRITE_IMM #N
    // ↓ 不等 #N 完成,继续取链表下一个
    while (req->next != NULL && req->data.empty()) {
        req = req->next;            // ← 流水线:立刻处理 #N+1
        s->ReturnSuccessfulWriteRequest(saved_req);  // 通知 #N 调用方"发出去了"
    }
    if (nw <= 0) {
        s->_transport->WaitEpollOut(...);  // 只在 SQ 满时才等
    }
} while (1);
```

如果改成"post 一条等一条":
1. **qd 永远是 1**——同一时刻只有 1 个 in-flight 请求,qps 暴跌到 1/RTT
2. **`_write_head` 链表堆积**——所有调用 `Write()` 的 bthread 都在等 KeepWrite
3. **失去 bthread 并发优势**——N 并发退化为 1

### 3.4 "只锁 post 调用"也不够

这正是当前 `_zerocopy_post_mutex` 做的事——只串行化 `urma_post_jetty_send_wr` 的调用瞬间,管不到 provider 内部 SQ 的 WR 执行顺序。

### 3.5 三种方案对比

| 方案 | 能否隔离 | 代价 |
|---|---|---|
| **bthread 同步(post 后等完成)** | 能 | qd=1,qps 暴跌,摧毁流水线 |
| **bthread 同步(只锁 post 调用)** | **不能** | mutex 只串行调用瞬间,管不到 SQ 内部执行 |
| **双 jetty** | 能 | 多一个 jetty 资源,无性能损失 |

---

## 四、UBS v2 方案一:hcom service 层(信号量同步隔离)

### 4.1 线程模型

| 线程 | 职责 |
|---|---|
| UBWorker poll 线程(`RunInThread`) | 忙轮询/事件轮询 CQ → `ProcessPollingResult` → 调用回调(`mNewRequestHandler`/`mSendPostedHandler`/`mOneSideDoneHandler`) + `RePostReceive` |
| 用户业务线程 | 同步调 `Send`/`Recv`/`Get` → `PostSend`/`PostRead` → `sem_wait` 阻塞等完成 |

### 4.2 关键代码证据

- `HcomChannelImp::SyncSendInner`(`service_channel_imp.cpp:516-568`):`ep->PostSend` 后 `syncParam.Wait()`(`:566`)阻塞
- `HcomServiceSelfSyncParam::Wait`(`service_common.h:427`):`sem_wait(&sem)` 阻塞
- `OneSideSyncWithSelfPoll`(`:2002`):`ep->PostRead` + `ep->WaitCompletion`(`:2031,2040`)同步串行
- `DispatchByContexetInfoType`(`ub_worker.h:399-403`):worker 线程只调回调,不 post 数据 WR

### 4.3 并发隔离时序图

```
═══════════════════════════════════════════════════════════════════════════
 客户端进程                                          服务端进程
═══════════════════════════════════════════════════════════════════════════

[用户业务线程]                                      [UBWorker poll 线程]
  │                                                  │ RunInThread: while(!stop)
  │ Send(req)                                        │   BusyPolling(轮询 CQ)
  │ ├─ PostSend(WRITE_IMM: RNDV请求)  ──网络──→     │   收到 RECEIVE CQE
  │ │   内含: 客户端数据buf地址+rkey                 │   ├─ mNewRequestHandler(同步回调)
  │ ├─ sem_wait  ← 用户线程阻塞                     │   │   ├─ brpc服务回调
  │ │   (挂起,不再发新 WR)                          │   │   │   └─ 用户Recv()
  │ │                                                │   │   │       └─ PostRead(READ #1)
  │ │                                                │   │   │           → SQ ← READ #1 入队
  │ │                                                │   │   │           (worker线程在此阻塞,
  │ │                                                │   │   │            不会同时post WRITE_IMM)
  │ │                                                │   │   └─ 回调返回
  │ │                                                │   └─ 继续轮询 CQ
  │ │                                                │
  │ │                                                │   收到 READ #1 完成 CQE
  │ │ ←───────网络────── READ 操作直接读客户端内存──┤   ├─ mOneSideDoneHandler
  │ │   (READ是单边操作,客户端CPU不参与)            │   │   └─ 通知用户:数据拉取完成
  │ │                                                │   └─ 继续轮询 CQ
  │ │                                                │
  │ │                                                │ [用户业务线程(另一个brpc worker)]
  │ │                                                  │ 数据处理完,准备响应
  │ │                                                  │ Send(resp)
  │ │                                                  │ ├─ PostSend(WRITE_IMM: 响应)
  │ │                                                  │ │   → SQ ← WRITE_IMM 入队
  │ │                                                  │ │   (此时SQ里没有READ,READ已完成)
  │ │                                                  │ └─ sem_wait  ← 阻塞
  │ │                                                  │
  │ │                                                  │ [UBWorker poll 线程]
  │ │                                                  │   收到 SEND 完成 CQE
  │ │                                                  │   ├─ mSendPostedHandler
  │ │                                                  │   │   └─ sem_post → 唤醒服务端用户线程
  │ │                                                  │   └─ 继续
  │ │                                                │
  │ 收到 RECEIVE CQE                                  │
  │ (服务端发来的WRITE_IMM响应)                       │
  │ ├─ mNewRequestHandler                             │
  │ │   └─ 通知用户:响应到达                          │
  │ │       └─ sem_post → 唤醒客户端用户线程          │
  │ │                                                │
  ▼ sem_wait 返回                                    │
  处理响应                                            │
  发起下一个请求...                                   │
═══════════════════════════════════════════════════════════════════════════
```

### 4.4 隔离机制

**信号量交替同步**:
1. worker 线程在**回调里阻塞**时 → 不会 post WRITE_IMM,只 post READ
2. 用户线程在 **sem_wait 阻塞**时 → 不会 post READ,只 post WRITE_IMM

两者用信号量交替唤醒,SQ 里同一时刻只有一种操作类型。

### 4.5 局限性

这是 **同步 RPC 语义**自带的串行性——用户调 `Send` 就是等响应。brpc 的异步流水线模型无法照搬,因为强行加同步等待等于 qd=1,失去异步框架的意义。

---

## 五、UBS v2 方案二:UBSocket 直连 URMA(单 EventDispatcher 线程串行)

### 5.1 架构

brpc → ubsocket_wrapper → UBSocket(UrmaSocket/UrmaTxOps/UrmaRxOps)→ URMA,不走 UMQ,不走 hcom service 层。

### 5.2 线程模型

| 线程 | 职责 |
|---|---|
| brpc EventDispatcher 线程 | `epoll_wait(JFCE fd)` → `HandleJfceEvent` → `PollAndDispatch` → `HandleCqe` → `HandleRecvCqe` → `IssueReadRequest` → `urma_post_jetty_send_wr(READ)`,全程内联 |
| Client bthread / brpc worker | 发送端 `Write → WriteZeroCopy → PostWriteImm(WRITE_IMM)`,小包内联完成 |

默认路径 A:CQE 处理和 brpc 消息处理在**同一次 `epoll_wait` 调用中内联完成**,无跨线程开销。

### 5.3 并发隔离完整时序图

```
═══════════════════════════════════════════════════════════════════════════════════
 客户端进程                                          服务端进程
═══════════════════════════════════════════════════════════════════════════════════

[Client bthread]                                    [brpc EventDispatcher 线程]
  │                                                  │ epoll_wait(JFCE fd, timeout)
  │ Channel::CallMethod                             │   (阻塞等 CQE)
  │ ├─ sock->Write(&buf, opt)                       │
  │ │   StartWrite(req)                             │
  │ │   _write_head.exchange(req)                   │
  │ │   [prev==NULL → 获得写权]                      │
  │ │   cut_into_fd(fd)                             │
  │ │   ubsocket_writev → UrmaTxOps::WriteV         │
  │ │   [64KB > 2000B → WriteZeroCopy]              │
  │ │   ├─ BuildBlockRefs(iov→BlockRef数组)          │
  │ │   ├─ 填充 PageBufferInMessage[addr,size,flags]│
  │ │   ├─ 每个 block->IncRef()                     │
  │ │   ├─ SetupSgeContext                          │
  │ │   │   src_sge: send_buf+描述符                │
  │ │   │   dst_sge: rmt_recv_buf+offset            │
  │ │   └─ PostWriteImm(PRE_WRITE)                  │
  │ │       urma_post_jetty_send_wr(WRITE_IMM)      │
  │ │       [只发描述符,不拷贝数据]      ──网络──→   │
  │ │                                               │ ← JFCE fd EPOLLIN 就绪
  │ │                                               │
  │ │                                               │ HandleJfceEvent()
  │ │                                               │ ├─ PollAndDispatch
  │ │                                               │ │   jfc_->Ack(1)
  │ │                                               │ │   jfc_->Poll(32)
  │ │                                               │ ├─ HandleCqe
  │ │                                               │ │   cr.opcode == WRITE_WITH_IMM
  │ │                                               │ └─ HandleRecvCqe
  │ │                                               │     [imm.opcode == PRE_WRITE]
  │ │                                               │     ├─ HandlePreWrite()
  │ │                                               │     │   DataReached()
  │ │                                               │     │   slot→READING
  │ │                                               │     ├─ IssueReadRequest()
  │ │                                               │     │   ├─ ImportRemoteSegments()
  │ │                                               │     │   ├─ AllocBlocksForReq()
  │ │                                               │     │   ├─ BuildReadWrs()
  │ │                                               │     │   │   构造 READ WR 链
  │ │                                               │     │   │   opcode=URMA_OPC_READ
  │ │                                               │     │   └─ urma_post_jetty_send_wr
  │ │                                               │     │       (READ WR 入服务端 SQ)
  │ │                                               │     │       ← 此刻 EventDispatcher 线程
  │ │                                               │     │         阻塞在 IssueReadRequest,
  │ │                                               │     │         不会同时 post WRITE_IMM
  │ │                                               │     │
  │ │                                               │     └─ DeliverReadableSocket()
  │ │                                               │        RecordReadableSocket(fd)
  │ │                                               │
  │ │                                               │ epoll_wait 返回 out 个 events
  │ │                                               │ ├─ CallInputEventCallback
  │ │                                               │ │   bthread_start_urgent()
  │ │                                               │ │   OnNewMessages()
  │ │                                               │ │   → readv()
  │ │                                               │ │
  │ │                                               │ │ [slot 在 READING,数据还没到]
  │ │                                               │ │   ReadV 返回 EAGAIN 或等待
  │ │                                               │
  │ ←───────READ 单边操作直接读客户端内存────────────┤
  │   (客户端 CPU 不参与,无 CQE)                     │
  │   (客户端 Block 数据被 DMA 读取)                  │
  │                                               │
  │                                               │ ← READ 完成,JFCE fd 再次就绪
  │                                               │
  │                                               │ HandleJfceEvent()
  │                                               │ ├─ PollAndDispatch
  │                                               │ ├─ HandleCqe
  │                                               │ │   cr.opcode == READ_COMPLETE
  │                                               │ └─ HandleRecvCqe
  │                                               │     [READ 完成]
  │                                               │     ├─ DataReadFinished()
  │                                               │     │   slot→DATA_READY
  │                                               │     └─ DeliverReadableSocket()
  │                                               │
  │                                               │ epoll_wait 返回
  │                                               │ ├─ CallInputEventCallback
  │                                               │ │   OnNewMessages() → readv()
  │                                               │ │   UrmaRxOps::ReadV()
  │                                               │ │   ├─ ConsumeBlockData()
  │                                               │ │   │   memcpy Block→iov
  │                                               │ │   └─ ResponseCtrlMessage(POST_WRITE)
  │                                               │ │       urma_post_jetty_send_wr
  │                                               │ │       (WRITE_IMM: POST_WRITE 确认)
  │ │ ←──────────────网络────── WRITE_IMM ─────────┤       [WRITE_IMM 入服务端 SQ]
  │ │   (POST_WRITE 确认到达客户端)                  │       (此刻 READ 已完成,SQ 无 READ)
  │ │                                               │
  │ │                                               │ [服务端处理完请求,准备响应]
  │ │                                               │ [brpc worker bthread 执行用户回调]
  │ │                                               │ └─ sock->Write(&resp, opt)
  │ │                                               │     StartWrite → cut_into_fd
  │ │                                               │     UrmaTxOps::WriteV
  │ │                                               │     [响应小包→WriteInline]
  │ │                                               │     PostWriteImm(WRITE_IN_BAND)
  │ │                                               │     urma_post_jetty_send_wr
  │ │                                               │     (WRITE_IMM 入服务端 SQ)
  │ │ ←──────────────网络────── WRITE_IMM ─────────┤
  │ │   (响应数据到达客户端)                         │
  │ │                                               │
  │ ├─ HandlePostWrite()                            │
  │ │   block->DecRef() × N (释放请求 Block)        │
  │ │                                               │
  │ ├─ Write 返回 0                                 │
  │ └─ bthread_id_join(cid)                         │
  │    [收到响应,同步等待结束]                       │
  ▼                                                  ▼
═══════════════════════════════════════════════════════════════════════════════════
```

### 5.4 服务端 EventDispatcher 线程内 READ 与 WRITE_IMM 的串行

```
服务端 EventDispatcher 线程时间线(放大):
  ┌───────────────────────────────────────────────────────────────┐
  │ epoll_wait: 收到 PRE_WRITE CQE                                │
  │   → HandlePreWrite → IssueReadRequest                         │
  │     → urma_post_jetty_send_wr(READ)     ← SQ: [READ]          │
  │     ← 线程阻塞在此(内联同步调用)                                │
  │                                                               │
  │ epoll_wait: 收到 READ 完成 CQE                                 │
  │   → DataReadFinished → slot→DATA_READY                        │
  │   → DeliverReadable → OnNewMessages → ReadV                   │
  │     → ConsumeBlockData                                        │
  │     → ResponseCtrlMessage(POST_WRITE)                         │
  │       → urma_post_jetty_send_wr(WRITE_IMM)← SQ: [WRITE_IMM]  │
  │       (此刻 READ 已完成,SQ 里没有 READ)                       │
  │                                                               │
  │ epoll_wait: 用户回调执行完,发响应                              │
  │   → sock->Write → WriteInline                                 │
  │     → urma_post_jetty_send_wr(WRITE_IMM)← SQ: [WRITE_IMM]    │
  │                                                               │
  │ epoll_wait: 回到等待下一个 CQE                                 │
  └───────────────────────────────────────────────────────────────┘

  SQ 任意时刻只有一种操作:先 READ,完成后才 WRITE_IMM,永不重叠
```

### 5.5 隔离机制(三个层面)

**层面 1:单 EventDispatcher 线程串行处理 CQE**

CQE 处理和 brpc 消息处理在**同一次 `epoll_wait` 调用中内联完成**。`HandleJfceEvent → PollAndDispatch → HandleCqe → HandleRecvCqe → IssueReadRequest → urma_post_jetty_send_wr(READ)` 全程在一个线程内同步执行,不切线程。

**层面 2:IssueReadRequest 是同步内联调用**

EventDispatcher 线程在 post READ 后才继续。当它后续 post WRITE_IMM(POST_WRITE 确认或响应)时,READ 已经完成。READ 和 WRITE_IMM 在时间上被 **epoll_wait 的事件边界**隔开。

**层面 3:READ 和 WRITE_IMM 在不同进程的不同 jetty 上**

READ 由**接收端 EventDispatcher 线程**在处理 PRE_WRITE CQE 时内联 post,WRITE_IMM 由**发送端**在另一个进程 post。两者在物理上隔离在不同进程的不同 jetty SQ 上,根本不会进同一个 SQ。

---

# 5.6 并发的线程安全约束
虽然 URMA 层支持混合 opcode，但 PostSendWr/PostWriteImm 不是线程安全的——同一 jetty 的 JFS 被多线程并发 post 需要外部串行化。当前代码的并发模型：
  - THREAD_PER_CONNECTION 模式：单 IO 线程同时负责发送和接收，天然串行，无并发问题
  - DISPATCHER_THREAD_POOL 模式：poller 线程和 IO 线程都可能操作同一 jetty 的 JFS（poller 内联回退时发 READ / POST_WRITE，IO 线程发 ACK / READ），用 dispatch_lock_ 互斥锁串行化：
// urma_data_rx_ops.cpp:601  DispatchReadInIoThread
/* 持锁排空并处理: 与 poller 队列满时的内联回退互斥, 保证同一时刻只有一个执行体推进 slot 状态机
 * (含 jetty RDMA READ / 响应发送, 避免同 QP 双线程并发 PostSendWr/PostWriteImm) */
std::lock_guard<std::mutex> lock(*dispatch_lock_);
注释明确指出"避免同 QP 双线程并发 PostSendWr/PostWriteImm"——即 urma_post_jetty_send_wr 底层非线程安全，需调用方加锁。
*并发的有序性影响*
同一 JFS 上混合 post WRITE_IMM 和 READ 时，URMA 硬件按 post 顺序执行，但两种操作的完成顺序不保证一致（READ 和 WRITE_IMM 走不同硬件路径）。UBSocket 的保序不依赖硬件完成顺序，而是靠 seq_no + DataSlot ring 在应用层保证消费顺序，所以混合 opcode 不破坏保序语义。

### 5.7 EventDispatcher 与 KeepWrite 的交互

#### 核心结论：UBS v2 方案二中没有独立的 KeepWrite bthread

UBS v2 UBSocket 直连方案是**两层架构**，EventDispatcher 与 KeepWrite 的交互方式和 brpc 原生 URMA 完全不同。KeepWrite 在 UBS v2 中退化为 `writev` 符号拦截，不再作为独立 bthread 存在。

#### 5.7.1 EventDispatcher 的角色变化：符号拦截 + JFCE 内联消化

brpc 的 `EventDispatcher::Run()` 调的不是原生 `epoll_wait`，而是被拦截的 `ubsocket_wrapper_epoll_wait` → `UrmaEventPoll::EpollWait()`。这个函数做了两件事：

```
UrmaEventPoll::EpollWait()
  ├─ epoll_wait(epoll_fd, events, ...)     ← 内核 epoll，返回 JFCE fd + 业务 socket
  ├─ for each event:
  │   if (JFCE fd 事件):
  │     HandleJfceEvent()                  ← 内联消化，不返回给 brpc
  │       ├─ PollAndDispatch → jfc_->Poll(32)
  │       ├─ HandleCqe → HandleRecvCqe
  │       │   ├─ HandlePreWrite → IssueReadRequest
  │       │   │   urma_post_jetty_send_wr(READ)   ← READ 在这里 post
  │       │   └─ DataReadFinished → RecordReadableSocket(fd)
  │       └─ 产出"可读 socket"事件给 brpc
  └─ 返回 out 个"可读 socket"事件给 brpc
```

**JFCE 事件被"吃"在 UBSocket 层内部**，brpc 的 EventDispatcher 看不到 JFCE 事件，只看到"数据可读"事件。

#### 5.7.2 KeepWrite 的角色退化：从独立 bthread 变成 writev 符号拦截

brpc 原生 URMA 中 KeepWrite 是独立 bthread，调 `UrmaEndpoint::WriteZeroCopy` → `urma_post_jetty_send_wr(WRITE_IMM)`。

UBS v2 中**没有 KeepWrite 独立 bthread**。发送路径变成：

```
brpc KeepWrite 逻辑（cut_into_file_descriptor）
  → ubsocket_wrapper_writev(fd, vec, nvec)   ← 符号拦截
    → UrmaTxOps::WriteV
      ├─ [64KB > 2000B → WriteZeroCopy]
      │   BuildBlockRefs → PostWriteImm(PRE_WRITE)
      │   urma_post_jetty_send_wr(WRITE_IMM)  ← 在调用线程内联完成
      └─ [小包 → WriteInline]
          PostWriteImm(WRITE_IN_BAND)
```

发送端（客户端）的 `WRITE_IMM` 由 **Client bthread 内联** post，不走 KeepWrite bthread。

#### 5.7.3 两者交互的完整时序

```
[Client bthread]                              [服务端 EventDispatcher 线程]
  Channel::CallMethod                            epoll_wait(JFCE fd) 阻塞
  ├─ sock->Write → cut_into_fd
  │   ubsocket_writev → UrmaTxOps::WriteV
  │   PostWriteImm(PRE_WRITE)                    ← JFCE fd 就绪
  │   urma_post_jetty_send_wr(WRITE_IMM) ─网络─→ HandleJfceEvent (内联)
  │                                                ├─ HandlePreWrite → IssueReadRequest
  │                                                │   urma_post_jetty_send_wr(READ) ← SQ: [READ]
  │                                                └─ RecordReadableSocket(fd)
  │                                                epoll_wait 返回"可读"事件
  │                                                CallInputEventCallback → OnNewMessages
  │                                                  readv → UrmaRxOps::ReadV
  │                                                  ConsumeBlockData
  │                                                  ResponseCtrlMessage(POST_WRITE)
  │                                                    urma_post_jetty_send_wr(WRITE_IMM) ← SQ: [WRITE_IMM]
  │                                                    (此刻 READ 已完成，SQ 无 READ)
  ←── READ 单边操作直接读客户端内存 ──────────────
  ←── WRITE_IMM(POST_WRITE 确认) ───网络──────────
  HandlePostWrite → block->DecRef()
  Write 返回 0
```

#### 5.7.4 为什么 READ 和 WRITE_IMM 不会并发

三个层面的隔离（详见 5.5 节）：

| 层面 | 机制 |
|------|------|
| **层面 1：单线程串行** | CQE 处理和消息处理在**同一次 `epoll_wait` 调用中内联完成**，`HandleJfceEvent → IssueReadRequest → urma_post_jetty_send_wr(READ)` 全程在一个线程内同步执行 |
| **层面 2：事件边界隔开** | EventDispatcher 线程 post READ 后才继续。当它后续 post WRITE_IMM（POST_WRITE 确认）时，READ 已经完成。两者被 `epoll_wait` 的事件边界天然隔开 |
| **层面 3：跨进程物理隔离** | READ 由**接收端** EventDispatcher 线程 post，WRITE_IMM 由**发送端**在另一个进程 post。两者在不同进程的不同 jetty SQ 上 |

服务端 SQ 任意时刻只有一种操作：先 READ，完成后才 WRITE_IMM，永不重叠。

#### 5.7.5 与 brpc 原生 URMA 的本质区别

| | UBS v2 UBSocket | brpc 原生 URMA |
|---|---|---|
| JFCE fd 归属 | UBSocket 层 `UrmaEventPoll::epoll_fd_` | brpc 的 `_cq_sid` Socket |
| JFCE 事件处理 | `epoll_wait` 内联消化 | EventDispatcher 返回事件 → `PollCq` 回调 |
| READ post 线程 | **EventDispatcher 线程**（内联） | PollCq 回调可能起**独立 bthread** |
| WRITE_IMM post 线程 | Client bthread 内联 / EventDispatcher 线程（readv 后） | **KeepWrite 独立 bthread** |
| 两者关系 | **同线程串行** | **两个 bthread 并发** |

**根本差异**：UBS v2 把 JFCE 事件"吃"在 UBSocket 层内部，READ 和 WRITE_IMM 都在 EventDispatcher 线程内串行执行，brpc 看不到 JFCE 事件。brpc 原生 URMA 把 JFCE 事件暴露给 EventDispatcher 作为独立 Socket 回调（PollCq），和 KeepWrite 并列，导致 READ 和 WRITE_IMM 在两个独立 bthread 中并发提交到同一个 jetty SQ。

## 六、三种方案的并发隔离机制对比

### 6.1 核心对比表

| 维度 | brpc 原生 URMA | UBS v2 hcom service | UBS v2 UBSocket 直连 |
|---|---|---|---|
| READ 由谁 post | PollCq bthread(`PostReadBatch`) | 用户线程(`PostRead`) | 接收端 EventDispatcher 线程(`IssueReadRequest`) |
| WRITE_IMM 由谁 post | KeepWrite bthread(`WriteZeroCopy`) | 用户线程(`PostSend`) | 发送端 Client bthread / brpc worker |
| 两者关系 | **同进程同 jetty,两个 bthread 并发** | **同进程同 jetty,信号量串行** | **不同进程不同 jetty,物理隔离** |
| 隔离机制 | 无(必须双 jetty) | sem_wait 信号量交替 | 单 EventDispatcher 线程内联 + 跨进程 |
| 单 jetty 够吗 | **不够** | **够**(同步 RPC 语义) | **够**(单线程串行 + 跨进程) |
| qd | N(流水线) | 1(同步等待) | N(多连接) |

### 6.2 为什么 brpc 原生 URMA 会重叠

| | brpc 原生 URMA | UBS v2 UBSocket |
|---|---|---|
| READ 由谁 post | **接收端 PollCq bthread**(`PostReadBatch`) | **接收端 EventDispatcher 线程**(`IssueReadRequest`) |
| WRITE_IMM 由谁 post | **发送端 KeepWrite bthread**(`WriteZeroCopy`) | **发送端 Client bthread**(内联) |
| 两者关系 | 同一进程的同一 jetty,两个 bthread 并发 | 不同进程的不同 jetty,物理隔离 |

**最关键的区别**:UBS v2 UBSocket 的 READ 由**接收端的 EventDispatcher 线程**在处理 CQE 时内联 post,而 WRITE_IMM 由**发送端**在另一个进程 post。两者在**不同进程的不同 jetty SQ 上**,根本不会进同一个 SQ。

而 brpc 原生 URMA 的 READ 和 WRITE_IMM 都在**同一进程的同一个 jetty 上**——PollCq bthread post READ(拉响应),KeepWrite bthread post WRITE_IMM(发新请求),两者抢同一个 SQ。

---

## 七、为什么 UBS v2 做得到单 jetty 无并发,brpc 原生 URMA 做不到

### 7.1 根本原因:两层架构 vs 单层架构

UBS v2 是**两层架构**:UBSocket 层接管了 epoll 事件循环,把 CQE 处理内联在 `epoll_wait` 里;brpc 原生 URMA 是**单层架构**,PollCq 作为独立 Socket 回调挂在 EventDispatcher 上,和 KeepWrite 并列。

### 7.2 UBS v2 的关键设计:epoll_wait 符号拦截 + JFCE 内联消化

UBS v2 的 brpc 没有 `src/brpc/urma/` 目录——它不用 brpc 原生 URMA 传输层。URMA 完全由 UBSocket 层(`ubsocket_wrapper` → `UrmaSocket`/`UrmaTxOps`/`UrmaRxOps`)实现,brpc 只通过 `writev`/`readv`/`epoll_wait` 符号拦截与之交互。

关键代码证据:
- `event_dispatcher_epoll.cpp:214`:`::ubsocket_wrapper_epoll_wait(...)` —— brpc 的 EventDispatcher 调的是 UBSocket 的 epoll_wait
- `urma_event_epoll.cpp:191-193`:JFCE 中断事件在 `epoll_wait` 返回后**内联处理**(`HandleJfceEvent`),不外发给 brpc
- `iobuf.cpp:880`:`::ubsocket_wrapper_writev(fd, vec, nvec)` —— brpc 的 `cut_into_file_descriptor` 调的是 UBSocket 的 writev

```
brpc EventDispatcher::Run()
  │
  │  ubsocket_wrapper_epoll_wait()   ← 符号拦截,不是原生 epoll_wait
  │  → UrmaEventPoll::EpollWait()
  │     │
  │     ├─ epoll_wait(epoll_fd, events, ...)  ← 内核 epoll
  │     │   返回 JFCE fd 就绪 + 业务 socket 就绪
  │     │
  │     ├─ for each event:
  │     │   if (JFCE fd 事件):
  │     │     HandleJfceEvent()          ← 内联消化!不返回给 brpc
  │     │       ├─ PollAndDispatch
  │     │       │   jfc_->Poll(32)
  │     │       ├─ HandleCqe
  │     │       │   HandleRecvCqe
  │     │       │   ├─ HandlePreWrite
  │     │       │   │   IssueReadRequest
  │     │       │   │   urma_post_jetty_send_wr(READ)  ← READ 在这里 post
  │     │       │   └─ ...
  │     │       └─ RecordReadableSocket(fd)  ← 产出"可读"事件给 brpc
  │     │
  │     └─ 返回 out 个"可读 socket"事件给 brpc
  │
  ├─ for each 可读事件:
  │   CallInputEventCallback → OnNewMessages → readv
  │   ubsocket_wrapper_readv → UrmaRxOps::ReadV
  │   ├─ ConsumeBlockData
  │   └─ ResponseCtrlMessage(POST_WRITE)
  │       urma_post_jetty_send_wr(WRITE_IMM)  ← WRITE_IMM 在这里 post
  │
  └─ for each 可写事件:
      CallOutputEventCallback → KeepWrite(如果需要)
```

**READ 和 WRITE_IMM 都在 EventDispatcher 线程内串行执行**:
- READ:JFCE 事件内联处理时 post(`IssueReadRequest`)
- WRITE_IMM:`readv` 消费数据后 post(`ResponseCtrlMessage`)

两者被 `epoll_wait` 的事件边界天然隔开,不可能并发。

### 7.3 brpc 原生 URMA 的设计:PollCq 独立 Socket + KeepWrite 独立 bthread

```
brpc EventDispatcher::Run()
  │
  │  epoll_wait(epfd, events, ...)   ← 原生 epoll_wait
  │  返回:JFCE fd 就绪(_cq_sid) + 业务 socket 就绪
  │
  ├─ for each 可读事件:
  │   if (fd == _cq_sid 的 fd):
  │     CallInputEventCallback → PollCq       ← 独立回调!
  │     ├─ drain_cq
  │     ├─ HandleCompletion
  │     │   HandleWriteImmCompletion
  │     │   PostReadBatch
  │     │   urma_post_jetty_send_wr(READ)     ← READ 在 PollCq 里 post
  │     └─ ...
  │   else:
  │     CallInputEventCallback → OnNewMessages → readv
  │
  └─ for each 可写事件:
      CallOutputEventCallback → KeepWrite      ← 独立 bthread!
      DoWrite → WriteZeroCopy
      urma_post_jetty_send_wr(WRITE_IMM)       ← WRITE_IMM 在 KeepWrite 里 post
```

**READ 和 WRITE_IMM 在两个独立回调里**,可以被 EventDispatcher 并发调度(brpc 的 `CallInputEventCallback` 会 `bthread_start_urgent` 起独立 bthread)。

### 7.4 两个关键差异

#### 差异 1:谁在处理 JFCE 事件(CQE)

| | UBS v2 UBSocket | brpc 原生 URMA |
|---|---|---|
| JFCE fd 归属 | UBSocket 层的 `UrmaEventPoll::epoll_fd_` | brpc 的 `_cq_sid` Socket(`urma_endpoint.cpp:694-712`) |
| JFCE 事件处理 | `ubsocket_wrapper_epoll_wait` 内联消化(`HandleJfceEvent`) | brpc EventDispatcher 返回事件 → `CallInputEventCallback` → `PollCq` 回调 |
| READ post 在哪 | `epoll_wait` 内联的 `HandleJfceEvent → IssueReadRequest` | 独立回调 `PollCq → HandleCompletion → PostReadBatch` |
| READ post 的线程 | **EventDispatcher 线程**(内联,不切 bthread) | PollCq 回调可能 `bthread_start_urgent` 起**独立 bthread** |

UBS v2 把 JFCE 事件"吃"在 UBSocket 层内部,brpc 看不到 JFCE 事件,只看到"数据可读"事件。brpc 原生 URMA 把 JFCE 事件暴露给 EventDispatcher,作为独立 Socket 的回调。

#### 差异 2:WRITE_IMM 的 post 路径

| | UBS v2 UBSocket | brpc 原生 URMA |
|---|---|---|
| 发送入口 | `ubsocket_wrapper_writev` → `UrmaTxOps::WriteV` → `PostWriteImm` | `UrmaEndpoint::WriteZeroCopy` → `urma_post_jetty_send_wr` |
| 调用者 | Client bthread 内联 / brpc worker(`readv` 后的 `ResponseCtrlMessage`) | KeepWrite bthread |
| 与 READ 的关系 | **同一线程串行**(都在 EventDispatcher 内) | **不同 bthread 并发**(KeepWrite vs PollCq) |

UBS v2 的 WRITE_IMM post 有两个路径:
1. **客户端发请求**:Client bthread 内联调 `writev` → `PostWriteImm`(在**客户端进程**,和**服务端**的 READ 在不同进程)
2. **服务端发响应/确认**:`readv` 消费数据后 `ResponseCtrlMessage` → `PostWriteImm`(在 **EventDispatcher 线程**,和之前的 READ 在**同一线程**串行)

brpc 原生 URMA 的 WRITE_IMM post 只有 KeepWrite 路径,和 PollCq 的 READ 在**同一进程的两个 bthread** 并发。

### 7.5 为什么 brpc 原生 URMA 不能改成 UBS v2 的模式

要改成 UBS v2 模式,brpc 原生 URMA 需要:

1. **把 PollCq 逻辑搬进 `epoll_wait` 内联消化** —— 需要 UBSocket 那样的符号拦截层(`ubsocket_wrapper_epoll_wait`),在 `epoll_wait` 返回后内联处理 JFCE 事件。但 brpc 原生 URMA 的 `PollCq` 是 brpc Socket 的 `on_edge_triggered_events` 回调,设计上就是让 EventDispatcher 调度的。改成内联等于重构 EventDispatcher 的事件分发模型。

2. **把 WRITE_IMM 的 post 从 KeepWrite 搬到 readv 回调里** —— 服务端的 POST_WRITE 确认和响应发送要在 `readv`(`HandleNewMessages`)里完成,而非 KeepWrite。但 brpc 的 `readv` 只负责读数据,发送是 `writev` 的职责,两者由不同 bthread 驱动。合并它们等于把 brpc 的读写分离模型改成单线程串行模型。

3. **取消 `_cq_sid` 独立 Socket** —— 不再将 JFCE fd 注册到 EventDispatcher 的 epoll,改为在 UBSocket 层内部管理。但这需要一套独立于 brpc Socket 的 epoll 循环——就是 UBS v2 的 `UrmaEventPoll`。等于在 brpc 内部再嵌一层事件循环,和 brpc 的 EventDispatcher 并存,架构复杂度大增。

**本质矛盾**:brpc 的设计哲学是"一个 EventDispatcher 管所有 fd,通过回调分发到 bthread"。UBS v2 的设计哲学是"UBSocket 层有自己的事件循环,CQE 在 epoll_wait 内联处理,只把'数据可读'事件返回给上层"。两种哲学不兼容——除非给 brpc 原生 URMA 加一层 UBSocket 式的拦截层,但那等于把 UBS v2 的架构复制过来,不再是"重构 PollCq/KeepWrite",而是"换一套传输层"。

### 7.6 重构 PollCq/KeepWrite 的可行性总结

| 方案 | 可行性 | 原因 |
|---|---|---|
| 合并成一个 bthread | **不可行** | 互相阻塞死锁(PollCq 停 → SQ 窗口不归还 → KeepWrite 永不唤醒) / 互相饿死(边沿触发忙循环) |
| PollCq 内联 post WRITE_IMM | **不可行** | PollCq 不知道 `_write_head` 上有什么;post 阻塞 CQ 排空 |
| KeepWrite 内联 poll CQ | **不可行** | 等 READ 完成 = qd=1;不等 = 没解决问题;epoll 事件源不匹配(JFCE fd 不在业务 Socket 的 epoll) |
| 单线程化(UBS v2 UBSocket 模式) | **理论可行,改动巨大** | 需要符号拦截层 + 内嵌事件循环 + 取消读写分离,等于换一套传输层架构 |
| PostReadBatch 延迟到 KeepWrite 空闲 | **不可行** | KeepWrite 挂起时 READ 永不触发 → 死锁 |
| **双 jetty(已实现)** | **可行,改动最小** | 不改线程模型,硬件层 SQ 隔离 |

### 7.7 结论

UBS v2 做得到是因为它有**独立的事件拦截层**(UBSocket),把 CQE 处理和发送都内联在 `epoll_wait` + `readv` 里,READ 和 WRITE_IMM 在同一 EventDispatcher 线程串行。brpc 原生 URMA 做不到是因为它的 PollCq 和 KeepWrite 是 EventDispatcher 下的两个独立回调,受 brpc 的"读写分离 + bthread 调度"架构约束,无法内联。双 jetty 是这个架构约束下代价最小的解法。

---

## 八、双 jetty 分离方案

### 7.1 解法:物理 SQ 隔离

`AllocateResources`(`urma_endpoint.cpp:655-677`)创建第二个 jetty `jetty_read`,与 `jetty`(write)共享同一 JFC(CQ)和 JFR,但 **SQ 完全独立**:

- READ WR → `jetty_read` 的 SQ(`:2589`)
- WRITE_IMM WR → `jetty` 的 SQ(`:1679`、`:1488`)

握手协议新增 `read_jetty_id`(`urma_handshake.proto:71-76`)让对端导入对应 read jetty。两个 SQ 物理隔离,provider 不可能在同一 SQ 内交错调度 READ 与 WRITE_IMM,从根本上消除 status=8。

### 7.2 双 jetty 模式下跳过 mutex

dual-jetty 模式下两条路径都**跳过** `_zerocopy_post_mutex`(`:2604-2611`、`:1673-1680`),READ post 不必等待 KeepWrite,并发性能恢复。

### 7.3 为什么双 jetty 是 brpc 架构下最优解

| 方案 | 能否隔离 | 代价 |
|---|---|---|
| bthread 同步(post 后等完成) | 能 | qd=1,qps 暴跌,摧毁流水线 |
| bthread 同步(只锁 post 调用) | **不能** | mutex 只串行调用瞬间,管不到 SQ 内部执行 |
| **双 jetty** | **能** | 多一个 jetty 资源,**无性能损失** |
| 改用纯 WRITE_IMM(`WriteInlineChunked`) | 能 | 无 READ 即无冲突,但牺牲零拷贝 |

双 jetty 是 brpc 在**保持流水线高 qps** 的同时根除 status=8 的唯一方案:不改变线程模型、不加同步等待、不牺牲并发度,只在硬件层把 READ 和 WRITE_IMM 的 SQ 物理隔开。

---

## 八、双 Jetty 方案的弊端:流控、乱序、可靠性

双 jetty 用第二个 SQ 物理隔离 READ 与 WRITE_IMM,根除了 status=8,但也引入了三个需要专门设计的新问题:窗口流控耦合、跨 jetty 乱序、错误处理与降级。下面逐一分析代码现状。

### 8.1 流控:READ 窗口独立,但 POST_WRITE ACK 仍占用 write SQ 窗口

**独立窗口。** 双 jetty 为 read jetty 单独维护 `_sq_window_size_read`(`urma_endpoint.h:342`),与 write jetty 的 `_sq_window_size` 完全独立。PostReadBatch 在 `:2491-2495` 选择窗口,HandleCompletion 在 `:2149-2155`、`:2265-2270` 按 `use_read_jetty` 归还对应窗口。这意味着 READ 的 in-flight 数量不再受 write SQ 深度限制,大消息(8MB / 256 blocks)可以灌满 read SQ 而不挤压 WRITE_IMM 的发送配额。

**耦合点:POST_WRITE ACK 走 write jetty。** 这是双 jetty 流控的关键薄弱环节。HandleReadCompletion 在所有 READ 完成后调用 `ResponseCtrlMessage(URMA_IO_POST_WRITE, ...)`(`:2950`)通知发送方释放 send_buf。而 ResponseCtrlMessage 内部用的是 `_resource->jetty` + `_resource->remote_jetty`(`:1866`、`:1904`),即 **write jetty**,并消耗 `_sq_window_size`(`:1917`)。

后果:
- **READ 完成到投递之间存在隐性依赖 write SQ 窗口**。若 write jetty SQ 被大量 WRITE_IMM 占满(高 qps 小消息场景),POST_WRITE ACK 会因 `_sq_window_size==0` 而 post 失败,HandleReadCompletion 直接返回 -1(`:2953-2957`),该 slot 卡在 DATA_READY 状态,**消息不投递**。
- **对端 send_buf 释放延迟**。POST_WRITE 失败意味着发送方收不到 ACK,send_buf slot 不释放(`HandlePostWrite` 不会被调用),后续大消息可能因 send_buf 耗尽而阻塞 KeepWrite。
- **read SQ 窗口已归还但消息未投递**。read SQ 空着却因为 write SQ 满而无法推进,形成"read 空闲 / write 拥塞 / 消息积压"的死锁倾向。

**缓解设计。** 当前代码未对 POST_WRITE 失败做重试(不像 PostReadBatch 有 `pending_retry` 机制)。这是一个可靠性缺口:POST_WRITE 失败即丢消息,只能靠上层 RPC 重试或连接重建恢复。

### 8.2 乱序:跨 jetty CQE 交错,靠 slot 分配序保证投递有序

**CQE 交错。** 双 jetty 共享同一 JFC(`:655-657` 注释明确),read jetty 的 READ CQE 和 write jetty 的 WRITE_IMM CQE 都落到同一个 CQ,PollCq 单线程 drain。不同 slot 的 READ 可能交错完成(slot A 的 block 5 与 slot B 的 block 2 先后到达)。

**投递有序的保证机制。**
1. **slot 状态机**。每个 slot 有 IDLE → READING → DATA_READY → IDLE 状态。HandleReadCompletion 只在 `slot.sq_slots_used == 0`(当前 batch 全部完成)且 `next_read_idx == total_blocks`(所有 batch 完成)时才置 DATA_READY(`:2944`)。
2. **DeliverReadySlots 严格按序**(`:2675-2699`)。从 `_rx_delivery_seq` 开始,只投递连续的 DATA_READY slot,遇到非 DATA_READY 即 break。这保证了 InputMessenger 看到的消息顺序 = PRE_WRITE 到达顺序 = slot 分配顺序。
3. **乱序 READ 的数据隔离**。READ 数据写入 `slot.recv_bufs`(per-slot),不直接 append 到 `_read_buf`(`:2888-2893` 注释明确)。只有 DeliverReadySlots 在 slot DATA_READY 后才把 recv_bufs append 到 `_read_buf`(`:2692-2694`),避免碎片污染。

**残余风险:Head-of-line blocking。** 若 slot N 的 POST_WRITE 失败(8.1 节)卡在 DATA_READY,即使 slot N+1..N+K 都已 DATA_READY,DeliverReadySlots 也会在 slot N 处 break,**后续消息全部阻塞**。这是按序投递的必然代价——双 jetty 没有引入新的 HoL,但也没有缓解既有的 HoL。

### 8.3 可靠性:错误处理与降级

**8.3.1 read jetty 创建失败的 fallback。**
AllocateResources 在 `urma_create_jetty(jetty_read)` 失败时只打 WARNING,不 return -1(`:667-671`)。后续 PostReadBatch 检查 `jetty_read != nullptr && remote_jetty_read != nullptr`(`:2491-2493`),任一为空即 `use_read_jetty=false`,回退到 write jetty + `_zerocopy_post_mutex` 串行化。**降级是自动的,但降级后 status=8 风险回归**——等于关闭双 jetty。

**8.3.2 握手不对称。**
read_jetty_id 是 v3 optional 字段(`:466`、`:560`)。若一端开启 `--urma_dual_jetty=true` 而对端关闭,本地创建了 `jetty_read` 但收不到对端的 `read_jetty_id`,`remote_jetty_read` 为空,`use_read_jetty=false`,**单端降级**。代码在 `:1059-1091` 处理:只有双方都 advertise read_jetty_id 且本地有 jetty_read 时才 import `remote_jetty_read`,否则 WARNING 并降级。

**8.3.3 READ 完成错误的窗口归还。**
HandleCompletion 对 READ WR 错误(`os_type == READ_DATA_REQUEST`)按 `use_read_jetty` 归还对应 SQ 窗口(`:2148-2167`),cap 取 `GetUrmaReadJettySqSize()`(read jetty)或 `_local_window_capacity`(write jetty)。窗口不会因错误泄漏。

**8.3.4 fatal 错误的处理。**
- `URMA_CR_WR_FLUSH_ERR` / `URMA_CR_WR_UNHANDLED`:jetty 被拆除,fatal,返回 EIO(`:2095-2106`)。双 jetty 下任一 jetty 拆除都会触发,因为共享 CQ。
- `URMA_CR_REM_ACCESS_ABORT_ERR`(status=8):双 jetty 本应根除此错误,但若降级到单 jetty(8.3.1/8.3.2)仍可能发生,代码仍按 fatal 处理(`:2120-2121`)。
- 其他 transient 错误(RNR_RETRY、ACK_TIMEOUT、LOC_ACCESS):non-fatal,归还窗口继续(`:2093-2094` 注释)。

**8.3.5 EAGAIN 重试机制(仅 READ)。**
PostReadBatch 在 SQ 窗口耗尽时返回 EAGAIN(`:2504-2513`),HandleReadCompletion 标记 `slot.pending_retry=true` 并推迟(`:2914-2925`),待下次 CQE 触发 RetryPendingReads(`:2630-2669`)恢复。这是 read jetty 独有的流控弹性:READ 可以暂停再续,不丢数据。但 POST_WRITE ACK(8.1 节)没有对应的重试机制。

### 8.4 弊端总结

| 维度 | 现状 | 风险等级 |
|---|---|---|
| 流控 | READ 窗口独立,但 POST_WRITE ACK 占用 write SQ 窗口 | **中** — write SQ 拥塞时 ACK 失败导致消息卡死,无重试 |
| 乱序 | DeliverReadySlots 按 slot 分配序投递,严格有序 | **低** — 正确性有保证,但 HoL 阻塞无法缓解 |
| 可靠性 - 降级 | jetty_read 创建失败 / 握手不对称自动降级到单 jetty | **中** — 降级后 status=8 风险回归,用户无感知 |
| 可靠性 - fatal | FLUSH/UNHANDLED/status=8 仍 fatal | **低** — 双 jetty 下 status=8 不应再发生 |
| 资源开销 | 多一个 jetty + 独立 SQ + 独立窗口计数器 | **低** — 内存与 jetty 对象开销可忽略 |

**最值得改进的点**:POST_WRITE ACK 失败无重试(8.1 节)。这是双 jetty 流控耦合的唯一硬伤,会导致 DATA_READY slot 永久卡死。可考虑为 POST_WRITE 增加 `pending_retry` 机制(类似 PostReadBatch),在 write SQ 窗口归还后重试投递。

---

## 九、结论

| 问题 | 结论 |
|---|---|
| status=8 根因 | URMA jetty SQ 不支持 READ+WRITE_IMM 在同一 SQ 并发处理(影响所有 URMA 设备,非 bonding 专属) |
| brpc 为何触发 | PollCq bthread 的 READ 与 KeepWrite bthread 的 WRITE_IMM 并发提交到同一 jetty SQ,qd≥4 时强制重叠 |
| UBS v2 为何不触发 | hcom 层靠信号量同步串行;UBSocket 层靠 epoll_wait 符号拦截 + JFCE 内联消化,READ 和 WRITE_IMM 在同一 EventDispatcher 线程串行 |
| brpc 能否用同步机制解决 | 不能。同步等待会 qd=1 摧毁流水线;只锁 post 调用管不到 SQ 内部执行 |
| brpc 能否重构 PollCq/KeepWrite 解决 | 不能。两者由不同 epoll 事件源驱动,合并会死锁/饿死;改成 UBS v2 模式需要符号拦截层 + 内嵌事件循环 + 取消读写分离,等于换一套传输层 |
| UBS v2 为什么做得到 | UBSocket 层有自己的 `UrmaEventPoll` 事件循环,JFCE 事件在 `epoll_wait` 内联消化(READ post),`readv` 后内联 post WRITE_IMM,两者在同一线程串行。brpc 只看到"数据可读"事件,看不到 JFCE 事件 |
| 双 jetty 的必要性 | brpc 单层架构下代价最小的解法——不改事件模型、不改线程模型、不改读写分离,只在 URMA 资源层用第二个 jetty 的 SQ 物理隔离 READ 和 WRITE_IMM |
