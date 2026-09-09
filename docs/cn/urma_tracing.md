<!-- SPDX-License-Identifier: Apache-2.0 -->

# URMA 全流程打点方案

本文档描述 brpc + URMA 通信全链路的打点（tracing/instrumentation）方案，覆盖从
应用层 RPC 调用到 URMA 硬件传输、网卡中断、bthread 调度的完整路径。

## 目录

- [1. 全景图](#1-全景图)
- [2. 现有打点能力分析](#2-现有打点能力分析)
- [3. 打点方案分层设计](#3-打点方案分层设计)
  - [3.1 第一层：brpc 内置打点](#31-第一层brpc-内置打点)
  - [3.2 第二层：eBPF 非侵入式追踪](#32-第二层ebpf-非侵入式追踪)
  - [3.3 第三层：LD\_PRELOAD 拦截](#33-第三层ld_preload-拦截)
- [4. 工具脚本一览](#4-工具脚本一览)
- [5. 一键全流程追踪](#5-一键全流程追踪)
- [6. 打点覆盖矩阵](#6-打点覆盖矩阵)
- [7. 实施路径](#7-实施路径)
- [8. 开销评估](#8-开销评估)

---

## 1. 全景图

下图标注了一次 URMA RPC 从客户端发起到服务端处理的完整数据流，以及每个阶段的
源码位置和打点状态。

```
 客户端                                                                     服务端
 ═══════                                                                    ═════

 [1] Channel::CallMethod                                              [20] ProcessRpcRequest
     src/brpc/channel.cpp:471                                             src/brpc/policy/baidu_rpc_protocol.cpp:581

 [2] PackRpcRequest                                                  [21] 反序列化请求
     src/brpc/policy/baidu_rpc_protocol.cpp:1085

 [3] Socket::Write → StartWrite                                      [22] svc->CallMethod (用户回调)
     src/brpc/socket.cpp:1624 → 1704

 [4] ConnectIfNot (连接建立)                                          [23] SendRpcResponse
     src/brpc/socket.cpp:1416 → 1259                                     src/brpc/policy/baidu_rpc_protocol.cpp:288

 ┌───────────────────── URMA 数据路径 ─────────────────────────────────┐
 │                                                                      │
 │ [5] UrmaTransport::CutFromIOBufList          [14] PollCq            │
 │     src/brpc/urma_transport.cpp:99              src/brpc/urma/urma_endpoint.cpp:1187
 │                                                                      │
 │ [6] UrmaEndpoint::CutFromIOBufList           [15] urma_poll_jfc    │
 │     src/brpc/urma/urma_endpoint.cpp:715        src/brpc/urma/urma_endpoint.cpp:1224
 │     · 检查双窗口 (remote_rq_window / sq_window)                     │
 │     · UrmaIOBuf::cut_into_sglist (零拷贝 SGE)                        │
 │     · 预扣双窗口信用                                                 │
 │                                                                      │
 │ [7] urma_post_jetty_send_wr                 [16] HandleCompletion    │
 │     src/brpc/urma/urma_endpoint.cpp:797      src/brpc/urma/urma_endpoint.cpp:971
 │                                                                      │
 │           [8] 网卡 DMA 发送                                          │
 │           [9] 网络物理传输                                           │
 │           [10] 对端网卡中断 (硬中断 + 软中断)                        │
 │           [11] URMA 内核处理 (硬件 → CQ)                             │
 │                                                                      │
 │                              [12] urma_post_jfr_wr (补充 recv WR)    │
 │                              src/brpc/urma/urma_endpoint.cpp:850     │
 │                                                                      │
 │                              [13] SendAck / SendImm (credit ACK)     │
 │                              src/brpc/urma/urma_endpoint.cpp:910/961 │
 └──────────────────────────────────────────────────────────────────────┘

 [17] DispatchReceivedBytes → InputMessenger::ProcessNewMessage
     src/brpc/urma/urma_endpoint.cpp:1148
     src/brpc/input_messenger.cpp:195

 [18] ParseRpcMessage
     src/brpc/policy/baidu_rpc_protocol.cpp:932(客户端) / 581(服务端)

 [19] Controller::OnRPCEnd → _done->Run() (用户回调)
     src/brpc/controller.cpp:1035 → 1012
```

> **注意**：URMA 数据路径完全绕过 TCP 内核协议栈（`tcp_sendmsg` / `tcp_recvmsg`
> 等）。TCP 仅在握手阶段和 fallback 场景使用，由 brpc uprobe 追踪覆盖，不纳入
> 内核网络栈打点。

---

## 2. 现有打点能力分析

### 2.1 已有打点（无需改动，直接使用）

| 打点 | 源码位置 | 查看方式 | 说明 |
|------|---------|---------|------|
| rpcz Span 6 个时间点 | `src/brpc/span.h:133-142` | `/rpcz` | received / start\_parse / start\_callback / start\_send / sent / end |
| `latency_us()` | `src/brpc/controller.h:232` | `/vars` | RPC 端到端延迟 |
| `event_dispatcher_read` bvar | `src/brpc/event_dispatcher_epoll.cpp:238` | `/vars` | epoll 读回调执行耗时 |
| `event_dispatcher_write` bvar | `src/brpc/event_dispatcher_epoll.cpp:246` | `/vars` | epoll 写回调执行耗时 |
| `bthread_creation` bvar | `src/bthread/task_group.cpp:446` | `/vars` | bthread 创建到运行的排队时间 |
| `bthread_worker_count/usage` | `src/bthread/task_control.cpp` | `/vars` | 工作线程数 / 使用数 |
| contention profiler | `src/bthread/mutex.cpp:122` | `/pprof/contention` | 锁等待采样 |
| cpu profiler | `src/brpc/builtin/pprof_service.cpp` | `/pprof/profile` | gperftools SIGPROF 采样 |
| `urma_send` 等（需新增） | `src/brpc/urma/urma_endpoint.cpp` | `/vars` | URMA 传输层统计 |

### 2.2 缺失打点

| 缺失项 | 影响范围 | 对应阶段 |
|--------|---------|---------|
| 序列化/反序列化耗时 | [2] [21] | TRACEPRINTF + bvar 增强 |
| Socket 写队列排队等待 | [3] | eBPF uprobe |
| URMA 窗口等待时间 | [6] | bvar + TRACEPRINTF |
| `urma_post_jetty_send_wr` 耗时 | [7] | LD\_PRELOAD + uprobe |
| `urma_poll_jfc` 耗时 / CQE 数 | [15] | LD\_PRELOAD + uprobe |
| HandleCompletion SEND/RECV 分段 | [16] | TRACEPRINTF |
| DispatchReceivedBytes dispatch 等待 | [17] | bvar + uprobe |
| CutInputMessage 协议切割耗时 | [18] | TRACEPRINTF |
| 网卡中断耗时 | [10] | eBPF tracepoint |
| bthread 调度延迟 (epoll→回调) | [3]→[14] | eBPF uprobe |
| URMA CQ 事件→PollCq 延迟 | [14] | eBPF uprobe |

### 2.3 已有工具

brpc 仓库 `tools/` 目录下已有以下追踪工具：

| 工具 | 文件 | 类型 | 覆盖范围 |
|------|------|------|---------|
| brpc 生命周期追踪 | `tools/brpc_lifecycle_trace.sh` | bpftrace uprobe | brpc 11 个函数阶段 |
| URMA 全管道追踪 | `tools/urma_pipeline_trace.sh` | bpftrace uprobe | brpc 10 函数 + URMA 7 数据面 + 26 控制面 |
| URMA uprobe 模板 | `tools/urma_uprobe_trace.bt` | bpftrace uprobe | 33 个 URMA 函数，数据面 1% 采样 |
| URMA LD\_PRELOAD 拦截 | `tools/urma_ldpreload_wrap.c` | C 源码 | 7 数据面 + 8 控制面 URMA 函数 |
| URMA LD\_PRELOAD 启动器 | `tools/urma_ldpreload_trace.sh` | Bash | 编译并加载 wrap .so |
| URMA 全栈追踪 | `tools/urma_fullstack_trace.sh` | Bash | LD\_PRELOAD + ubsocket 内建 profiling |
| 管道延迟分析 | `tools/urma_pipeline_summary.py` | Python | 解析 bpftrace 输出，构建管道树 + P50/P90/P99 |
| 单函数延迟分析 | `tools/urma_trace_summary.py` | Python | 解析 bpftrace 输出，P50/P90/P99/P99.9 表格 |
| 内核 URMA 追踪 | `tools/kernel_urma_trace.bt` | bpftrace | eventfd / 中断 / 调度延迟（不含 TCP 栈） |

---

## 3. 打点方案分层设计

### 3.1 第一层：brpc 内置打点

#### 3.1.1 已有 rpcz Span 时间点

brpc 在 `Span` 类中定义了 6 个关键时间点（`src/brpc/span.h:133-142`）：

```c++
void set_received_us(int64_t tm);        // 请求被接收
void set_start_parse_us(int64_t tm);    // 开始解析协议
void set_start_callback_us(int64_t tm); // 用户回调开始
void set_start_send_us(int64_t tm);     // 开始发送
void set_sent_us(int64_t tm);           // 发送完成
```

所有时间点通过 `butil::cpuwide_time_us() + _base_real_us` 转换为绝对墙上时间。
调用点遍布所有协议实现（`baidu_rpc_protocol.cpp`、`sofa_pbrpc_protocol.cpp`
等共 16 个协议文件，74 处调用点）。

开启方式：

```bash
# 启动时开启
./your_server -enable_rpcz

# 运行时动态开启（不需重启）
curl http://SERVER_URL/rpcz/enable
```

#### 3.1.2 TRACEPRINTF 增强注解

在 URMA 收发路径关键函数中插入 `TRACEPRINTF`，将时间数据写入 rpcz Span 事件流。
`TRACEPRINTF` 在 rpcz 未开启时参数**不求值**，零开销（`src/brpc/traceprintf.h:46`）。

```c++
#include <brpc/traceprintf.h>

// --- 发送路径：UrmaEndpoint::CutFromIOBufList (urma_endpoint.cpp:715) ---
TRACEPRINTF("UrmaSend: enter ndata=%zu", ndata);
// ... 窗口检查 ...
if (remote_wnd == 0 || sq_wnd == 0) {
    TRACEPRINTF("UrmaSend: blocked remote_wnd=%u sq_wnd=%u",
                remote_wnd, sq_wnd);
    errno = EAGAIN;
    return -1;
}
// ... SGE 构建 + urma_post_jetty_send_wr ...
TRACEPRINTF("UrmaSend: post sge_index=%d total_len=%zu",
            sge_index, total_len);

// --- 接收路径：HandleCompletion (urma_endpoint.cpp:971) ---
if (cr.flag.bs.s_r == 0) {
    // SEND 完成
    TRACEPRINTF("UrmaSendComplete: sq_slot=%d", (int)cr.user_ctx);
} else {
    // RECV 完成
    TRACEPRINTF("UrmaRecvComplete: len=%d opcode=%d",
                cr.completion_len, cr.flag.bs.op_code);
}

// --- 接收路径：DispatchReceivedBytes (urma_endpoint.cpp:1148) ---
TRACEPRINTF("UrmaDispatch: pending=%ld state=%d", pending, (int)state);

// --- 接收路径：PollCq (urma_endpoint.cpp:1187) ---
TRACEPRINTF("UrmaPollCq: event_mode=%d bytes=%ld", event_mode ? 1 : 0, bytes);
```

#### 3.1.3 新增 bvar 监控 URMA 传输层

在 `urma_endpoint.cpp` 顶部新增 bvar，持续监控 URMA 传输层性能：

```c++
#include <bvar/bvar.h>
#include <butil/time.h>

// URMA 传输层 bvar
bvar::LatencyRecorder g_urma_send_latency("urma_send");
bvar::LatencyRecorder g_urma_recv_latency("urma_recv");
bvar::LatencyRecorder g_urma_pollcq_latency("urma_pollcq");
bvar::LatencyRecorder g_urma_dispatch_latency("urma_dispatch");
bvar::Adder<int64_t> g_urma_send_blocked_count("urma_send_blocked_count");
bvar::Adder<int64_t> g_urma_recv_post_count("urma_recv_post_count");
bvar::Adder<int64_t> g_urma_ack_count("urma_ack_count");
```

在关键路径中埋点：

```c++
// CutFromIOBufList (urma_endpoint.cpp:715)
butil::Timer tm;
tm.start();
// ... urma_post_jetty_send_wr ...
tm.stop();
g_urma_send_latency << tm.u_elapsed();
if (blocked) {
    g_urma_send_blocked_count << 1;
}

// PollCq (urma_endpoint.cpp:1187)
tm.start();
// ... urma_poll_jfc 循环 ...
tm.stop();
g_urma_pollcq_latency << tm.u_elapsed();

// DispatchReceivedBytes (urma_endpoint.cpp:1148)
tm.start();
// ... ProcessNewMessage ...
tm.stop();
g_urma_dispatch_latency << tm.u_elapsed();

// PostRecv (urma_endpoint.cpp:868)
g_urma_recv_post_count << n;

// SendAck (urma_endpoint.cpp:961)
g_urma_ack_count << 1;
```

通过 `/vars/urma_*` 查看所有 URMA 传输层 bvar。

#### 3.1.4 跨 bthread 传递 trace 上下文

当业务在处理请求时创建子 bthread 并在子 bthread 中发起 RPC 调用时，需使用
`BTHREAD_INHERIT_SPAN` 标志保持 trace 链不断裂（`src/bthread/types.h:55`）：

```c++
static const bthread_attr_t BTHREAD_ATTR_NORMAL_WITH_SPAN = {
    BTHREAD_STACKTYPE_NORMAL, BTHREAD_INHERIT_SPAN, NULL, BTHREAD_TAG_INVALID
};

bthread_t tid;
bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL_WITH_SPAN, ChildFunc, arg);
```

brpc 使用 `shared_ptr` / `weak_ptr` 管理 Span 生命周期（`src/brpc/span.h:64`），
即使 server 在子 bthread 完成前返回 response 也不会 use-after-free。

---

### 3.2 第二层：eBPF 非侵入式追踪

此层不改 brpc 源码，通过 eBPF uprobe / kprobe / tracepoint 在运行时追踪。

#### 3.2.1 已有工具：brpc 生命周期追踪

`tools/brpc_lifecycle_trace.sh` 追踪 11 个 brpc 函数阶段：

```bash
# 方式1：启动并追踪
sudo ./tools/brpc_lifecycle_trace.sh -e ./server -- --port 8080

# 方式2：附加到运行中的进程
sudo ./tools/brpc_lifecycle_trace.sh $(pidof server)

# 方式3：按进程名追踪
sudo ./tools/brpc_lifecycle_trace.sh -n server
```

追踪的 11 个阶段（`tools/brpc_lifecycle_trace.sh:127-139`）：

| 阶段 | 函数 |
|------|------|
| 1. CallMethod | `brpc::Channel::CallMethod` |
| 2. PackRpcRequest | `brpc::policy::PackRpcRequest` |
| 3. SocketWrite | `brpc::Socket::Write` |
| 4. UrmaSend | `CutFromIOBufList` |
| 5. UrmaPoll | `PollCq` |
| 6. UrmaDispatch | `DispatchReceivedBytes` |
| 7. ProcessNewMsg | `InputMessenger::ProcessNewMessage` |
| 8. ParseRpcMsg | `brpc::policy::ParseRpcMessage` |
| 9. ProcessRpcReq | `brpc::policy::ProcessRpcRequest` |
| 10. SendRpcResp | `brpc::policy::SendRpcResponse` |
| 11. ProcessRpcResp | `brpc::policy::ProcessRpcResponse` |

输出：每阶段延迟直方图（`@lat`）、总时间（`@sum`）、最大延迟（`@max`）、
慢调用计数（`@slow`，> 1ms）。

#### 3.2.2 已有工具：URMA 全管道追踪

`tools/urma_pipeline_trace.sh` 追踪 brpc 10 函数 + URMA 33 函数（7 数据面 +
26 控制面）：

```bash
# 追踪运行中的进程
sudo ./tools/urma_pipeline_trace.sh -p $(pidof server)

# 用 Python 解析输出
python3 tools/urma_pipeline_summary.py /tmp/urma_pipeline_*.log
```

URMA 数据面函数（`tools/urma_pipeline_trace.sh:112-120`）：

| 阶段 | URMA SDK 函数 | 源码调用点 |
|------|--------------|-----------|
| 11. urma\_post\_jetty\_send\_wr | 提交发送 WR | `urma_endpoint.cpp:797` |
| 12. urma\_post\_jetty\_recv\_wr | 提交接收 WR | （预留） |
| 13. urma\_post\_jfr\_wr | 提交接收 WR | `urma_endpoint.cpp:850` |
| 14. urma\_poll\_jfc | 轮询完成队列 | `urma_endpoint.cpp:1224` |
| 15. urma\_rearm\_jfc | 重武装 CQ 通知 | `urma_endpoint.cpp:1707` |
| 16. urma\_wait\_jfc | 等待 JFCE 事件 | `urma_endpoint.cpp:1678` |
| 17. urma\_ack\_jfc | 确认 JFC 事件 | `urma_endpoint.cpp:1251` |

`urma_pipeline_summary.py` 构建完整管道树，计算每阶段 P50/P90/P99 以及
exclusive（自）时间 = 总时间 - 子阶段时间。

#### 3.2.3 内核 URMA 追踪

`tools/kernel_urma_trace.bt` 追踪 URMA 路径相关的内核事件，**不含 TCP 内核
协议栈打点**（URMA 数据路径绕过 TCP 栈）：

```bash
# 替换 BRPC_BIN 占位符
sed "s|\${BRPC_BIN}|$(readlink -f /proc/$(pidof server)/exe)|g" \
    tools/kernel_urma_trace.bt > /tmp/kernel_urma_resolved.bt

# 运行
sudo bpftrace -p $(pidof server) /tmp/kernel_urma_resolved.bt
```

追踪的 6 个段落：

| 段落 | 探针类型 | 说明 |
|------|---------|------|
| 1. eventfd read | tracepoint | URMA event mode 的 CQ 事件通知（`sys_enter_read` / `sys_exit_read`） |
| 2. epoll\_wait 返回 | kretprobe | `do_epoll_wait` 返回 > 0 时记录时间戳，作为调度延迟基准 |
| 3. 网卡中断 | tracepoint | 硬中断（`irq_handler_entry/exit`）+ 软中断（`softirq_entry/exit`，NET\_RX） |
| 4. epoll→OnNewMessages 调度延迟 | uprobe | `InputMessenger::OnNewMessages` 入口，计算与 epoll 返回的时间差 |
| 5. epoll→PollCq 调度延迟 | uprobe | `UrmaEndpoint::PollCq` 入口，URMA CQ 事件到轮询的延迟 |
| 6. epoll→DispatchReceivedBytes 调度延迟 | uprobe | `UrmaEndpoint::DispatchReceivedBytes` 入口 |

> **设计说明**：TCP 内核协议栈打点（`tcp_sendmsg`、`tcp_recvmsg`、
> `tcp_v4_rcv`、`skb_copy_datagram_iter`）已被排除，因为 URMA 数据路径不经过
> TCP 栈。TCP 仅用于握手和 fallback，由 brpc upobe 追踪覆盖。

---

### 3.3 第三层：LD_PRELOAD 拦截

#### 3.3.1 已有工具：URMA LD_PRELOAD 拦截

`tools/urma_ldpreload_wrap.c` 通过 `dlsym(RTLD_NEXT, ...)` 拦截 URMA SDK
函数，记录每次调用的 count / total\_ns / max\_ns，> 1ms 时打印
`[SLOW-URMA]`。

```bash
# 编译
gcc -shared -fPIC -O2 -o liburma_trace_wrap.so tools/urma_ldpreload_wrap.c -ldl -lpthread

# 使用
LD_PRELOAD=./liburma_trace_wrap.so ./server

# 或通过启动器
./tools/urma_ldpreload_trace.sh ./server --port 8080
```

拦截的函数（`tools/urma_ldpreload_wrap.c:94-120`）：

**数据面（7 个）**：
`urma_post_jetty_send_wr`、`urma_post_jetty_recv_wr`、`urma_post_jfr_wr`、
`urma_poll_jfc`、`urma_rearm_jfc`、`urma_wait_jfc`、`urma_ack_jfc`

**控制面（8 个）**：
`urma_init`、`urma_uninit`、`urma_create_jetty`、`urma_import_jetty`、
`urma_import_seg`、`urma_register_seg`、`urma_create_context`、`urma_user_ctl`

进程退出时 destructor 将统计写入 `URMA_TRACE_FILE`（默认
`/tmp/urma_ldpreload_trace.log`）。

#### 3.3.2 已有工具：全栈追踪

`tools/urma_fullstack_trace.sh` 整合 LD\_PRELOAD 拦截 + ubsocket 内建
profiling，覆盖 brpc → ubsocket → umq → URMA 全链路：

```bash
# 在原有的启动命令前加上脚本
LD_PRELOAD=/path/to/libubsocket.so UBSOCKET_DEV_NAME="udmac0d1e2" \
    ./tools/urma_fullstack_trace.sh taskset -c 80-95 ./server --port=8333
```

自动完成：
1. 编译 `urma_ldpreload_wrap.c` → `/tmp/liburma_trace_wrap.so`
2. 设置 `UBSOCKET_SPLIT_TRACE_*` 和 `UBSOCKET_PROF_*` 环境变量
3. 将 wrap .so 注入 `LD_PRELOAD`（在 `libubsocket.so` 之前）
4. `exec` 目标进程，继承所有环境变量

---

## 4. 工具脚本一览

### 4.1 已有工具

| 文件 | 类型 | 开销/次 | 侵入性 |
|------|------|---------|--------|
| `tools/brpc_lifecycle_trace.sh` | bpftrace uprobe | ~200-500ns | 零 |
| `tools/urma_pipeline_trace.sh` | bpftrace uprobe | ~200-500ns | 零 |
| `tools/urma_uprobe_trace.bt` | bpftrace uprobe 模板 | ~200-500ns | 零 |
| `tools/urma_ldpreload_wrap.c` | LD\_PRELOAD | ~10-20ns | 低（需重启） |
| `tools/urma_ldpreload_trace.sh` | Bash 启动器 | - | 低 |
| `tools/urma_fullstack_trace.sh` | Bash 全栈 | - | 低 |
| `tools/urma_pipeline_summary.py` | Python 分析 | - | 零 |
| `tools/urma_trace_summary.py` | Python 分析 | - | 零 |
| `tools/kernel_urma_trace.bt` | bpftrace 内核 | ~100-200ns | 零 |

### 4.2 辅助工具

| 文件 | 用途 |
|------|------|
| `tools/pprof` | Google pprof 工具，分析 cpu/heap/contention profile |
| `tools/gdb_bthread_stack.py` | GDB 插件，打印运行中进程的 bthread 调用栈 |
| `tools/wireshark_baidu_std.lua` | Wireshark 插件，解析 baidu\_rpc 协议报文 |

---

## 5. 一键全流程追踪

以下脚本整合三层追踪，一键运行：

```bash
#!/bin/bash
# full_urma_trace.sh — URMA 全流程一键追踪
# 用法: sudo ./full_urma_trace.sh <pid> [duration]

set -euo pipefail
PID=$1
DURATION=${2:-60}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRPC_BIN=$(readlink -f /proc/$PID/exe)

echo "=== Full URMA Trace ==="
echo "PID: $PID  Binary: $BRPC_BIN  Duration: ${DURATION}s"

# 1. 开启 rpcz（如果未开启）
curl -s "http://localhost:8080/rpcz/enable" 2>/dev/null || true

# 2. 启动 brpc lifecycle trace（后台）
LOG1="/tmp/brpc_lifecycle_$(date +%Y%m%d_%H%M%S).log"
sudo bpftrace -p $PID \
    "$(${SCRIPT_DIR}/brpc_lifecycle_trace.sh --dry-run ${BRPC_BIN})" \
    2>&1 | tee "$LOG1" &
PID1=$!

# 3. 启动 URMA pipeline trace（后台）
LOG2="/tmp/urma_pipeline_$(date +%Y%m%d_%H%M%S).log"
sudo bpftrace -p $PID \
    "$(${SCRIPT_DIR}/urma_pipeline_trace.sh --dry-run ${BRPC_BIN})" \
    2>&1 | tee "$LOG2" &
PID2=$!

# 4. 启动内核 URMA trace（后台）
LOG3="/tmp/kernel_urma_$(date +%Y%m%d_%H%M%S).log"
sed "s|\${BRPC_BIN}|${BRPC_BIN}|g" "${SCRIPT_DIR}/kernel_urma_trace.bt" \
    > /tmp/kernel_urma_resolved.bt
sudo bpftrace -p $PID /tmp/kernel_urma_resolved.bt 2>&1 | tee "$LOG3" &
PID3=$!

# 5. 收集 ethtool 统计
IFACE=$(ip route show default 2>/dev/null | grep -oP 'dev \K\S+' || echo eth0)
ethtool -S $IFACE > /tmp/ethtool_before.txt 2>/dev/null || true

# 6. 收集 brpc bvar 快照
curl -s "http://localhost:8080/vars/" 2>/dev/null > /tmp/bvar_before.txt || true

# 7. 等待
echo "Tracing for ${DURATION}s... (Ctrl-C to stop early)"
sleep $DURATION

# 8. 停止所有 bpftrace
kill -INT $PID1 $PID2 $PID3 2>/dev/null || true
wait $PID1 $PID2 $PID3 2>/dev/null || true

# 9. 收集后续快照
ethtool -S $IFACE > /tmp/ethtool_after.txt 2>/dev/null || true
curl -s "http://localhost:8080/vars/" 2>/dev/null > /tmp/bvar_after.txt || true

# 10. 汇总
echo ""
echo "=== Summary ==="
echo "brpc lifecycle: $LOG1"
echo "urma pipeline:  $LOG2"
echo "kernel trace:   $LOG3"

diff /tmp/ethtool_before.txt /tmp/ethtool_after.txt > /tmp/ethtool_diff.txt 2>/dev/null || true
diff /tmp/bvar_before.txt /tmp/bvar_after.txt > /tmp/bvar_diff.txt 2>/dev/null || true

# 11. 解析 URMA pipeline 输出
if [[ -f "${SCRIPT_DIR}/urma_pipeline_summary.py" ]]; then
    echo ""
    echo "=== URMA Pipeline Summary ==="
    python3 "${SCRIPT_DIR}/urma_pipeline_summary.py" "$LOG2" 2>/dev/null || true
fi
if [[ -f "${SCRIPT_DIR}/urma_trace_summary.py" ]]; then
    echo ""
    echo "=== URMA Trace Summary ==="
    python3 "${SCRIPT_DIR}/urma_trace_summary.py" "$LOG2" 2>/dev/null || true
fi
```

---

## 6. 打点覆盖矩阵

| # | 阶段 | 源码位置 | 已有打点 | 增强方案 | 工具 |
|---|------|---------|---------|---------|------|
| 1 | CallMethod 入口 | `channel.cpp:476` | `gettimeofday_us` → `_begin_time_us` | TRACEPRINTF 序列化分段 | rpcz |
| 2 | PackRpcRequest | `baidu_rpc_protocol.cpp:1085` | 无 | bvar: `rpc_serialize` + TRACEPRINTF | bvar |
| 3 | Socket::Write | `socket.cpp:1624` | 无 | uprobe: `Socket::Write` | `brpc_lifecycle_trace.sh` |
| 4 | 连接建立 | `socket.cpp:1259` | 无 | uprobe: `ConnectIfNot` | `urma_pipeline_trace.sh` |
| 5 | Transport 路径选择 | `urma_transport.cpp:99` | 无 | bvar: TCP/URMA 路径计数 | bvar |
| 6 | CutFromIOBufList | `urma_endpoint.cpp:715` | 无 | uprobe + TRACEPRINTF | `urma_pipeline_trace.sh` |
| 7 | 窗口等待 | `urma_endpoint.cpp:758` | 无 | bvar: `urma_send_blocked_count` | bvar |
| 8 | `urma_post_jetty_send_wr` | `urma_endpoint.cpp:797` | LD\_PRELOAD | LD\_PRELOAD + uprobe | `urma_ldpreload_wrap.c` |
| 9 | 网卡 DMA 发送 | 硬件 | 无 | ethtool + eBPF tracepoint | `kernel_urma_trace.bt` |
| 10 | 网络物理传输 | 物理层 | 无 | 硬件时间戳 | `ethtool -S` |
| 11 | 对端网卡中断 | 硬件 | 无 | eBPF tracepoint: `irq_handler` | `kernel_urma_trace.bt` |
| 12 | `urma_post_jfr_wr` (补充 recv) | `urma_endpoint.cpp:850` | LD\_PRELOAD | LD\_PRELOAD + bvar | `urma_ldpreload_wrap.c` |
| 13 | SendAck / SendImm | `urma_endpoint.cpp:910/961` | 无 | bvar: `urma_ack_count` | bvar |
| 14 | `urma_poll_jfc` | `urma_endpoint.cpp:1224` | LD\_PRELOAD | uprobe + bvar | `urma_pipeline_trace.sh` |
| 15 | `urma_rearm_jfc` | `urma_endpoint.cpp:1707` | LD\_PRELOAD | uprobe | `urma_pipeline_trace.sh` |
| 16 | HandleCompletion | `urma_endpoint.cpp:971` | 无 | TRACEPRINTF SEND/RECV 分段 | rpcz |
| 17 | DispatchReceivedBytes | `urma_endpoint.cpp:1148` | 无 | uprobe + bvar | `urma_pipeline_trace.sh` |
| 18 | ProcessNewMessage | `input_messenger.cpp:195` | `received_us` + `base_realtime` | uprobe | `brpc_lifecycle_trace.sh` |
| 19 | ParseRpcMessage | `baidu_rpc_protocol.cpp:581/932` | `start_parse_us` | TRACEPRINTF | rpcz |
| 20 | ProcessRpcRequest | `baidu_rpc_protocol.cpp:581` | Span 完整 | TRACEPRINTF | rpcz |
| 21 | 反序列化请求 | `baidu_rpc_protocol.cpp:831` | 无 | bvar: `rpc_deserialize` + TRACEPRINTF | bvar |
| 22 | svc->CallMethod (用户回调) | `baidu_rpc_protocol.cpp:863` | `start_callback_us` + `AsParent()` | TRACEPRINTF | rpcz |
| 23 | SendRpcResponse | `baidu_rpc_protocol.cpp:288` | `start_send_us` + `sent_us` | TRACEPRINTF | rpcz |
| 24 | ProcessRpcResponse | `baidu_rpc_protocol.cpp:932` | Span 完整 | TRACEPRINTF | rpcz |
| 25 | SubmitSpan | `controller.cpp:1059` | `cpuwide_time_us` | — | rpcz |
| 26 | OnRPCEnd | `controller.cpp:1035` | `gettimeofday_us` | — | `/vars` |
| 27 | epoll 调度延迟 | `event_dispatcher_epoll.cpp:213` | 回调耗时 bvar | eBPF: epoll→回调 | `kernel_urma_trace.bt` |
| 28 | bthread 排队 | `task_group.cpp:446` | `bthread_creation` bvar | eBPF: epoll→PollCq | `kernel_urma_trace.bt` |
| 29 | 锁竞争 | `mutex.cpp:122` | contention profiler | — | `/pprof/contention` |

---

## 7. 实施路径

### 第一步：零改动快速诊断

无需修改 brpc 源码，直接使用已有工具：

```bash
# 1. 开启 rpcz
curl http://SERVER_URL/rpcz/enable

# 2. 运行 brpc 生命周期追踪
sudo ./tools/brpc_lifecycle_trace.sh -e ./server -- --port 8080

# 3. 运行 URMA 全管道追踪
sudo ./tools/urma_pipeline_trace.sh -p $(pidof server)

# 4. 运行内核 URMA 追踪
sed "s|\${BRPC_BIN}|$(readlink -f /proc/$(pidof server)/exe)|g" \
    tools/kernel_urma_trace.bt > /tmp/kernel_urma_resolved.bt
sudo bpftrace -p $(pidof server) /tmp/kernel_urma_resolved.bt

# 5. 解析管道输出
python3 tools/urma_pipeline_summary.py /tmp/urma_pipeline_*.log

# 6. 锁竞争分析
curl "http://SERVER_URL/pprof/contention?seconds=30" > contention.prof
python tools/pprof --text contention.prof
```

### 第二步：部署 bvar 持续监控

在 `urma_endpoint.cpp` 中新增 bvar（约 20 行代码），重编译后通过
`/vars/urma_*` 持续监控 URMA 传输层。

同时可在用户业务代码中部署业务级 bvar：

```c++
bvar::LatencyRecorder g_serialize_latency("rpc_serialize");
bvar::LatencyRecorder g_deserialize_latency("rpc_deserialize");
bvar::LatencyRecorder g_biz_logic_latency("biz_logic");

void HandleRequest() {
    butil::Timer tm;
    tm.start();
    // ... serialize ...
    tm.stop();
    g_serialize_latency << tm.u_elapsed();
    TRACEPRINTF("Serialize: %dus", tm.u_elapsed());
}
```

### 第三步：内核层深度追踪

```bash
# 生成并运行内核追踪脚本
sed "s|\${BRPC_BIN}|$(readlink -f /proc/$(pidof server)/exe)|g" \
    tools/kernel_urma_trace.bt > /tmp/kernel_urma_resolved.bt
sudo bpftrace -p $(pidof server) /tmp/kernel_urma_resolved.bt
```

### 第四步：一键全流程

```bash
sudo ./full_urma_trace.sh $(pidof server) 60
```

---

## 8. 开销评估

| 方案 | 额外开销/每RPC | 适用场景 |
|------|---------------|---------|
| rpcz + TRACEPRINTF | ~1-2 μs | 生产持续运行 |
| bvar (URMA 传输层) | ~2-3 μs | 生产持续运行 |
| uprobe (pipeline 33 函数) | ~10-15 μs | 临时诊断 |
| eBPF 内核追踪 (6 段) | ~1-2 μs | 临时诊断 |
| LD\_PRELOAD (URMA SDK 15 函数) | ~0.3 μs | 持续可用 |
| contention profiler | 采样式 | 锁问题诊断 |
| cpu profiler | 采样式 | CPU 热点诊断 |

**生产环境推荐**：rpcz + bvar + LD\_PRELOAD，总开销 < 5 μs。

**诊断环境推荐**：上述 + uprobe + eBPF kprobe，总开销 < 20 μs。

---

## 附录：URMA 数据路径详解

### 发送路径

```
Socket::Write(IOBuf)                                  [socket.cpp:1624]
  → StartWrite                                         [socket.cpp:1704]
    → _transport->CutFromIOBuf(&req->data)             [socket.cpp:1764]
      → UrmaTransport::CutFromIOBufList                 [urma_transport.cpp:99]
        → _urma_ep->CutFromIOBufList(buf, ndata)       [urma_transport.cpp:102]
          → UrmaEndpoint::CutFromIOBufList             [urma_endpoint.cpp:715]
            · 检查 _remote_rq_window_size + _sq_window_size
            · UrmaIOBuf::cut_into_sglist (零拷贝 SGE)   [urma_endpoint.cpp:679]
            · 预扣双窗口信用
            · urma_post_jetty_send_wr(URMA_OPC_SEND)   [urma_endpoint.cpp:797]
            · 失败则回滚窗口
```

### 接收路径

```
epoll(JFCE fd) 或 polling bthread
  → UrmaEndpoint::PollCq                                [urma_endpoint.cpp:1187]
    · event mode: urma_wait_jfc                         [urma_endpoint.cpp:1678]
    · urma_poll_jfc (循环排空 CQ)                       [urma_endpoint.cpp:1224]
      → HandleCompletion                                [urma_endpoint.cpp:971]
        · SEND 完成: 回收 SQ 窗口 → WakeAsEpollOut
        · RECV 完成: _rbuf[].cutn(_socket->_read_buf)  [urma_endpoint.cpp:1135]
                     → PostRecv → DoPostRecv            [urma_endpoint.cpp:835]
                       → urma_post_jfr_wr              [urma_endpoint.cpp:850]
                     → SendAck(1)                       [urma_endpoint.cpp:1143]
    · urma_ack_jfc (event mode)                        [urma_endpoint.cpp:1251]
    · urma_rearm_jfc (event mode)                       [urma_endpoint.cpp:1707]
    → DispatchReceivedBytes                             [urma_endpoint.cpp:1148]
      → InputMessenger::ProcessNewMessage               [input_messenger.cpp:195]
        → CutInputMessage → ParseRpcMessage → ProcessRpcRequest
        → UrmaTransport::QueueMessage (bthread 处理)    [urma_transport.cpp:155]
```

### 握手路径（TCP fd 上跑）

```
客户端: UrmaConnect::StartConnect                        [urma_endpoint.cpp:1589]
  → bthread: ProcessHandshakeAtClient                   [urma_endpoint.cpp:1436]
    · AllocateResources (create_jfce/jfc/jfr/jetty + CQ socket)
    · PostRecv (urma_post_jfr_wr × rq_size)
    · SendLocalHello (TCP fd: "URMA"/"URM3" + hello)
    · ReceiveAndParseRemoteHello
    · ApplyRemoteHello (设窗口)
    · ImportPeer (urma_import_seg → urma_import_jetty)
    · WriteToFd(ACK) → ESTABLISHED

服务端: OnNewDataFromTcp(UNINIT)                         [urma_endpoint.cpp:1313]
  → bthread: ProcessHandshakeAtServer                   [urma_endpoint.cpp:1497]
    · ReadFromFd(magic) → CreateServerHandshakeByMagic
    · (后续同客户端流程，顺序略不同)
```

### 关键配置

| Flag | 默认值 | 说明 |
|------|--------|------|
| `urma_use_polling` | `false` | 使用 busy poll (true) 还是 event mode (false) |
| `urma_poller_num` | `1` | 每个 bthread tag 的 poller 数 |
| `urma_sq_size` | `128` | 本地 JFS 深度 [16, 4096] |
| `urma_rq_size` | `128` | 本地 JFR 深度 [16, 4096] |
| `urma_cqe_poll_once` | `32` | 每次 `urma_poll_jfc` 最大 CQE 数 |
| `urma_recv_zerocopy` | `true` | 接收零拷贝 |
| `urma_zerocopy_min_size` | `512` | 小于此值则拷贝 |
| `urma_max_sge` | `0` | 每 WR 最大 SGE (0=设备最大) |
| `urma_buffer_size` | `8192` | buffer pool 每块大小 |
| `urma_buffer_count` | `65536` | buffer pool 块数 |
| `enable_rpcz` | `false` | 开启 rpcz |
| `rpcz_keep_span_seconds` | `3600` | Span 保留时间（秒） |
| `rpcz_save_span_min_latency_us` | `0` | 仅保存延时超过此值的 Span |
