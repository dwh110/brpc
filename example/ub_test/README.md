# ub_test — brpc UB 性能压测工具

基于 brpc 的性能压测工具集，用于测量 brpc 服务（TCP / RDMA / UBsocket）的建连性能、首包延迟与稳态吞吐，支持进程级并发控制、精确限流、跨实例 P99 聚合。

## 目录结构

| 文件 | 说明 |
|---|---|
| `test.proto` | RPC 协议定义（`PerfTestService.Test`） |
| `server.cpp` | 被压测服务端：请求回显、CPU/内存采样、超时后输出最终报表 |
| `client.cpp` | 压测客户端：建连统计 + 首包统计 + 稳态性能压测 |
| `aggregate_p99.py` | 跨实例延迟分位聚合脚本（合并多个客户端导出的百分位样本） |
| `CMakeLists.txt` | cmake 构建（备选；官方构建走 bazel） |

## 构建

```bash
# 基础构建（TCP）
bazel build -c opt //example:ub_test_client //example:ub_test_server

# 启用 UBsocket（URMA）
bazel build -c opt --define brpc_with_urma=true --linkopt=-L/usr/lib64/urma \
    //example:ub_test_client //example:ub_test_server
```

> 注：启用 URMA 的构建需要可访问 ubsocket 依赖仓库（gitcode.com）。离线环境可参照 `CMakeLists.txt` 用已安装的 brpc 头文件/静态库手工编译。

## 快速开始

```bash
# 终端 1：启动服务端（监听 8002）
./bazel-bin/example/ub_test_server --port=8002

# 终端 2：启动压测（100 条连接、全局并发 32、压测 20s）
./bazel-bin/example/ub_test_client --servers=127.0.0.1:8002 \
    --link_num=100 --queue_depth=32 --test_seconds=20
```

## 测试流程

客户端一次运行分三个阶段（`--warmup` 开启时加预热阶段）：

```
阶段0（可选）预热建链    — 预先建立 warmup_connections 条连接后立即释放，消除服务端冷启动效应
阶段1  分批建链          — bthread 池有界并发建链（batch_size/batch_interval_ms 控制节奏），
                           每条连接执行一次首包 RPC，收集首包统计
阶段2  首包统计输出      — Channel-Init-Latency / First-RPC-Latency / First-RPC-Memory
阶段3  稳态压测          — 中央发送泵模型，全部连接一次性激活，进程级并发 = queue_depth
```

### 并发模型（中央发送泵）

`queue_depth` 为**整个 client 进程的全局在途请求上限**（跨所有连接共享），而非每连接并发：

```
令牌桶(可选) ──► SendPump × K ──轮询──► 连接池 [全部活跃连接]
 expected_qps     inflight wait → token wait       │
                    ▲                               ▼
            sem_post│                    HandleResponse（统计 / 全局停止判定）
```

- 发送泵个数 K 由 `--sender_thread_num` 指定（默认 auto = `min(queue_depth, 16)`）

**`queue_depth` 与 `sender_thread_num` 的区别**（两者正交，作用于不同维度）：

| 参数 | 控制维度 | 说明 |
|---|---|---|
| `queue_depth` | 并发上限 | 进程级在途请求数上限（全局信号量 `g_inflight_sem` 的许可数）。决定流量上限：实测 QPS 上限 ≈ `queue_depth / 平均延迟` |
| `sender_thread_num` | 发送并行度 | 发送泵 bthread 个数 K。决定客户端能否足够快地把许可转化为实际发送，泵本身不占用在途名额 |

- 恒有 `在途请求数 ≤ queue_depth`；`sender_thread_num` 超过 `queue_depth` 时会被截断为 `queue_depth`（多余的泵永远拿不到许可）
- 类比：`queue_depth` = 道路同时行驶车辆数上限（容量），`sender_thread_num` = 入口收费站通道数（放行能力）
- 调优：泵执行一次"等许可 → 组装请求 → 发送"是微秒级，默认 auto 对绝大多数场景（含几十万 QPS）足够；仅当 `queue_depth` 很大（如 1000+）且服务端延迟极低、实测 QPS 明显低于 `queue_depth / 平均延迟` 理论值时，再尝试调大（如 `--sender_thread_num=32`）
- 与限流正交：泵先取许可（`queue_depth`，管并发，由响应释放）再取令牌（`expected_qps`，管速率）——许可按响应节奏逐个到达，令牌消费被响应节奏摊平，避免令牌批量到账时多个泵同时抢令牌造成发送突发

- 停止条件为**全局**：`test_iterations` = 压测阶段总完成请求数；`test_seconds` = 压测阶段总时长，任一先到即停
- 停止时在途请求会继续完成，最终完成数最多超出 `test_iterations` 约 `queue_depth` 个

## 客户端参数

### 连接与建链

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--link_num` | -1 | 总连接数（优先于 `thread_num`）；≤0 时进入扫描模式 |
| `--thread_num` | 1 | 总连接数（旧语义，`link_num` 已设置则忽略） |
| `--batch_size` | 0 | 建链分批大小（0 = 一次全部） |
| `--batch_interval_ms` | 0 | 建链批间间隔（毫秒） |
| `--thread_pool_size` | 8 | 建链 worker 池大小（有界并发） |
| `--warmup` | false | 正式建链前先预热（消除冷启动效应） |
| `--warmup_connections` | 100 | 预热连接数（0 = 同 link_num/thread_num） |
| `--servers` | `0.0.0.0:8002+0.0.0.0:8002` | 服务端列表，`+` 分隔（轮询选取） |
| `--servers_file` | "" | 从文件读取服务端列表（每行一个，`#` 开头为注释） |
| `--connection_type` | single | Channel 连接类型（single/pooled/short） |
| `--use_connection_group` | false | 每条连接使用独立 connection_group |

### 并发与限流

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--queue_depth` | 1 | **进程级**全局在途请求上限（跨所有连接） |
| `--sender_thread_num` | 0 | 发送泵 bthread 数（0 = 自动 `min(queue_depth, 16)`） |
| `--expected_qps` | 0 | 期望 QPS 限流（0 = 不限流，令牌桶实现） |
| `--initial_tokens` | 10 | 令牌桶初始令牌数（越小启动越平缓、初始突发越少） |
| `--token_tick_us` | 1000 | 令牌生成 tick 间隔（微秒）。tick 越小令牌发放越平滑，避免批量唤醒泵造成发送突发与尾延迟劣化；高 QPS（>10 万）时可适当调小 |
| `--token_interval_period` | 1 | 令牌生成 tick 倍率（实际 tick = `token_tick_us × 该值`） |
| `--max_random_delay_us` | 5000 | 限流模式下发送前随机抖动上限（微秒），避免突发 |

### 停止条件（全局）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--test_seconds` | 20 | 压测阶段总时长（秒）。**0 = 不限时长**（纯 iterations 模式） |
| `--test_iterations` | 0 | 压测阶段总完成请求数（0 = 不限次数） |
| `--only_first_rpc` | false | 只测首包：建连后保持连接 `test_seconds` 秒即退出，跳过压测 |
| `--test_keep_alive` | false | 压测结束后保持连接 10 秒再释放 |
| `--max_thread_num` | 16 | 扫描模式下连接数上限 |

> `test_seconds` 与 `test_iterations` 至少一个 > 0，否则压测永不停止、直接报错退出。

### RPC 与协议

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--protocol` | baidu_std | RPC 协议类型 |
| `--use_rdma` | false | 使用 RDMA 传输 |
| `--rpc_timeout_ms` | 2000 | RPC 超时（毫秒） |
| `--connect_timeout_ms` | 2000 | 连接超时（毫秒） |
| `--max_retry` | 3 | 每 RPC 最大重试次数（映射 ChannelOptions::max_retry） |
| `--connect_retry_interval` | 200 | 首包 RPC 手动重试退避基数（毫秒），实际延迟随机于 `[v, 2v]` |
| `--client_ignore_oc` | false | 忽略 eovercrowded 错误 |
| `--dummy_port` | 8001 | 客户端内置 dummy server 端口（RDMA 场景需要，避免与占用端口冲突） |

### 负载

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--attachment_size` | 0 | 请求 attachment 大小（字节，随机内容）；**<0 时进入大小扫描模式** |
| `--echo_attachment` | false | 服务端回显 attachment（用于测回显带宽） |
| `--req_size` | "" | 请求 name 字段大小（字节），逗号分隔支持多个 size（如 `128,1024,4096`）；请求按全局发送索引轮询平均分配各 size，首包 RPC 固定使用第一个 size；设置后带宽按响应 name 计，否则按 attachment 计 |

### 统计输出

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--debug_latency` | false | 打印时延明细：每个成功请求的时延 + 全部首包时延列表 |
| `--export_percentile_file` | "" | 导出百分位样本到二进制文件（供 aggregate_p99.py 跨实例聚合） |
| `--test_connections_per_round` | 0 | **[deprecated]** 仅保留 CLI 兼容，不再生效 |
| `--round_period_ms` | 0 | **[deprecated]** 仅保留 CLI 兼容，不再生效 |

### 扫描模式（main 内自动组合）

| 条件 | 行为 |
|---|---|
| `link_num > 0` 且 `attachment_size >= 0` | 单次压测 |
| `link_num <= 0` 且 `attachment_size >= 0` | 连接数从 1 到 `max_thread_num` 翻倍扫描 |
| `link_num > 0` 且 `attachment_size < 0` | attachment 从 1B 到 1024B ×4 扫描 |
| `link_num <= 0` 且 `attachment_size < 0` | 双重扫描 |

## 服务端参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--port` | 8002 | 监听端口 |
| `--use_rdma` | false | 使用 RDMA |
| `--num_threads` | 5 | brpc server 线程数 |
| `--max_concurrency` | 128 | 服务端最大并发 |
| `--server_bthread_concurrency` | 5 | 服务端 bthread 并发度 |
| `--rsp_size` | 0 | 响应 name 字段大小（字节）；**0 时回显请求 name**（配合客户端多 size `req_size` 可测各 size 往返带宽） |
| `--server_ignore_oc` | false | 忽略 eovercrowded |
| `--stats_timeout_seconds` | 3 | 压测结束后等待统计的宽限（秒） |
| `--test_seconds` | 40 | 服务端统计窗口（秒），**应 ≥ 客户端压测时长 + 建链耗时 + stats_timeout_seconds** |

服务端从收到**第一个请求**开始计时，达到 `test_seconds + stats_timeout_seconds` 后输出最终报表（总请求数 / QPS / 带宽 / 延迟分位 / CPU / 内存）。客户端建链阶段的首包 RPC 也计入服务端统计。

## UB（UBsocket）相关参数

以下 flag 由 brpc 提供，client 与 server 均支持（需以 `--define brpc_with_urma=true` 构建）：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--ubsocket_enable` | false | 启用 ubsocket 插桩层：socket 调用路由到 ubsocket |
| `--ubsocket_use_ub` | false | 使用 UB 原生数据面（ubs_post/ubs_poll） |
| `--ubsocket_degrade_enable` | true | UB 失败时允许降级为 TCP（UB→TCP 降级总开关） |

降级机制详见 [ADR-0001](../../docs/adr/0001-ub-tcp-degradation-detection-over-recreation.md)（UB_PENDING/UB_ON/UB_OFF 三态模型）。完整的 UB→TCP 降级 E2E 测试可参考仓库根目录 `run_ub_degrade_test.sh`（覆盖 6 个场景：正常 UB / 降级 / 跨协议 / 运行时故障恢复等）。

典型 UB 压测：

```bash
# 服务端
./bazel-bin/example/ub_test_server --port=8002 \
    --ubsocket_enable=true --ubsocket_use_ub=true --ubsocket_degrade_enable=false

# 客户端
./bazel-bin/example/ub_test_client --servers=127.0.0.1:8002 \
    --ubsocket_enable=true --ubsocket_use_ub=true --ubsocket_degrade_enable=true \
    --link_num=100 --queue_depth=32 --test_seconds=20
```

## 输出指标说明

### 首包统计（客户端阶段 2）

| 指标 | 含义 |
|---|---|
| Channel-Init-Latency | `Channel::Init()` 调用耗时 |
| First-RPC-Latency | 首次 RPC 端到端耗时（含真正的 TCP/UB 建连与握手；手动重试测量单次成功 RPC，不叠加重试时间） |
| First-RPC-Memory | 首包成功时刻进程常驻内存 |

### 压测统计（客户端阶段 3）

| 指标 | 含义 |
|---|---|
| Avg/50th/90th/99th/99.9th/99.99th-Latency | 客户端侧 RPC 延迟（bvar::LatencyRecorder 聚合，无锁） |
| Throughput | 响应字节带宽（MiB/s；未设置 `req_size` 时按 attachment 计，否则按响应 name 计） |
| QPS | 完成请求数 / 压测时长 |
| Server CPU(avg/max) | 服务端 CPU 占用（服务端每 100ms 采样一次，每周期仅一个响应携带，其余为空——不污染热路径） |
| Client CPU/Memory(avg/max) | 客户端每 100ms 采样一次 |
| Total Sent / Total Requests / Total Errors | 已发送 / 已完成 / 失败数，Error rate = 失败占比 |

### 服务端最终报表

统计窗口结束后打印：总请求数、QPS、带宽、服务端延迟分位（avg/p99/p999/p9999）、CPU（avg/max）、内存（avg/max）。

## 跨实例 P99 聚合

单实例的百分位样本不足以代表多客户端整体延迟。客户端可用 `--export_percentile_file` 导出水库采样快照，再用 `aggregate_p99.py` 按 bvar 原生 `combine_of` 算法合并：

```bash
# 各客户端压测时导出样本
./bazel-bin/example/ub_test_client ... --export_percentile_file=client_1.pct
./bazel-bin/example/ub_test_client ... --export_percentile_file=client_2.pct

# 聚合（可选 --seed 保证结果可复现）
python3 aggregate_p99.py --seed 42 client_1.pct client_2.pct

# JSON 输出、自定义分位
python3 aggregate_p99.py --json --percentiles 50,90,99,99.9,99.99 client_*.pct
```

## 典型场景

```bash
# 1. 首包延迟专项（大量连接建连性能）
./ub_test_client --servers=10.0.0.1:8002 --link_num=5000 --only_first_rpc \
    --batch_size=500 --batch_interval_ms=100 --thread_pool_size=16

# 2. 精确限流压测（1 万 QPS，全局并发 64；initial_tokens 用默认 10 平缓启动）
./ub_test_client --servers=10.0.0.1:8002 --link_num=200 --queue_depth=64 \
    --expected_qps=10000 --test_seconds=60

# 3. 大包回显带宽测试（1MB attachment，回显）
./ub_test_client --servers=10.0.0.1:8002 --link_num=32 --queue_depth=16 \
    --attachment_size=1048576 --echo_attachment=true --test_seconds=30

# 4. 纯迭代次数模式（完成 10 万请求即停）
./ub_test_client --servers=10.0.0.1:8002 --link_num=100 --queue_depth=32 \
    --test_iterations=100000 --test_seconds=0

# 5. 压测后保持连接观察资源占用
./ub_test_client --servers=10.0.0.1:8002 --link_num=100 --queue_depth=32 \
    --test_seconds=20 --test_keep_alive=true

# 6. 混合包大小压测（64B/1KB/16KB 按发送索引轮询平均分配）
#    服务端 rsp_size=0（默认）时回显请求 name，客户端 Throughput 反映各 size 混合往返带宽
./ub_test_client --servers=10.0.0.1:8002 --link_num=100 --queue_depth=32 \
    --req_size=64,1024,16384 --test_seconds=30
```

## 注意事项与已知限制

1. **全局并发语义**：`queue_depth` 是进程级上限。例如 `link_num=100`、`queue_depth=10` 表示全部 100 条连接共享 10 个在途请求（旧版语义为每连接 10 个、共 1000 个）。
2. **停止条件至少其一**：`test_seconds` 与 `test_iterations` 不能同时为 0。
3. **服务端统计窗口**：服务端 `--test_seconds` 需覆盖客户端建链耗时 + 压测时长 + `stats_timeout_seconds`，否则服务端会提前收统计。
4. **亚秒级运行的延迟统计为 0**：客户端延迟/CPU/内存统计基于 bvar `Window`（1s 采样 tick），运行时长 < 1s 时这些指标显示 0，属既有行为，非故障。
5. **大包压测**：attachment 或响应超过 brpc 默认限制时，需同步调大 brpc 内置 flag，如 `--max_body_size`、`--socket_max_unwritten_bytes`（见 `run_ub_degrade_test.sh` 示例）。
6. **端口占用**：客户端启动时会固定占用 dummy server 端口（默认 8001），并预申请 fd 20000（规避压测期间 fd 表扩容抖动）。
7. **`--warmup` 不计统计**：预热建链的延迟/内存不进入首包统计。
8. **停止溢出**：`test_iterations` 模式下，触发停止时仍在途的请求会继续完成，最终完成数最多多出 `queue_depth` 个。
