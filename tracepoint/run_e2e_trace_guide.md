# run_e2e_trace.py 操作指导

URMA 端到端时延打点脚本，用于定位 io_mode=2 1KB QPS=1000 场景下的时延分布。

## 1. 前置条件

### 本地环境
- Python 3.x
- paramiko 库：`pip install paramiko`

### 远端环境
| 节点 | IP | 角色 | 路径 |
|------|-----|------|------|
| 202 | 141.61.17.202 | server + 编译机 | `/home/d00836578/brpc_workspace/brpc_urma/brpc` |
| 204 | 141.61.17.204 | client | `/home/d00836578/brpc_workspace/brpc_urma/ub_test_client` |

- 两台节点已安装 bazel、numactl、URMA 用户态库
- SSH 密码已配置在脚本中（`USER='d00836578'`, `PASS='dwh123456'`）

### 上传文件列表（13 个源文件）
```
src/brpc/controller.cpp
src/brpc/controller.h
src/brpc/input_message_base.h
src/brpc/input_messenger.cpp
src/brpc/input_messenger.h
src/brpc/transport.h
src/brpc/channel.cpp
src/brpc/policy/baidu_rpc_protocol.cpp
src/brpc/urma/urma_endpoint.cpp
src/brpc/urma/urma_endpoint.h
src/brpc/urma/urma_helper.cpp
src/brpc/urma/urma_one_sided.h
example/BUILD.bazel
```

## 2. 使用方式

### 2.1 完整流程（上传 → 编译 → 同步 → 测试 → 分析）

```bash
python run_e2e_trace.py
```

适用场景：首次运行或源代码有修改时。耗时约 5-10 分钟（主要是 bazel 编译）。

### 2.2 跳过上传（编译 → 同步 → 测试 → 分析）

```bash
python run_e2e_trace.py --skip-upload
```

适用场景：远端源码已是最新，只需重新编译。

### 2.3 仅测试+分析（复用已有 binary）

```bash
python run_e2e_trace.py --test-only
# 或等价的
python run_e2e_trace.py --skip-build
```

适用场景：远端 binary 已包含 E2E trace 代码，只需跑测试。耗时约 30 秒。

### 2.4 仅分析（离线分析本地日志）

```bash
python run_e2e_trace.py --analyze-only
```

适用场景：已有 `e2e_client_trace.log`，只需重新分析。耗时 <1 秒。

### 2.5 参数汇总

| 参数 | 跳过的阶段 | 执行的阶段 | 耗时 |
|------|-----------|-----------|------|
| （无） | 无 | upload + build + sync + test + analyze | 5-10 min |
| `--skip-upload` | upload | build + sync + test + analyze | 5-10 min |
| `--skip-build` | upload + build + sync | test + analyze | ~30s |
| `--test-only` | upload + build + sync | test + analyze | ~30s |
| `--analyze-only` | upload + build + sync + test | analyze | <1s |

## 3. 五个阶段详解

### Stage 1: Upload
将本地修改的 13 个源文件通过 SFTP 上传到 202 的 `$REMOTE_BRPC/src/brpc/` 对应目录。

### Stage 2: Build
在 202 上执行 bazel 编译（与 build_and_test.py 一致）：
```bash
cd /home/d00836578/brpc_workspace/brpc_urma/brpc && \
export http_proxy="http://141.1.37.126:7777" && \
export https_proxy="http://141.1.37.126:7777" && \
export CPLUS_INCLUDE_PATH=//usr/include/ub/:/usr/include/ub/umdk/:/usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && \
bazel build //example:ub_test_server //example:ub_test_client \
  --define=BRPC_WITH_URMA=true --repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0 -c opt \
  --spawn_strategy=standalone --define=BUTIL_USE_CPU_FREQUENCY=true
```
- `--define=BRPC_WITH_URMA=true`：启用 URMA 传输层代码（`#if BRPC_WITH_URMA` 条件编译块）
- `--repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0`：使用本地 URMA 头文件，不远程下载
- `--spawn_strategy=standalone`：禁用 bazel sandbox 隔离，确保 external repo（@umdk）头文件可访问
- `--define=BUTIL_USE_CPU_FREQUENCY=true`：启用 CPU 频率相关功能
- `http_proxy/https_proxy`：通过代理访问外部资源
- `CPLUS_INCLUDE_PATH`：指定 URMA 用户态库头文件路径

### Stage 3: Sync
将 202 上编译出的 `ub_test_client` 通过本地中转（SFTP download → SFTP upload）拷贝到 204。

### Stage 4: Test
1. 在 202 启动 server（numactl 绑定 CPU 96-111、node 1）
2. 在 204 运行 client（5 秒，QPS=1000，1KB 请求）
3. 从 client 日志中 grep `[URMA-E2E]` 行，保存到本地 `e2e_client_trace.log`

关键 gflag：`--urma_trace_latency=true`（server 和 client 都需要加）
- 控制每 RPC 的 19 阶段时间戳打点
- 控制每 RPC 的 `LOG(INFO)` 输出
- 控制 bvar 统计聚合

### Stage 5: Analyze
解析 `[URMA-E2E]` 日志行，输出：
1. **Client 侧 9 阶段**（monotonic 时钟，可靠）：c_ser, c_queue, c_post, c_event, c_cq, c_msg, c_bthread, c_deser, c_done
2. **Network 残差**：`total - sum(client stages)`，包含上行链路 + server 处理 + 下行链路
3. **全 19 阶段表格**：包含 server 侧阶段（通过 RpcMeta user_fields 传递，若可用）
4. **时延分解**：Client local vs Network 占比
5. **Total 时延分布**：p50/p75/p90/p95/p99/p99.9/p99.99
6. **Sample 记录**（前 5 条原始数据）
7. **瀑布图**：按 RPC 执行顺序展示各阶段 avg 时延的横向条形图

数据处理规则：
- 跳过前 100 条预热数据
- outlier 过滤：>10000us 的数据单独标注 `[filtered]` 统计

## 4. 输出解读

### 典型输出示例

```
BREAKDOWN (averages)
------------------------------------------------------------
  Client local: 8.0 us (25%)
  Network:      24.6 us (75%)
  Total:        32.6 us
```

- **Client local**：客户端本地处理耗时（序列化 + 发送 + 接收 + 反序列化）
- **Network**：网络往返 + server 处理（残差法计算，因为两台服务器有 25 秒时钟偏移，无法单独测量上行/下行）

### 瀑布图示例

```
================================================================================
WATERFALL CHART (avg latency per stage, in RPC execution order)
================================================================================
  Total avg: 32.6 us  |  bar scale: 24.6 us

  CLIENT Client serialize           0.3 us  #                                                   ( 1.1%)
  CLIENT Client queue               0.0 us  #                                                   ( 0.0%)
  CLIENT Client URMA post           1.4 us  ##                                                  ( 4.4%)

  NET   Uplink (net)           ~  24.6 us  [network residual, clock-skewed]

  SERVER Server event               0.0 us  [not transmitted]
  SERVER Server CQ drain            0.0 us  [not transmitted]
  ...

  NET   Downlink (net)         ~  24.6 us  [network residual, clock-skewed]

  CLIENT Client event               4.3 us  ########                                            (13.1%)
  CLIENT Client CQ drain            0.3 us  #                                                   ( 1.0%)
  ...

  NET   Network (residual)        24.6 us  ==================================================  (75.4%)
  NET   Client local sum           8.0 us
  NET   Total                     32.6 us
```

瀑布图按 RPC 执行顺序从上到下排列，分为三组：
- **CLIENT**：客户端本地阶段（monotonic 时钟，可靠）
- **SERVER**：server 侧阶段（通过 RpcMeta user_fields 传递；若未传递则标记 `[not transmitted]`）
- **NET**：网络阶段（uplink/downlink 因时钟偏移无法单独测量，用残差法合并为 Network residual）

条形图宽度与该阶段 avg 时延成正比，`#` 表示客户端/服务端阶段，`=` 表示网络残差。括号内百分比为该阶段占 total avg 的比例。

**如何阅读瀑布图**：
- 最长的条是瓶颈所在——上例中 Network (residual) 占 75.4%，说明主要时间花在网络往返 + server 处理
- Client event (4.3us, 13.1%) 是客户端最大的单阶段耗时，对应 URMA CQ 中断 → bthread 调度延迟
- 若 server 阶段全部为 `[not transmitted]`，说明 binary 未实现 RpcMeta user_fields 传递，需重新编译

### 19 阶段说明

| # | 阶段 | 含义 | 时钟 |
|---|------|------|------|
| 1 | c_ser | client 序列化 | monotonic |
| 2 | c_queue | client 发送排队 | monotonic |
| 3 | c_post | client URMA post | monotonic |
| 4 | uplink | 上行链路（client post → server recv） | gettimeofday（跨网络） |
| 5 | s_event | server 中断/bthread 调度 | monotonic |
| 6 | s_cq | server CQ drain | monotonic |
| 7 | s_msg | server 消息接收 | monotonic |
| 8 | s_bthread | server 消息处理 bthread 调度 | monotonic |
| 9 | s_deser | server 反序列化 | monotonic |
| 10 | s_svc | server 服务处理 | monotonic |
| 11 | s_ser | server 序列化 | monotonic |
| 12 | s_queue | server 发送排队 | monotonic |
| 13 | s_post | server URMA post | monotonic |
| 14 | downlink | 下行链路（server post → client recv） | gettimeofday（跨网络） |
| 15 | c_event | client 中断/bthread 调度 | monotonic |
| 16 | c_cq | client CQ drain | monotonic |
| 17 | c_msg | client 消息接收 | monotonic |
| 18 | c_bthread | client 消息处理 bthread 调度 | monotonic |
| 19 | c_deser | client 反序列化 | monotonic |

### 已知限制

- **时钟偏移**：两台服务器有 ~25 秒时钟差，NTP 不可用。`uplink` 和 `downlink` 单独测量值不可靠（显示为 25336306us），因此用残差法计算合并网络延迟。
- **Server 阶段传递**：server 侧时间戳通过 RpcMeta `user_fields` 传递到 client。若 binary 版本不支持，server 阶段全部为 0。
- **`c_done`**：client EndRPC 完成时间戳，标记整个 RPC 结束。

## 5. 测试参数

脚本中的默认参数（可在脚本顶部修改）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| IO_MODE | 2 | URMA IO 模式（0=SEND, 1=WRITE, 2=HYBRID） |
| REQ_SIZE | 1024 | 请求大小（字节） |
| RSP_SIZE | 1024 | 响应大小（字节） |
| EXPECTED_QPS | 1000 | 目标 QPS |
| TEST_SECONDS | 5 | 测试持续时间（秒） |
| SEND_BUF_KB | 2048 | 单边发送缓冲（KB） |
| RECV_BUF_KB | 2048 | 单边接收缓冲（KB） |
| NUM_THREADS | 16 | server 线程数 |
| PORT | 10918 | 服务端口 |

numactl 参数：`-C 96-111 -m 1`（绑定 CPU 96-111，内存 node 1）

## 6. 输出文件

| 文件 | 位置 | 说明 |
|------|------|------|
| `e2e_client_trace.log` | 本地脚本目录 | client 侧 `[URMA-E2E]` 日志，可重复用 `--analyze-only` 分析 |
| `/tmp/e2e_client.log` | 204 (client) | client 完整 stdout（含 trace + 测试结果） |
| `/tmp/ub_test_server.log` | 202 (server) | server 日志 |

## 7. 常见问题

### Q: `[URMA-E2E] lines: 0`
**原因**：远端 binary 不含 E2E trace 代码（旧版本编译）。
**解决**：跑完整流程 `python run_e2e_trace.py` 重新编译。

### Q: 编译报错 `UrmaReasmCtx was not declared`
**原因**：`urma_one_sided.h` 未上传到远端。
**解决**：确认 FILES_TO_UPLOAD 列表包含 `src/brpc/urma/urma_one_sided.h`。

### Q: 编译报错 `undefined symbol: brpc::urma::fLB::FLAGS_urma_trace_latency`
**原因**：编译命令缺少 `--define BRPC_WITH_URMA=true`。
**解决**：确认 BUILD_CMD 包含该参数（脚本已内置）。

### Q: 编译报错 `urma_types.h: No such file or directory`
**原因**：bazel sandbox 隔离导致 external repo 头文件不可访问。
**解决**：确认 BUILD_CMD 包含 `--spawn_strategy=standalone`（脚本已内置）。

### Q: `Server failed to start`
**原因**：端口被占用或 binary 路径错误。
**解决**：检查 `pkill -f ub_test_server` 是否清理干净，确认 SERVER_BIN 路径正确。

### Q: downlink/uplink 显示 25336306us
**原因**：两台服务器 ~25 秒时钟偏移，gettimeofday 跨网络测量不可靠。
**解决**：这是预期行为，用 Network 残差值替代单向上行/下行测量。

### Q: server 阶段（s_event, s_cq 等）全部为 0
**原因**：binary 版本未实现 RpcMeta user_fields 传递 server 时间戳。
**解决**：重新编译包含完整 E2E trace 打点的代码。

## 8. 快速参考

```bash
# 首次使用（完整流程）
python run_e2e_trace.py

# 日常调试（复用 binary）
python run_e2e_trace.py --test-only

# 反复分析已有日志
python run_e2e_trace.py --analyze-only
```
