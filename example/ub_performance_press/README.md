# 简介
ub_performance_press 是 bRPC 性能测试工具，用于测试 bRPC over UBSocket 场景的性能。

本目录基于同仓的 `example/ub_performance` 拷贝并演进，适配更新版本的 protobuf 协议结构和更贴近真实业务负载的数据生成方式，适配 UBSocket 使用。

> 说明：
> 与 `example/ub_performance` 相比，本目录（press 变体）的差异见下文「与 ub_performance 的区别」。

# 使用方式
下载bRPC源码，参考[bazel编译bRPC](https://gitcode.com/fanzhaonan/brpc/blob/develop/docs/cn/bazel_brpc.md)及[brpc支持rdma](https://github.com/apache/brpc/blob/release-1.15/docs/cn/rdma.md)的介绍进行编译和运行即可。

建议编译命令：

`bazel build -c opt //example:ub_performance_press_server --define brpc_with_urma=true`

`bazel build -c opt //example:ub_performance_press_client --define brpc_with_urma=true`

> 说明：
> 不同的测试用例，在执行client、server时需要指定不同的参数，可在参考文档中了解。

# 与 ub_performance 的区别

`ub_performance` 与 `ub_performance_press` 都是 bRPC over UBSocket 的性能测试用例，主要差异如下：

| 维度 | ub_performance | ub_performance_press |
|------|----------------|----------------------|
| proto 文件 | `test.proto`，`syntax=proto2`，`package test` | `press.proto`，`syntax=proto3`，`package press` |
| 消息结构 | `PerfTestRequest{echo_attachment,name}` / `PerfTestResponse{cpu_usage,name}`，定长 bytes 填充 | `Request{common_feat, repeated ad_feat, repeated labels}` + `ValueMap{map<string,Value>}` + `Value`(oneof 8 类型：string/double/int64/bytes 及对应的 list)，结构与真实业务特征一致 |
| 请求数据生成 | client 启动时按 `--req_size` 生成单个定长 `name` 字符串，每个请求复用同一内容 | client 启动时预生成 Request 池（`--num_datasets` 个），每个 Request 由 `data_generator.h` 按 5 桶对数均匀分布(1K~1M)随机填充，RPC 时 round-robin 引用池中请求 |
| 请求数据控制 flag | `--req_size`（定长填充字节数） | `--num_datasets` / `--seed` / `--min_size` / `--max_size` / `--fixed_size`（固定大小时走 fixed_size） |
| 响应数据生成 | server 按 `--rsp_size` 生成定长 `name` 填入 response | server 直接 `Swap(request)` 把请求回传为响应，无独立 rsp_size |
| echo_attachment | 通过 proto 字段 `echo_attachment` 在 client 设置并随 RPC 传递给 server | server 端 flag `--echo_attachment` 直接读取，不再经 proto 传递 |
| 服务端消息处理 | `CopyFrom` 拷贝 request 数据；默认消息工厂 | `Swap`（O(1) 指针交换）；启用 Arena 消息工厂(`GetArenaRpcPBMessageFactory`) |
| 服务端 CPU 上报 | response 携带 `cpu_usage` 字段回传 client，client 统计 `g_server_cpu_recorder` | 不上报服务端 CPU（移除 `g_server_cpu_recorder` / `cpu_usage` 字段），仅保留 client CPU 统计 |
| 吞吐统计口径 | `--req_size!=0` 时按 response.name 长度，否则按 attachment 长度 | 按「预计算的 request ByteSizeLong + attachment 长度」累加 |

**选用建议**：
- `ub_performance`：偏「带宽/吞吐」的传统压测，结构简单、参数少，适合快速对比裸传输性能。
- `ub_performance_press`：proto 结构与数据分布更贴近真实业务（多 map、oneof、混合类型），预生成请求池消除了运行时数据生成开销，适合评估序列化/反序列化及真实负载下的性能表现。
