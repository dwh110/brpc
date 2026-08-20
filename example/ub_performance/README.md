# 简介
ub_performance是bRPC性能测试工具，用于测试bRPC over UBSocket场景的性能。

本代码仓中的ub_performance基于[bRPC_v1.15.0版本example/rdma_performance](https://github.com/apache/brpc/tree/release-1.15/example/rdma_performance)做修改，经历多次迭代，修改增加了request和response包大小等一系列配置，适配UBSocket使用。

> 说明：
> bRPC代码仓中的rdma_performance工具，已经较长时间（10个月）未修改。bRPC_v1.12.1至bRPC_v1.15.0版本的rdma_performance工具完全一致。

# 使用方式
下载bRPC源码，参考[bazel编译bRPC](https://gitcode.com/fanzhaonan/brpc/blob/develop/docs/cn/bazel_brpc.md)及[brpc支持rdma](https://github.com/apache/brpc/blob/release-1.15/docs/cn/rdma.md)的介绍进行编译和运行即可。

建议编译命令：

`bazel build -c opt //example:ub_performance_server --define brpc_with_urma=true`

`bazel build -c opt //example:ub_performance_client --define brpc_with_urma=true`

> 说明：
> 不同的测试用例，在执行client、server时需要指定不同的参数，可在参考文档中了解。
