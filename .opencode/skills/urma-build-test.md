---
name: urma-build-test
description: Compile, deploy, and test brpc URMA performance between two remote machines. Triggered by keywords like "URMA编译", "URMA构建", "URMA测试", "urma build", "urma test", "编译部署测试URMA".
---

# URMA Compile, Deploy & Test

## Environment

| Role | Machine | User | Path |
|------|---------|------|------|
| Build + Server | 141.61.17.206 | root/Huawei12#$ | /home/d00836578/brpc_urma/brpc |
| Client | 141.61.17.208 | root/Huawei12#$ | /home/d00836578/brpc_urma/ |

## Step 1: Build on 206

SSH to 206, cd to brpc workspace, set proxy + include path, then bazel build:

```bash
ssh root@141.61.17.206 'cd /home/d00836578/brpc_urma/brpc && \
  export http_proxy="http://141.1.40.221:7777" && \
  export https_proxy="http://141.1.40.221:7777" && \
  export CPLUS_INCLUDE_PATH=/usr/include/ub/:/usr/include/ub/umdk/:/usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && \
  bazel build -c opt //example:urma_performance_server //example:urma_performance_client \
    --define BRPC_WITH_URMA=true \
    --repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0'
```

## Step 2: Sync binaries to 208

After build succeeds, copy bazel-bin output to 208:

```bash
ssh root@141.61.17.206 'cd /home/d00836578/brpc_urma/brpc && \
  scp -r bazel-bin/example/urma_performance_client bazel-bin/example/urma_performance_server \
  root@141.61.17.208:/home/d00836578/brpc_urma/'
```

## Step 3: Start server on 206

```bash
ssh root@141.61.17.206 'cd /home/d00836578/brpc_urma/brpc && \
  nohup ./bazel-bin/example/urma_performance_server \
  --use_urma=true --port=8333 --urma_device="udmac0d1e2" \
  > /tmp/urma_server.log 2>&1 &'
```

Verify server started:

```bash
ssh root@141.61.17.206 'grep -c "serving on port=8333" /tmp/urma_server.log'
```

## Step 4: Run client on 208

```bash
ssh root@141.61.17.208 'cd /home/d00836578/brpc_urma && \
  ./urma_performance_client \
  --server="141.61.17.206:8333" \
  --use_urma=true \
  --urma_device="udmac0d1e2" \
  --attachment_size=1024'
```

## Step 5: Cleanup

Kill server on 206 after test:

```bash
ssh root@141.61.17.206 'pkill -f urma_performance_server'
```

## All-in-One Script

Run all steps sequentially (build → sync → start server → test → cleanup):

```bash
# 1. Build
ssh root@141.61.17.206 'cd /home/d00836578/brpc_urma/brpc && \
  export http_proxy="http://141.1.40.221:7777" && \
  export https_proxy="http://141.1.40.221:7777" && \
  export CPLUS_INCLUDE_PATH=/usr/include/ub/:/usr/include/ub/umdk/:/usr/include/ub/umdk/urma:$CPLUS_INCLUDE_PATH && \
  bazel build -c opt //example:urma_performance_server //example:urma_performance_client \
    --define BRPC_WITH_URMA=true --repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0' && \
# 2. Sync
ssh root@141.61.17.206 'cd /home/d00836578/brpc_urma/brpc && \
  scp -r bazel-bin/example/urma_performance_client bazel-bin/example/urma_performance_server \
  root@141.61.17.208:/home/d00836578/brpc_urma/' && \
# 3. Start server
ssh root@141.61.17.206 'pkill -f urma_performance_server 2>/dev/null; sleep 1; cd /home/d00836578/brpc_urma/brpc && \
  nohup ./bazel-bin/example/urma_performance_server \
  --use_urma=true --port=8333 --urma_device="udmac0d1e2" \
  > /tmp/urma_server.log 2>&1 &' && \
  sleep 3 && \
# 4. Verify server
ssh root@141.61.17.206 'grep -c "serving on port=8333" /tmp/urma_server.log || echo "SERVER NOT READY"' && \
# 5. Run client
ssh root@141.61.17.208 'cd /home/d00836578/brpc_urma && \
  ./urma_performance_client \
  --server="141.61.17.206:8333" \
  --use_urma=true \
  --urma_device="udmac0d1e2" \
  --attachment_size=1024' && \
# 6. Cleanup
ssh root@141.61.17.206 'pkill -f urma_performance_server'
```

## Common Variations

- **No attachment**: remove `--attachment_size=1024` from client command
- **Different attachment size**: change `--attachment_size=N`
- **Multiple threads**: add `--thread_num=16 --queue_depth=16` to client
- **TCP mode**: set `--use_urma=false` on both server and client
- **ubsocket mode**: add `LD_PRELOAD=/path/to/libubsocket.so` + `UBSOCKET_*` env vars, set `--use_urma=false`
- **With tracing**: prepend `./tools/urma_fullstack_trace.sh` before the target command

## SSH Password

All SSH commands use password `Huawei12#$`. Use `sshpass` for non-interactive:

```bash
sshpass -p 'Huawei12#$' ssh root@141.61.17.206 '...'
sshpass -p 'Huawei12#$' scp ... root@141.61.17.208:...
```

If `sshpass` is not installed:

```bash
yum install -y sshpass
```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| bazel build fails | Proxy env vars set? CPLUS_INCLUDE_PATH correct? |
| scp fails | 208 reachable? ssh key or password correct? |
| Server not ready | Check `/tmp/urma_server.log` for errors |
| Client timeout | Server started? Port 8333 open? Firewall? |
| status=8 error | jfr max_sge fix applied? Check `max_sge(jfr)` in server log |
| uasid=0 warning | Kernel driver version; try `--urma_uasid=1` on both sides |
