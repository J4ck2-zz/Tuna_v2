# Tuna_v2

Tuna_v2 是基于未认证 DAG 的 Go 共识项目，复用现有网络、存储、密码学、交易池和内存池。Mysticeti 的论文和 Rust 工程作为 DAG 架构与直接判定的参考；Tuna_v2 后续将采用自己的方法解决未确定区块的一致性。

目前已实现 DAG、三轮直接判定、多提案者流水线、区块与载荷同步、有序提交、NutsDB 持久化和重启恢复。

**未决一致性新机制尚未实现。** 当前遇到 `Undecided` 时继续收块、同步、出块和计算后续槽位，有序输出停在第一个未决槽位之前。迟到的直接证据可以解除阻塞；系统不会搜索后续锚点进行间接判定。某些未决槽位可能永久阻塞输出。

完整设计和实施状态见 [PLAN.md](PLAN.md)。

## 环境与构建

需要 Go 1.21.1 或以上版本。直接运行节点不需要 Python、Fabric 或 tmux。

```shell
go build -o bin/tuna .
go test ./...
go test -race ./...
go vet ./...
```

Go 测试包含真实 TCP 的 4／7 节点子进程测试、迟启动节点追赶及集群重启。使用 `go test -short ./...` 可跳过这些子进程测试。

## 本地启动四个节点

生成匹配的密钥、成员地址和参数：

```shell
bin/tuna genesis --nodes 4 --port 9000 --path local-testbed
```

生成的成员使用 `9000…9003` 作为 DAG 端口，`9004…9007` 作为内存池端口。每个节点都必须读取相同成员配置和 `leader_count`。

在各自终端启动节点，把下面命令中的节点编号和路径编号分别改成 `0`、`1`、`2`、`3`：

```shell
bin/tuna run \
  --keys local-testbed/.node-key-0.json \
  --committee local-testbed/committee.json \
  --parameters local-testbed/parameters.json \
  --store local-testbed/db-0 \
  --log_out local-testbed/logs \
  --node_id 0
```

也可以在 Bash 中一次启动：

```bash
pids=()
trap 'kill -TERM "${pids[@]}" 2>/dev/null; wait' EXIT INT TERM
for i in 0 1 2 3; do
  bin/tuna run \
    --keys "local-testbed/.node-key-$i.json" \
    --committee local-testbed/committee.json \
    --parameters local-testbed/parameters.json \
    --store "local-testbed/db-$i" \
    --log_out local-testbed/logs --node_id "$i" &
  pids+=("$!")
done
wait
```

按 Ctrl-C 退出。恢复节点时使用原密钥、原成员配置和原数据库目录，直接重新执行 `run`；不要重新生成密钥。数据库会校验节点身份及共识配置，避免用不同身份或 `leader_count` 打开旧状态。

节点不要求全部成员上线。等权节点的法定人数为 `floor(2N/3)+1`；四节点配置下，三个在线节点可以推进 DAG，其余节点上线后通过同步追赶。临时故障仍可能形成未决槽位，此时提交按上述语义等待。

## 配置

配置文件包含 `pool`、`mempool`、`consensus` 三部分。省略字段使用默认值，未知字段会报错。示例见 [examples/parameters.json](examples/parameters.json)。

| 共识参数 | 默认值 | 含义 |
| --- | --- | --- |
| `leader_count` | 2 | 每轮提案槽位数，范围 `1…N` |
| `leader_timeout_ms` | 300 | 出块前等待上一轮相关 leader 的最长时间 |
| `min_block_delay_ms` | 20 | 本地连续提案的最小间隔 |
| `sync_interval_ms` | 100 | 交换 DAG heads、请求缺失父块的周期 |
| `max_payloads_per_block` | 32 | 单个区块最多引用的载荷数量 |
| `max_parents` | 1024 | 父引用数量上限，至少为 `N+1` |
| `max_block_bytes` | 1048576 | 区块规范编码与签名的字节上限 |
| `max_pending_blocks` | 10000 | 等待依赖的区块数量上限 |
| `cache_rounds` | 100 | 区块体内存缓存窗口，`0` 表示不主动淘汰 |

缓存淘汰只释放区块体，保留索引和持久化证据，查询旧区块时从磁盘重载。当前没有删除旧证据的磁盘 GC；未决状态长期积累时，索引与磁盘使用量会增长。

`mempool` 的 `retry_delay`、`payload_delay_send`、`Requst_Pload_delay` 单位为毫秒，字段拼写保留原有格式。`sync_timeout` 保留供压测脚本设置启动观察时间，节点启动不再用它等待所有成员。

普通密钥工具 `keys` 和可选门限密钥工具 `threshold_keys` 继续保留。当前共识使用 Ed25519，运行节点无需提供 `--threshold_keys`。

网络使用有长度上限的 Gob 帧，载荷采用新的规范哈希。请为该版本使用新生成的配置和数据库；不能混用旧协议节点或旧格式载荷数据。

## 判定与提交语义

槽位顺序按 `(round, offset)` 排列，成员先按 ID 排序，再使用轮转选举。区块父引用的顺序参与签名，并决定对同槽位多个候选块的支持关系。

第 `r+1` 轮的足量支持票形成隐式证书，第 `r+2` 轮再由足量不同作者的区块携带这些证书，才满足直接提交。仅收齐支持票不会提前提交。投票、证书和门限时钟均按作者去重。

父块和引用载荷全部验证、获取完成后，区块才进入可参与判定的 DAG。直接跳过只消费槽位，不删除该槽位的候选块。实际交付由已提交 leader 的因果子 DAG 线性化产生，区块按 `(round, author, digest)` 排序，载荷按摘要去重。

`dag.UnresolvedResolver` 定义了后续未决一致性机制的接口边界，当前运行路径没有安装或调用其实现。

## 提交消费与恢复

应用可在 `Node.RunContext(ctx)` 之前设置 `Node.CommitConsumer`，接收带递增 `Index` 的 `dag.CommittedSubDag`。消费通道是有界通道时，慢消费者会产生背压。

本地提案在广播前持久化；提交记录和游标在交付前通过原子事务写入。重启不会重新产生同轮不同的本地提案，交易批次编号也会接着已存载荷继续分配。

实时通道不会自动重放历史提交。应用应在启动运行循环之前调用 `Node.ReadCommits(afterIndex)` 读取持久化历史，再接收实时提交，并按提交序号幂等处理。进程故障可能发生在持久化和实时通知之间，提交日志是恢复依据。

## 日志与压测

每个节点在 `--log_out` 目录生成 `node-info-ID.log` 等日志。主要事件为：

- `Tuna_v2 proposal`：本地提案的轮次、作者、摘要与载荷数量。
- `Tuna_v2 commit`：实际提交序号、leader、载荷、批次及交易数量的 JSON 记录。
- `Tuna_v2 stats`：轮次、提交游标、未决槽位、待处理区块、缓存数量和当前阻塞时间。
- `Tuna_v2 decision`：槽位直接判定，写入 debug 日志；该事件不表示已经交付。

压测工具需要 Python、Fabric 和 tmux：

```shell
cd benchmark
pip install -r requirements.txt
fab local
```

压测解析实际提交记录，验证各节点重叠提交序号上的 leader 与载荷序列一致，并统计公共窗口的提交交易数和吞吐。端到端延迟只使用同一节点日志中的本地生成批次与提交时间，避免依赖跨节点时钟比较。

单独分析日志或比较提交序列：

```shell
PYTHONPATH=benchmark python3 -c 'from benchmark.logs import LogParser; print(LogParser.process("local-testbed/logs").result())'
python3 benchmark/consistency.py --allow-prefix \
  --pattern 'Tuna_v2 commit .*"leader":"([0-9a-f]+)"' \
  local-testbed/logs/node-info-0.log local-testbed/logs/node-info-1.log
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s benchmark/tests
```

云主机管理工具保留在 `benchmark/alibaba/`，配置继续使用 `benchmark/settings.json` 中的 `ports.node` 与 `ports.mempool`。

## 目录

| 目录 | 职责 |
| --- | --- |
| `dag/` | 区块、DAG、门限时钟、依赖管理、运行循环、同步、线性化、恢复 |
| `consensus/tuna_v2/` | 轮转选举、三轮映射、支持关系、直接判定 |
| `node/` | 组件组装、内存池适配与提交交付 |
| `network/`、`store/`、`crypto/` | 网络、存储与密码学基础设施 |
| `pool/`、`mempool/` | 交易生成与载荷传播 |
| `benchmark/` | 压测、云端管理和日志工具 |
