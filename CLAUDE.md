# Tuna_v2 项目协作说明

## 项目与技术栈

- 共识研究项目，Go 模块名为 `Tuna_v2`，`go.mod` 声明 Go 1.21.1。
- Cobra CLI、TCP/Gob、NutsDB、SHA-256/Ed25519；Kyber 门限密钥工具为可选项。
- Python/Fabric/tmux 仅用于压测；日志单元测试使用 Python 标准库。
- 入门路线见 [ONBOARDING.md](ONBOARDING.md)，运行说明见 [readme.md](readme.md)，协议设计见 [PLAN.md](PLAN.md)。

## 目录与依赖方向

- `main.go`、`config/`：CLI、密钥、成员配置、默认参数与参数加载。
- `node/`：组件组装、`payloadBridge`、生命周期和提交消费。
- `dag/`：区块验证、依赖、状态、门限时钟、运行循环、同步、线性化、持久化与恢复。
- `consensus/tuna_v2/`：包名 `tunav2`，实现 `dag.Consensus`，负责轮转选举和三轮直接判定。
- `core/`：身份、成员地址簿、通用消息传输；DAG 核心位于 `dag/core.go`。
- `pool/`、`mempool/`：测试交易生成、载荷传播、选择、补取与恢复。
- `network/`、`store/`、`crypto/`、`logger/`：基础设施。
- `benchmark/`：本地和阿里云实验、参数生成与日志分析；`examples/`：参数样例。
- 接口由 `dag` 定义、共识包实现；保持依赖方向，避免循环导入。

## 代码风格与并发

- Go 修改使用 `gofmt`；沿用包内文件命名、显式构造函数和 `error` 返回模式。
- 导入路径使用 `Tuna_v2/...`。保留已有公共标识和 JSON 拼写，包括 `Requst_Pload_delay`。
- `dag.Core` 的可变状态由 `dag.Runtime` 单个事件循环拥有；通过 channel 提交事件。
- 后台工作使用 `context.Context` 取消并等待退出；mempool 和 Store 按现有锁保护共享状态。
- 新参数同步对应默认值、`Validate`、示例、文档及压测配置；参数加载拒绝未知字段。

## 协议不变量

- 当前只实现直接提交和直接跳过。`UnresolvedResolver` 尚无实现或运行时调用。
- `Undecided` 阻塞有序输出，但后续收块、同步、出块、直接判定继续；不要擅自添加间接判定或用超时跳过。
- 完整区块引用为 `(round, author, digest)`；同槽位多候选必须保留。
- 父引用顺序参与签名并决定支持关系，不能重排；哈希使用规范字段编码，不能使用 Gob 编码求哈希。
- 等权门限为 `floor(2N/3)+1`，票、证书和门限时钟按作者去重；直接提交要求两层证据。
- 槽位按 `(round, offset)` 消费；区块线性化按 `(round, author, digest)` 排序；载荷按摘要去重。
- 父块与载荷验证齐备后才接纳区块；`DirectSkip` 不删除候选块。
- 本地提案与自身提案指针先原子持久化再广播；提交记录与游标先原子持久化再交付。
- 每个诚实节点每轮至多一个提案，重启后也要保持；提案失败时归还已取载荷。
- `cache_rounds` 仅控制区块体缓存，不能因此删除未决判定或恢复所需的持久化证据。
- 应用在运行前读取 `ReadCommits(afterIndex)`，再接收 `CommitConsumer`，按 `Index` 幂等处理；实时通道不自动重放。

## 构建、运行与验证

从仓库根目录执行：

```bash
go build -o bin/tuna .
go test -short ./...
go test ./...
go test -race ./...
go vet ./...
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s benchmark/tests
```

- Go 测试与源码同包目录，命名 `*_test.go`；Python 测试位于 `benchmark/tests/`。
- `-short` 跳过 `node/` 的 TCP 子进程集成测试；完整测试覆盖四／七节点、迟启动追赶和集群重启。
- 共识规则优先验证 `consensus/tuna_v2/committer_test.go`；状态与恢复看 `dag/core_test.go`。
- 仓库未配置覆盖率门槛或专用 lint 工具；按改动范围运行相关检查，不把未运行的检查报告为通过。
- 新实验配置：`bin/tuna genesis --nodes 4 --port 9000 --path local-testbed`。
- 启动方式见 README 四节点示例；`run` 必需 `--keys`、`--committee`、`--parameters`、`--store`。
- 恢复节点复用原密钥、成员配置和数据库，直接执行 `run`；不要重新执行 `genesis` 覆盖原密钥。
- 压测 `fab local` 会清理工作目录中的 `db-*`、隐藏 JSON 并执行 `tmux kill-server`，执行前检查现有实验和 tmux 会话。

## 工作区与 Git

- 修改前查看 `git status`，保留已有工作区与暂存区改动。
- 生成本说明时仓库尚无提交历史，无法确定提交消息、分支及 PR 工作流约定。
