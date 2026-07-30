# 第 1 章——为什么是协议谱系

[English](../../tutorial/01-why-a-spectrum.md)

MiniDist 不是第 101 个迷你 Raft 实现，而是一个受控的复制协议家族对照实验。每个家族都必须运行同一个极小 KV 工作负载，使用同一套确定性故障实验室，并通过同一套词汇公开保证。这样，变化的变量是协议，而不是应用、时钟或测试工具。

## 学习目标

完成本章后，你将能够：

1. 解释 Redis 风格异步复制与 Raft 对确认和故障转移问题给出不同答案的原因；
2. 找到仓库中的仿真、共享 API、异步主从、Raft、实验和测试层；
3. 运行两种实现的正常复制实验，指出各自在什么边界确认写入；
4. 区分逻辑 tick 与耗时或吞吐量；以及
5. 找到全部四种已实现协议家族，并准确说出仍未实现的生产特性。

## 问题不是“哪个协议赢了？”

分布式系统没有统一选择教科书式共识，因为它们优化的契约不同。Redis primary 通常先在本地执行写入，再向 replica 发送异步流。普通写路径因此简单而快速，但故障转移可能丢失旧 primary 已确认的写。Raft 则等到多数派后才把条目称为已提交；更强的边界同时引入选举、任期、法定人数可用性、日志协调和更多协作。

其他系统位于谱系的不同位置。PostgreSQL 运输 WAL，并允许运维者选择同步确认强度；Kafka 的 ISR 与 high watermark 围绕动态的同步副本集合推理；一些系统只对元数据做共识，对大块数据使用另一套复制方式。它们不是同一算法的失败版本，而是对“客户端看到成功时必须已经成立什么”给出的不同答案。

MiniDist 保留这种差异。`src/minidist/protocols/types.py` 中的共享 API 定义 `AckLevel`、`ReadLevel`、`WriteResult`、`ReadResult` 与 `ReplicationGroup`。它不承诺每个实现都支持枚举的每个成员。模块说明和 `UnsupportedLevelError` 明确规定：无法提供某项保证时必须拒绝，不能静默降低语义。

因此，共享接口是一套实验词汇，不是最低共同标准。第 3 章会有意触发“不支持”错误。

## 把仓库读成一组实验层

先读 `README.md` 的 “Repository Layout”，再逐层跟随依赖：

```text
labs/ 与 tests/
        |
        v
src/minidist/protocols/types.py       共享实验词汇
        |
        +--> protocols/async_primary/group.py
        |
        +--> protocols/wal_shipping/group.py
        |
        +--> protocols/isr/group.py
        |
        +--> protocols/raft/group.py
                    |
                    v
src/minidist/sim/                    时钟、调度、网络、故障、Trace
```

底层控制时间与故障。`src/minidist/sim/clock.py` 中的 `SimClock.advance` 只在实验工具要求时改变整数。`src/minidist/sim/scheduler.py` 中的 `Scheduler.tick` 推进一次时钟并执行所有到期事件。`src/minidist/sim/network.py` 中的 `SimNet.send` 从私有 `random.Random(seed)` 获得延迟、丢弃和乱序选择。`src/minidist/sim/trace.py` 中的 `Trace.record` 追加带序号的观察。

协议层拥有复制语义。`src/minidist/protocols/async_primary/group.py` 的
`AsyncPrimaryGroup.client_write` 修改 primary、为每个 replica 入队条目，然后
立即返回。`src/minidist/protocols/wal_shipping/group.py` 的
`WalShippingGroup.client_write` flush 本地逻辑 WAL，并可等待配置的同步
standby。`src/minidist/protocols/isr/group.py` 的 `IsrGroup.client_write`
在请求 `ALL_ISR` 时等待动态 ISR 与 high watermark。
`src/minidist/protocols/raft/group.py` 的 `RaftGroup.client_write` 只接受
`AckLevel.QUORUM`，并驱动集群，直到多数派复制或有界尝试失败。相同的方法名
没有抹平不同的确认契约。

顶层存放可执行论断。`labs/exp01_normal_replication.py` 只使用公开 group 操作；`tests/labs/test_experiments.py::test_experiment_1_observes_delayed_convergence` 把观察变成回归断言。最可信的教程句子应能从正文追到实验、公开方法、内部机制和测试。

### 源码阅读纪律

请按上述顺序使用仓库，不要一开始就打开最大的实现。先从 lab 确认可观察问题，再读共享 type 了解请求的保证；进入具体 group 的公开方法，注意它相对网络推进在何处返回；然后才跟随私有 handler 与 scheduler callback，最后到固定该观察的回归测试。

例如，“异步 primary 在 replica 投递前确认”不是从类名猜出的。lab 在 `tick` 前读取 replica；`AsyncPrimaryGroup.client_write` 不 tick 就返回；`SimNet.send` 只调度 `_deliver`；`test_write_acks_before_replica_converges` 观察 replica 在配置延迟经过前仍为空。四条独立证据互相吻合。

还要区分机制与实验工具。`probe`、`isolate`、`run_until_converged` 方便实验，但不是客户端线协议命令。反过来，`_apply_entry` 等私有方法实现机制，却不允许 lab 绕过公开路径。第 8 章判断输出来自真实协议路径还是测试布置时，这条纪律尤其重要。

### 贯穿全书的四个问题

评价每种协议前，先写下四个边界：写在成功返回前必须发生什么？哪个节点可以服务读取，又凭什么获得授权？断连后，哪些位置与历史标识允许增量追赶？谁可以选择继任者，过期权威如何被 fence？

async group 的答案是本地 primary 确认、local/primary 读取、replication ID + offset，以及手工 promotion + generation 检查。Raft 的答案是多数派提交、leader/read-index 读取、term/index 日志匹配，以及多数派选举 + 高任期 fencing。后续章节会逐一拆开。固定问题能够防止功能清单取代语义比较。

故障结果还必须绑定前提。“已确认写可能丢失”适用于实验 2 展示的 async `LEADER` 边界，不表示每次 crash 都会删除所有已复制异步写。“Raft 保留已确认写”依赖 lab 中的 `QUORUM` 成功和存活多数派，不表示缺少多数派时仍可写。只有精确写出确认边界、存活拓扑和读取级别，轶事才成为协议论断。

最后要区分 safety、liveness、durability 和 performance。协议可能在 partition 中拒绝写以保持安全；拒绝不是数据丢失。值留在 MiniDist 进程内 durable mapping 不是磁盘耐久证明。更少 tick 完成也不表示真实毫秒更快。共同实验室比较的是选定的安全与推进边界，不会把这些性质压成一个分数。

## 第一次对话：一个 key，两种协议

正常复制实验写入 `project=MiniDist`。异步分支构造 `AsyncPrimaryGroup`，调用 `AsyncPrimaryGroup.client_write(..., AckLevel.LEADER)`，在推进前读取 replica，调用一次 `AsyncPrimaryGroup.tick`，然后再读。Raft 分支先调用 `RaftGroup.run_until_leader`，以 `AckLevel.QUORUM` 写入，再调用 `RaftGroup.run_until_converged`。

字节串是有意的。`src/minidist/protocols/types.py` 中的 `ReplicationGroup.client_write` 规定 key 与 value 为 `bytes`。MiniDist 不教授文本编码、客户端会话或线协议，而是让状态机足够小，使复制决策保持可见。

两条路径打印的 offset 也属于各自协议。异步 group 的 offset 计算有序写流位置；Raft 报告日志索引。新 leader 先追加 no-op，因此用户写通常位于索引 2。不同协议探针中看似相等的整数并不是通用耐久性单位。

## 动手实验：观察两种确认边界

在仓库根目录运行：

```bash
uv sync --dev
uv run python --version
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

本仓实测输出：

```text
Python 3.12.2
实验 1：正常复制（异步主从）
1) 写入在 offset=1 返回 ack；primary 立即 ack，复制仍在途中。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=1, follower=1；副本已收敛。
实验 1：正常复制（Raft）
1) 写入在 offset=2 返回 ack；多数派复制后才 ack，随后 leaderCommit 传播到全部 follower。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=2, follower=2；副本已收敛。
```

不要因为某个 Raft follower 最初为 `None` 就认为 Raft “也丢了”值。`RaftGroup.client_write` 只要求多数派，不要求全部三节点；被观察的 follower 可以落后，而已提交条目已经存在于多数派。异步 primary 则是在任何 replica 参与前返回。第 8 章会解释完整对照实验。

## 对照真实系统

异步路径有意接近 Redis 默认复制：普通写不等待 replica，replica 消费有序流，offset 描述流进度。Raft 路径遵循 Raft 论文 Figure 2 的状态与 RPC 规则，包括任期、投票、AppendEntries 和多数派提交。

相似性有明确边界：

- MiniDist 完全在进程内运行；`SimNet.send` 运输 Python 值，不运输 TCP 包或 Redis/Raft 序列化帧。
- tick 表示顺序，不表示毫秒；tick 数不能推出吞吐或延迟。
- 协议 2（WAL 运输）与协议 3（ISR + high watermark）已作为第 9、10 章的
  有界教学模型实现，不是生产 server。
- 没有 Redis Sentinel、Cluster、`WAIT`、RDB/AOF 策略、socket 客户端、Raft 成员变更或磁盘/fsync 模型。
- 异步 crash 模型保留教学节点数据，以隔离复制丢失；这不是无持久化 Redis 的恢复保证。

精确对应与简化列在[机制与真实系统映射](../mapping.md)中；实现与未来状态列在[复制协议实验矩阵](../experiments.md#复制协议实验矩阵)中。它们是契约：测试通过不能证明标为 “Not implemented” 或 “Future experiment” 的行。

## 练习

### 理解题

1. 如果两种协议拒绝不同成员，为什么还要共享 `AckLevel` 枚举？
2. 实验 1 中，为什么 `client_write` 成功返回后被选中的 Raft follower 仍可能读取 `None`？
3. 哪个源码函数证明仿真 tick 是显式推进的？为什么不能把 tick 解释成毫秒？

??? note "参考答案"

    1. 它提出共同问题——成功前应完成多少复制工作——但不假装每种协议都有每个答案。`UnsupportedLevelError` 保留语义差异。
    2. QUORUM 要求多数派而不是全部节点。两个节点覆盖条目后即可提交，剩余 follower 的 leaderCommit 传播可以稍后发生。
    3. `src/minidist/sim/clock.py::SimClock.advance` 只在实验工具请求时改变逻辑整数，不读取墙上时钟，因此该数记录顺序而非时长。

### 动手题

4. 不修改 `src/`，新增一个临时测试：用 async 协议运行实验 1 两次，并断言两个 `ExperimentResult` 相等。

   **验收：**保存到 `/tmp/test_minidist_ch01.py`，运行 `uv run pytest -q /tmp/test_minidist_ch01.py`，必须得到 `1 passed`。

??? note "参考答案"

    ```python
    from labs.exp01_normal_replication import run_experiment

    def test_normal_experiment_is_repeatable():
        assert run_experiment(protocol="async", verbose=False) == run_experiment(
            protocol="async", verbose=False
        )
    ```

    若 `/tmp` 测试环境未自动加入仓库根目录，运行
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch01.py`。

## 小结

MiniDist 用一个状态机、一套故障实验室、一套显式词汇和多组诚实保证，把协议选择变成受控实验。第一个实验已经区分“primary 接受了写”与“多数派复制了日志条目”。为了相信后续故障实验，我们还必须理解实验室为何能够精确重放。第 2 章将逐事件拆开确定性仿真基座。
