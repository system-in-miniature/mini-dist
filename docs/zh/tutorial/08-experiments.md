# 第 8 章：对照实验课

MiniDist 的价值在于：不同复制语义使用同一状态机、逻辑时钟、网络与故障词汇
运行。本章把实验 1–3 当作受控对照来解读：正常收敛、已确认写丢失，以及少数派
分区。最后讨论读一致性——相似的方法名有意不代表相同保证。

## 学习目标

完成本章后，你将能够：

- 找出每个 lab 的自变量与观察边界；
- 解释异步确认与 quorum 确认为什么会在故障后产生不同结果；
- 区分双主脏写与 Raft 任期栅栏；
- 在不抹平语义的前提下比较 local、leader-routed 与 linearizable read；
- 复现六次协议运行，并把输出连接到源码与测试。

## 1. 如何阅读确定性实验

三个 lab 都使用 `src/minidist/protocols/types.py` 中共享词汇定义的公开
`ReplicationGroup` 操作：write、read、tick、crash、restart、isolate、heal 与
probe。具体实现分别是
`src/minidist/protocols/async_primary/group.py` 的 `AsyncPrimaryGroup`，以及
`src/minidist/protocols/raft/group.py` 的 `RaftGroup`。

共享底座控制了干扰变量。两个 group 都使用 `SimClock`、`Scheduler`、`SimNet`
与 `Trace`；相同 seed 和脚本会产生相同事件顺序。Tick 不是真实流逝时间。这些
lab 比较语义，不比较吞吐、延迟、持久性或真实 socket 行为。

每项打印结果都由 `tests/labs/test_experiments.py` 的断言支持。文字输出帮助人类
解读，返回 dataclass 则为测试提供精确事实。结论不应超出脚本控制的状态转换。

阅读一次运行时，要把三个时刻分开：故障前状态、操作的确认边界，以及恢复或
收敛后的状态。只比较最终字典，会隐藏客户端是否曾被告知某个不安全写入已经
成功；只比较确认，又会隐藏系统最终选定的权威状态。实验结论来自这三项观察之间
的关系。

## 2. 实验 1：正常复制

`labs/exp01_normal_replication.py:run_experiment()` 写入
`course=MiniDist`，在确认边界观察 follower，驱动系统收敛，再比较最终值与
offset。

### 异步主从

运行：

```bash
uv run python labs/exp01_normal_replication.py --protocol async
```

实测输出：

```text
实验 1：正常复制（异步主从）
1) 写入在 offset=1 返回 ack；primary 立即 ack，复制仍在途中。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=1, follower=1；副本已收敛。
```

在 `AsyncPrimaryGroup.client_write()` 中，主节点修改字典后只调度副本消息就
返回。`AsyncPrimaryGroup.tick()` 才推进投递。因此，确认边界的空 follower
是预期行为，不是 lab 中的竞态。随后收敛只证明在这个无故障调度下最终完成投递。

### Raft

运行：

```bash
uv run python labs/exp01_normal_replication.py --protocol raft
```

实测输出：

```text
实验 1：正常复制（Raft）
1) 写入在 offset=2 返回 ack；多数派复制后才 ack，随后 leaderCommit 传播到全部 follower。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=2, follower=2；副本已收敛。
```

`RaftGroup.client_write()` 会调用 `_wait_for_commit()`，因此成功边界需要多数派
提交。不过，被选来观察的 follower 仍可能是 `None`。多数派可能由 leader 与
**另一个** follower 组成，commit index 传播到所有节点还需要后续
AppendEntries。Quorum 确认意味着“多数派支撑的已提交日志 entry”，不等于所有
节点同步 apply。Offset 2 包含 index 1 的选举 no-op。

受控结论不是“两个协议都收敛，所以同样安全”，而是“无故障时都收敛，但确认边界
不同”。

## 3. 实验 2：确认后立即 crash

`labs/exp02_acked_write_loss.py:run_experiment()` 写入
`order:42=confirmed`，从当前 leader 读取它，不增加无关投递 tick 就 crash 该
leader，完成换主，再从新 leader 读取。

### 异步主从：已确认写丢失

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
```

实测输出：

```text
实验 2：ack 后立即 crash leader（异步主从）
1) client 写入 order:42=confirmed；leader 在 offset=1 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash primary，随后完成 failover。
4) 新 leader=replica-1，读取 order:42：None。
结论：已确认写丢失。
```

写结果为 true，是因为 `AsyncPrimaryGroup.client_write()` 把 `LEADER` 定义为
本地应用。`AsyncPrimaryGroup.crash()` 标记节点死亡并切断链路，使排队消息不能
越过预期故障边界。`AsyncPrimaryGroup.promote()` 把落后的副本设为权威。缺失
读结果正是“把这种确认理解为换主持久性”的反例。

### Raft：quorum 确认写存活

```bash
uv run python labs/exp02_acked_write_loss.py --protocol raft
```

实测输出：

```text
实验 2：ack 后立即 crash leader（Raft）
1) client 写入 order:42=confirmed；leader 在 offset=2 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash node-1，随后完成 failover。
4) 新 leader=node-2，读取 order:42：b'confirmed'。
结论：多数派确认的写在换主后仍在。
```

这里 `RaftGroup._advance_commit()` 在客户端成功前要求多数派存储当前任期
entry。`RaftGroup._receive_request_vote()` 只给日志至少一样新的 candidate
投票，所以新的多数派 leader 包含已提交前缀。最后的
`ReadLevel.LINEARIZABLE` 还会在返回 `confirmed` 前取得 read-index quorum。

该结果假设 MiniDist 的持久字段在模型 crash 中存活；它没有测试磁盘、相关故障
或多数节点同时丢失。

## 4. 实验 3：旧 leader 位于少数派

`labs/exp03_partition_old_leader.py` 双向隔离旧权威，让多数派侧继续运行，随后
愈合分区并检查收敛。

### 异步协议：两段被确认的脏历史

```bash
uv run python labs/exp03_partition_old_leader.py --protocol async
```

实测输出：

```text
实验 3：异步主从的双主脏写
1) 隔离旧 primary primary；其本地写 ack=True。
2) 显式 promote replica-1；新 primary 本地写 ack=True。
3) 分区中两侧状态：旧侧 old=b'old-primary', new=None；新侧 old=None, new=b'new-primary'。
4) 愈合后按新 primary 世代全量收敛：converged=True, old=None, new=b'new-primary'。
结论：协议 1 没有任期 fencing；分区中两侧都可确认本地脏写。
```

旧侧可以接受，是因为 `AsyncPrimaryGroup.client_write()` 不等待任何 peer。Lab
在另一侧手动调用 `promote()`，该侧也能本地接受。这是实验中真实的双主脏状态，
不是两段都已提交的共识历史。愈合后，generation/lineage fencing 与 full
synchronization 选择新主节点历史；旧侧写被丢弃而不是合并。

### Raft：少数派拒绝与日志修复

```bash
uv run python labs/exp03_partition_old_leader.py --protocol raft
```

实测输出：

```text
实验 3：旧 leader 位于少数派的 Raft 网络分区
1) term=1 的旧 leader node-1 被双向隔离；其新写 QUORUM ack=False。
2) 多数派在 term=3 选出 node-3；继续写入的 QUORUM ack=True。
3) 分区愈合后旧 leader 角色=follower；日志收敛=True。
4) 最终状态：minority-only=None, majority=b'continues'。
结论：高任期完成 fencing，少数派未提交分叉被新 leader 日志覆盖。
```

隔离的 leader 可以本地 append，但 `RaftGroup._wait_for_commit()` 无法观察到
quorum，因此返回 false。两个连通节点选出更高任期 leader 并完成提交。愈合后，
`RaftGroup._receive_append_entries()` 让旧 leader 降级；
`prevLogIndex`/`prevLogTerm` 拒绝与 `_receive_append_response()` 重试会把
`nextIndex` 逐步后退，直到新 leader 覆盖未提交分叉。

在这个 seed 与调度中，新 leader 出现在 term 3。重要声明是“权威任期单调提高”
与 quorum 行为，而不是每次运行都恰好在 term 3 选出 leader。

## 5. 读一致性是另一项独立选择

共享 enum 并不承诺两个协议实现所有 level。`AsyncPrimaryGroup.client_read()`
支持：

- `LOCAL`：读取指定节点，可能过期；
- `LEADER`：路由到手动指定的当前主节点，不做 quorum 证明。

它拒绝 `LINEARIZABLE`。`RaftGroup.client_read()` 拒绝 `LOCAL`，支持直接
`LEADER` read，并实现简化的 `LINEARIZABLE` barrier。Barrier 会创建 read
context，在当前任期广播空 AppendEntries，等到多数成功响应后才读取 leader
已应用字典。

直接 Raft `LEADER` read 有意更弱：本地已知 leader 可能被分区，还不知道已有
更高任期。实验 3 的旧 leader 展示了这种风险。Read-index round 在本模型中
证明当前任期多数派可达，而且不会把读命令写入日志。

[实验行为矩阵](../experiments.md)与
[DIFFERENCES 映射](../mapping.md)
明确了边界：这是单轮教学 barrier，没有 lease、batching、跨线程 apply wait
或完整客户端 session 机制。

## 6. 这些实验能与不能证明什么

仓库的[完整实验矩阵](../experiments.md)把协议 1 与协议 4 的结论标为 asserted，
而协议 2/3 是 not implemented。不能靠类比填补空白列。

这些 lab 证明两个进程内实现的确定性状态与故障语义。它们不证明：

- 真实网络互操作或 wire compatibility；
- 性能、延迟或可用性百分比；
- 磁盘持久性、fsync 顺序或断电恢复；
- Redis Sentinel/Cluster 正确性；
- 生产 Raft 成员变更、snapshot、storage 或 client session；
- Byzantine 节点下的安全。

明确这些限制不会削弱 lab，反而确定了受控变量：相同 crash 与 partition 调度
下的确认、权威、日志与读规则。

## 7. 测试实验结论

运行：

```bash
uv run pytest -q tests/labs/test_experiments.py
```

实测输出：

```text
......                                                                   [100%]
6 passed in 0.03s
```

这六项断言分别是 async 正常、Raft 正常、换主丢失、换主存活、async 双主，
以及 Raft 栅栏。

如需同时检查协议机制与 lab 输出，运行：

```bash
uv run pytest -q tests/protocols/test_async_primary.py \
  tests/protocols/test_raft.py tests/labs/test_experiments.py
```

实测输出：

```text
............................                                             [100%]
28 passed in 0.06s
```

不同机器的具体耗时可能变化；验收事实是通过数量与零失败。

## 8. 练习

### 理解题

1. 实验 1 中，为什么 QUORUM 写已成功时，被观察的 Raft follower 仍可能为空？
2. 实验 3 中，更高任期与 AppendEntries 冲突修复分别完成什么工作？

??? note "参考答案"

    1. 三节点多数派只需 leader 加一个 follower。被观察者可能是另一个 follower，
       尚未收到使其 apply entry 的 commit-index 传播。
    2. 更高任期建立并限制权威，迫使旧 leader 降级；前缀检查、后缀删除与
       `nextIndex` 重试则用权威日志替换未提交分支。

### 动手题

3. 运行本章六条命令，只把 stdout 保存到临时对照笔记。验收：为每个实验找出
   一项确认边界观察与一项最终状态观察；不要把 tick 数解释成毫秒。
4. 创建临时脚本，选出 Raft leader，隔离它，再分别尝试
   `ReadLevel.LEADER` 与 `ReadLevel.LINEARIZABLE`。验收：记录直接 leader
   read 结果，以及 linearizable read 包含 `read-index quorum` 的
   `RuntimeError`。不要修改 `src/`。

??? note "参考答案"

    3. 实验 1 把即时 follower 值与最终收敛配对；实验 2 把 `ack=True` 与换主
       后的值配对；实验 3 把分区中的写入接受/拒绝与愈合后的收敛状态配对。
       上述六个实测输出块就是参考。
    4. 使用 `RaftGroup.run_until_leader()`，先以 `AckLevel.QUORUM` 写入准备值，
       调用 `isolate(leader)`，再执行两种读。`LEADER` 不做 barrier，所以读取
       本地已应用值；`LINEARIZABLE` 驱动 tick，多数派不响应时抛出异常。这与
       `tests/protocols/test_raft.py` 的
       `test_linearizable_read_fails_without_a_current_term_quorum()` 一致。

## 小结

三个受控实验展示了协议谱系。正常投递下两个实现都会收敛，但确认边界不同。
立即 crash 后，本地异步确认可能消失，Raft quorum commit 则会存活。分区时，
两个手动指定的异步 primary 能接受不兼容脏写；被隔离的 Raft leader 无法提交，
之后会被栅栏并修复。读 level 又增加一条轴：路由到 leader 不等于证明当前多数
权威。总的来说，这些 lab 说明分布式系统 API 必须命名真实保证，不能用同一个
成功返回值抹平不同机制。
