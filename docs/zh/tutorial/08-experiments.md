# 第 8 章：七个实验对照四种协议

[English](../../tutorial/08-experiments.md)

MiniDist 现在把四个复制家族放在同一套确定性仿真器上：异步主从、
PostgreSQL 式 WAL shipping、Kafka 式 ISR/HW，以及 Raft。本章是全书的
对照台。七个实验每次只改变一个故障或一致性边界，再解释为什么相似的最终值
可能来自不同的确认、membership、history 与 authority 规则。

## 学习目标

完成本章后，你将能够：

1. 找出七个实验各自控制的边界与观察量；
2. 对照四种协议，而不把共享 enum 名称误当成同等保证；
3. 从 membership 与 retention 规则解释慢副本可用性和重连行为；
4. 阅读完整 `ReadLevel × protocol` 矩阵，包括 unsupported 与 blocked；
5. 区分换主后的 stale local read 和 authority-checked read。

## 1. 对照实验的证据规则

共享词汇位于 `src/minidist/protocols/types.py`；具体机制分别位于
`async_primary/group.py`、`wal_shipping/group.py`、`isr/group.py` 与
`raft/group.py`。每个 lab 只使用 public group method，并返回由
`tests/labs/test_experiments.py` 断言的 dataclass。`SimClock`、
`Scheduler`、`SimNet` 与 `Trace` 让一条选定事件历史可以重放。

Tick 只表示排序，绝不是毫秒。实验不测 latency、throughput、磁盘或真实 socket。
阅读时必须分开四个时刻：故障前状态、请求的 acknowledgement boundary、权威
不明确时的状态，以及收敛后状态。最终 dict 相等本身无法说明客户端此前是否收到
过不安全的成功。

实验 1–3 早于协议 2/3，因此现成脚本直接展示 async 与 Raft 两列。完整矩阵中的
WAL/ISR 单元格由定向 protocol test 和相同 public transition 支撑；不能假装
脚本接受不存在的 `--protocol wal|isr` 参数。实验 4–7 则直接运行全部四列。

## 2. 实验 1–3：确认与权威

### 实验 1——正常复制

运行：

```bash
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

实测输出：

```text
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

`AsyncPrimaryGroup.client_write()` 本地 apply 并调度消息后返回。
`RaftGroup.client_write()` 等待多数派 commit，但被观察的第三节点可能还没有
收到 `leaderCommit`。WAL `LEADER` 在 primary 模拟 WAL flush 后返回；
WAL `QUORUM` 等全部配置的同步 standby。ISR `LEADER` 在本地 append 后返回；
`ALL_ISR` 等当前 ISR 全部成员及 HW。四种系统都能最终收敛，却跨越四种成功边界。

### 实验 2——确认后立即 crash

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
uv run python labs/exp02_acked_write_loss.py --protocol raft
```

实测结果：

```text
实验 2：ack 后立即 crash leader（异步主从）
1) client 写入 order:42=confirmed；leader 在 offset=1 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash primary，随后完成 failover。
4) 新 leader=replica-1，读取 order:42：None。
结论：已确认写丢失。
实验 2：ack 后立即 crash leader（Raft）
1) client 写入 order:42=confirmed；leader 在 offset=2 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash node-1，随后完成 failover。
4) 新 leader=node-2，读取 order:42：b'confirmed'。
结论：多数派确认的写在换主后仍在。
```

Async crash 前没有加入 delivery tick。Stale replica 成为权威，只有本地的
acknowledgement 消失。Raft committed entry 位于多数派，投票 freshness 规则
阻止缺少此前缀的 candidate 获胜。

WAL shipping 的结果取决于请求模式与 promotion candidate：`LEADER` 写已在
本地 flush，却可能尚未到异步 standby；成功 `QUORUM` 写已经由全部配置的同步
standby flush。ISR 成功 `ALL_ISR` 表示 entry 位于当前 ISR 全部成员的 HW
以下；clean controller election 从这些成员中选择，并且只截断 HW 以上内容。
决定存活的是前置条件，不是协议品牌。

### 实验 3——旧 leader 位于少数派

```bash
uv run python labs/exp03_partition_old_leader.py --protocol both
```

实测结论：

```text
实验 3：异步主从的双主脏写
1) 隔离旧 primary primary；其本地写 ack=True。
2) 显式 promote replica-1；新 primary 本地写 ack=True。
4) 愈合后按新 primary 世代全量收敛：converged=True, old=None, new=b'new-primary'。
结论：协议 1 没有任期 fencing；分区中两侧都可确认本地脏写。

实验 3：旧 leader 位于少数派的 Raft 网络分区
1) term=1 的旧 leader node-1 被双向隔离；其新写 QUORUM ack=False。
2) 多数派在 term=3 选出 node-3；继续写入的 QUORUM ack=True。
3) 分区愈合后旧 leader 角色=follower；日志收敛=True。
4) 最终状态：minority-only=None, majority=b'continues'。
```

Async 手工 promotion 创建新 generation 与 replication ID；delivery fence
选择该历史，但分区中两个本地 primary 都可确认脏写。WAL promotion 同样增加
timeline 并拒绝排队的旧 timeline 字节；它保护分支 apply，却不能让异步写自动
出现在被提升 standby。ISR controller 增加 leader epoch、选择 ISR member，
并截断到 HW。Raft 多数派选出更高 term leader，再由 AppendEntries conflict
repair 覆盖未提交后缀。Fence 选择权威，另一套 catch-up 机制负责数据收敛。

## 3. 实验 4：一个慢副本

```bash
uv run python labs/exp04_slow_replica.py
```

实测输出：

```text
实验 4 [async]：write_available=True
slow_replica_stale=True; membership=replica is not a voter
实验 4 [wal]：write_available=True
slow_replica_stale=True; membership=async standby does not gate commit
实验 4 [isr]：write_available=True
slow_replica_stale=True; membership=removed from ISR
实验 4 [raft]：write_available=True
slow_replica_stale=True; membership=fixed voter; majority unaffected
```

Boolean 列本身信息不足。Async replica 从不投票。WAL lab 把慢 standby 配成
异步，因此本地 flush 的 `LEADER` commit 不等待；若配置为同步 standby，
`QUORUM` 结果会改变。ISR 检测 lag 并改变动态 membership；这里只因两成员仍
满足 `min.insync.replicas=2` 而可写。Raft membership 固定，三节点中两个仍是
多数派。“一个慢副本时可用”来自四种机制。

## 4. 实验 5：Retention 窗口内外重连

```bash
uv run python labs/exp05_replica_reconnect.py
```

实测输出：

```text
实验 5 [async]：inside_window=incremental; outside_window=full
实验 5 [wal]：inside_window=incremental; outside_window=full
实验 5 [isr]：inside_window=incremental; outside_window=full
实验 5 [raft]：inside_window=log replay; outside_window=log replay
```

Async 检查 replication ID 与有界 backlog coverage；WAL 检查 timeline 与
retained LSN；ISR 检查 follower 下一 offset 是否仍在 retained leader log，
且追上后才重入。三者在教学窗口外都原子替换 whole state。Raft 是刻意例外：
M3 没有 snapshot/compaction，`nextIndex` fallback 对两种规模都 replay 无界
日志。“Full” 不是通用协议操作；没有 full transfer 也不自动更好，因为日志
被允许无限增长。

## 5. 实验 6：收官的读一致性矩阵

```bash
uv run python labs/exp06_read_consistency.py
```

实测输出：

```text
实验 6：ReadLevel × 协议
async: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
wal: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
isr: LOCAL=stale, LEADER=fresh, LINEARIZABLE=blocked
raft: LOCAL=stale, LEADER=fresh, LINEARIZABLE=fresh
```

| 协议 | `LOCAL` 观察 | `LEADER` 观察 | 权威检查结果 |
|---|---|---|---|
| 异步主从 | lagging replica stale | 当前 primary 有 `v` | `LINEARIZABLE` 不支持 |
| WAL 运输 | lagging standby stale | primary 有本地 flush 的 `v` | `LINEARIZABLE` 不支持 |
| ISR + HW | lagging follower stale | leader 暴露 HW 以上 append | 缺少 HW/lease 证明，blocked |
| Raft | isolated follower stale | 当前 leader 有 committed `v` | read-index quorum 后 fresh |

这是全书收官图，因为它阻止两种常见偷换。第一，`LEADER=fresh` 只描述本次
schedule 观察到的值，不是通用 authority proof；分区旧 leader 仍可返回旧本地
值。第二，unsupported 与 blocked 不同：async/WAL 根本不实现该保证；ISR
具有 guard，但本次 entry 在 HW 以上所以拒绝；Raft 获得当前任期多数派证据。

## 6. 实验 7：旧侧 stale 与 authority guard

```bash
uv run python labs/exp07_split_brain_lease.py
```

实测输出：

```text
实验 7 [async]：old_LOCAL=b'before'; current=b'after'; authority_guard=unsupported
实验 7 [wal]：old_LOCAL=b'before'; current=b'after'; authority_guard=unsupported
实验 7 [isr]：old_LOCAL=b'before'; current=b'after'; authority_guard=lease fenced
实验 7 [raft]：old_LOCAL=b'before'; current=b'after'; authority_guard=read-index fenced
```

所有旧侧都保留可读的换主前值，证明 write fence 不会删除 stale local state。
Async generation 与 WAL timeline 检查 fence replication traffic，但两者都不
提供 linearizable/lease read guard。ISR `lease_read()` 要求 controller
ownership、未过期逻辑 lease、足够 ISR 与没有 HW 以上 append。Raft
linearizable path 要求当前任期 read-index 多数派。后二者都 fail closed，
但一个是教学 controller/HW lease，另一个是 quorum protocol operation。

## 7. 验证与支持边界

运行 lab assertion suite：

```bash
uv run pytest -q tests/labs/test_experiments.py
```

实测输出：

```text
...........                                                              [100%]
11 passed in 0.07s
```

这些断言覆盖最初六个 async/Raft run，以及实验 4–7 全部四列。
[实验矩阵](../experiments.md)记录所有单元格，第 [9](09-wal-shipping.md) 与
第 [10](10-isr.md) 章解释新协议机制。

这些实验只证明确定性进程内状态转换；不证明 network interoperability、
wall-clock availability、磁盘行为、Byzantine safety、Redis/PostgreSQL/Kafka
wire compatibility 或生产运维。显式 `lose_unfsynced=True` 路径只建模一个
durability 边界，不是真实 storage。

## 8. 练习

### 理解题

1. 实验 4 的 `write_available=True` 为什么不是共同保证？
2. ISR `LINEARIZABLE=blocked` 为什么比 `unsupported` 包含更多信息？
3. 分区愈合后，哪两个独立机制修复旧 Raft leader？

??? note "参考答案"

    1. 配置请求了不同 level，也用不同方式处理 membership：不投票、异步
       standby、动态 ISR 或固定多数派。
    2. ISR 实现 HW/lease/min-insync guard，只是当前状态无法通过；unsupported
       协议完全没有该路径。
    3. 更高 term 建立权威并强制 step-down；log prefix check、suffix deletion
       与 `nextIndex` retry 修复数据。

### 动手题

4. 编写临时 pytest：调用实验 6 两次，断言完整 matrix 相等；再按协议顺序断言
   四个 `LINEARIZABLE` status。

??? note "参考答案"

    ```python
    from labs.exp06_read_consistency import run_experiment

    def test_read_matrix_replays():
        first = run_experiment(verbose=False)
        assert first == run_experiment(verbose=False)
        assert [first[p]["LINEARIZABLE"].status for p in
                ("async", "wal", "isr", "raft")] == [
            "unsupported", "unsupported", "blocked", "fresh"
        ]
    ```

    保存到 `/tmp`，运行
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch08.py`。

## 小结

七个实验把协议谱系变成可观察边界。正常收敛隐藏不同确认规则；failover 暴露成功
写是否到达未来权威；partition 区分 fencing 与 data repair；慢副本与重连副本
揭示静态、配置式和动态 membership；最后的读矩阵则区分位置与权威证据。共享
API 让问题可比，而显式拒绝、HW blocking、timeline/epoch check 与 read-index
让答案诚实地保持不同。
