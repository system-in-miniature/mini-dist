# 第 10 章：ISR、High Watermark 与 Controller Epoch

[English](../../tutorial/10-isr.md)

协议 3 建模一个 Kafka 式 partition。Controller 拥有当前 leader、leader
epoch 与 in-sync replica set（ISR）。Leader 可以先行 append，follower 复制
有序日志，high watermark（HW）则标识当前 ISR 共享的前缀。这形成了不同于
固定 Raft 多数派与配置式 WAL standby 的动态确认边界。

## 学习目标

完成本章后，你将能够：

1. 跟踪 `LEADER` 与 `ALL_ISR` 写从 leader append 到 HW 推进；
2. 把 ISR 收缩、重入和 `min.insync.replicas` 解释为三个独立决策；
3. 区分读取中的 log-end data 与 HW-committed data；
4. 跟随 controller election，理解 leader-epoch fencing 与 clean-prefix
   truncation；
5. 对照 MiniDist 教学 lease 与真实 Kafka 复制机制，而不声称 API 等价。

## 1. Controller state 定义 partition

`src/minidist/protocols/isr/group.py` 中的 `IsrGroup.__init__()` 创建与
其他协议相同的 `SimClock`、`Scheduler`、`SimNet` 和 `Trace`，并创建四个
controller-owned 值：`_leader`、`_leader_epoch`、`_controller_epoch`
与 `_isr`。初始时所有节点都在 ISR 中，HW 为 0，选定 leader 从 epoch 1 开始。

Controller 是进程内权威，不是另一个模拟复制服务。`controller_elect()` 是
公开 lab control operation。真实 Kafka 通过 controller/metadata quorum
存储元数据，也必须处理控制面故障；MiniDist 假设该决定可用，从而单独观察
ISR、HW、leader epoch 与副本追赶。

每个 `LogEntry` 包含 offset、产生它的 epoch、bytes key 与 value。节点
`data` 包含所有本地 append，`committed_data` 只包含 HW 以下前缀。
`probe()` 同时公开两个 map、`log_end_offset`、`applied_hw`、durable
offset 与 catch-up mode。这个拆分至关重要：“存在于 leader”与“可由权威读
返回”是不同事实。

## 2. `LEADER` 与 `ALL_ISR` 确认不同边界

`IsrGroup.client_write()` 只支持两个 level：

- `AckLevel.LEADER` 本地 append、调度 follower message 后立即返回，不等待
  ISR 或 HW；
- `AckLevel.ALL_ISR` 等待**当前 ISR**每个成员到达该 entry，且 HW 推进通过它。

`NONE` 与 `QUORUM` 抛出 `UnsupportedLevelError`。`ALL_ISR` 不是“所有配置
replica 永远不变”；集合可以在 bounded wait 之前或期间收缩。

Append `ALL_ISR` 写之前，`_refresh_isr_and_hw()` 先执行，并检查
`len(_isr) >= min_insync_replicas`。若已经不满足，就不添加 entry，直接返回
`accepted=False`。等待期间会再次检查同一条件；成员太少会使请求失败。若剩余
成员足够，且当前 ISR 全部日志到达该 offset，HW 才能推进并让写成功。

`_refresh_isr_and_hw()` 把 HW 计算为所有 alive ISR node 的最小 log-end
offset，并且只允许它前进。`_apply_high_watermark()` 随后把该位置以前的
entry 复制进各节点 `committed_data`，推进 `applied_hw`。

因此 “all current ISR replicas acknowledged and HW advanced” 同时包含两项
事实：membership 仍达到下限，且 committed prefix 覆盖 entry。它不表示固定
多数派投票，也不表示所有配置 replica 都拥有该 entry。

## 3. ISR 收缩有条件地保留可用性

MiniDist 用 `set_replica_slow(node, True)` 建模慢 follower。
`_send_entry()` 会记录 `isr_replication_delayed`，并且不向该节点发送。
每次 tick 都调用 `_refresh_isr_and_hw()`；若 follower 落后持续
`replica_lag_timeout` 个逻辑 tick，就移出 ISR，并记录
`isr_member_removed`。

移出后，剩余 ISR 可能推进 HW，但这不是无限收缩许可。
`min.insync.replicas` 是 `ALL_ISR` 写的下限。三节点、minimum=2 时，一个
lagging follower 可以离开且写继续；两个 follower 都离开时，虽然 leader
仍在 ISR，下一次 `ALL_ISR` 写会被拒绝。

所以必须分开三个步骤：

1. lag detection 决定 follower 是否仍在 ISR；
2. `min.insync.replicas` 决定 strong write 能否准入；
3. 结果 ISR 的最小 log-end offset 决定 HW。

恢复 follower 不会只因网络 heal 就重入。`set_replica_slow(node, False)`
先启动 `_synchronize()`；只有 log-end 追上 leader 且不再 slow，
`_refresh_isr_and_hw()` 才记录 `isr_member_added`。

实验 4 显示 membership 对照：

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

这个配置中四行都可写，但原因各不相同；只有 ISR 行改变 membership。

## 4. Retained log 与重入

与 WAL shipping 相似，ISR 保存有界 `_retained` deque。Follower 已追平，
或下一条缺失 offset 仍被保留时，`_synchronize()` 选择 `INCREMENTAL`；
否则发送含 leader log、data 与 HW 的 `isr_snapshot`，并标记 `FULL`。

增量 append message 同时携带 packet 当前 `leader_epoch` 与 record 原始
`entry_leader_epoch`。这样，新 leader 能 replay 旧 epoch 产生但已 committed
的 entry，同时仍拒绝过期 leader 发送的 packet。Snapshot installation 也只
按 advertised HW 重建 `committed_data`。

真实 Kafka 通过 fetch request 与 segment file 追赶。MiniDist 全量路径原子
替换 Python state；没有 batch、segment index、fetch session、throttling 或
传输成本。实验保留的是 retention 决策与“追上后才能重入”规则。

## 5. Controller election 的 fencing 与 truncation

`controller_elect(node)` 只接受 live ISR member。它同时递增 controller 与
leader epoch，选择新 leader，并把 candidate log 截断到 HW：

```python
candidate.log = [
    entry for entry in candidate.log
    if entry.offset <= self._high_watermark
]
```

Candidate 从 clean prefix 重建本地 data；旧 leader 被移出 ISR，设置新的逻辑
lease expiry，并让所有 live follower 开始同步。

每个 append/snapshot message 携带 packet 当前 leader epoch 与 source leader。
`_receive()` 先把 epoch mismatch 拒绝为 `stale_leader_epoch`，再拒绝非当前
source，之后才允许 append 或替换。已 committed 的旧 epoch entry 仍可 replay，
因为 `entry_leader_epoch` 表示数据历史；packet authority 来自当前新 epoch。

这是 controller-directed clean election，不是 Raft election。节点不会竞选或
交换投票；controller 只能选 ISR member，HW 定义安全前缀。真实 Kafka 还具有
unclean-election policy 和许多 metadata 故障模式，本模型没有覆盖。

## 6. Read level 暴露 HW 与 lease 边界

`client_read()` 刻意分开三种观察：

- `LOCAL` 读取指定节点完整本地 `data`，可能 stale；
- `LEADER` 读取当前 leader 完整本地 `data`，可能包含 HW 以上 append；
- `LINEARIZABLE` 只有在 leader 没有 HW 以上 entry、逻辑 lease 未过期且 ISR
  仍满足 minimum 时，才读取 `committed_data`；否则抛出 `RuntimeError`。

方法名是课程共享词汇。MiniDist predicate 是教学权威 guard，不是 Kafka 提供
linearizable read API 的证明。`lease_read(node, key)` 还要求指定节点是
controller 当前 leader 且 lease 有效，再委托给 HW-checked read；被替换旧
leader 因而 fail closed。

实验 6 给出完整矩阵：

```text
实验 6：ReadLevel × 协议
async: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
wal: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
isr: LOCAL=stale, LEADER=fresh, LINEARIZABLE=blocked
raft: LOCAL=stale, LEADER=fresh, LINEARIZABLE=fresh
```

“Blocked” 有具体含义：ISR leader 本地已经 append `k=v`，但 HW 尚未到达，
所以 authority read 拒绝暴露未提交值。

## 7. 动手实验：收缩、重入并检查 HW

运行：

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel
from minidist.protocols.isr import IsrGroup

g = IsrGroup(replica_lag_timeout=2, min_insync_replicas=2)
slow = next(n for n in g.probe().nodes if n != g.leader)
g.set_replica_slow(slow, True)
for i in range(3):
    result = g.client_write(f"k{i}".encode(), str(i).encode(),
                            AckLevel.ALL_ISR)
state = g.probe()
print("write:", result.accepted)
print("ISR after lag:", sorted(state.isr))
print("slow LEO / HW:", state.nodes[slow].log_end_offset,
      state.high_watermark)
g.set_replica_slow(slow, False)
g.run_until_idle()
state = g.probe()
print("ISR after catch-up:", sorted(state.isr))
print("slow LEO / HW:", state.nodes[slow].log_end_offset,
      state.high_watermark)
PY
```

实测输出：

```text
write: True
ISR after lag: ['node-1', 'node-3']
slow LEO / HW: 0 3
ISR after catch-up: ['node-1', 'node-2', 'node-3']
slow LEO / HW: 3 3
```

Follower 不是被提前声明健康；它先接收 retained entry，到达 leader offset 3
后才允许重入 ISR。

运行定向测试：

```bash
uv run pytest -q tests/protocols/test_isr.py
```

实测输出：

```text
...........                                                              [100%]
11 passed in 0.03s
```

## 8. 对照真实 Kafka

[协议映射](../mapping.md#协议-3-与-kafka-isr-复制的映射)把 ISR 收缩/重入、
HW、`acks=all` 加 `min.insync.replicas`、leader epoch 和 clean-prefix
truncation 分类为本实验室保留的机制。

MiniDist 省略 broker/client、record batch、producer ID/epoch、idempotence、
transaction、log segment、按 bytes/time retention、fetch session、replica
selector、KRaft metadata replication、reassignment、rack awareness 与真实
durability。进程内 controller 永不故障。`lease_read()` 是教学操作，不是
Kafka client API。`fsync()` 与 `lose_unfsynced=True` 公开模拟本地 durable
image，不是生产 Kafka page cache 与 flush 配置。

诚实对应关系是：动态同步集合定义 committed prefix 与写准入边界。它不是
“换名的 Raft”；动态 `ALL_ISR` 甚至可能覆盖少于固定多数派的节点。

## 9. 练习

### 理解题

1. 移出慢 follower 为什么可能提高进展，却不会自动削弱
   `min.insync.replicas`？
2. Leader `data` 与 `committed_data` 有何区别？
3. 为什么 `leader_epoch` 与 `entry_leader_epoch` 必须分开？

??? note "参考答案"

    1. 移出只改变 ISR；剩余大小仍在 `ALL_ISR` 准入前独立与 minimum 比较。
    2. `data` 包含所有本地 append；`committed_data` 只含 HW 以前前缀，
       所以 `LEADER` read 可能比 authority read 看得更多。
    3. Packet epoch 证明当前发送权威；entry epoch 记录历史来源，新 leader
       因此可以 replay committed 旧前缀而不接受旧 leader packet。

### 动手题

4. 编写临时测试：把两个 follower 都标 slow，推进两个 tick，断言
   `ALL_ISR` 写被拒，且 leader log-end offset 不变。

??? note "参考答案"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.isr import IsrGroup

    def test_minimum_rejects_before_append():
        g = IsrGroup(replica_lag_timeout=1, min_insync_replicas=2)
        followers = [n for n in g.probe().nodes if n != g.leader]
        for node in followers:
            g.set_replica_slow(node, True)
        g.tick(); g.tick()
        before = g.probe().nodes[g.leader].log_end_offset
        result = g.client_write(b"k", b"v", AckLevel.ALL_ISR)
        assert not result.accepted
        assert g.probe().nodes[g.leader].log_end_offset == before
    ```

    保存为 `/tmp/test_isr_ch10.py`，运行
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_isr_ch10.py`，并保持
    `src/` 不变。

## 小结

ISR replication 把 membership 变成写边界的一部分。Lagging replica 可以离开，
但 `min.insync.replicas` 限制集合能缩到多小；HW 是剩余 ISR 的最小 log end，
只有该前缀 committed。追平 replica 可以重入，而 controller election 截断到
HW，并递增用于 fence 旧 packet 的 leader epoch。最终比较不是一条由弱到强的
阶梯：配置式 WAL standby、动态 ISR、固定 Raft 多数派与异步 sink 回答的是不同
运维问题。共享 enum 名称看似能抹平差异时，请回到第 8 章七实验矩阵。
