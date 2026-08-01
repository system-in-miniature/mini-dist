# Stage 03 · Asynchronous primary replication / 异步主从复制

<!-- journey: chapter=4 tests_added=6 -->

## English

### Goal

Build a primary–replica group whose acknowledgement can precede replica delivery, then make the resulting lag, catch-up, and failover-loss window observable.

### Deliverable files

- `src/minidist/protocols/async_primary/__init__.py`
- `src/minidist/protocols/async_primary/group.py`
- `tests/protocols/test_async_primary.py`

### The problem at this point

The shared driver can ask for an acknowledgement, but no implementation yet gives that word a concrete boundary. This stage needs a protocol where local acceptance is deliberately weaker than replicated or durable agreement, so later comparisons have a real low-latency endpoint.

### Test contract

#### See the failure first

The first contract writes with `AckLevel.LEADER` while delivery takes two ticks. It immediately reads the replica and expects no value, then advances logical time until the value appears. A helper that drains the network before returning would erase the intended acknowledgement window and teach synchronous replication by accident.

<!-- journey-file: tests/protocols/test_async_primary.py -->
#### Asynchronous visibility and failover contract

##### What this test locks

The suite locks early leader acknowledgement, honest rejection of stronger levels, partial versus full resynchronization, and the possibility that an acknowledged write disappears after promoting a lagging replica.

##### How it constructs the counterexample

It keeps network delivery pending, crashes or disconnects a replica across a bounded backlog, and promotes a node that has not received the latest primary write.

##### Key test statement

```python
assert result.accepted
assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
```

##### What a failure means

Either the acknowledgement boundary silently waited for a replica, or the protocol claimed a guarantee its topology cannot provide.

### Basic concepts

The primary is the only write entry point; replicas are sinks, not voters. The replication position is a lineage identity plus an offset. A bounded backlog can replay a known prefix gap, while a replica outside that window needs a full state copy. Promotion changes which node accepts writes but does not retroactively replicate data the candidate never received.

### Why this mechanism is necessary

“Fast acknowledgement” only becomes meaningful when the learner can point to work that has not happened yet. Making delivery and catch-up explicit exposes the tradeoff: lower response latency buys a window in which failover can discard accepted work.

### Runtime mental model

`client_write` mutates the current primary, appends one lineage-scoped entry to the backlog, schedules a message for every replica, and returns. Ticks deliver messages. Restart chooses partial replay when the replica still shares the lineage and retained prefix; otherwise it sends a full snapshot. Manual promotion changes the primary immediately.

### Mechanism blocks

<!-- journey-file: src/minidist/protocols/async_primary/group.py -->
#### Primary acknowledgement and asynchronous replica flow

##### What it is and why it appears

This group owns primary state, replica state, the retained backlog, delayed messages, restart synchronization, and manual promotion as one coherent protocol model.

##### Runtime role

It applies writes locally before scheduling replication. Reads may target a local node or the current primary, while unsupported quorum and linearizable requests fail instead of being weakened.

##### Key code

```python
for node_id in self._nodes:
    if node_id != self._primary:
        self._send_entry(self._primary, node_id, entry, "stream")
```

##### Statement understanding

Scheduling one message per replica is intentionally before neither a wait nor a quorum check. The later return therefore proves only that this primary applied the entry.

<!-- journey-file: src/minidist/protocols/async_primary/__init__.py -->
#### Async-primary package boundary

This export file is supporting wiring for the group and `SyncMode`; it does not add another replication owner.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/03-async-primary/tests.txt)`. Six contracts demonstrate delayed visibility, support boundaries, catch-up modes, and acknowledged-write loss.

### Durable takeaways

Leader acknowledgement is not replication. A backlog proves only retained continuity, and promotion cannot preserve a write absent from the promoted node.

### Explain it in your own words

Explain the exact interval between `client_write` returning and the replica applying the entry, then explain why a crash and promotion inside that interval may lose an accepted value.

### Textbook

[Chapter 4](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/04-async-primary.md)

## 中文

### 目标

建立一个确认可早于副本投递的主从复制组，并让由此产生的延迟、追赶与故障转移丢失窗口可观察。

### 交付文件

- `src/minidist/protocols/async_primary/__init__.py`
- `src/minidist/protocols/async_primary/group.py`
- `tests/protocols/test_async_primary.py`

### 当前遇到的问题

公共驱动已经可以请求确认，但还没有实现为这个词赋予具体边界。本阶段需要一个本地接受明确弱于复制或持久共识的协议，给后续比较提供真实的低延迟端点。

### 测试契约

#### 先看会坏在哪里

第一条契约在投递延迟为两个 Tick 时使用 `AckLevel.LEADER` 写入。它立刻读取副本并期待没有值，再推进逻辑时间直到值出现。如果 helper 在返回前偷偷排空网络，就会抹掉刻意设计的确认窗口，误教成同步复制。

<!-- journey-file: tests/protocols/test_async_primary.py -->
#### 异步可见性与故障转移契约

##### 测试锁定什么

锁定主节点提前确认、对更强 Level 的诚实拒绝、部分/全量重同步，以及提升落后副本后已确认写入仍可能消失。

##### 如何构造反例

测试让网络消息保持待投递，让副本跨越有限 Backlog 断开或崩溃，并提升尚未收到最新写入的节点。

##### 关键测试语句

```python
assert result.accepted
assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
```

##### 失败意味着什么

确认边界要么暗中等待了副本，要么协议声称了当前拓扑无法提供的保证。

### 基本概念

Primary 是唯一写入口；Replica 是接收端，不参与投票。复制位置由 Lineage Identity 与 Offset 共同表示。有限 Backlog 可以重放仍保留的前缀缺口，超出窗口的副本需要全量状态复制。Promotion 只改变接收新写入的节点，不能追溯性补回候选节点从未收到的数据。

### 为什么需要这个机制

只有学习者能指出尚未发生的工作，“快速确认”才有准确含义。显式化投递与追赶可直接看见取舍：更低响应延迟换来了故障转移时丢弃已接受工作的一段窗口。

### 运行时心智模型

`client_write` 先修改当前 Primary，把带 Lineage 的 Entry 放入 Backlog，为每个 Replica 安排消息，然后返回。Tick 驱动投递。Restart 在副本仍共享 Lineage 且缺口仍保留时选择部分重放，否则发送全量快照。手动 Promotion 立即改变 Primary。

### 机制板块

<!-- journey-file: src/minidist/protocols/async_primary/group.py -->
#### 主节点确认与异步副本流

##### 是什么，为什么现在需要

该 Group 在一个协议模型中共同拥有主节点状态、副本状态、保留 Backlog、延迟消息、重启同步与手动提升。

##### 在运行时做什么

它先在本地应用写入，再安排复制。读取可以指向本地节点或当前 Primary；不支持的 Quorum 与 Linearizable 请求会失败，而不是被静默降级。

##### 关键代码

```python
for node_id in self._nodes:
    if node_id != self._primary:
        self._send_entry(self._primary, node_id, entry, "stream")
```

##### 关键语句理解

逐副本安排消息之后既没有等待，也没有 Quorum 检查。因此后续返回只证明当前 Primary 已应用 Entry。

<!-- journey-file: src/minidist/protocols/async_primary/__init__.py -->
#### 异步主从包边界

这个导出文件只为 Group 与 `SyncMode` 提供支撑接线，不引入第二个复制所有者。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/03-async-primary/tests.txt)`。六条契约展示延迟可见性、支持边界、追赶模式与已确认写入丢失。

### 需要真正记住的内容

Leader 确认不等于复制。Backlog 只证明仍保留的连续性，Promotion 无法保住提升节点上不存在的写入。

### 用自己的话讲清楚

说明 `client_write` 返回到副本应用 Entry 之间的精确区间，再说明为何在该区间内崩溃并提升会丢失已接受值。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/04-async-primary.md)
