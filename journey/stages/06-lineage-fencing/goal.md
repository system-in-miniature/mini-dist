# Stage 06 · Promotion lineage fencing / 提升谱系 Fencing

<!-- journey: chapter=5 tests_added=5 -->

## English

### Goal

Prevent delayed replication from an obsolete primary lineage from contaminating the post-promotion history, while making async split-brain behavior directly comparable with Raft.

### Deliverable files

- `labs/exp03_partition_old_leader.py`
- `src/minidist/protocols/async_primary/group.py`
- `tests/labs/test_experiments.py`
- `tests/protocols/test_async_primary.py`

### The problem at this point

Stage 03 can promote a replica, but messages already queued by the old primary still arrive later. Checking only offsets is unsafe because two lineages can both contain offset 1. The receiver needs to know whether the sender still owns current authority when the message arrives.

### Test contract

#### See the failure first

One contract queues a full sync from the old primary, promotes another replica before delivery, and then drains the network. The queued snapshot must be rejected as `non_current_primary`; otherwise delayed old history can overwrite the newly promoted lineage.

<!-- journey-file: tests/protocols/test_async_primary.py -->
#### Delayed-message fencing contract

##### What this test locks

Four focused cases lock rejection by current-primary identity, generation, and replication ID, plus active convergence of alive replicas immediately after promotion.

##### How it constructs the counterexample

It deliberately keeps stream entries or full snapshots in flight, changes authority, and then lets those messages reach the receiver.

##### Key test statement

```python
assert rejected
assert rejected[0].details["reason"] == "non_current_primary"
```

##### What a failure means

The receiver is trusting a plausible offset or payload without proving it belongs to the current authority lineage.

<!-- journey-file: tests/labs/test_experiments.py -->
#### Split-brain comparison contract

##### What this test locks

The lab must expose that both async sides accept writes during partition, while healing chooses the promoted lineage and removes the old dirty value.

##### How it constructs the counterexample

It isolates the old primary, writes different keys on both sides, promotes the majority-side replica, heals, and observes both pre-heal and post-heal values.

##### Key test statement

```python
assert result.old_primary_write_accepted
assert result.new_primary_write_accepted
```

##### What a failure means

The experiment hid split-brain acceptance or failed to show which lineage wins after healing.

### Basic concepts

Promotion creates a new generation of authority. A replication ID names the data lineage; an offset is meaningful only inside it. Fencing is a receive-time decision because a message can be valid when sent and obsolete when delivered. Active convergence means promotion immediately resynchronizes reachable replicas instead of waiting for another client write.

### Why this mechanism is necessary

Distributed systems reorder authority changes and data messages. Without lineage fencing, a delayed packet can resurrect state from an old primary after failover appears complete.

### Runtime mental model

Promotion increments the generation, assigns a new replication identity, marks the chosen node primary, and synchronizes alive replicas. Every arriving entry or full sync is checked against current sender identity, generation, and lineage before any state mutation; rejected messages become trace evidence.

### Mechanism blocks

<!-- journey-file: src/minidist/protocols/async_primary/group.py -->
#### Promotion lineage and receive-time fencing

##### What it is and why it appears

The async group gains network isolation/healing plus a single rejection classifier shared by stream and full-sync delivery.

##### Runtime role

The classifier runs at receive time, before `_apply_entry` or `_apply_full_sync`, and records a stable reason for every rejected old-authority message.

##### Key code

```python
if payload.get("generation") != self._generation:
    return "stale_generation"
if payload.get("replication_id") != primary.replication_id:
    return "stale_replication_id"
```

##### Statement understanding

Generation fences the authority era; replication ID fences the history within that era. Both are required because matching offsets alone do not identify the same history.

<!-- journey-file: labs/exp03_partition_old_leader.py -->
#### Async split-brain experiment

##### What it is and why it appears

Experiment 3 now selects `async` or `raft` and returns protocol-specific facts from one public scenario.

##### Runtime role

The async branch observes divergent writes during isolation and convergence after promotion and healing; the Raft branch retains its quorum-authority evidence.

##### Key code

```python
if protocol == "async":
    return _run_async_experiment(verbose=verbose)
if protocol != "raft":
    raise ValueError("protocol must be 'async' or 'raft'")
```

##### Statement understanding

Protocol selection changes the implementation, not the comparison question. Unknown values fail explicitly so the matrix cannot silently omit a column.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/06-lineage-fencing/tests.txt)`. The focused and lab contracts cover stale full sync, stale stream entries, active convergence, classifier reasons, and visible split brain.

### Durable takeaways

Offsets require lineage context. Authority is checked when data arrives, and healing must converge replicas without waiting for unrelated traffic.

### Explain it in your own words

Explain how a message can be valid when sent but unsafe when delivered, and why sender identity, generation, and replication ID answer different questions.

### Textbook

[Chapter 5](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/05-failover-fencing.md)

## 中文

### 目标

阻止过期 Primary Lineage 的延迟复制污染 Promotion 后历史，并让异步 Split-brain 行为可以与 Raft 直接比较。

### 交付文件

- `labs/exp03_partition_old_leader.py`
- `src/minidist/protocols/async_primary/group.py`
- `tests/labs/test_experiments.py`
- `tests/protocols/test_async_primary.py`

### 当前遇到的问题

Stage 03 可以提升 Replica，但旧 Primary 已排队的消息仍会稍后到达。只检查 Offset 不安全，因为两条 Lineage 都可能拥有 Offset 1。Receiver 必须在消息到达时确认 Sender 是否仍拥有当前权威。

### 测试契约

#### 先看会坏在哪里

一条契约先让旧 Primary 排队 Full Sync，在投递前提升另一 Replica，再排空网络。该快照必须以 `non_current_primary` 被拒绝，否则延迟旧历史可以覆盖刚建立的 Lineage。

<!-- journey-file: tests/protocols/test_async_primary.py -->
#### 延迟消息 Fencing 契约

##### 测试锁定什么

四个聚焦场景锁定按当前 Primary Identity、Generation 与 Replication ID 拒绝，并锁定 Promotion 后立即主动收敛存活 Replica。

##### 如何构造反例

测试刻意让 Stream Entry 或 Full Snapshot 保持在途，改变权威，再让消息到达 Receiver。

##### 关键测试语句

```python
assert rejected
assert rejected[0].details["reason"] == "non_current_primary"
```

##### 失败意味着什么

Receiver 在没有证明 Payload 属于当前权威 Lineage 时，只凭看似合理的 Offset 或内容就接受它。

<!-- journey-file: tests/labs/test_experiments.py -->
#### Split-brain 比较契约

##### 测试锁定什么

Lab 必须展示分区时异步两侧都接受写入，而 Heal 后选择已提升 Lineage 并移除旧 Dirty Value。

##### 如何构造反例

它隔离旧 Primary，在两侧写入不同 Key，提升多数侧 Replica，恢复网络，并观察 Heal 前后的值。

##### 关键测试语句

```python
assert result.old_primary_write_accepted
assert result.new_primary_write_accepted
```

##### 失败意味着什么

实验隐藏了 Split-brain 接受事实，或者没有展示 Heal 后哪条 Lineage 获胜。

### 基本概念

Promotion 创建新的权威 Generation。Replication ID 命名数据 Lineage；Offset 只在其中有意义。Fencing 必须发生在接收时，因为消息发送时可能有效、投递时已经过期。主动收敛表示 Promotion 立即同步可达 Replica，而不是等待下一次客户端写入。

### 为什么需要这个机制

分布式系统会重排权威变更与数据消息。没有 Lineage Fencing，延迟 Packet 可以在故障转移看似完成后复活旧 Primary 状态。

### 运行时心智模型

Promotion 增加 Generation、分配新 Replication Identity、标记新 Primary，并同步存活 Replica。每个到达的 Entry 或 Full Sync 在任何状态变更前检查当前 Sender Identity、Generation 与 Lineage；被拒绝消息形成 Trace 证据。

### 机制板块

<!-- journey-file: src/minidist/protocols/async_primary/group.py -->
#### 提升谱系与接收时 Fencing

##### 是什么，为什么现在需要

异步 Group 增加网络隔离/恢复，以及 Stream 与 Full-sync 投递共享的单一拒绝分类器。

##### 在运行时做什么

分类器在接收时、`_apply_entry` 或 `_apply_full_sync` 以前运行，并为每条旧权威消息记录稳定原因。

##### 关键代码

```python
if payload.get("generation") != self._generation:
    return "stale_generation"
if payload.get("replication_id") != primary.replication_id:
    return "stale_replication_id"
```

##### 关键语句理解

Generation 隔离权威纪元；Replication ID 隔离该纪元内的历史。两者都需要，因为相同 Offset 不代表同一历史。

<!-- journey-file: labs/exp03_partition_old_leader.py -->
#### 异步 Split-brain 实验

##### 是什么，为什么现在需要

实验 3 现在可选择 `async` 或 `raft`，并从同一个公开场景返回各协议事实。

##### 在运行时做什么

异步分支观察隔离期间的分叉写入，以及 Promotion/Heal 后收敛；Raft 分支保留多数派权威证据。

##### 关键代码

```python
if protocol == "async":
    return _run_async_experiment(verbose=verbose)
if protocol != "raft":
    raise ValueError("protocol must be 'async' or 'raft'")
```

##### 关键语句理解

Protocol Selection 改变实现，不改变比较问题。未知值显式失败，防止矩阵静默漏掉一列。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/06-lineage-fencing/tests.txt)`。聚焦契约与 Lab 覆盖过期 Full Sync、过期 Stream Entry、主动收敛、拒绝原因与可见 Split Brain。

### 需要真正记住的内容

Offset 需要 Lineage 上下文；数据到达时校验权威；Heal 必须在不等待无关流量时完成收敛。

### 用自己的话讲清楚

说明一条消息为何发送时有效、投递时却不安全，以及 Sender Identity、Generation 与 Replication ID 分别回答什么问题。

### 教材

[第 5 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/05-failover-fencing.md)
