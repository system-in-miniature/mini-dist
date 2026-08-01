# Stage 09 · ISR membership and high watermark / ISR 成员与 High Watermark

<!-- journey: chapter=10 tests_added=10 -->

## English

### Goal

Build an ISR replication group where dynamic membership, `min.insync`, high-watermark visibility, controller epochs, retained catch-up, and lease reads form one coherent authority model.

### Deliverable files

- `src/minidist/protocols/isr/__init__.py`
- `src/minidist/protocols/isr/group.py`
- `tests/protocols/test_isr.py`

### The problem at this point

Neither fixed Raft voters nor configured WAL standbys model a broker leader whose acknowledgement set changes with observed lag. We need to separate a leader's log end from the committed high watermark and make controller-driven authority changes fence old-epoch traffic.

### Test contract

#### See the failure first

The slow-replica contract keeps one follower behind until it leaves ISR, then proves writes remain available with the two current members. A static membership implementation would either wait forever for the slow node or incorrectly advance the high watermark past an in-sync member.

<!-- journey-file: tests/protocols/test_isr.py -->
#### ISR, watermark, and epoch contract

##### What this test locks

Ten contracts lock `ALL_ISR`, membership shrink and `min.insync`, safe rejoin, controller/leader epoch fencing, committed-prefix replay, catch-up modes, local versus high-watermark reads, support boundaries, and trace replay.

##### How it constructs the counterexample

The suite marks replicas slow, advances lag time, elects only from ISR, queues old-epoch appends, and reconnects replicas on both sides of retention.

##### Key test statement

```python
assert state.high_watermark >= result.offset
assert all(state.nodes[node].log_end_offset >= result.offset for node in state.isr)
```

##### What a failure means

The group acknowledged before the current ISR reached the record, exposed uncommitted data, or allowed stale controller authority to mutate the log.

### Basic concepts

ISR is the current set of replicas caught up closely enough to participate in `ALL_ISR`. `min.insync.replicas` is an admission rule after membership is refreshed. The high watermark is the minimum log end across alive ISR and bounds committed visibility. Controller and leader epochs fence old authority. Clean election chooses an ISR member and truncates any suffix beyond HW. A replica rejoins only after catching up to the committed boundary.

### Why this mechanism is necessary

Dynamic membership trades some redundancy for availability under lag, but only if the committed boundary and minimum set remain explicit. Without HW, local log presence is easily mistaken for committed data; without epochs, old leaders can append after reassignment.

### Runtime mental model

Before writes and on ticks, the group refreshes lag timers, removes or adds ISR members, derives a monotonic HW, and applies committed entries through that offset. `ALL_ISR` waits for the current set if it still meets `min.insync`. Controller election increments epochs, selects an ISR replica, truncates beyond HW, and synchronizes followers. Lease reads require current authority.

### Mechanism blocks

<!-- journey-file: src/minidist/protocols/isr/group.py -->
#### ISR membership, high watermark, epochs, and read authority

##### What it is and why it appears

This group owns broker logs, dynamic ISR, HW application, lag tracking, clean election, epoch fencing, retained catch-up, fsync state, and lease reads.

##### Runtime role

Membership refresh precedes acknowledgement decisions and derives HW from alive current ISR members; application never passes each node's local log end.

##### Key code

```python
candidate_hw = min(node.log_end_offset for node in alive_isr)
if candidate_hw > self._high_watermark:
    self._high_watermark = candidate_hw
```

##### Statement understanding

The minimum makes HW a prefix every current in-sync member has reached. Monotonic advancement prevents a later membership change from “uncommitting” visible records.

<!-- journey-file: src/minidist/protocols/isr/__init__.py -->
#### ISR package boundary

This supporting export exposes catch-up, entry, group, and probe vocabulary without owning membership or watermark transitions.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/09-isr-high-watermark/tests.txt)`. Ten contracts cover availability, commitment, authority, recovery, observation, and deterministic replay.

### Durable takeaways

ISR is dynamic membership, HW is committed visibility, and `min.insync` is a write admission threshold. Epochs fence authority; clean election preserves only the HW prefix.

### Explain it in your own words

Explain why removing a slow replica can preserve write availability without allowing HW to include data that a current ISR member has not received.

### Textbook

[Chapter 10](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/10-isr.md)

## 中文

### 目标

建立一个让动态成员、`min.insync`、High-watermark 可见性、Controller Epoch、保留追赶与 Lease Read 组成统一权威模型的 ISR Group。

### 交付文件

- `src/minidist/protocols/isr/__init__.py`
- `src/minidist/protocols/isr/group.py`
- `tests/protocols/test_isr.py`

### 当前遇到的问题

固定 Raft Voter 与配置 WAL Standby 都无法表示 Ack 集合随观测 Lag 变化的 Broker Leader。我们需要区分 Leader Log End 与已提交 High Watermark，并让 Controller 驱动的权威变化 Fencing 旧 Epoch 流量。

### 测试契约

#### 先看会坏在哪里

慢副本契约让一个 Follower 持续落后直到离开 ISR，再证明两个当前成员仍可写。静态成员实现要么永远等待慢节点，要么错误地让 High Watermark 越过仍在同步集合中的成员。

<!-- journey-file: tests/protocols/test_isr.py -->
#### ISR、Watermark 与 Epoch 契约

##### 测试锁定什么

十条契约锁定 `ALL_ISR`、成员收缩与 `min.insync`、安全 Rejoin、Controller/Leader Epoch Fencing、已提交前缀重放、Catch-up Mode、本地与 HW 读取、支持边界与 Trace 重放。

##### 如何构造反例

套件标记慢副本、推进 Lag 时间、只从 ISR 选举、排队旧 Epoch Append，并让 Replica 在 Retention 两侧重连。

##### 关键测试语句

```python
assert state.high_watermark >= result.offset
assert all(state.nodes[node].log_end_offset >= result.offset for node in state.isr)
```

##### 失败意味着什么

Group 在当前 ISR 到达 Record 前确认、暴露未提交数据，或允许过期 Controller 权威修改 Log。

### 基本概念

ISR 是当前足够接近 Leader、可参与 `ALL_ISR` 的 Replica 集合。`min.insync.replicas` 是刷新成员后的准入规则。High Watermark 是 Alive ISR 最小 Log End，限制已提交可见性。Controller 与 Leader Epoch Fencing 旧权威。Clean Election 选择 ISR 成员并截断 HW 之后后缀。Replica 只有追到已提交边界后才能 Rejoin。

### 为什么需要这个机制

动态成员通过降低部分冗余换取 Lag 下可用性，但必须显式保持 Commit Boundary 与最小集合。没有 HW，本地 Log Presence 很容易被误作已提交数据；没有 Epoch，旧 Leader 会在重新分配后继续 Append。

### 运行时心智模型

写入前与 Tick 时，Group 刷新 Lag Timer、移除或加入 ISR Member、推导单调 HW，并应用到该 Offset 的已提交 Entry。`ALL_ISR` 在当前集合仍满足 `min.insync` 时等待它。Controller Election 增加 Epoch、选择 ISR Replica、截断 HW 之后内容并同步 Follower。Lease Read 需要当前权威。

### 机制板块

<!-- journey-file: src/minidist/protocols/isr/group.py -->
#### ISR 成员、High Watermark、Epoch 与读取权威

##### 是什么，为什么现在需要

该 Group 拥有 Broker Log、动态 ISR、HW Apply、Lag Tracking、Clean Election、Epoch Fencing、Retained Catch-up、Fsync State 与 Lease Read。

##### 在运行时做什么

成员刷新先于 Ack Decision，并从存活当前 ISR 推导 HW；Apply 永不越过每个节点的本地 Log End。

##### 关键代码

```python
candidate_hw = min(node.log_end_offset for node in alive_isr)
if candidate_hw > self._high_watermark:
    self._high_watermark = candidate_hw
```

##### 关键语句理解

取最小值让 HW 成为每个当前同步成员都已到达的前缀。单调推进防止后续成员变化把已可见 Record “取消提交”。

<!-- journey-file: src/minidist/protocols/isr/__init__.py -->
#### ISR 包边界

这个支撑导出公开 Catch-up、Entry、Group 与 Probe 词汇，不拥有成员或 Watermark 迁移。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/09-isr-high-watermark/tests.txt)`。十条契约覆盖可用性、提交、权威、恢复、观察与确定性重放。

### 需要真正记住的内容

ISR 是动态成员，HW 是已提交可见性，`min.insync` 是写入准入阈值。Epoch Fencing 权威；Clean Election 只保留 HW 前缀。

### 用自己的话讲清楚

说明移除慢 Replica 为何能保持写可用性，同时不会让 HW 包含当前 ISR 成员尚未收到的数据。

### 教材

[第 10 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/10-isr.md)
