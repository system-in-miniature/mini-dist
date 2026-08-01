# Stage 10 · Four-protocol experiment matrix / 四协议实验矩阵

<!-- journey: chapter=8 tests_added=5 -->

## English

### Goal

Complete Experiments 4–7 across async primary, WAL shipping, ISR, and Raft so membership, catch-up, read consistency, and stale-authority behavior can be compared from returned evidence.

### Deliverable files

- `labs/exp04_slow_replica.py`
- `labs/exp05_replica_reconnect.py`
- `labs/exp06_read_consistency.py`
- `labs/exp07_split_brain_lease.py`
- `tests/labs/test_experiments.py`

### The problem at this point

The four protocols now exist, but isolated implementation tests do not answer the project's closing questions: what a slow replica changes, when reconnect becomes full copy, which reads may be stale, and how an old side is prevented from claiming current authority.

### Test contract

#### See the failure first

The read-matrix contract requires every protocol's local read to be stale and leader read fresh under one lag script, while linearizable status remains `unsupported`, `blocked`, or `fresh` according to the actual mechanism. Collapsing all exceptions into one value would erase the distinction between missing capability and temporarily unavailable authority.

<!-- journey-file: tests/labs/test_experiments.py -->
#### Four-protocol matrix contract

##### What this test locks

Five contracts lock all four columns for slow-replica availability, catch-up triggers, read-level results, and split-brain authority guards.

##### How it constructs the counterexample

Tests invoke each protocol through the same experiment entry point and assert normalized statuses plus protocol-specific membership or guard explanations.

##### Key test statement

```python
assert matrix["isr"]["LINEARIZABLE"].status == "blocked"
assert matrix["raft"]["LINEARIZABLE"].status == "fresh"
```

##### What a failure means

The matrix hid a semantic difference, a scenario stopped being comparable, or a protocol reported authority it could not prove.

### Basic concepts

A slow replica affects voting, synchronous wait policy, or dynamic membership differently. Retention is a protocol-specific continuity rule: async backlog, WAL retention, ISR retained log, or Raft log replay. Read consistency has both location and proof dimensions. A stale local value can remain observable without being authoritative; leases and read-index barriers are distinct guards.

### Why this mechanism is necessary

The final learning artifact is the comparison itself. It should let a learner predict outcomes from mechanisms rather than memorize product labels, while keeping unsupported and blocked capabilities visible.

### Runtime mental model

Each lab has a protocol selector, one fixed fault script, and an immutable result. Experiments 4–5 return availability/membership and catch-up mode. Experiment 6 converts reads or failures into `fresh`, `stale`, `unsupported`, or `blocked`. Experiment 7 reads the isolated old side locally, reads current state through the protocol's authority path, and reports its guard.

### Mechanism blocks

<!-- journey-file: labs/exp04_slow_replica.py -->
#### Slow-replica membership effects

##### What it is and why it appears

Experiment 4 asks whether writes remain available and what membership meaning a slow node has in each protocol.

##### Runtime role

The script isolates or slows one node, performs the protocol's supported acknowledged write, and returns stale visibility plus a named membership effect.

##### Key code

```python
write = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
state = group.probe()
stale = state.nodes[slow_node].data.get(b"k") is None
```

##### Statement understanding

All four columns execute the same experiment API; the returned membership explanation preserves why availability holds.

<!-- journey-file: labs/exp05_replica_reconnect.py -->
#### Retention-window catch-up matrix

##### What it is and why it appears

Experiment 5 disconnects a replica for a short and long gap and reports the resulting catch-up mode.

##### Runtime role

It drives each concrete group's crash, write, restart, and idle operations, then translates protocol vocabulary into comparable result strings.

##### Key code

```python
_write_many(inside, AckLevel.LEADER, 2)
inside.restart("r")
inside.run_until_idle()
```

##### Statement understanding

The number of missed writes is controlled relative to retention; reconnect mode is an observed result, not selected by the lab.

<!-- journey-file: labs/exp06_read_consistency.py -->
#### Read-level outcome matrix

##### What it is and why it appears

Experiment 6 places one follower behind and evaluates LOCAL, LEADER, and LINEARIZABLE reads across every protocol.

##### Runtime role

`_observe` turns a returned value, `UnsupportedLevelError`, or authority `RuntimeError` into distinct statuses without erasing the cause.

##### Key code

```python
except UnsupportedLevelError:
    return ReadObservation("unsupported", None)
except RuntimeError:
    return ReadObservation("blocked", None)
```

##### Statement understanding

Unsupported means the mechanism does not provide that guarantee; blocked means the guarantee exists but current authority evidence is unavailable.

<!-- journey-file: labs/exp07_split_brain_lease.py -->
#### Stale-side read and authority guard

##### What it is and why it appears

Experiment 7 proves an isolated old side may still expose its prior local value while only the current side can satisfy an authority-guarded read.

##### Runtime role

It creates shared state, partitions the old authority, changes authority, writes a new value, then records old-local, current, and guard results.

##### Key code

```python
old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
```

##### Statement understanding

Observability is not authority. Returning an old local value is allowed; presenting it through the guarded current read would be the safety violation.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/10-advanced-experiments/tests.txt)`. The five new contracts complete the seven-experiment, four-protocol evidence matrix.

### Durable takeaways

Membership, retention, read location, and authority proof are independent axes. Preserve `unsupported`, `blocked`, `stale`, and `fresh` as different outcomes.

### Explain it in your own words

Choose one row of the final matrix and predict all four protocol results from their mechanisms, including why a stale local read is not itself a safety failure.

### Textbook

[Chapter 8](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/08-experiments.md)

## 中文

### 目标

完成异步主从、WAL Shipping、ISR 与 Raft 的实验 4–7，用返回证据比较成员、追赶、读取一致性与旧权威行为。

### 交付文件

- `labs/exp04_slow_replica.py`
- `labs/exp05_replica_reconnect.py`
- `labs/exp06_read_consistency.py`
- `labs/exp07_split_brain_lease.py`
- `tests/labs/test_experiments.py`

### 当前遇到的问题

四种协议已经存在，但孤立实现测试不能回答项目最后的问题：慢 Replica 改变什么、何时重连变成全量复制、哪些读取可能陈旧，以及如何阻止旧侧声称当前权威。

### 测试契约

#### 先看会坏在哪里

读取矩阵契约要求在同一 Lag 脚本下，每种协议的 Local Read 都陈旧、Leader Read 都新鲜，而 Linearizable 状态按真实机制分别为 `unsupported`、`blocked` 或 `fresh`。把所有异常折成同一个值会抹掉“没有能力”和“当前权威暂不可用”的区别。

<!-- journey-file: tests/labs/test_experiments.py -->
#### 四协议矩阵契约

##### 测试锁定什么

五条契约锁定慢副本可用性、Catch-up Trigger、Read-level Result 与 Split-brain Authority Guard 的全部四列。

##### 如何构造反例

测试通过同一 Experiment Entry 调用每个协议，并断言归一化 Status 与协议特有 Membership/Guard 解释。

##### 关键测试语句

```python
assert matrix["isr"]["LINEARIZABLE"].status == "blocked"
assert matrix["raft"]["LINEARIZABLE"].status == "fresh"
```

##### 失败意味着什么

矩阵隐藏了语义差异、场景不再可比较，或者协议报告了无法证明的权威。

### 基本概念

慢 Replica 对投票、同步等待策略或动态成员有不同影响。Retention 是协议特有的连续性规则：异步 Backlog、WAL Retention、ISR Retained Log 或 Raft Log Replay。读取一致性同时包含位置与证明维度。陈旧本地值可以继续可见而不具权威；Lease 与 Read-index Barrier 是不同 Guard。

### 为什么需要这个机制

最终学习产物就是比较本身。它应让学习者从机制预测结果，而不是记产品标签，同时保持 Unsupported 与 Blocked 能力可见。

### 运行时心智模型

每个 Lab 都有 Protocol Selector、固定 Fault Script 与不可变 Result。实验 4–5 返回 Availability/Membership 与 Catch-up Mode。实验 6 把读取或失败转成 `fresh`、`stale`、`unsupported` 或 `blocked`。实验 7 本地读取被隔离旧侧，通过协议权威路径读取当前状态，并报告 Guard。

### 机制板块

<!-- journey-file: labs/exp04_slow_replica.py -->
#### 慢副本成员影响

##### 是什么，为什么现在需要

实验 4 询问每种协议能否继续写，以及慢节点具有何种成员含义。

##### 在运行时做什么

脚本隔离或放慢一个节点，执行协议支持的确认写入，再返回陈旧可见性与命名 Membership Effect。

##### 关键代码

```python
write = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
state = group.probe()
stale = state.nodes[slow_node].data.get(b"k") is None
```

##### 关键语句理解

四列执行相同 Experiment API；返回的 Membership Explanation 保留可用性成立的原因。

<!-- journey-file: labs/exp05_replica_reconnect.py -->
#### Retention Window 追赶矩阵

##### 是什么，为什么现在需要

实验 5 让 Replica 分别错过短间隔与长间隔，并报告由此产生的 Catch-up Mode。

##### 在运行时做什么

它驱动具体 Group 的 Crash、Write、Restart 与 Idle 操作，再把协议词汇翻译成可比较 Result String。

##### 关键代码

```python
_write_many(inside, AckLevel.LEADER, 2)
inside.restart("r")
inside.run_until_idle()
```

##### 关键语句理解

错过写入数量相对 Retention 受控；Reconnect Mode 是观察结果，不是 Lab 自己选择。

<!-- journey-file: labs/exp06_read_consistency.py -->
#### Read-level 结果矩阵

##### 是什么，为什么现在需要

实验 6 让一个 Follower 落后，并跨所有协议求值 LOCAL、LEADER 与 LINEARIZABLE Read。

##### 在运行时做什么

`_observe` 把返回值、`UnsupportedLevelError` 或 Authority `RuntimeError` 转成不同 Status，不抹掉原因。

##### 关键代码

```python
except UnsupportedLevelError:
    return ReadObservation("unsupported", None)
except RuntimeError:
    return ReadObservation("blocked", None)
```

##### 关键语句理解

Unsupported 表示机制不提供该保证；Blocked 表示保证存在，但当前没有足够权威证据。

<!-- journey-file: labs/exp07_split_brain_lease.py -->
#### 旧侧读取与权威 Guard

##### 是什么，为什么现在需要

实验 7 证明被隔离旧侧仍可暴露此前本地值，但只有当前侧能满足受权威保护的读取。

##### 在运行时做什么

它创建共享状态、分区旧权威、改变权威、写入新值，再记录 Old-local、Current 与 Guard Result。

##### 关键代码

```python
old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
```

##### 关键语句理解

可观察性不等于权威。返回旧本地值可以允许；若通过受保护 Current Read 提供它才是安全违规。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/10-advanced-experiments/tests.txt)`。五条新增契约完成七实验、四协议证据矩阵。

### 需要真正记住的内容

Membership、Retention、Read Location 与 Authority Proof 是独立轴。`unsupported`、`blocked`、`stale` 与 `fresh` 必须保持不同结果。

### 用自己的话讲清楚

选择最终矩阵的一行，从机制预测四种协议结果，并说明陈旧 Local Read 为何本身不是安全故障。

### 教材

[第 8 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/08-experiments.md)
