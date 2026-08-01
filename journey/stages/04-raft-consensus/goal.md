# Stage 04 · Raft authority and committed replication / Raft 权威与已提交复制

<!-- journey: chapter=6 tests_added=8 -->

## English

### Goal

Build a three-node Raft group in which seeded elections establish authority, log-prefix repair converges replicas, and only quorum-committed writes survive failover.

### Deliverable files

- `src/minidist/protocols/raft/__init__.py`
- `src/minidist/protocols/raft/group.py`
- `tests/protocols/test_raft.py`

### The problem at this point

Async replication exposes a real acknowledgement-loss window but cannot prove that a promoted node represents a majority history. We now need authority and data survival to be coupled by one protocol rather than by an operator choosing a replica.

### Test contract

#### See the failure first

The partition contract isolates the old leader, asks it for a quorum write, elects a new leader on the majority side, and later heals the network. The minority write must remain uncommitted and disappear during log repair. Counting local append as success would create two accepted histories.

<!-- journey-file: tests/protocols/test_raft.py -->
#### Election, commit, and failover contract

##### What this test locks

The suite locks single seeded leadership, quorum-only writes, linearizable read barriers, durable term/vote/log state, minority rejection, higher-term step-down, convergence, and exact trace replay.

##### How it constructs the counterexample

It partitions the elected leader from two followers, lets the majority elect in a higher term, writes on both sides, then heals and compares every log and applied value.

##### Key test statement

```python
assert not rejected.accepted
assert committed.accepted
```

##### What a failure means

The group either acknowledged without majority evidence, allowed stale authority to commit, or failed to repair a divergent prefix.

### Basic concepts

A term is a logical era of authority. Elections require a majority and an up-to-date log. `AppendEntries` proves a shared prefix before extending it. Replication and commitment differ: a leader advances `commit_index` only when a current-term entry is stored on a quorum; applying committed entries is another monotonic cursor. A linearizable read needs current-term quorum evidence, not merely a leader label.

### Why this mechanism is necessary

Failover safety cannot come from copying more eagerly alone. The system needs a rule for which history is authoritative and proof that an acknowledgement is represented on enough voters to survive one failure.

### Runtime mental model

Seeded election deadlines trigger candidacy and votes. A winner initializes per-follower `next_index`/`match_index`, appends client commands, and sends prefix-constrained AppendEntries. Responses update replication positions; current-term quorum evidence advances commit and application. Higher terms force old leaders to step down and their conflicting suffixes are overwritten.

### Mechanism blocks

<!-- journey-file: src/minidist/protocols/raft/group.py -->
#### Election, log matching, commit, and authoritative reads

##### What it is and why it appears

This module owns the complete teaching-scale Raft state machine: durable term/vote/log state, volatile roles and indices, elections, replication, commit, reads, partitions, and recovery.

##### Runtime role

Every tick handles timeouts, heartbeats, and network delivery. Client writes append only on the current leader and return accepted only after the wait loop observes commitment.

##### Key code

```python
if (
    replicated >= self._quorum
    and leader.log[index].term == leader.current_term
):
    leader.commit_index = index
```

##### Statement understanding

A majority count alone is insufficient for an older-term entry. Requiring the entry's term to equal the leader's current term makes committing it also commit the preceding safe prefix.

<!-- journey-file: src/minidist/protocols/raft/__init__.py -->
#### Raft package boundary

This supporting export makes roles, entries, group state, and the driver available without duplicating protocol behavior.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/04-raft-consensus/tests.txt)`. Eight contracts cover election, write/read guarantees, failover, restart ownership, partition healing, and deterministic replay.

### Durable takeaways

Leader identity is term-scoped. Stored is not committed; committed is not applied. Linearizable reads and writes both require current authority evidence.

### Explain it in your own words

Explain why the isolated old leader may append locally but cannot return a successful quorum acknowledgement, and how the higher-term leader repairs that suffix after healing.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/06-raft-election.md) · [Chapter 7](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/07-raft-replication.md)

## 中文

### 目标

建立一个三节点 Raft Group：用带 Seed 的选举建立权威，用日志前缀修复收敛副本，并让只有 Quorum 提交的写入在故障转移后继续存在。

### 交付文件

- `src/minidist/protocols/raft/__init__.py`
- `src/minidist/protocols/raft/group.py`
- `tests/protocols/test_raft.py`

### 当前遇到的问题

异步复制暴露了真实的确认丢失窗口，却无法证明被提升节点代表多数历史。现在需要由同一协议把权威与数据存活联系起来，而不是让运维者任选一个副本。

### 测试契约

#### 先看会坏在哪里

分区契约隔离旧 Leader，向它请求 Quorum 写入，让多数侧选出新 Leader，最后修复网络。少数侧写入必须保持未提交并在日志修复中消失。若把本地 Append 当成成功，就会产生两份都声称已接受的历史。

<!-- journey-file: tests/protocols/test_raft.py -->
#### 选举、提交与故障转移契约

##### 测试锁定什么

锁定带 Seed 的单 Leader、仅 Quorum 写入、线性一致读屏障、Term/Vote/Log 持久状态、少数侧拒绝、高 Term 降级、日志收敛与 Trace 精确重放。

##### 如何构造反例

测试把已选 Leader 与两个 Follower 隔开，让多数侧在更高 Term 选举，在两侧分别写入，再恢复连接并比较所有日志与已应用值。

##### 关键测试语句

```python
assert not rejected.accepted
assert committed.accepted
```

##### 失败意味着什么

Group 要么在没有多数证据时确认，要么允许过期权威提交，要么无法修复分叉前缀。

### 基本概念

Term 是权威的逻辑纪元。选举需要多数票与不落后的日志。`AppendEntries` 在扩展日志前证明共享前缀。复制与提交不同：Leader 只有在当前 Term Entry 存储于 Quorum 时才推进 `commit_index`；应用已提交 Entry 又是另一个单调 Cursor。线性一致读需要当前 Term 的 Quorum 证据，而不只是一个 Leader 标签。

### 为什么需要这个机制

仅仅更积极地复制无法得到故障转移安全。系统必须规定哪份历史具有权威，并证明一次确认已经落在足够多 Voter 上，可以承受一个节点失败。

### 运行时心智模型

带 Seed 的选举 Deadline 触发 Candidacy 与投票。赢家初始化每个 Follower 的 `next_index`/`match_index`，追加客户端命令并发送受前缀约束的 AppendEntries。响应推进复制位置；当前 Term 的多数证据推进 Commit 与 Apply。更高 Term 迫使旧 Leader 降级，冲突后缀随后被覆盖。

### 机制板块

<!-- journey-file: src/minidist/protocols/raft/group.py -->
#### 选举、日志匹配、提交与权威读取

##### 是什么，为什么现在需要

该模块拥有完整的教学规模 Raft 状态机：持久 Term/Vote/Log、易失 Role 与 Index、选举、复制、提交、读取、分区与恢复。

##### 在运行时做什么

每个 Tick 处理 Timeout、Heartbeat 与网络投递。客户端写入只在当前 Leader 追加，并且只有等待循环观察到提交后才返回 Accepted。

##### 关键代码

```python
if (
    replicated >= self._quorum
    and leader.log[index].term == leader.current_term
):
    leader.commit_index = index
```

##### 关键语句理解

只计算多数副本不足以安全提交旧 Term Entry。要求 Entry Term 等于 Leader 当前 Term，使提交它的同时安全提交此前前缀。

<!-- journey-file: src/minidist/protocols/raft/__init__.py -->
#### Raft 包边界

这个支撑导出让 Role、Entry、Group State 与驱动可用，不复制协议行为。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/04-raft-consensus/tests.txt)`。八条契约覆盖选举、读写保证、故障转移、重启所有权、分区恢复与确定性重放。

### 需要真正记住的内容

Leader 身份受 Term 限定。Stored 不等于 Committed，Committed 不等于 Applied；线性一致读写都需要当前权威证据。

### 用自己的话讲清楚

说明为什么被隔离旧 Leader 可以本地 Append，却不能成功返回 Quorum 确认，以及更高 Term Leader 如何在恢复连接后修复该后缀。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/06-raft-election.md) · [第 7 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/07-raft-replication.md)
