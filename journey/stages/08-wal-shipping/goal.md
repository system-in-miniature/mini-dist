# Stage 08 · WAL shipping and synchronous flush / WAL Shipping 与同步刷盘

<!-- journey: chapter=9 tests_added=8 -->

## English

### Goal

Build a WAL-shipping group where local WAL durability, configured synchronous-standby flush, retained catch-up windows, and promotion timelines are independently observable.

### Deliverable files

- `src/minidist/protocols/wal_shipping/__init__.py`
- `src/minidist/protocols/wal_shipping/group.py`
- `tests/protocols/test_wal_shipping.py`

### The problem at this point

Async primary replicates logical key/value entries but cannot express a database-style byte-log position or configured remote flush policy. Raft couples replication to voting; WAL shipping needs a different authority model where selected standbys may gate commit without becoming consensus voters.

### Test contract

#### See the failure first

The synchronous-standby contract configures two synchronous and one asynchronous standby, writes with `AckLevel.QUORUM`, and requires both configured standbys plus the primary to have flushed the returned LSN. Counting any arbitrary majority would implement Raft-like semantics instead of the declared WAL policy.

<!-- journey-file: tests/protocols/test_wal_shipping.py -->
#### WAL durability and timeline contract

##### What this test locks

Eight contracts lock ordered WAL bytes, local flush before acknowledgement, every configured synchronous standby, timeout on an unreachable synchronous standby, timeline fencing, retained incremental/full catch-up, fsync loss, and deterministic replay.

##### How it constructs the counterexample

The suite varies the configured synchronous set independently of cluster size, keeps old-timeline records queued across promotion, and disconnects standbys inside and outside the retention window.

##### Key test statement

```python
assert all(state.nodes[node].flushed_lsn >= result.offset for node in ("sync-1", "sync-2"))
assert state.nodes["primary"].durable_lsn >= result.offset
```

##### What a failure means

The acknowledgement no longer names its real flush boundary, or promotion/catch-up accepted WAL from the wrong history.

### Basic concepts

A WAL record is an ordered byte payload identified by `(timeline, LSN)`. `applied_lsn`, `flushed_lsn`, and `durable_lsn` describe different progress. `LEADER` waits for local WAL durability; this project's `QUORUM` waits for every explicitly configured synchronous standby, not a numeric majority. Retention determines incremental replay versus full copy. Promotion increments the timeline so queued old-history WAL can be fenced.

### Why this mechanism is necessary

Database replication tradeoffs depend on what was flushed where, not merely whether a replica received a value. Separating local durability from remote wait policy makes synchronous replication configurable without pretending it is consensus.

### Runtime mental model

A write encodes one logical record, appends and applies it on the primary, fsyncs locally, schedules delivery, and optionally ticks until all configured synchronous standbys report flush. Restart uses retained WAL when continuity is provable; otherwise it copies full state. Promotion increments timeline before resynchronizing peers.

### Mechanism blocks

<!-- journey-file: src/minidist/protocols/wal_shipping/group.py -->
#### WAL positions, synchronous flush, retention, and timelines

##### What it is and why it appears

This group owns primary/standby WAL positions, logical byte encoding, local fsync, configured remote acknowledgements, catch-up, timeline promotion, and stale-WAL rejection.

##### Runtime role

It always flushes primary WAL before returning; only `QUORUM` enters the wait loop over the named synchronous set.

##### Key code

```python
required = set(self._synchronous)
for _ in range(self._ack_timeout):
    if required <= self._flush_acks[lsn]:
        break
    self.tick()
```

##### Statement understanding

The subset check means every configured synchronous standby has acknowledged this LSN. It intentionally does not derive a voter majority from node count.

<!-- journey-file: src/minidist/protocols/wal_shipping/__init__.py -->
#### WAL-shipping package boundary

This supporting export exposes record, sync mode, state probes, and group without creating a second WAL owner.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/08-wal-shipping/tests.txt)`. Eight contracts prove acknowledgement, position, catch-up, fencing, durability-loss, and replay boundaries.

### Durable takeaways

Timeline plus LSN names WAL history. Local flush and remote waiting are separate decisions; configured synchronous standbys are not consensus voters.

### Explain it in your own words

Explain why this stage's `QUORUM` name does not mean “any majority,” and identify exactly what has happened before it returns successfully.

### Textbook

[Chapter 9](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/09-wal-shipping.md)

## 中文

### 目标

建立一个可独立观察本地 WAL 持久、配置的同步 Standby 刷盘、保留追赶窗口与 Promotion Timeline 的 WAL Shipping Group。

### 交付文件

- `src/minidist/protocols/wal_shipping/__init__.py`
- `src/minidist/protocols/wal_shipping/group.py`
- `tests/protocols/test_wal_shipping.py`

### 当前遇到的问题

异步主从复制逻辑 Key/Value Entry，却无法表达数据库式字节日志位置或配置化远端刷盘策略。Raft 把复制与投票绑定；WAL Shipping 需要另一种权威模型：选定 Standby 可以阻塞 Commit，但不会因此成为共识 Voter。

### 测试契约

#### 先看会坏在哪里

同步 Standby 契约配置两个同步和一个异步 Standby，使用 `AckLevel.QUORUM` 写入，并要求两个已配置 Standby 与 Primary 都已刷过返回 LSN。若计算任意数值多数派，就会实现成 Raft 风格语义，而不是声明的 WAL 策略。

<!-- journey-file: tests/protocols/test_wal_shipping.py -->
#### WAL 持久性与 Timeline 契约

##### 测试锁定什么

八条契约锁定有序 WAL 字节、确认前本地 Flush、所有配置同步 Standby、同步 Standby 不可达时 Timeout、Timeline Fencing、保留窗口内外追赶、Fsync Loss 与确定性重放。

##### 如何构造反例

套件独立于集群规模改变同步集合，让旧 Timeline Record 跨 Promotion 保持排队，并让 Standby 在 Retention Window 内外断开。

##### 关键测试语句

```python
assert all(state.nodes[node].flushed_lsn >= result.offset for node in ("sync-1", "sync-2"))
assert state.nodes["primary"].durable_lsn >= result.offset
```

##### 失败意味着什么

确认不再命名真实 Flush Boundary，或者 Promotion/Catch-up 接受了错误历史的 WAL。

### 基本概念

WAL Record 是由 `(timeline, LSN)` 标识的有序字节 Payload。`applied_lsn`、`flushed_lsn` 与 `durable_lsn` 表示不同进度。`LEADER` 等待本地 WAL 持久；本项目的 `QUORUM` 等待每个显式配置的同步 Standby，而不是数值多数。Retention 决定增量重放或全量复制。Promotion 增加 Timeline，使排队旧历史 WAL 可被 Fencing。

### 为什么需要这个机制

数据库复制取舍取决于什么在何处 Flush，而不只是 Replica 是否收到值。分离本地持久与远端等待策略，让同步复制可配置，又不伪装成共识。

### 运行时心智模型

一次写入编码逻辑 Record，在 Primary Append/Apply、本地 Fsync、安排投递，并可选地 Tick 到所有配置同步 Standby 报告 Flush。Restart 在可证明连续时使用 Retained WAL，否则复制全量状态。Promotion 先增加 Timeline，再同步 Peer。

### 机制板块

<!-- journey-file: src/minidist/protocols/wal_shipping/group.py -->
#### WAL 位置、同步刷盘、保留窗口与 Timeline

##### 是什么，为什么现在需要

该 Group 拥有 Primary/Standby WAL Position、逻辑字节编码、本地 Fsync、配置远端确认、Catch-up、Timeline Promotion 与旧 WAL 拒绝。

##### 在运行时做什么

它总在返回前 Flush Primary WAL；只有 `QUORUM` 进入针对命名同步集合的等待循环。

##### 关键代码

```python
required = set(self._synchronous)
for _ in range(self._ack_timeout):
    if required <= self._flush_acks[lsn]:
        break
    self.tick()
```

##### 关键语句理解

子集检查表示每个已配置同步 Standby 都确认了该 LSN。它刻意不从节点数推导 Voter Majority。

<!-- journey-file: src/minidist/protocols/wal_shipping/__init__.py -->
#### WAL Shipping 包边界

这个支撑导出公开 Record、Sync Mode、State Probe 与 Group，不创建第二个 WAL 所有者。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/08-wal-shipping/tests.txt)`。八条契约证明确认、位置、追赶、Fencing、持久丢失与重放边界。

### 需要真正记住的内容

Timeline 加 LSN 命名 WAL 历史。本地 Flush 与远端等待是不同决策；配置同步 Standby 不是共识 Voter。

### 用自己的话讲清楚

说明本阶段 `QUORUM` 为什么不表示“任意多数派”，并指出它成功返回以前究竟发生了什么。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/09-wal-shipping.md)
