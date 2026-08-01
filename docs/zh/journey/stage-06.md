# Stage 06 · 提升谱系 Fencing

### 目标

阻止过期 Primary Lineage 的延迟复制污染 Promotion 后历史，并让异步 Split-brain 行为可以与 Raft 直接比较。

??? note "交付文件"
    - `labs/exp03_partition_old_leader.py`
    - `src/minidist/protocols/async_primary/group.py`
    - `tests/labs/test_experiments.py`
    - `tests/protocols/test_async_primary.py`

### 当前遇到的问题

Stage 03 可以提升 Replica，但旧 Primary 已排队的消息仍会稍后到达。只检查 Offset 不安全，因为两条 Lineage 都可能拥有 Offset 1。Receiver 必须在消息到达时确认 Sender 是否仍拥有当前权威。

### 测试契约

#### 先看会坏在哪里

一条契约先让旧 Primary 排队 Full Sync，在投递前提升另一 Replica，再排空网络。该快照必须以 `non_current_primary` 被拒绝，否则延迟旧历史可以覆盖刚建立的 Lineage。

??? note "文件差异：tests/labs/test_experiments.py"
    ```diff
    diff --git a/tests/labs/test_experiments.py b/tests/labs/test_experiments.py
    index eaedcc8ed27ab20a386554ce4428a061576cf763..c65cd9cb45f36eb7b93cbcf00483b01c3ed52c7e 100644
    --- a/tests/labs/test_experiments.py
    +++ b/tests/labs/test_experiments.py
    @@ -34,7 +34,7 @@ def test_experiment_2_raft_acknowledged_write_survives_failover() -> None:


     def test_experiment_3_old_leader_is_fenced_and_cluster_converges() -> None:
    -    result = run_partition(verbose=False)
    +    result = run_partition(protocol="raft", verbose=False)
         assert not result.old_leader_write_committed
         assert result.new_leader_write_committed
         assert result.new_leader != result.old_leader
    @@ -42,3 +42,16 @@ def test_experiment_3_old_leader_is_fenced_and_cluster_converges() -> None:
         assert result.logs_converged
         assert result.majority_value == b"continues"
         assert result.minority_value is None
    +
    +
    +def test_experiment_3_async_primary_exposes_split_brain_dirty_writes() -> None:
    +    result = run_partition(protocol="async", verbose=False)
    +    assert result.old_primary_write_accepted
    +    assert result.new_primary_write_accepted
    +    assert result.old_primary_value_during_partition == b"old-primary"
    +    assert result.old_primary_new_value_during_partition is None
    +    assert result.new_primary_value_during_partition == b"new-primary"
    +    assert result.new_primary_old_value_during_partition is None
    +    assert result.converged_after_heal
    +    assert result.old_dirty_value_after_heal is None
    +    assert result.new_dirty_value_after_heal == b"new-primary"
    ```

**测试锁定什么**

Lab 必须展示分区时异步两侧都接受写入，而 Heal 后选择已提升 Lineage 并移除旧 Dirty Value。

**如何构造反例**

它隔离旧 Primary，在两侧写入不同 Key，提升多数侧 Replica，恢复网络，并观察 Heal 前后的值。

**关键测试语句**

```python
assert result.old_primary_write_accepted
assert result.new_primary_write_accepted
```

**失败意味着什么**

实验隐藏了 Split-brain 接受事实，或者没有展示 Heal 后哪条 Lineage 获胜。

??? note "文件差异：tests/protocols/test_async_primary.py"
    ```diff
    diff --git a/tests/protocols/test_async_primary.py b/tests/protocols/test_async_primary.py
    index d3fbff1a2e094e4374049c2618d582d4b014814d..07dbe73933b2cab132f4835f47c57c8939e62204 100644
    --- a/tests/protocols/test_async_primary.py
    +++ b/tests/protocols/test_async_primary.py
    @@ -73,3 +73,103 @@ def test_manual_promotion_can_lose_acknowledged_write() -> None:
         assert result.accepted
         assert group.client_read(b"paid", ReadLevel.LEADER).value is None
         assert group.probe().primary == "r1"
    +
    +
    +def test_queued_old_primary_full_sync_is_rejected_after_promotion() -> None:
    +    group = AsyncPrimaryGroup(
    +        replica_ids=("r1", "r2"),
    +        backlog_size=1,
    +        replication_delay=2,
    +    )
    +    group.crash("r2")
    +    group.client_write(b"first", b"old-history", AckLevel.LEADER)
    +    group.client_write(b"second", b"old-history", AckLevel.LEADER)
    +    group.run_until_idle()
    +
    +    group.restart("r2")
    +    group.promote("r1")
    +    group.run_until_idle()
    +
    +    rejected = [
    +        event
    +        for event in group.trace.events
    +        if event.kind == "replication_message_rejected"
    +        and event.details["message_type"] == "full_sync"
    +        and event.details["source_primary"] == "primary"
    +    ]
    +    assert rejected
    +    assert rejected[0].details["reason"] == "non_current_primary"
    +    state = group.probe()
    +    assert state.nodes["r2"].data == state.nodes["r1"].data
    +    assert state.nodes["r2"].replication_id == state.nodes["r1"].replication_id
    +
    +
    +def test_promotion_converges_alive_replicas_without_a_new_write() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1", "r2"), replication_delay=1)
    +    group.isolate("r2")
    +    group.client_write(b"before-promotion", b"present", AckLevel.LEADER)
    +    group.run_until_idle()
    +    group.heal("r2")
    +
    +    group.promote("r1")
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert state.nodes["r2"].data == state.nodes["r1"].data
    +    assert state.nodes["r2"].offset == state.nodes["r1"].offset
    +    assert state.nodes["r2"].replication_id == state.nodes["r1"].replication_id
    +
    +
    +def test_queued_old_primary_backlog_entry_is_rejected_after_promotion() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    +    group.client_write(b"old-primary", b"dirty", AckLevel.LEADER)
    +
    +    group.promote("r1")
    +    group.run_until_idle()
    +
    +    rejected = [
    +        event
    +        for event in group.trace.events
    +        if event.kind == "replication_message_rejected"
    +        and event.details["message_type"] == "entry"
    +    ]
    +    assert rejected
    +    assert rejected[0].details["reason"] == "non_current_primary"
    +
    +
    +@pytest.mark.parametrize(
    +    ("generation", "replication_id", "reason"),
    +    [
    +        (0, "repl-1", "stale_generation"),
    +        (1, "obsolete-lineage", "stale_replication_id"),
    +    ],
    +)
    +def test_current_primary_delivery_requires_current_generation_and_replication_id(
    +    generation: int,
    +    replication_id: str,
    +    reason: str,
    +) -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=1)
    +    group.network.send(
    +        "primary",
    +        "r1",
    +        {
    +            "type": "entry",
    +            "mode": "stream",
    +            "source_primary": "primary",
    +            "generation": generation,
    +            "replication_id": replication_id,
    +            "offset": 1,
    +            "key": b"fenced",
    +            "value": b"dirty",
    +        },
    +    )
    +
    +    group.run_until_idle()
    +
    +    assert group.probe().nodes["r1"].data == {}
    +    assert any(
    +        event.kind == "replication_message_rejected"
    +        and event.details["reason"] == reason
    +        for event in group.trace.events
    +    )
    ```

**测试锁定什么**

四个聚焦场景锁定按当前 Primary Identity、Generation 与 Replication ID 拒绝，并锁定 Promotion 后立即主动收敛存活 Replica。

**如何构造反例**

测试刻意让 Stream Entry 或 Full Snapshot 保持在途，改变权威，再让消息到达 Receiver。

**关键测试语句**

```python
assert rejected
assert rejected[0].details["reason"] == "non_current_primary"
```

**失败意味着什么**

Receiver 在没有证明 Payload 属于当前权威 Lineage 时，只凭看似合理的 Offset 或内容就接受它。

### 基本概念

Promotion 创建新的权威 Generation。Replication ID 命名数据 Lineage；Offset 只在其中有意义。Fencing 必须发生在接收时，因为消息发送时可能有效、投递时已经过期。主动收敛表示 Promotion 立即同步可达 Replica，而不是等待下一次客户端写入。

### 为什么需要这个机制

分布式系统会重排权威变更与数据消息。没有 Lineage Fencing，延迟 Packet 可以在故障转移看似完成后复活旧 Primary 状态。

### 运行时心智模型

Promotion 增加 Generation、分配新 Replication Identity、标记新 Primary，并同步存活 Replica。每个到达的 Entry 或 Full Sync 在任何状态变更前检查当前 Sender Identity、Generation 与 Lineage；被拒绝消息形成 Trace 证据。

### 机制板块

#### 提升谱系与接收时 Fencing

让 Promotion 开启新 Generation，并拒绝 Sender、Generation 或复制 Lineage 已过期的排队消息。

??? note "文件差异：src/minidist/protocols/async_primary/group.py"
    ```diff
    diff --git a/src/minidist/protocols/async_primary/group.py b/src/minidist/protocols/async_primary/group.py
    index 2c0b24863d4d7b248357bd35567645d42129b382..2e69a2d37569df295c0f82500ed0515fced1322f 100644
    --- a/src/minidist/protocols/async_primary/group.py
    +++ b/src/minidist/protocols/async_primary/group.py
    @@ -41,6 +41,7 @@ class SyncMode(Enum):

     @dataclass(frozen=True, slots=True)
     class _Entry:
    +    generation: int
         replication_id: str
         offset: int
         key: bytes
    @@ -153,6 +154,7 @@ class AsyncPrimaryGroup:
             primary.offset += 1
             primary.data[key] = value
             entry = _Entry(
    +            generation=self._generation,
                 replication_id=primary.replication_id,
                 offset=primary.offset,
                 key=key,
    @@ -260,6 +262,28 @@ class AsyncPrimaryGroup:
             if node != self._primary:
                 self._synchronize(node)

    +    def isolate(self, node: NodeId) -> None:
    +        """Bidirectionally partition one alive node from every peer."""
    +
    +        target = self._node(node)
    +        self._require_alive(target)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "protocol_node_isolated", node=node)
    +
    +    def heal(self, node: NodeId) -> None:
    +        """Heal one node's links and resynchronize it when it is a replica."""
    +
    +        target = self._node(node)
    +        self._require_alive(target)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.heal(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "protocol_node_healed", node=node)
    +        if node != self._primary:
    +            self._synchronize(node)
    +
         def promote(self, node: NodeId) -> None:
             """Manually make an alive replica the new primary."""

    @@ -283,6 +307,9 @@ class AsyncPrimaryGroup:
                 replication_id=candidate.replication_id,
                 offset=candidate.offset,
             )
    +        for replica_id, replica in self._nodes.items():
    +            if replica_id != self._primary and replica.alive:
    +                self._synchronize(replica_id)

         def probe(self) -> GroupState:
             """Return value snapshots for assertions without exposing mutability."""
    @@ -336,6 +363,8 @@ class AsyncPrimaryGroup:
             replica.sync_mode = SyncMode.FULL
             payload = {
                 "type": "full_sync",
    +            "source_primary": self._primary,
    +            "generation": self._generation,
                 "replication_id": primary.replication_id,
                 "offset": primary.offset,
                 "data": dict(primary.data),
    @@ -358,6 +387,8 @@ class AsyncPrimaryGroup:
                 {
                     "type": "entry",
                     "mode": mode,
    +                "source_primary": source,
    +                "generation": entry.generation,
                     "replication_id": entry.replication_id,
                     "offset": entry.offset,
                     "key": entry.key,
    @@ -379,6 +410,19 @@ class AsyncPrimaryGroup:
             payload = message.payload
             if not isinstance(payload, dict):
                 raise TypeError("protocol payload must be a dict")
    +        rejection_reason = self._replication_rejection_reason(message, payload)
    +        if rejection_reason is not None:
    +            self.trace.record(
    +                self.clock.now,
    +                "replication_message_rejected",
    +                message_id=message.message_id,
    +                message_type=payload.get("type"),
    +                source_primary=payload.get("source_primary"),
    +                generation=payload.get("generation"),
    +                replication_id=payload.get("replication_id"),
    +                reason=rejection_reason,
    +            )
    +            return
             if payload["type"] == "full_sync":
                 self._apply_full_sync(target, payload)
             elif payload["type"] == "entry":
    @@ -386,6 +430,22 @@ class AsyncPrimaryGroup:
             else:
                 raise ValueError(f"unknown replication message: {payload['type']}")

    +    def _replication_rejection_reason(
    +        self, message: Message, payload: dict[str, Any]
    +    ) -> str | None:
    +        primary = self._nodes[self._primary]
    +        if (
    +            message.source != self._primary
    +            or payload.get("source_primary") != self._primary
    +            or payload.get("source_primary") != message.source
    +        ):
    +            return "non_current_primary"
    +        if payload.get("generation") != self._generation:
    +            return "stale_generation"
    +        if payload.get("replication_id") != primary.replication_id:
    +            return "stale_replication_id"
    +        return None
    +
         def _apply_entry(self, target: _Node, payload: dict[str, Any]) -> None:
             primary = self._nodes[self._primary]
             if (
    ```

**是什么，为什么现在需要**

异步 Group 增加网络隔离/恢复，以及 Stream 与 Full-sync 投递共享的单一拒绝分类器。

**在运行时做什么**

分类器在接收时、`_apply_entry` 或 `_apply_full_sync` 以前运行，并为每条旧权威消息记录稳定原因。

**关键代码**

```python
if payload.get("generation") != self._generation:
    return "stale_generation"
if payload.get("replication_id") != primary.replication_id:
    return "stale_replication_id"
```

**关键语句理解**

Generation 隔离权威纪元；Replication ID 隔离该纪元内的历史。两者都需要，因为相同 Offset 不代表同一历史。

#### 异步 Split-brain 实验

扩展实验 3，让异步主从与 Raft 回答同一个少数侧权威问题。

??? note "文件差异：labs/exp03_partition_old_leader.py"
    ```diff
    diff --git a/labs/exp03_partition_old_leader.py b/labs/exp03_partition_old_leader.py
    index e8f871cf0ac204720d04afbf65197dc94902bbeb..7901707564ccc87c275d87b120c9019554538a9c 100644
    --- a/labs/exp03_partition_old_leader.py
    +++ b/labs/exp03_partition_old_leader.py
    @@ -1,18 +1,28 @@
    -"""Experiment 3: isolate a Raft leader, replace it, then heal its stale log.
    -
    -Only public ``RaftGroup`` operations are used.  The old leader remains leader
    -in its own obsolete term while isolated, but cannot turn a local append into a
    -QUORUM acknowledgement.  The majority elects a higher-term leader and keeps
    -serving.  On healing, the higher term fences the old leader and AppendEntries'
    -prefix check replaces its conflicting suffix.
    -"""
    +"""Experiment 3: contrast async split brain with Raft term fencing."""

    +import argparse
     from dataclasses import dataclass

    -from minidist.protocols import AckLevel
    +from minidist.protocols import AckLevel, ReadLevel
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
     from minidist.protocols.raft import RaftGroup


    +@dataclass(frozen=True, slots=True)
    +class AsyncExperimentResult:
    +    old_primary: str
    +    new_primary: str
    +    old_primary_write_accepted: bool
    +    new_primary_write_accepted: bool
    +    old_primary_value_during_partition: bytes | None
    +    old_primary_new_value_during_partition: bytes | None
    +    new_primary_value_during_partition: bytes | None
    +    new_primary_old_value_during_partition: bytes | None
    +    converged_after_heal: bool
    +    old_dirty_value_after_heal: bytes | None
    +    new_dirty_value_after_heal: bytes | None
    +
    +
     @dataclass(frozen=True, slots=True)
     class ExperimentResult:
         old_leader: str
    @@ -27,7 +37,111 @@ class ExperimentResult:
         majority_value: bytes | None


    -def run_experiment(*, verbose: bool = True) -> ExperimentResult:
    +def run_experiment(
    +    *,
    +    protocol: str = "raft",
    +    verbose: bool = True,
    +) -> AsyncExperimentResult | ExperimentResult:
    +    """Run one partition protocol using only its public group API."""
    +
    +    if protocol == "async":
    +        return _run_async_experiment(verbose=verbose)
    +    if protocol != "raft":
    +        raise ValueError("protocol must be 'async' or 'raft'")
    +    return _run_raft_experiment(verbose=verbose)
    +
    +
    +def _run_async_experiment(*, verbose: bool) -> AsyncExperimentResult:
    +    group = AsyncPrimaryGroup(
    +        replica_ids=("replica-1", "replica-2"),
    +        replication_delay=1,
    +        seed=303,
    +    )
    +    old_primary = group.primary
    +    group.isolate(old_primary)
    +
    +    old_write = group.client_write(
    +        b"old-primary-dirty",
    +        b"old-primary",
    +        AckLevel.LEADER,
    +    )
    +    group.promote("replica-1")
    +    new_primary = group.primary
    +    new_write = group.client_write(
    +        b"new-primary-dirty",
    +        b"new-primary",
    +        AckLevel.LEADER,
    +    )
    +    group.run_until_idle()
    +
    +    old_old_value = group.client_read(
    +        b"old-primary-dirty",
    +        ReadLevel.LOCAL,
    +        node=old_primary,
    +    ).value
    +    old_new_value = group.client_read(
    +        b"new-primary-dirty",
    +        ReadLevel.LOCAL,
    +        node=old_primary,
    +    ).value
    +    new_new_value = group.client_read(
    +        b"new-primary-dirty",
    +        ReadLevel.LEADER,
    +    ).value
    +    new_old_value = group.client_read(
    +        b"old-primary-dirty",
    +        ReadLevel.LEADER,
    +    ).value
    +
    +    group.heal(old_primary)
    +    group.run_until_idle()
    +    state = group.probe()
    +    reference = state.nodes[new_primary]
    +    result = AsyncExperimentResult(
    +        old_primary=old_primary,
    +        new_primary=new_primary,
    +        old_primary_write_accepted=old_write.accepted,
    +        new_primary_write_accepted=new_write.accepted,
    +        old_primary_value_during_partition=old_old_value,
    +        old_primary_new_value_during_partition=old_new_value,
    +        new_primary_value_during_partition=new_new_value,
    +        new_primary_old_value_during_partition=new_old_value,
    +        converged_after_heal=all(
    +            node.data == reference.data
    +            and node.offset == reference.offset
    +            and node.replication_id == reference.replication_id
    +            for node in state.nodes.values()
    +        ),
    +        old_dirty_value_after_heal=reference.data.get(b"old-primary-dirty"),
    +        new_dirty_value_after_heal=reference.data.get(b"new-primary-dirty"),
    +    )
    +
    +    if verbose:
    +        print("实验 3：异步主从的双主脏写")
    +        print(
    +            f"1) 隔离旧 primary {old_primary}；其本地写 ack="
    +            f"{result.old_primary_write_accepted}。"
    +        )
    +        print(
    +            f"2) 显式 promote {new_primary}；新 primary 本地写 ack="
    +            f"{result.new_primary_write_accepted}。"
    +        )
    +        print(
    +            "3) 分区中两侧状态："
    +            f"旧侧 old={old_old_value!r}, new={old_new_value!r}；"
    +            f"新侧 old={new_old_value!r}, new={new_new_value!r}。"
    +        )
    +        print(
    +            "4) 愈合后按新 primary 世代全量收敛："
    +            f"converged={result.converged_after_heal}, "
    +            f"old={result.old_dirty_value_after_heal!r}, "
    +            f"new={result.new_dirty_value_after_heal!r}。"
    +        )
    +        print("结论：协议 1 没有任期 fencing；分区中两侧都可确认本地脏写。")
    +    return result
    +
    +
    +def _run_raft_experiment(*, verbose: bool) -> ExperimentResult:
         """Run the minority-old-leader partition and return assertion-friendly facts."""

         group = RaftGroup(
    @@ -94,4 +208,15 @@ def run_experiment(*, verbose: bool = True) -> ExperimentResult:


     if __name__ == "__main__":
    -    run_experiment()
    +    parser = argparse.ArgumentParser()
    +    parser.add_argument(
    +        "--protocol",
    +        choices=("async", "raft", "both"),
    +        default="both",
    +    )
    +    args = parser.parse_args()
    +    protocols = ("async", "raft") if args.protocol == "both" else (args.protocol,)
    +    for index, selected_protocol in enumerate(protocols):
    +        if index:
    +            print()
    +        run_experiment(protocol=selected_protocol)
    ```

**是什么，为什么现在需要**

实验 3 现在可选择 `async` 或 `raft`，并从同一个公开场景返回各协议事实。

**在运行时做什么**

异步分支观察隔离期间的分叉写入，以及 Promotion/Heal 后收敛；Raft 分支保留多数派权威证据。

**关键代码**

```python
if protocol == "async":
    return _run_async_experiment(verbose=verbose)
if protocol != "raft":
    raise ValueError("protocol must be 'async' or 'raft'")
```

**关键语句理解**

Protocol Selection 改变实现，不改变比较问题。未知值显式失败，防止矩阵静默漏掉一列。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/06-lineage-fencing/tests.txt)`。聚焦契约与 Lab 覆盖过期 Full Sync、过期 Stream Entry、主动收敛、拒绝原因与可见 Split Brain。

### 需要真正记住的内容

Offset 需要 Lineage 上下文；数据到达时校验权威；Heal 必须在不等待无关流量时完成收敛。

### 用自己的话讲清楚

说明一条消息为何发送时有效、投递时却不安全，以及 Sender Identity、Generation 与 Replication ID 分别回答什么问题。

### 教材

[第 5 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/05-failover-fencing.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-05...stage-06)

完成后可运行 `git checkout stage-06` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/06-lineage-fencing/stage.patch)
