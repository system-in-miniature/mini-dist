# Stage 04 · Raft 权威与已提交复制

### 目标

建立一个三节点 Raft Group：用带 Seed 的选举建立权威，用日志前缀修复收敛副本，并让只有 Quorum 提交的写入在故障转移后继续存在。

??? note "交付文件"
    - `src/minidist/protocols/raft/__init__.py`
    - `src/minidist/protocols/raft/group.py`
    - `tests/protocols/test_raft.py`

### 当前遇到的问题

异步复制暴露了真实的确认丢失窗口，却无法证明被提升节点代表多数历史。现在需要由同一协议把权威与数据存活联系起来，而不是让运维者任选一个副本。

### 测试契约

#### 先看会坏在哪里

分区契约隔离旧 Leader，向它请求 Quorum 写入，让多数侧选出新 Leader，最后修复网络。少数侧写入必须保持未提交并在日志修复中消失。若把本地 Append 当成成功，就会产生两份都声称已接受的历史。

??? note "文件差异：tests/protocols/test_raft.py"
    ```diff
    diff --git a/tests/protocols/test_raft.py b/tests/protocols/test_raft.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..56957b59ea2752d541d167fed3f09173f950e7ee
    --- /dev/null
    +++ b/tests/protocols/test_raft.py
    @@ -0,0 +1,141 @@
    +from __future__ import annotations
    +
    +import pytest
    +
    +from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
    +from minidist.protocols.raft import RaftGroup, Role
    +
    +
    +def elected_group(*, seed: int = 7) -> RaftGroup:
    +    group = RaftGroup(
    +        node_ids=("n1", "n2", "n3"),
    +        seed=seed,
    +        election_timeout_range=(4, 7),
    +        heartbeat_interval=1,
    +    )
    +    group.run_until_leader()
    +    return group
    +
    +
    +def test_seeded_timeouts_elect_exactly_one_leader() -> None:
    +    group = elected_group(seed=11)
    +
    +    state = group.probe()
    +    leaders = [node for node in state.nodes.values() if node.role is Role.LEADER]
    +    assert len(leaders) == 1
    +    assert state.leader == leaders[0].node_id
    +    assert leaders[0].current_term >= 1
    +
    +
    +@pytest.mark.parametrize(
    +    "level",
    +    [AckLevel.NONE, AckLevel.LEADER, AckLevel.ALL_ISR],
    +)
    +def test_raft_rejects_non_quorum_ack_levels(level: AckLevel) -> None:
    +    group = elected_group()
    +
    +    with pytest.raises(UnsupportedLevelError, match=level.name):
    +        group.client_write(b"k", b"v", level)
    +
    +
    +def test_quorum_write_commits_and_linearizable_read_uses_barrier() -> None:
    +    group = elected_group()
    +
    +    result = group.client_write(b"course", b"MiniDist", AckLevel.QUORUM)
    +
    +    assert result.accepted
    +    assert group.client_read(b"course", ReadLevel.LEADER).value == b"MiniDist"
    +    assert (
    +        group.client_read(b"course", ReadLevel.LINEARIZABLE).value == b"MiniDist"
    +    )
    +    state = group.probe()
    +    assert sum(node.commit_index >= result.offset for node in state.nodes.values()) >= 2
    +
    +
    +def test_acknowledged_write_survives_leader_crash_and_failover() -> None:
    +    group = elected_group(seed=19)
    +    old_leader = group.probe().leader
    +    result = group.client_write(b"order:42", b"confirmed", AckLevel.QUORUM)
    +
    +    group.crash(old_leader)
    +    new_leader = group.run_until_leader(exclude=old_leader)
    +
    +    assert result.accepted
    +    assert new_leader != old_leader
    +    assert (
    +        group.client_read(b"order:42", ReadLevel.LINEARIZABLE).value
    +        == b"confirmed"
    +    )
    +
    +
    +def test_restart_keeps_term_vote_and_log_but_rebuilds_volatile_state() -> None:
    +    group = elected_group(seed=23)
    +    follower = next(
    +        node_id for node_id in group.probe().nodes if node_id != group.probe().leader
    +    )
    +    group.client_write(b"durable", b"entry", AckLevel.QUORUM)
    +    before = group.probe().nodes[follower]
    +
    +    group.crash(follower)
    +    group.restart(follower)
    +    after = group.probe().nodes[follower]
    +
    +    assert after.current_term == before.current_term
    +    assert after.voted_for == before.voted_for
    +    assert after.log == before.log
    +    assert after.role is Role.FOLLOWER
    +    assert after.commit_index == 0
    +
    +
    +def test_partitioned_old_leader_cannot_commit_and_logs_converge_after_heal() -> None:
    +    group = elected_group(seed=29)
    +    old_leader = group.probe().leader
    +    group.isolate(old_leader)
    +
    +    rejected = group.client_write(
    +        b"minority-only",
    +        b"must-not-commit",
    +        AckLevel.QUORUM,
    +    )
    +    new_leader = group.run_until_leader(exclude=old_leader)
    +    committed = group.client_write(b"majority", b"continues", AckLevel.QUORUM)
    +
    +    assert not rejected.accepted
    +    assert committed.accepted
    +    assert new_leader != old_leader
    +
    +    group.heal(old_leader)
    +    group.run_until_converged()
    +    state = group.probe()
    +
    +    assert state.nodes[old_leader].role is Role.FOLLOWER
    +    assert state.nodes[old_leader].current_term == state.nodes[new_leader].current_term
    +    assert all(node.log == state.nodes[new_leader].log for node in state.nodes.values())
    +    assert all(node.data.get(b"majority") == b"continues" for node in state.nodes.values())
    +    assert all(b"minority-only" not in node.data for node in state.nodes.values())
    +
    +
    +def test_linearizable_read_fails_without_a_current_term_quorum() -> None:
    +    group = elected_group(seed=31)
    +    old_leader = group.probe().leader
    +    group.isolate(old_leader)
    +
    +    with pytest.raises(RuntimeError, match="read-index quorum"):
    +        group.client_read(b"k", ReadLevel.LINEARIZABLE)
    +
    +
    +def _election_partition_heal_trace(seed: int) -> list[dict[str, object]]:
    +    group = elected_group(seed=seed)
    +    old_leader = group.probe().leader
    +    group.client_write(b"before", b"partition", AckLevel.QUORUM)
    +    group.isolate(old_leader)
    +    group.run_until_leader(exclude=old_leader)
    +    group.client_write(b"during", b"majority", AckLevel.QUORUM)
    +    group.heal(old_leader)
    +    group.run_until_converged()
    +    return group.trace.as_dicts()
    +
    +
    +def test_same_seed_replays_election_and_partition_heal_exactly() -> None:
    +    assert _election_partition_heal_trace(37) == _election_partition_heal_trace(37)
    +
    ```

**测试锁定什么**

锁定带 Seed 的单 Leader、仅 Quorum 写入、线性一致读屏障、Term/Vote/Log 持久状态、少数侧拒绝、高 Term 降级、日志收敛与 Trace 精确重放。

**如何构造反例**

测试把已选 Leader 与两个 Follower 隔开，让多数侧在更高 Term 选举，在两侧分别写入，再恢复连接并比较所有日志与已应用值。

**关键测试语句**

```python
assert not rejected.accepted
assert committed.accepted
```

**失败意味着什么**

Group 要么在没有多数证据时确认，要么允许过期权威提交，要么无法修复分叉前缀。

### 基本概念

Term 是权威的逻辑纪元。选举需要多数票与不落后的日志。`AppendEntries` 在扩展日志前证明共享前缀。复制与提交不同：Leader 只有在当前 Term Entry 存储于 Quorum 时才推进 `commit_index`；应用已提交 Entry 又是另一个单调 Cursor。线性一致读需要当前 Term 的 Quorum 证据，而不只是一个 Leader 标签。

### 为什么需要这个机制

仅仅更积极地复制无法得到故障转移安全。系统必须规定哪份历史具有权威，并证明一次确认已经落在足够多 Voter 上，可以承受一个节点失败。

### 运行时心智模型

带 Seed 的选举 Deadline 触发 Candidacy 与投票。赢家初始化每个 Follower 的 `next_index`/`match_index`，追加客户端命令并发送受前缀约束的 AppendEntries。响应推进复制位置；当前 Term 的多数证据推进 Commit 与 Apply。更高 Term 迫使旧 Leader 降级，冲突后缀随后被覆盖。

### 机制板块

#### 选举、日志匹配、提交与权威读取

把选举与复制视为同一权威协议：Term 选择 Leader，前缀证明修复日志，Quorum 证据推进 Commit。

??? note "文件差异：src/minidist/protocols/raft/group.py"
    ```diff
    diff --git a/src/minidist/protocols/raft/group.py b/src/minidist/protocols/raft/group.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..632998bf9d79d18a69c437808dfeafbed65e11ab
    --- /dev/null
    +++ b/src/minidist/protocols/raft/group.py
    @@ -0,0 +1,866 @@
    +"""Textbook Raft over MiniDist's deterministic simulation substrate.
    +
    +Each node persists ``currentTerm``, ``votedFor``, and its log before an RPC
    +response can expose the change.  Role, election deadline, commit/applied
    +indices, leader replication cursors, and the KV state machine are volatile and
    +are rebuilt after a crash.  RequestVote and AppendEntries travel through
    +``SimNet`` rather than calling peers directly, so partitions and delay affect
    +the protocol at the same boundary as the other MiniDist implementations.
    +
    +The module intentionally implements only static-membership, unbounded-log
    +Raft.  Snapshot installation, log compaction, and joint-consensus membership
    +changes are not implemented; they are separate teaching topics rather than
    +hidden approximations here.
    +"""
    +
    +from __future__ import annotations
    +
    +import random
    +from dataclasses import dataclass, field
    +from enum import Enum
    +from types import MappingProxyType
    +from typing import Any, Mapping
    +
    +from minidist.protocols.types import (
    +    AckLevel,
    +    NodeId,
    +    ReadLevel,
    +    ReadResult,
    +    UnsupportedLevelError,
    +    WriteResult,
    +)
    +from minidist.sim import Message, Scheduler, SimClock, SimNet, Trace
    +
    +
    +class Role(Enum):
    +    """A Raft server's volatile role in its current term."""
    +
    +    FOLLOWER = "follower"
    +    CANDIDATE = "candidate"
    +    LEADER = "leader"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class LogEntry:
    +    """One replicated state-machine command; ``key=None`` denotes a no-op."""
    +
    +    index: int
    +    term: int
    +    key: bytes | None
    +    value: bytes | None
    +
    +
    +_SENTINEL = LogEntry(index=0, term=0, key=None, value=None)
    +
    +
    +@dataclass(slots=True)
    +class _Node:
    +    node_id: NodeId
    +
    +    # Raft Figure 2 persistent state: these fields survive crash/restart.
    +    current_term: int = 0
    +    voted_for: NodeId | None = None
    +    log: list[LogEntry] = field(default_factory=lambda: [_SENTINEL])
    +
    +    # Everything below is volatile and rebuilt by _reset_volatile().
    +    alive: bool = True
    +    role: Role = Role.FOLLOWER
    +    commit_index: int = 0
    +    last_applied: int = 0
    +    data: dict[bytes, bytes] = field(default_factory=dict)
    +    election_deadline: int = 0
    +    heartbeat_due: int = 0
    +    votes: set[NodeId] = field(default_factory=set)
    +    next_index: dict[NodeId, int] = field(default_factory=dict)
    +    match_index: dict[NodeId, int] = field(default_factory=dict)
    +    read_acks: dict[int, set[NodeId]] = field(default_factory=dict)
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class NodeState:
    +    """Read-only state exposed to experiments and assertions."""
    +
    +    node_id: NodeId
    +    alive: bool
    +    role: Role
    +    current_term: int
    +    voted_for: NodeId | None
    +    log: tuple[LogEntry, ...]
    +    commit_index: int
    +    last_applied: int
    +    data: Mapping[bytes, bytes]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class GroupState:
    +    """Read-only snapshot of a Raft group."""
    +
    +    tick: int
    +    leader: NodeId
    +    nodes: Mapping[NodeId, NodeState]
    +
    +
    +class RaftGroup:
    +    """A deterministic three-or-more-node textbook Raft replication group."""
    +
    +    def __init__(
    +        self,
    +        *,
    +        node_ids: tuple[NodeId, ...] = ("node-1", "node-2", "node-3"),
    +        seed: int = 0,
    +        election_timeout_range: tuple[int, int] = (8, 14),
    +        heartbeat_interval: int = 2,
    +        network_delay: int = 1,
    +    ) -> None:
    +        if len(node_ids) < 3 or len(set(node_ids)) != len(node_ids):
    +            raise ValueError("Raft requires at least three unique node IDs")
    +        low, high = election_timeout_range
    +        if low <= heartbeat_interval or high < low:
    +            raise ValueError(
    +                "election timeout range must be ordered and exceed heartbeat interval"
    +            )
    +        if heartbeat_interval <= 0 or network_delay <= 0:
    +            raise ValueError("heartbeat interval and network delay must be positive")
    +
    +        self.clock = SimClock()
    +        self.trace = Trace()
    +        self.scheduler = Scheduler(self.clock, self.trace)
    +        self.network = SimNet(
    +            seed=seed,
    +            clock=self.clock,
    +            scheduler=self.scheduler,
    +            trace=self.trace,
    +            min_delay=network_delay,
    +            max_delay=network_delay,
    +        )
    +        # This is the only election randomness source.  It is private rather
    +        # than module-global so an injected seed fully determines all draws.
    +        self._rng = random.Random(seed)
    +        self._timeout_low = low
    +        self._timeout_high = high
    +        self._heartbeat_interval = heartbeat_interval
    +        self._node_ids = node_ids
    +        self._nodes = {node_id: _Node(node_id=node_id) for node_id in node_ids}
    +        self._next_read_context = 1
    +
    +        for node_id in node_ids:
    +            self.network.register(
    +                node_id,
    +                lambda message, target=node_id: self._receive(target, message),
    +            )
    +        for node in self._nodes.values():
    +            self._reset_election_deadline(node)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_group_created",
    +            nodes=list(node_ids),
    +            election_timeout_range=list(election_timeout_range),
    +            heartbeat_interval=heartbeat_interval,
    +            seed=seed,
    +        )
    +
    +    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult:
    +        """Append a command and return only after a majority commits it."""
    +
    +        if ack is not AckLevel.QUORUM:
    +            raise UnsupportedLevelError(f"Raft does not support AckLevel.{ack.name}")
    +        self._validate_bytes(key, "key")
    +        self._validate_bytes(value, "value")
    +        leader = self._leader()
    +        entry = LogEntry(
    +            index=len(leader.log),
    +            term=leader.current_term,
    +            key=key,
    +            value=value,
    +        )
    +        leader.log.append(entry)
    +        leader.match_index[leader.node_id] = entry.index
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_client_entry_appended",
    +            leader=leader.node_id,
    +            term=entry.term,
    +            index=entry.index,
    +            key=key,
    +            value=value,
    +        )
    +        self._broadcast_append(leader)
    +
    +        accepted = self._wait_for_commit(leader, entry)
    +        message = (
    +            "entry committed by a majority"
    +            if accepted
    +            else "entry was not committed by a majority"
    +        )
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_client_write_result",
    +            leader=leader.node_id,
    +            term=entry.term,
    +            index=entry.index,
    +            accepted=accepted,
    +        )
    +        return WriteResult(accepted=accepted, offset=entry.index, message=message)
    +
    +    def client_read(
    +        self,
    +        key: bytes,
    +        level: ReadLevel,
    +        node: NodeId | None = None,
    +    ) -> ReadResult:
    +        """Serve a leader read or first prove authority with a read-index round."""
    +
    +        self._validate_bytes(key, "key")
    +        if level is ReadLevel.LOCAL:
    +            raise UnsupportedLevelError("Raft does not support ReadLevel.LOCAL")
    +        if node is not None:
    +            raise ValueError("Raft leader reads do not accept an explicit node")
    +        leader = self._leader()
    +        if level is ReadLevel.LINEARIZABLE:
    +            context = self._next_read_context
    +            self._next_read_context += 1
    +            leader.read_acks[context] = {leader.node_id}
    +            self.trace.record(
    +                self.clock.now,
    +                "raft_read_index_started",
    +                leader=leader.node_id,
    +                term=leader.current_term,
    +                context=context,
    +            )
    +            self._broadcast_append(leader, read_context=context)
    +            for _ in range(self._timeout_high * 2):
    +                if len(leader.read_acks.get(context, set())) >= self._quorum:
    +                    break
    +                self.tick()
    +            else:
    +                leader.read_acks.pop(context, None)
    +                raise RuntimeError("read-index quorum was not reached")
    +            leader.read_acks.pop(context, None)
    +            self.trace.record(
    +                self.clock.now,
    +                "raft_read_index_completed",
    +                leader=leader.node_id,
    +                term=leader.current_term,
    +                context=context,
    +            )
    +        elif level is not ReadLevel.LEADER:
    +            raise UnsupportedLevelError(f"Raft does not support ReadLevel.{level.name}")
    +
    +        value = leader.data.get(key)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_client_read",
    +            leader=leader.node_id,
    +            level=level.name,
    +            key=key,
    +            value=value,
    +            commit_index=leader.commit_index,
    +        )
    +        return ReadResult(
    +            value=value,
    +            node=leader.node_id,
    +            offset=leader.commit_index,
    +        )
    +
    +    def tick(self) -> None:
    +        """Advance delivery, then run deterministic timer transitions."""
    +
    +        self.scheduler.tick()
    +        for node_id in self._node_ids:
    +            node = self._nodes[node_id]
    +            if not node.alive:
    +                continue
    +            if node.role is Role.LEADER:
    +                if self.clock.now >= node.heartbeat_due:
    +                    self._broadcast_append(node)
    +                    node.heartbeat_due = self.clock.now + self._heartbeat_interval
    +            elif self.clock.now >= node.election_deadline:
    +                self._start_election(node)
    +
    +    def run_until_leader(
    +        self,
    +        *,
    +        exclude: NodeId | None = None,
    +        max_ticks: int = 1_000,
    +    ) -> NodeId:
    +        """Drive timers until a leader other than ``exclude`` is elected."""
    +
    +        for _ in range(max_ticks + 1):
    +            leaders = [
    +                node
    +                for node in self._nodes.values()
    +                if node.alive
    +                and node.role is Role.LEADER
    +                and node.node_id != exclude
    +            ]
    +            if leaders:
    +                return max(leaders, key=lambda candidate: candidate.current_term).node_id
    +            self.tick()
    +        raise RuntimeError("Raft did not elect a leader")
    +
    +    def run_until_converged(self, *, max_ticks: int = 1_000) -> int:
    +        """Drive the group until all alive nodes share log and applied state."""
    +
    +        for ticks in range(max_ticks + 1):
    +            alive = [node for node in self._nodes.values() if node.alive]
    +            if alive and self._current_leader_or_none() is not None:
    +                first = alive[0]
    +                if all(
    +                    node.log == first.log
    +                    and node.data == first.data
    +                    and node.commit_index == first.commit_index
    +                    for node in alive[1:]
    +                ):
    +                    return ticks
    +            self.tick()
    +        raise RuntimeError("Raft group did not converge")
    +
    +    def crash(self, node: NodeId) -> None:
    +        """Crash a server, preserving only term, vote, and log."""
    +
    +        target = self._node(node)
    +        if not target.alive:
    +            raise ValueError(f"node already crashed: {node}")
    +        target.alive = False
    +        self._clear_volatile(target)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_node_crashed",
    +            node=node,
    +            current_term=target.current_term,
    +            log_length=len(target.log) - 1,
    +        )
    +
    +    def restart(self, node: NodeId) -> None:
    +        """Restart as a follower with durable state and empty volatile state."""
    +
    +        target = self._node(node)
    +        if target.alive:
    +            raise ValueError(f"node already running: {node}")
    +        target.alive = True
    +        self._reset_volatile(target)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_node_restarted",
    +            node=node,
    +            current_term=target.current_term,
    +            log_length=len(target.log) - 1,
    +        )
    +
    +    def isolate(self, node: NodeId) -> None:
    +        """Public lab API: place one node in a bidirectional minority partition."""
    +
    +        self._node(node)
    +        for peer in self._node_ids:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "raft_node_isolated", node=node)
    +
    +    def heal(self, node: NodeId) -> None:
    +        """Public lab API: remove one node's bidirectional partition links."""
    +
    +        self._node(node)
    +        for peer in self._node_ids:
    +            if peer != node:
    +                self.network.heal(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "raft_node_healed", node=node)
    +
    +    def probe(self) -> GroupState:
    +        """Return immutable snapshots without exposing mutation hooks."""
    +
    +        leader = self._current_leader_or_none()
    +        nodes = {
    +            node_id: NodeState(
    +                node_id=node_id,
    +                alive=node.alive,
    +                role=node.role,
    +                current_term=node.current_term,
    +                voted_for=node.voted_for,
    +                log=tuple(node.log),
    +                commit_index=node.commit_index,
    +                last_applied=node.last_applied,
    +                data=MappingProxyType(dict(node.data)),
    +            )
    +            for node_id, node in self._nodes.items()
    +        }
    +        return GroupState(
    +            tick=self.clock.now,
    +            leader="" if leader is None else leader.node_id,
    +            nodes=MappingProxyType(nodes),
    +        )
    +
    +    @property
    +    def _quorum(self) -> int:
    +        return len(self._node_ids) // 2 + 1
    +
    +    def _start_election(self, candidate: _Node) -> None:
    +        candidate.role = Role.CANDIDATE
    +        candidate.current_term += 1
    +        candidate.voted_for = candidate.node_id
    +        candidate.votes = {candidate.node_id}
    +        self._reset_election_deadline(candidate)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_election_started",
    +            candidate=candidate.node_id,
    +            term=candidate.current_term,
    +            last_log_index=len(candidate.log) - 1,
    +            last_log_term=candidate.log[-1].term,
    +        )
    +        for peer in self._peers(candidate.node_id):
    +            self.network.send(
    +                candidate.node_id,
    +                peer,
    +                {
    +                    "type": "request_vote",
    +                    "term": candidate.current_term,
    +                    "candidate_id": candidate.node_id,
    +                    "last_log_index": len(candidate.log) - 1,
    +                    "last_log_term": candidate.log[-1].term,
    +                },
    +            )
    +
    +    def _become_leader(self, node: _Node) -> None:
    +        node.role = Role.LEADER
    +        last_index = len(node.log) - 1
    +        node.next_index = {peer: last_index + 1 for peer in self._node_ids}
    +        node.match_index = {peer: 0 for peer in self._node_ids}
    +        node.match_index[node.node_id] = last_index
    +        node.heartbeat_due = self.clock.now + self._heartbeat_interval
    +
    +        # A current-term no-op gives the new leader a safe entry it can commit.
    +        # Once it commits, all preceding entries become committed as a prefix.
    +        no_op = LogEntry(
    +            index=len(node.log),
    +            term=node.current_term,
    +            key=None,
    +            value=None,
    +        )
    +        node.log.append(no_op)
    +        node.match_index[node.node_id] = no_op.index
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_leader_elected",
    +            leader=node.node_id,
    +            term=node.current_term,
    +            no_op_index=no_op.index,
    +        )
    +        self._broadcast_append(node)
    +
    +    def _receive(self, target_id: NodeId, message: Message) -> None:
    +        target = self._nodes[target_id]
    +        source = self._nodes.get(message.source)
    +        if not target.alive or source is None or not source.alive:
    +            self.trace.record(
    +                self.clock.now,
    +                "raft_message_ignored",
    +                message_id=message.message_id,
    +                reason="endpoint_not_alive",
    +            )
    +            return
    +        payload = message.payload
    +        if not isinstance(payload, dict):
    +            raise TypeError("Raft RPC payload must be a dict")
    +        rpc_type = payload.get("type")
    +        if rpc_type == "request_vote":
    +            self._receive_request_vote(target, message.source, payload)
    +        elif rpc_type == "request_vote_response":
    +            self._receive_vote_response(target, message.source, payload)
    +        elif rpc_type == "append_entries":
    +            self._receive_append_entries(target, message.source, payload)
    +        elif rpc_type == "append_entries_response":
    +            self._receive_append_response(target, message.source, payload)
    +        else:
    +            raise ValueError(f"unknown Raft RPC: {rpc_type}")
    +
    +    def _receive_request_vote(
    +        self,
    +        target: _Node,
    +        source: NodeId,
    +        payload: dict[str, Any],
    +    ) -> None:
    +        term = self._integer(payload, "term")
    +        if term > target.current_term:
    +            self._step_down(target, term, reason="higher_term_request_vote")
    +        up_to_date = (
    +            self._integer(payload, "last_log_term"),
    +            self._integer(payload, "last_log_index"),
    +        ) >= (target.log[-1].term, len(target.log) - 1)
    +        granted = (
    +            term == target.current_term
    +            and target.voted_for in (None, source)
    +            and up_to_date
    +        )
    +        if granted:
    +            target.voted_for = source
    +            self._reset_election_deadline(target)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_vote_decided",
    +            voter=target.node_id,
    +            candidate=source,
    +            term=target.current_term,
    +            granted=granted,
    +            up_to_date=up_to_date,
    +        )
    +        self.network.send(
    +            target.node_id,
    +            source,
    +            {
    +                "type": "request_vote_response",
    +                "term": target.current_term,
    +                "granted": granted,
    +            },
    +        )
    +
    +    def _receive_vote_response(
    +        self,
    +        target: _Node,
    +        source: NodeId,
    +        payload: dict[str, Any],
    +    ) -> None:
    +        term = self._integer(payload, "term")
    +        if term > target.current_term:
    +            self._step_down(target, term, reason="higher_term_vote_response")
    +            return
    +        if (
    +            target.role is not Role.CANDIDATE
    +            or term != target.current_term
    +            or not payload.get("granted")
    +        ):
    +            return
    +        target.votes.add(source)
    +        if len(target.votes) >= self._quorum:
    +            self._become_leader(target)
    +
    +    def _broadcast_append(
    +        self,
    +        leader: _Node,
    +        *,
    +        read_context: int | None = None,
    +    ) -> None:
    +        if not leader.alive or leader.role is not Role.LEADER:
    +            return
    +        for peer in self._peers(leader.node_id):
    +            self._send_append(leader, peer, read_context=read_context)
    +
    +    def _send_append(
    +        self,
    +        leader: _Node,
    +        peer: NodeId,
    +        *,
    +        read_context: int | None = None,
    +    ) -> None:
    +        next_index = leader.next_index.get(peer, len(leader.log))
    +        prev_index = max(0, next_index - 1)
    +        entries = [
    +            (entry.index, entry.term, entry.key, entry.value)
    +            for entry in leader.log[next_index:]
    +        ]
    +        self.network.send(
    +            leader.node_id,
    +            peer,
    +            {
    +                "type": "append_entries",
    +                "term": leader.current_term,
    +                "leader_id": leader.node_id,
    +                "prev_log_index": prev_index,
    +                "prev_log_term": leader.log[prev_index].term,
    +                "entries": entries,
    +                "leader_commit": leader.commit_index,
    +                "read_context": read_context,
    +            },
    +        )
    +
    +    def _receive_append_entries(
    +        self,
    +        target: _Node,
    +        source: NodeId,
    +        payload: dict[str, Any],
    +    ) -> None:
    +        term = self._integer(payload, "term")
    +        if term < target.current_term:
    +            self._send_append_response(
    +                target,
    +                source,
    +                success=False,
    +                match_index=0,
    +                conflict_index=len(target.log),
    +                read_context=payload.get("read_context"),
    +            )
    +            return
    +        if term > target.current_term or target.role is not Role.FOLLOWER:
    +            self._step_down(target, term, reason="append_entries_authority")
    +        self._reset_election_deadline(target)
    +
    +        prev_index = self._integer(payload, "prev_log_index")
    +        prev_term = self._integer(payload, "prev_log_term")
    +        if prev_index >= len(target.log):
    +            self._send_append_response(
    +                target,
    +                source,
    +                success=False,
    +                match_index=0,
    +                conflict_index=len(target.log),
    +                read_context=payload.get("read_context"),
    +            )
    +            return
    +        if target.log[prev_index].term != prev_term:
    +            conflict_term = target.log[prev_index].term
    +            conflict_index = prev_index
    +            while (
    +                conflict_index > 1
    +                and target.log[conflict_index - 1].term == conflict_term
    +            ):
    +                conflict_index -= 1
    +            self._send_append_response(
    +                target,
    +                source,
    +                success=False,
    +                match_index=0,
    +                conflict_index=conflict_index,
    +                read_context=payload.get("read_context"),
    +            )
    +            return
    +
    +        for raw_entry in payload["entries"]:
    +            index, entry_term, key, value = raw_entry
    +            entry = LogEntry(index=index, term=entry_term, key=key, value=value)
    +            if index < len(target.log):
    +                if target.log[index].term != entry.term:
    +                    # AppendEntries' prefix check makes it safe to discard only
    +                    # the conflicting suffix; the agreed prefix is immutable.
    +                    del target.log[index:]
    +                    target.log.append(entry)
    +            else:
    +                target.log.append(entry)
    +
    +        leader_commit = self._integer(payload, "leader_commit")
    +        if leader_commit > target.commit_index:
    +            target.commit_index = min(leader_commit, len(target.log) - 1)
    +            self._apply_committed(target)
    +        match_index = prev_index + len(payload["entries"])
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_append_accepted",
    +            follower=target.node_id,
    +            leader=source,
    +            term=term,
    +            match_index=match_index,
    +            commit_index=target.commit_index,
    +        )
    +        self._send_append_response(
    +            target,
    +            source,
    +            success=True,
    +            match_index=match_index,
    +            conflict_index=len(target.log),
    +            read_context=payload.get("read_context"),
    +        )
    +
    +    def _send_append_response(
    +        self,
    +        follower: _Node,
    +        leader_id: NodeId,
    +        *,
    +        success: bool,
    +        match_index: int,
    +        conflict_index: int,
    +        read_context: object,
    +    ) -> None:
    +        self.network.send(
    +            follower.node_id,
    +            leader_id,
    +            {
    +                "type": "append_entries_response",
    +                "term": follower.current_term,
    +                "success": success,
    +                "match_index": match_index,
    +                "conflict_index": conflict_index,
    +                "read_context": read_context,
    +            },
    +        )
    +
    +    def _receive_append_response(
    +        self,
    +        target: _Node,
    +        source: NodeId,
    +        payload: dict[str, Any],
    +    ) -> None:
    +        term = self._integer(payload, "term")
    +        if term > target.current_term:
    +            self._step_down(target, term, reason="higher_term_append_response")
    +            return
    +        if target.role is not Role.LEADER or term != target.current_term:
    +            return
    +        if not payload.get("success"):
    +            target.next_index[source] = max(
    +                1,
    +                self._integer(payload, "conflict_index"),
    +            )
    +            self._send_append(
    +                target,
    +                source,
    +                read_context=self._optional_integer(payload, "read_context"),
    +            )
    +            return
    +
    +        match_index = self._integer(payload, "match_index")
    +        target.match_index[source] = max(target.match_index.get(source, 0), match_index)
    +        target.next_index[source] = target.match_index[source] + 1
    +        context = self._optional_integer(payload, "read_context")
    +        if context is not None:
    +            target.read_acks.setdefault(context, {target.node_id}).add(source)
    +        self._advance_commit(target)
    +
    +    def _advance_commit(self, leader: _Node) -> None:
    +        for index in range(len(leader.log) - 1, leader.commit_index, -1):
    +            replicated = sum(
    +                match >= index for match in leader.match_index.values()
    +            )
    +            # Raft paper §5.4.2 gives the counterexample for counting replicas
    +            # alone: an old-term entry stored on a majority can still be
    +            # overwritten after a later election.  Leaders therefore advance
    +            # commitIndex by counting replicas only for their current-term
    +            # entries; committing such an entry commits its entire prefix.
    +            if (
    +                replicated >= self._quorum
    +                and leader.log[index].term == leader.current_term
    +            ):
    +                leader.commit_index = index
    +                self._apply_committed(leader)
    +                self.trace.record(
    +                    self.clock.now,
    +                    "raft_commit_advanced",
    +                    leader=leader.node_id,
    +                    term=leader.current_term,
    +                    commit_index=index,
    +                )
    +                self._broadcast_append(leader)
    +                return
    +
    +    def _apply_committed(self, node: _Node) -> None:
    +        while node.last_applied < node.commit_index:
    +            node.last_applied += 1
    +            entry = node.log[node.last_applied]
    +            if entry.key is not None:
    +                assert entry.value is not None
    +                node.data[entry.key] = entry.value
    +            self.trace.record(
    +                self.clock.now,
    +                "raft_entry_applied",
    +                node=node.node_id,
    +                index=entry.index,
    +                term=entry.term,
    +                key=entry.key,
    +                value=entry.value,
    +            )
    +
    +    def _wait_for_commit(self, leader: _Node, entry: LogEntry) -> bool:
    +        for _ in range(self._timeout_high * 2):
    +            if (
    +                leader.alive
    +                and leader.role is Role.LEADER
    +                and leader.current_term == entry.term
    +                and leader.commit_index >= entry.index
    +            ):
    +                return True
    +            self.tick()
    +        return False
    +
    +    def _step_down(self, node: _Node, term: int, *, reason: str) -> None:
    +        old_role = node.role
    +        old_term = node.current_term
    +        if term > node.current_term:
    +            node.current_term = term
    +            node.voted_for = None
    +        node.role = Role.FOLLOWER
    +        node.votes.clear()
    +        node.next_index.clear()
    +        node.match_index.clear()
    +        node.read_acks.clear()
    +        self._reset_election_deadline(node)
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_became_follower",
    +            node=node.node_id,
    +            old_role=old_role.value,
    +            old_term=old_term,
    +            term=node.current_term,
    +            reason=reason,
    +        )
    +
    +    def _reset_volatile(self, node: _Node) -> None:
    +        self._clear_volatile(node)
    +        self._reset_election_deadline(node)
    +
    +    @staticmethod
    +    def _clear_volatile(node: _Node) -> None:
    +        node.role = Role.FOLLOWER
    +        node.commit_index = 0
    +        node.last_applied = 0
    +        node.data.clear()
    +        node.election_deadline = 0
    +        node.heartbeat_due = 0
    +        node.votes.clear()
    +        node.next_index.clear()
    +        node.match_index.clear()
    +        node.read_acks.clear()
    +
    +    def _reset_election_deadline(self, node: _Node) -> None:
    +        timeout = self._rng.randint(self._timeout_low, self._timeout_high)
    +        node.election_deadline = self.clock.now + timeout
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_election_deadline_reset",
    +            node=node.node_id,
    +            deadline=node.election_deadline,
    +            timeout=timeout,
    +        )
    +
    +    def _leader(self) -> _Node:
    +        leader = self._current_leader_or_none()
    +        if leader is None:
    +            raise RuntimeError("Raft has no elected leader")
    +        return leader
    +
    +    def _current_leader_or_none(self) -> _Node | None:
    +        leaders = [
    +            node
    +            for node in self._nodes.values()
    +            if node.alive and node.role is Role.LEADER
    +        ]
    +        if not leaders:
    +            return None
    +        return max(leaders, key=lambda candidate: candidate.current_term)
    +
    +    def _node(self, node_id: NodeId) -> _Node:
    +        try:
    +            return self._nodes[node_id]
    +        except KeyError as error:
    +            raise KeyError(f"unknown node: {node_id}") from error
    +
    +    def _peers(self, node_id: NodeId) -> tuple[NodeId, ...]:
    +        return tuple(peer for peer in self._node_ids if peer != node_id)
    +
    +    @staticmethod
    +    def _integer(payload: dict[str, Any], key: str) -> int:
    +        value = payload[key]
    +        if not isinstance(value, int):
    +            raise TypeError(f"Raft RPC field {key!r} must be int")
    +        return value
    +
    +    @staticmethod
    +    def _optional_integer(payload: dict[str, Any], key: str) -> int | None:
    +        value = payload.get(key)
    +        if value is None:
    +            return None
    +        if not isinstance(value, int):
    +            raise TypeError(f"Raft RPC field {key!r} must be int or None")
    +        return value
    +
    +    @staticmethod
    +    def _validate_bytes(value: bytes, field_name: str) -> None:
    +        if not isinstance(value, bytes):
    +            raise TypeError(f"{field_name} must be bytes")
    +
    ```

**是什么，为什么现在需要**

该模块拥有完整的教学规模 Raft 状态机：持久 Term/Vote/Log、易失 Role 与 Index、选举、复制、提交、读取、分区与恢复。

**在运行时做什么**

每个 Tick 处理 Timeout、Heartbeat 与网络投递。客户端写入只在当前 Leader 追加，并且只有等待循环观察到提交后才返回 Accepted。

**关键代码**

```python
if (
    replicated >= self._quorum
    and leader.log[index].term == leader.current_term
):
    leader.commit_index = index
```

**关键语句理解**

只计算多数副本不足以安全提交旧 Term Entry。要求 Entry Term 等于 Leader 当前 Term，使提交它的同时安全提交此前前缀。

#### Raft 包边界

把 Group、Role、LogEntry 与 Probe 词汇作为支撑 API 导出。

??? note "支撑文件差异（1 个文件）"
    **`src/minidist/protocols/raft/__init__.py`**

    ```diff
    diff --git a/src/minidist/protocols/raft/__init__.py b/src/minidist/protocols/raft/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..1cd8b29841e74fbcc6d469a59ad621c79e36b46d
    --- /dev/null
    +++ b/src/minidist/protocols/raft/__init__.py
    @@ -0,0 +1,17 @@
    +"""Public surface of MiniDist's textbook Raft replication group.
    +
    +The implementation covers leader election, log replication, commitment, crash
    +recovery, and a small read-index barrier.  Snapshots and membership changes
    +are deliberately out of scope: the log is unbounded and membership is static
    +for the lifetime of a group.
    +"""
    +
    +from .group import GroupState, LogEntry, NodeState, RaftGroup, Role
    +
    +__all__ = [
    +    "GroupState",
    +    "LogEntry",
    +    "NodeState",
    +    "RaftGroup",
    +    "Role",
    +]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/04-raft-consensus/tests.txt)`。八条契约覆盖选举、读写保证、故障转移、重启所有权、分区恢复与确定性重放。

### 需要真正记住的内容

Leader 身份受 Term 限定。Stored 不等于 Committed，Committed 不等于 Applied；线性一致读写都需要当前权威证据。

### 用自己的话讲清楚

说明为什么被隔离旧 Leader 可以本地 Append，却不能成功返回 Quorum 确认，以及更高 Term Leader 如何在恢复连接后修复该后缀。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/06-raft-election.md) · [第 7 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/07-raft-replication.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-03...stage-04)

完成后可运行 `git checkout stage-04` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/04-raft-consensus/stage.patch)
