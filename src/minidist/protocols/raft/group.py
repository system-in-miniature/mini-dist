"""Textbook Raft over MiniDist's deterministic simulation substrate.

Each node persists ``currentTerm``, ``votedFor``, and its log before an RPC
response can expose the change.  Role, election deadline, commit/applied
indices, leader replication cursors, and the KV state machine are volatile and
are rebuilt after a crash.  RequestVote and AppendEntries travel through
``SimNet`` rather than calling peers directly, so partitions and delay affect
the protocol at the same boundary as the other MiniDist implementations.

The module intentionally implements only static-membership, unbounded-log
Raft.  Snapshot installation, log compaction, and joint-consensus membership
changes are not implemented; they are separate teaching topics rather than
hidden approximations here.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from minidist.protocols.types import (
    AckLevel,
    NodeId,
    ReadLevel,
    ReadResult,
    UnsupportedLevelError,
    WriteResult,
)
from minidist.sim import Message, Scheduler, SimClock, SimNet, Trace


class Role(Enum):
    """A Raft server's volatile role in its current term."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True, slots=True)
class LogEntry:
    """One replicated state-machine command; ``key=None`` denotes a no-op."""

    index: int
    term: int
    key: bytes | None
    value: bytes | None


_SENTINEL = LogEntry(index=0, term=0, key=None, value=None)


@dataclass(slots=True)
class _Node:
    node_id: NodeId

    # Raft Figure 2 persistent state: these fields survive crash/restart.
    current_term: int = 0
    voted_for: NodeId | None = None
    log: list[LogEntry] = field(default_factory=lambda: [_SENTINEL])

    # Everything below is volatile and rebuilt by _reset_volatile().
    alive: bool = True
    role: Role = Role.FOLLOWER
    commit_index: int = 0
    last_applied: int = 0
    data: dict[bytes, bytes] = field(default_factory=dict)
    election_deadline: int = 0
    heartbeat_due: int = 0
    votes: set[NodeId] = field(default_factory=set)
    next_index: dict[NodeId, int] = field(default_factory=dict)
    match_index: dict[NodeId, int] = field(default_factory=dict)
    read_acks: dict[int, set[NodeId]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NodeState:
    """Read-only state exposed to experiments and assertions."""

    node_id: NodeId
    alive: bool
    role: Role
    current_term: int
    voted_for: NodeId | None
    log: tuple[LogEntry, ...]
    commit_index: int
    last_applied: int
    durable_index: int
    data: Mapping[bytes, bytes]


@dataclass(frozen=True, slots=True)
class GroupState:
    """Read-only snapshot of a Raft group."""

    tick: int
    leader: NodeId
    nodes: Mapping[NodeId, NodeState]


class RaftGroup:
    """A deterministic three-or-more-node textbook Raft replication group."""

    def __init__(
        self,
        *,
        node_ids: tuple[NodeId, ...] = ("node-1", "node-2", "node-3"),
        seed: int = 0,
        election_timeout_range: tuple[int, int] = (8, 14),
        heartbeat_interval: int = 2,
        network_delay: int = 1,
    ) -> None:
        if len(node_ids) < 3 or len(set(node_ids)) != len(node_ids):
            raise ValueError("Raft requires at least three unique node IDs")
        low, high = election_timeout_range
        if low <= heartbeat_interval or high < low:
            raise ValueError(
                "election timeout range must be ordered and exceed heartbeat interval"
            )
        if heartbeat_interval <= 0 or network_delay <= 0:
            raise ValueError("heartbeat interval and network delay must be positive")
        minimum_timeout = 2 * network_delay + heartbeat_interval
        if low <= minimum_timeout:
            raise ValueError(
                "lowest election timeout must exceed twice the maximum network "
                "delay plus the heartbeat interval"
            )

        self.clock = SimClock()
        self.trace = Trace()
        self.scheduler = Scheduler(self.clock, self.trace)
        self.network = SimNet(
            seed=seed,
            clock=self.clock,
            scheduler=self.scheduler,
            trace=self.trace,
            min_delay=network_delay,
            max_delay=network_delay,
        )
        # This is the only election randomness source.  It is private rather
        # than module-global so an injected seed fully determines all draws.
        self._rng = random.Random(seed)
        self._timeout_low = low
        self._timeout_high = high
        self._heartbeat_interval = heartbeat_interval
        self._node_ids = node_ids
        self._nodes = {node_id: _Node(node_id=node_id) for node_id in node_ids}
        self._next_read_context = 1

        for node_id in node_ids:
            self.network.register(
                node_id,
                lambda message, target=node_id: self._receive(target, message),
            )
        for node in self._nodes.values():
            self._reset_election_deadline(node)
        self.trace.record(
            self.clock.now,
            "raft_group_created",
            nodes=list(node_ids),
            election_timeout_range=list(election_timeout_range),
            heartbeat_interval=heartbeat_interval,
            seed=seed,
        )

    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult:
        """Append a command and return only after a majority commits it."""

        if ack is not AckLevel.QUORUM:
            raise UnsupportedLevelError(f"Raft does not support AckLevel.{ack.name}")
        self._validate_bytes(key, "key")
        self._validate_bytes(value, "value")
        leader = self._leader()
        entry = LogEntry(
            index=len(leader.log),
            term=leader.current_term,
            key=key,
            value=value,
        )
        leader.log.append(entry)
        leader.match_index[leader.node_id] = entry.index
        self.trace.record(
            self.clock.now,
            "raft_client_entry_appended",
            leader=leader.node_id,
            term=entry.term,
            index=entry.index,
            key=key,
            value=value,
        )
        self._broadcast_append(leader)

        accepted = self._wait_for_commit(leader, entry)
        message = (
            "entry committed by a majority"
            if accepted
            else "entry was not committed by a majority"
        )
        self.trace.record(
            self.clock.now,
            "raft_client_write_result",
            leader=leader.node_id,
            term=entry.term,
            index=entry.index,
            accepted=accepted,
        )
        return WriteResult(accepted=accepted, offset=entry.index, message=message)

    def client_read(
        self,
        key: bytes,
        level: ReadLevel,
        node: NodeId | None = None,
    ) -> ReadResult:
        """Serve a leader read or first prove authority with a read-index round."""

        self._validate_bytes(key, "key")
        if level is ReadLevel.LOCAL:
            selected = self._leader().node_id if node is None else node
            target = self._node(selected)
            if not target.alive:
                raise RuntimeError(f"node is crashed: {selected}")
            return ReadResult(
                value=target.data.get(key),
                node=selected,
                offset=target.commit_index,
            )
        if node is not None:
            raise ValueError("Raft leader reads do not accept an explicit node")
        leader = self._leader()
        if level is ReadLevel.LINEARIZABLE:
            context = self._next_read_context
            self._next_read_context += 1
            leader.read_acks[context] = {leader.node_id}
            self.trace.record(
                self.clock.now,
                "raft_read_index_started",
                leader=leader.node_id,
                term=leader.current_term,
                context=context,
            )
            self._broadcast_append(leader, read_context=context)
            for _ in range(self._timeout_high * 2):
                if len(leader.read_acks.get(context, set())) >= self._quorum:
                    break
                self.tick()
            else:
                leader.read_acks.pop(context, None)
                raise RuntimeError("read-index quorum was not reached")
            leader.read_acks.pop(context, None)
            self.trace.record(
                self.clock.now,
                "raft_read_index_completed",
                leader=leader.node_id,
                term=leader.current_term,
                context=context,
            )
        elif level is not ReadLevel.LEADER:
            raise UnsupportedLevelError(f"Raft does not support ReadLevel.{level.name}")

        value = leader.data.get(key)
        self.trace.record(
            self.clock.now,
            "raft_client_read",
            leader=leader.node_id,
            level=level.name,
            key=key,
            value=value,
            commit_index=leader.commit_index,
        )
        return ReadResult(
            value=value,
            node=leader.node_id,
            offset=leader.commit_index,
        )

    def tick(self) -> None:
        """Advance delivery, then run deterministic timer transitions."""

        self.scheduler.tick()
        for node_id in self._node_ids:
            node = self._nodes[node_id]
            if not node.alive:
                continue
            if node.role is Role.LEADER:
                if self.clock.now >= node.heartbeat_due:
                    self._broadcast_append(node)
                    node.heartbeat_due = self.clock.now + self._heartbeat_interval
            elif self.clock.now >= node.election_deadline:
                self._start_election(node)

    def run_until_leader(
        self,
        *,
        exclude: NodeId | None = None,
        max_ticks: int = 1_000,
    ) -> NodeId:
        """Drive timers until a leader other than ``exclude`` is elected."""

        for _ in range(max_ticks + 1):
            leaders = [
                node
                for node in self._nodes.values()
                if node.alive
                and node.role is Role.LEADER
                and node.node_id != exclude
            ]
            if leaders:
                return max(leaders, key=lambda candidate: candidate.current_term).node_id
            self.tick()
        raise RuntimeError("Raft did not elect a leader")

    def run_until_converged(self, *, max_ticks: int = 1_000) -> int:
        """Drive the group until all alive nodes share log and applied state."""

        for ticks in range(max_ticks + 1):
            alive = [node for node in self._nodes.values() if node.alive]
            if alive and self._current_leader_or_none() is not None:
                first = alive[0]
                if all(
                    node.log == first.log
                    and node.data == first.data
                    and node.commit_index == first.commit_index
                    for node in alive[1:]
                ):
                    return ticks
            self.tick()
        raise RuntimeError("Raft group did not converge")

    def fsync(self, node: NodeId) -> None:
        """Expose Raft's stable-storage boundary for cross-protocol labs.

        Raft persists term/vote/log before the RPC or client-visible action
        that depends on them, so the in-process representation is already at
        this boundary when this method is called.
        """

        target = self._node(node)
        if not target.alive:
            raise RuntimeError(f"node is crashed: {node}")
        self.trace.record(
            self.clock.now,
            "raft_node_fsynced",
            node=node,
            durable_index=len(target.log) - 1,
            current_term=target.current_term,
        )

    def crash(self, node: NodeId, *, lose_unfsynced: bool = False) -> None:
        """Crash a server, preserving only term, vote, and log."""

        target = self._node(node)
        if not target.alive:
            raise ValueError(f"node already crashed: {node}")
        target.alive = False
        self._clear_volatile(target)
        self.trace.record(
            self.clock.now,
            "raft_node_crashed",
            node=node,
            current_term=target.current_term,
            log_length=len(target.log) - 1,
            lost_unfsynced=lose_unfsynced,
        )

    def restart(self, node: NodeId) -> None:
        """Restart as a follower with durable state and empty volatile state."""

        target = self._node(node)
        if target.alive:
            raise ValueError(f"node already running: {node}")
        target.alive = True
        self._reset_volatile(target)
        self.trace.record(
            self.clock.now,
            "raft_node_restarted",
            node=node,
            current_term=target.current_term,
            log_length=len(target.log) - 1,
        )

    def isolate(self, node: NodeId) -> None:
        """Public lab API: place one node in a bidirectional minority partition."""

        self._node(node)
        for peer in self._node_ids:
            if peer != node:
                self.network.partition(node, peer, bidirectional=True)
        self.trace.record(self.clock.now, "raft_node_isolated", node=node)

    def heal(self, node: NodeId) -> None:
        """Public lab API: remove one node's bidirectional partition links."""

        self._node(node)
        for peer in self._node_ids:
            if peer != node:
                self.network.heal(node, peer, bidirectional=True)
        self.trace.record(self.clock.now, "raft_node_healed", node=node)

    def probe(self) -> GroupState:
        """Return immutable snapshots without exposing mutation hooks."""

        leader = self._current_leader_or_none()
        nodes = {
            node_id: NodeState(
                node_id=node_id,
                alive=node.alive,
                role=node.role,
                current_term=node.current_term,
                voted_for=node.voted_for,
                log=tuple(node.log),
                commit_index=node.commit_index,
                last_applied=node.last_applied,
                durable_index=len(node.log) - 1,
                data=MappingProxyType(dict(node.data)),
            )
            for node_id, node in self._nodes.items()
        }
        return GroupState(
            tick=self.clock.now,
            leader="" if leader is None else leader.node_id,
            nodes=MappingProxyType(nodes),
        )

    @property
    def _quorum(self) -> int:
        return len(self._node_ids) // 2 + 1

    def _start_election(self, candidate: _Node) -> None:
        candidate.role = Role.CANDIDATE
        candidate.current_term += 1
        candidate.voted_for = candidate.node_id
        candidate.votes = {candidate.node_id}
        self._reset_election_deadline(candidate)
        self.trace.record(
            self.clock.now,
            "raft_election_started",
            candidate=candidate.node_id,
            term=candidate.current_term,
            last_log_index=len(candidate.log) - 1,
            last_log_term=candidate.log[-1].term,
        )
        for peer in self._peers(candidate.node_id):
            self.network.send(
                candidate.node_id,
                peer,
                {
                    "type": "request_vote",
                    "term": candidate.current_term,
                    "candidate_id": candidate.node_id,
                    "last_log_index": len(candidate.log) - 1,
                    "last_log_term": candidate.log[-1].term,
                },
            )

    def _become_leader(self, node: _Node) -> None:
        node.role = Role.LEADER
        last_index = len(node.log) - 1
        node.next_index = {peer: last_index + 1 for peer in self._node_ids}
        node.match_index = {peer: 0 for peer in self._node_ids}
        node.match_index[node.node_id] = last_index
        node.heartbeat_due = self.clock.now + self._heartbeat_interval

        # A current-term no-op gives the new leader a safe entry it can commit.
        # Once it commits, all preceding entries become committed as a prefix.
        no_op = LogEntry(
            index=len(node.log),
            term=node.current_term,
            key=None,
            value=None,
        )
        node.log.append(no_op)
        node.match_index[node.node_id] = no_op.index
        self.trace.record(
            self.clock.now,
            "raft_leader_elected",
            leader=node.node_id,
            term=node.current_term,
            no_op_index=no_op.index,
        )
        self._broadcast_append(node)

    def _receive(self, target_id: NodeId, message: Message) -> None:
        target = self._nodes[target_id]
        source = self._nodes.get(message.source)
        if not target.alive or source is None or not source.alive:
            self.trace.record(
                self.clock.now,
                "raft_message_ignored",
                message_id=message.message_id,
                reason="endpoint_not_alive",
            )
            return
        payload = message.payload
        if not isinstance(payload, dict):
            raise TypeError("Raft RPC payload must be a dict")
        rpc_type = payload.get("type")
        if rpc_type == "request_vote":
            self._receive_request_vote(target, message.source, payload)
        elif rpc_type == "request_vote_response":
            self._receive_vote_response(target, message.source, payload)
        elif rpc_type == "append_entries":
            self._receive_append_entries(target, message.source, payload)
        elif rpc_type == "append_entries_response":
            self._receive_append_response(target, message.source, payload)
        else:
            raise ValueError(f"unknown Raft RPC: {rpc_type}")

    def _receive_request_vote(
        self,
        target: _Node,
        source: NodeId,
        payload: dict[str, Any],
    ) -> None:
        term = self._integer(payload, "term")
        if term > target.current_term:
            self._step_down(target, term, reason="higher_term_request_vote")
        up_to_date = (
            self._integer(payload, "last_log_term"),
            self._integer(payload, "last_log_index"),
        ) >= (target.log[-1].term, len(target.log) - 1)
        granted = (
            term == target.current_term
            and target.voted_for in (None, source)
            and up_to_date
        )
        if granted:
            target.voted_for = source
            self._reset_election_deadline(target)
        self.trace.record(
            self.clock.now,
            "raft_vote_decided",
            voter=target.node_id,
            candidate=source,
            term=target.current_term,
            granted=granted,
            up_to_date=up_to_date,
        )
        self.network.send(
            target.node_id,
            source,
            {
                "type": "request_vote_response",
                "term": target.current_term,
                "granted": granted,
            },
        )

    def _receive_vote_response(
        self,
        target: _Node,
        source: NodeId,
        payload: dict[str, Any],
    ) -> None:
        term = self._integer(payload, "term")
        if term > target.current_term:
            self._step_down(target, term, reason="higher_term_vote_response")
            return
        if (
            target.role is not Role.CANDIDATE
            or term != target.current_term
            or not payload.get("granted")
        ):
            return
        target.votes.add(source)
        if len(target.votes) >= self._quorum:
            self._become_leader(target)

    def _broadcast_append(
        self,
        leader: _Node,
        *,
        read_context: int | None = None,
    ) -> None:
        if not leader.alive or leader.role is not Role.LEADER:
            return
        for peer in self._peers(leader.node_id):
            self._send_append(leader, peer, read_context=read_context)

    def _send_append(
        self,
        leader: _Node,
        peer: NodeId,
        *,
        read_context: int | None = None,
    ) -> None:
        next_index = leader.next_index.get(peer, len(leader.log))
        prev_index = max(0, next_index - 1)
        entries = [
            (entry.index, entry.term, entry.key, entry.value)
            for entry in leader.log[next_index:]
        ]
        self.network.send(
            leader.node_id,
            peer,
            {
                "type": "append_entries",
                "term": leader.current_term,
                "leader_id": leader.node_id,
                "prev_log_index": prev_index,
                "prev_log_term": leader.log[prev_index].term,
                "entries": entries,
                "leader_commit": leader.commit_index,
                "read_context": read_context,
            },
        )

    def _receive_append_entries(
        self,
        target: _Node,
        source: NodeId,
        payload: dict[str, Any],
    ) -> None:
        term = self._integer(payload, "term")
        if term < target.current_term:
            self._send_append_response(
                target,
                source,
                success=False,
                match_index=0,
                conflict_index=len(target.log),
                read_context=payload.get("read_context"),
            )
            return
        if term > target.current_term or target.role is not Role.FOLLOWER:
            self._step_down(target, term, reason="append_entries_authority")
        self._reset_election_deadline(target)

        prev_index = self._integer(payload, "prev_log_index")
        prev_term = self._integer(payload, "prev_log_term")
        if prev_index >= len(target.log):
            self._send_append_response(
                target,
                source,
                success=False,
                match_index=0,
                conflict_index=len(target.log),
                read_context=payload.get("read_context"),
            )
            return
        if target.log[prev_index].term != prev_term:
            conflict_term = target.log[prev_index].term
            conflict_index = prev_index
            while (
                conflict_index > 1
                and target.log[conflict_index - 1].term == conflict_term
            ):
                conflict_index -= 1
            self._send_append_response(
                target,
                source,
                success=False,
                match_index=0,
                conflict_index=conflict_index,
                read_context=payload.get("read_context"),
            )
            return

        for raw_entry in payload["entries"]:
            index, entry_term, key, value = raw_entry
            entry = LogEntry(index=index, term=entry_term, key=key, value=value)
            if index < len(target.log):
                if target.log[index].term != entry.term:
                    # AppendEntries' prefix check makes it safe to discard only
                    # the conflicting suffix; the agreed prefix is immutable.
                    del target.log[index:]
                    target.log.append(entry)
            else:
                target.log.append(entry)

        leader_commit = self._integer(payload, "leader_commit")
        if leader_commit > target.commit_index:
            target.commit_index = min(leader_commit, len(target.log) - 1)
            self._apply_committed(target)
        match_index = prev_index + len(payload["entries"])
        self.trace.record(
            self.clock.now,
            "raft_append_accepted",
            follower=target.node_id,
            leader=source,
            term=term,
            match_index=match_index,
            commit_index=target.commit_index,
        )
        self._send_append_response(
            target,
            source,
            success=True,
            match_index=match_index,
            conflict_index=len(target.log),
            read_context=payload.get("read_context"),
        )

    def _send_append_response(
        self,
        follower: _Node,
        leader_id: NodeId,
        *,
        success: bool,
        match_index: int,
        conflict_index: int,
        read_context: object,
    ) -> None:
        self.network.send(
            follower.node_id,
            leader_id,
            {
                "type": "append_entries_response",
                "term": follower.current_term,
                "success": success,
                "match_index": match_index,
                "conflict_index": conflict_index,
                "read_context": read_context,
            },
        )

    def _receive_append_response(
        self,
        target: _Node,
        source: NodeId,
        payload: dict[str, Any],
    ) -> None:
        term = self._integer(payload, "term")
        if term > target.current_term:
            self._step_down(target, term, reason="higher_term_append_response")
            return
        if target.role is not Role.LEADER or term != target.current_term:
            return
        if not payload.get("success"):
            target.next_index[source] = max(
                1,
                self._integer(payload, "conflict_index"),
            )
            self._send_append(
                target,
                source,
                read_context=self._optional_integer(payload, "read_context"),
            )
            return

        match_index = self._integer(payload, "match_index")
        target.match_index[source] = max(target.match_index.get(source, 0), match_index)
        target.next_index[source] = target.match_index[source] + 1
        context = self._optional_integer(payload, "read_context")
        if context is not None:
            target.read_acks.setdefault(context, {target.node_id}).add(source)
        self._advance_commit(target)

    def _advance_commit(self, leader: _Node) -> None:
        for index in range(len(leader.log) - 1, leader.commit_index, -1):
            replicated = sum(
                match >= index for match in leader.match_index.values()
            )
            # Raft paper §5.4.2 gives the counterexample for counting replicas
            # alone: an old-term entry stored on a majority can still be
            # overwritten after a later election.  Leaders therefore advance
            # commitIndex by counting replicas only for their current-term
            # entries; committing such an entry commits its entire prefix.
            if (
                replicated >= self._quorum
                and leader.log[index].term == leader.current_term
            ):
                leader.commit_index = index
                self._apply_committed(leader)
                self.trace.record(
                    self.clock.now,
                    "raft_commit_advanced",
                    leader=leader.node_id,
                    term=leader.current_term,
                    commit_index=index,
                )
                self._broadcast_append(leader)
                return

    def _apply_committed(self, node: _Node) -> None:
        while node.last_applied < node.commit_index:
            node.last_applied += 1
            entry = node.log[node.last_applied]
            if entry.key is not None:
                assert entry.value is not None
                node.data[entry.key] = entry.value
            self.trace.record(
                self.clock.now,
                "raft_entry_applied",
                node=node.node_id,
                index=entry.index,
                term=entry.term,
                key=entry.key,
                value=entry.value,
            )

    def _wait_for_commit(self, leader: _Node, entry: LogEntry) -> bool:
        for _ in range(self._timeout_high * 2):
            if (
                leader.alive
                and leader.role is Role.LEADER
                and leader.current_term == entry.term
                and leader.commit_index >= entry.index
            ):
                return True
            self.tick()
        return False

    def _step_down(self, node: _Node, term: int, *, reason: str) -> None:
        old_role = node.role
        old_term = node.current_term
        if term > node.current_term:
            node.current_term = term
            node.voted_for = None
        node.role = Role.FOLLOWER
        node.votes.clear()
        node.next_index.clear()
        node.match_index.clear()
        node.read_acks.clear()
        self._reset_election_deadline(node)
        self.trace.record(
            self.clock.now,
            "raft_became_follower",
            node=node.node_id,
            old_role=old_role.value,
            old_term=old_term,
            term=node.current_term,
            reason=reason,
        )

    def _reset_volatile(self, node: _Node) -> None:
        self._clear_volatile(node)
        self._reset_election_deadline(node)

    @staticmethod
    def _clear_volatile(node: _Node) -> None:
        node.role = Role.FOLLOWER
        node.commit_index = 0
        node.last_applied = 0
        node.data.clear()
        node.election_deadline = 0
        node.heartbeat_due = 0
        node.votes.clear()
        node.next_index.clear()
        node.match_index.clear()
        node.read_acks.clear()

    def _reset_election_deadline(self, node: _Node) -> None:
        timeout = self._rng.randint(self._timeout_low, self._timeout_high)
        node.election_deadline = self.clock.now + timeout
        self.trace.record(
            self.clock.now,
            "raft_election_deadline_reset",
            node=node.node_id,
            deadline=node.election_deadline,
            timeout=timeout,
        )

    def _leader(self) -> _Node:
        leader = self._current_leader_or_none()
        if leader is None:
            raise RuntimeError("Raft has no elected leader")
        return leader

    def _current_leader_or_none(self) -> _Node | None:
        leaders = [
            node
            for node in self._nodes.values()
            if node.alive and node.role is Role.LEADER
        ]
        if not leaders:
            return None
        return max(leaders, key=lambda candidate: candidate.current_term)

    def _node(self, node_id: NodeId) -> _Node:
        try:
            return self._nodes[node_id]
        except KeyError as error:
            raise KeyError(f"unknown node: {node_id}") from error

    def _peers(self, node_id: NodeId) -> tuple[NodeId, ...]:
        return tuple(peer for peer in self._node_ids if peer != node_id)

    @staticmethod
    def _integer(payload: dict[str, Any], key: str) -> int:
        value = payload[key]
        if not isinstance(value, int):
            raise TypeError(f"Raft RPC field {key!r} must be int")
        return value

    @staticmethod
    def _optional_integer(payload: dict[str, Any], key: str) -> int | None:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, int):
            raise TypeError(f"Raft RPC field {key!r} must be int or None")
        return value

    @staticmethod
    def _validate_bytes(value: bytes, field_name: str) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"{field_name} must be bytes")
