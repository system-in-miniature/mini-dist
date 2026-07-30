from __future__ import annotations

import pytest

from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
from minidist.protocols.raft import RaftGroup, Role


def elected_group(*, seed: int = 7) -> RaftGroup:
    group = RaftGroup(
        node_ids=("n1", "n2", "n3"),
        seed=seed,
        election_timeout_range=(4, 7),
        heartbeat_interval=1,
    )
    group.run_until_leader()
    return group


def test_seeded_timeouts_elect_exactly_one_leader() -> None:
    group = elected_group(seed=11)

    state = group.probe()
    leaders = [node for node in state.nodes.values() if node.role is Role.LEADER]
    assert len(leaders) == 1
    assert state.leader == leaders[0].node_id
    assert leaders[0].current_term >= 1


@pytest.mark.parametrize(
    "level",
    [AckLevel.NONE, AckLevel.LEADER, AckLevel.ALL_ISR],
)
def test_raft_rejects_non_quorum_ack_levels(level: AckLevel) -> None:
    group = elected_group()

    with pytest.raises(UnsupportedLevelError, match=level.name):
        group.client_write(b"k", b"v", level)


def test_quorum_write_commits_and_linearizable_read_uses_barrier() -> None:
    group = elected_group()

    result = group.client_write(b"course", b"MiniDist", AckLevel.QUORUM)

    assert result.accepted
    assert group.client_read(b"course", ReadLevel.LEADER).value == b"MiniDist"
    assert (
        group.client_read(b"course", ReadLevel.LINEARIZABLE).value == b"MiniDist"
    )
    state = group.probe()
    assert sum(node.commit_index >= result.offset for node in state.nodes.values()) >= 2


def test_acknowledged_write_survives_leader_crash_and_failover() -> None:
    group = elected_group(seed=19)
    old_leader = group.probe().leader
    result = group.client_write(b"order:42", b"confirmed", AckLevel.QUORUM)

    group.crash(old_leader)
    new_leader = group.run_until_leader(exclude=old_leader)

    assert result.accepted
    assert new_leader != old_leader
    assert (
        group.client_read(b"order:42", ReadLevel.LINEARIZABLE).value
        == b"confirmed"
    )


def test_restart_keeps_term_vote_and_log_but_rebuilds_volatile_state() -> None:
    group = elected_group(seed=23)
    follower = next(
        node_id for node_id in group.probe().nodes if node_id != group.probe().leader
    )
    group.client_write(b"durable", b"entry", AckLevel.QUORUM)
    before = group.probe().nodes[follower]

    group.crash(follower)
    group.restart(follower)
    after = group.probe().nodes[follower]

    assert after.current_term == before.current_term
    assert after.voted_for == before.voted_for
    assert after.log == before.log
    assert after.role is Role.FOLLOWER
    assert after.commit_index == 0


def test_partitioned_old_leader_cannot_commit_and_logs_converge_after_heal() -> None:
    group = elected_group(seed=29)
    old_leader = group.probe().leader
    group.isolate(old_leader)

    rejected = group.client_write(
        b"minority-only",
        b"must-not-commit",
        AckLevel.QUORUM,
    )
    new_leader = group.run_until_leader(exclude=old_leader)
    committed = group.client_write(b"majority", b"continues", AckLevel.QUORUM)

    assert not rejected.accepted
    assert committed.accepted
    assert new_leader != old_leader

    group.heal(old_leader)
    group.run_until_converged()
    state = group.probe()

    assert state.nodes[old_leader].role is Role.FOLLOWER
    assert state.nodes[old_leader].current_term == state.nodes[new_leader].current_term
    assert all(node.log == state.nodes[new_leader].log for node in state.nodes.values())
    assert all(node.data.get(b"majority") == b"continues" for node in state.nodes.values())
    assert all(b"minority-only" not in node.data for node in state.nodes.values())


def test_linearizable_read_fails_without_a_current_term_quorum() -> None:
    group = elected_group(seed=31)
    old_leader = group.probe().leader
    group.isolate(old_leader)

    with pytest.raises(RuntimeError, match="read-index quorum"):
        group.client_read(b"k", ReadLevel.LINEARIZABLE)


def _election_partition_heal_trace(seed: int) -> list[dict[str, object]]:
    group = elected_group(seed=seed)
    old_leader = group.probe().leader
    group.client_write(b"before", b"partition", AckLevel.QUORUM)
    group.isolate(old_leader)
    group.run_until_leader(exclude=old_leader)
    group.client_write(b"during", b"majority", AckLevel.QUORUM)
    group.heal(old_leader)
    group.run_until_converged()
    return group.trace.as_dicts()


def test_same_seed_replays_election_and_partition_heal_exactly() -> None:
    assert _election_partition_heal_trace(37) == _election_partition_heal_trace(37)


def test_local_read_can_observe_a_lagging_raft_follower() -> None:
    group = elected_group(seed=43)
    state = group.probe()
    follower = next(node for node in state.nodes if node != state.leader)
    group.isolate(follower)
    group.client_write(b"k", b"v", AckLevel.QUORUM)

    result = group.client_read(b"k", ReadLevel.LOCAL, node=follower)

    assert result.value is None
    assert result.node == follower


def test_quorum_ack_survives_explicit_unfsynced_loss_injection() -> None:
    group = elected_group(seed=47)
    old_leader = group.probe().leader
    result = group.client_write(b"stable", b"raft", AckLevel.QUORUM)

    group.crash(old_leader, lose_unfsynced=True)
    new_leader = group.run_until_leader(exclude=old_leader)

    assert result.accepted
    assert group.client_read(b"stable", ReadLevel.LINEARIZABLE).value == b"raft"
    assert group.probe().nodes[new_leader].durable_index >= result.offset
