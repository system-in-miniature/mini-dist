from __future__ import annotations

import pytest

from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
from minidist.protocols.isr import CatchUpMode, IsrGroup


def test_all_isr_ack_advances_hw_after_every_current_isr_member() -> None:
    group = IsrGroup(node_ids=("n1", "n2", "n3"), min_insync_replicas=2)

    result = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
    state = group.probe()

    assert result.accepted
    assert state.high_watermark >= result.offset
    assert all(state.nodes[node].log_end_offset >= result.offset for node in state.isr)


def test_slow_replica_leaves_isr_while_two_member_writes_remain_available() -> None:
    group = IsrGroup(
        node_ids=("n1", "n2", "n3"),
        min_insync_replicas=2,
        replica_lag_timeout=2,
    )
    slow = next(node for node in group.probe().nodes if node != group.probe().leader)
    group.set_replica_slow(slow, True)
    for number in range(3):
        assert group.client_write(
            f"k{number}".encode(), str(number).encode(), AckLevel.ALL_ISR
        ).accepted

    state = group.probe()
    assert slow not in state.isr
    assert len(state.isr) == 2


def test_min_insync_rejects_all_isr_write_after_isr_shrinks_too_far() -> None:
    group = IsrGroup(
        node_ids=("n1", "n2", "n3"),
        min_insync_replicas=2,
        replica_lag_timeout=1,
    )
    followers = tuple(node for node in group.probe().nodes if node != group.probe().leader)
    for follower in followers:
        group.set_replica_slow(follower, True)
    group.tick()
    group.tick()

    result = group.client_write(b"k", b"v", AckLevel.ALL_ISR)

    assert not result.accepted
    assert "min.insync" in result.message


def test_recovered_replica_rejoins_isr_only_after_catching_up_to_hw() -> None:
    group = IsrGroup(replica_lag_timeout=1)
    follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    group.set_replica_slow(follower, True)
    group.client_write(b"k", b"v", AckLevel.ALL_ISR)
    group.tick()
    assert follower not in group.probe().isr

    group.set_replica_slow(follower, False)
    group.run_until_idle()

    state = group.probe()
    assert follower in state.isr
    assert state.nodes[follower].log_end_offset >= state.high_watermark


def test_controller_election_increments_epoch_and_fences_old_epoch_append() -> None:
    group = IsrGroup(replication_delay=2)
    old_leader = group.probe().leader
    new_leader = next(node for node in group.probe().isr if node != old_leader)
    old_epoch = group.probe().leader_epoch

    group.client_write(b"old", b"queued", AckLevel.LEADER)
    group.controller_elect(new_leader)
    group.run_until_idle()

    state = group.probe()
    assert state.leader == new_leader
    assert state.leader_epoch == old_epoch + 1
    assert any(
        event.kind == "isr_append_rejected"
        and event.details["reason"] == "stale_leader_epoch"
        for event in group.trace.events
    )


def test_new_epoch_can_replay_committed_old_epoch_prefix_after_heal() -> None:
    group = IsrGroup(
        node_ids=("node-1", "node-2", "node-3", "node-4"),
        replication_delay=1,
    )
    lagging = "node-4"
    group.crash(lagging)
    assert group.client_write(b"before", b"kept", AckLevel.ALL_ISR).accepted
    old_leader = group.probe().leader
    group.isolate(old_leader)
    new_leader = next(node for node in group.probe().isr if node != old_leader)
    group.controller_elect(new_leader)
    assert group.client_write(b"after", b"kept", AckLevel.ALL_ISR).accepted

    group.restart(lagging)
    group.run_until_idle()

    state = group.probe()
    assert state.nodes[lagging].data == state.nodes[new_leader].data
    assert state.nodes[lagging].data == {b"before": b"kept", b"after": b"kept"}


def test_reconnect_is_incremental_with_retained_log_and_full_after_compaction() -> None:
    incremental = IsrGroup(log_retention=3)
    follower = next(
        node for node in incremental.probe().nodes if node != incremental.probe().leader
    )
    incremental.crash(follower)
    incremental.client_write(b"a", b"1", AckLevel.ALL_ISR)
    incremental.client_write(b"b", b"2", AckLevel.ALL_ISR)
    incremental.restart(follower)
    incremental.run_until_idle()
    assert incremental.probe().nodes[follower].catch_up_mode is CatchUpMode.INCREMENTAL

    full = IsrGroup(log_retention=2)
    follower = next(node for node in full.probe().nodes if node != full.probe().leader)
    full.crash(follower)
    for number in range(3):
        full.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.ALL_ISR)
    full.restart(follower)
    full.run_until_idle()
    state = full.probe()
    assert state.nodes[follower].catch_up_mode is CatchUpMode.FULL
    assert state.nodes[follower].data == state.nodes[state.leader].data


def test_local_read_can_be_stale_but_hw_read_returns_committed_value() -> None:
    group = IsrGroup(replication_delay=2)
    follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    group.client_write(b"k", b"v", AckLevel.LEADER)

    assert group.client_read(b"k", ReadLevel.LOCAL, node=follower).value is None
    assert group.client_read(b"k", ReadLevel.LEADER).value == b"v"
    with pytest.raises(RuntimeError, match="high watermark"):
        group.client_read(b"k", ReadLevel.LINEARIZABLE)


@pytest.mark.parametrize("level", [AckLevel.NONE, AckLevel.QUORUM])
def test_isr_rejects_non_kafka_ack_levels(level: AckLevel) -> None:
    group = IsrGroup()
    with pytest.raises(UnsupportedLevelError, match=level.name):
        group.client_write(b"k", b"v", level)


def _trace(seed: int) -> list[dict[str, object]]:
    group = IsrGroup(seed=seed, replica_lag_timeout=1)
    follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    group.set_replica_slow(follower, True)
    group.client_write(b"a", b"1", AckLevel.ALL_ISR)
    group.tick()
    group.set_replica_slow(follower, False)
    group.run_until_idle()
    return group.trace.as_dicts()


def test_same_seed_replays_isr_event_for_event() -> None:
    assert _trace(73) == _trace(73)
