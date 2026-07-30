from __future__ import annotations

import pytest

from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
from minidist.protocols.wal_shipping import SyncMode, WalShippingGroup


def test_leader_ack_exposes_ordered_logical_wal_bytes_before_standby_apply() -> None:
    group = WalShippingGroup(
        standby_ids=("s1",), replication_delay=2, synchronous_standby_ids=()
    )

    result = group.client_write(b"course", b"MiniDist", AckLevel.LEADER)
    before = group.probe()

    assert result.accepted
    assert before.nodes["primary"].wal_bytes
    assert before.nodes["primary"].applied_lsn == result.offset
    assert before.nodes["s1"].applied_lsn == 0
    group.run_until_idle()
    assert group.client_read(b"course", ReadLevel.LOCAL, node="s1").value == b"MiniDist"


def test_quorum_means_every_configured_synchronous_standby_flushed() -> None:
    group = WalShippingGroup(
        standby_ids=("sync-1", "sync-2", "async-1"),
        synchronous_standby_ids=("sync-1", "sync-2"),
    )

    result = group.client_write(b"k", b"v", AckLevel.QUORUM)
    state = group.probe()

    assert result.accepted
    assert all(state.nodes[node].flushed_lsn >= result.offset for node in ("sync-1", "sync-2"))
    assert state.nodes["primary"].durable_lsn >= result.offset


def test_quorum_write_fails_when_a_synchronous_standby_is_unreachable() -> None:
    group = WalShippingGroup(
        standby_ids=("s1", "s2"),
        synchronous_standby_ids=("s1", "s2"),
        ack_timeout=4,
    )
    group.isolate("s2")

    result = group.client_write(b"k", b"v", AckLevel.QUORUM)

    assert not result.accepted
    assert "synchronous standby" in result.message


@pytest.mark.parametrize("level", [AckLevel.NONE, AckLevel.ALL_ISR])
def test_wal_shipping_rejects_unrelated_ack_levels(level: AckLevel) -> None:
    group = WalShippingGroup()
    with pytest.raises(UnsupportedLevelError, match=level.name):
        group.client_write(b"k", b"v", level)


def test_promotion_increments_timeline_and_rejects_queued_old_timeline_wal() -> None:
    group = WalShippingGroup(standby_ids=("s1", "s2"), replication_delay=2)
    group.client_write(b"old", b"history", AckLevel.LEADER)
    group.tick()
    old_timeline = group.probe().timeline

    group.promote("s1")
    group.run_until_idle()

    state = group.probe()
    assert state.timeline == old_timeline + 1
    assert state.primary == "s1"
    assert any(
        event.kind == "wal_rejected"
        and event.details["reason"] == "stale_timeline"
        for event in group.trace.events
    )


def test_reconnect_uses_incremental_wal_inside_retention_and_full_outside() -> None:
    partial = WalShippingGroup(standby_ids=("s1",), wal_retention=3)
    partial.crash("s1")
    partial.client_write(b"a", b"1", AckLevel.LEADER)
    partial.client_write(b"b", b"2", AckLevel.LEADER)
    partial.restart("s1")
    partial.run_until_idle()
    assert partial.probe().nodes["s1"].sync_mode is SyncMode.INCREMENTAL

    full = WalShippingGroup(standby_ids=("s1",), wal_retention=2)
    full.crash("s1")
    for number in range(3):
        full.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.LEADER)
    full.restart("s1")
    full.run_until_idle()
    state = full.probe()
    assert state.nodes["s1"].sync_mode is SyncMode.FULL
    assert state.nodes["s1"].data == state.nodes[state.primary].data


def test_fsync_loss_rolls_back_only_unflushed_standby_wal() -> None:
    group = WalShippingGroup(standby_ids=("s1",), replication_delay=1)
    group.client_write(b"k", b"v", AckLevel.LEADER)
    group.run_until_idle()
    assert group.probe().nodes["s1"].applied_lsn == 1
    assert group.probe().nodes["s1"].durable_lsn == 0

    group.crash("s1", lose_unfsynced=True)
    group.restart("s1")
    group.run_until_idle()

    assert group.client_read(b"k", ReadLevel.LOCAL, node="s1").value == b"v"
    assert any(event.kind == "wal_fsync_loss" for event in group.trace.events)


def _trace(seed: int) -> list[dict[str, object]]:
    group = WalShippingGroup(
        standby_ids=("s1", "s2"),
        synchronous_standby_ids=("s1",),
        seed=seed,
    )
    group.client_write(b"a", b"1", AckLevel.QUORUM)
    group.isolate("s2")
    group.client_write(b"b", b"2", AckLevel.LEADER)
    group.heal("s2")
    group.run_until_idle()
    return group.trace.as_dicts()


def test_same_seed_replays_wal_shipping_event_for_event() -> None:
    assert _trace(41) == _trace(41)
