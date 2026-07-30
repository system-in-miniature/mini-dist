from __future__ import annotations

import pytest

from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
from minidist.protocols.async_primary import AsyncPrimaryGroup, SyncMode


def test_write_acks_before_replica_converges() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)

    result = group.client_write(b"course", b"MiniDist", AckLevel.LEADER)

    assert result.accepted
    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
    group.tick()
    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
    group.tick()
    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value == b"MiniDist"


@pytest.mark.parametrize("level", [AckLevel.QUORUM, AckLevel.ALL_ISR])
def test_async_primary_rejects_stronger_ack_levels(level: AckLevel) -> None:
    group = AsyncPrimaryGroup()
    with pytest.raises(UnsupportedLevelError, match=level.name):
        group.client_write(b"k", b"v", level)


def test_async_primary_rejects_linearizable_reads() -> None:
    group = AsyncPrimaryGroup()
    with pytest.raises(UnsupportedLevelError, match="LINEARIZABLE"):
        group.client_read(b"k", ReadLevel.LINEARIZABLE)


def test_restart_uses_partial_resync_inside_backlog() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=3, replication_delay=1)
    group.client_write(b"a", b"1", AckLevel.LEADER)
    group.tick()
    group.crash("r1")
    group.client_write(b"b", b"2", AckLevel.LEADER)
    group.client_write(b"c", b"3", AckLevel.LEADER)

    group.restart("r1")
    group.run_until_idle()

    state = group.probe()
    assert state.nodes["r1"].sync_mode is SyncMode.PARTIAL
    assert state.nodes["r1"].data == {b"a": b"1", b"b": b"2", b"c": b"3"}


def test_restart_uses_full_resync_outside_backlog() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=2, replication_delay=1)
    group.crash("r1")
    for number in range(3):
        group.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.LEADER)

    group.restart("r1")
    group.run_until_idle()

    state = group.probe()
    assert state.nodes["r1"].sync_mode is SyncMode.FULL
    assert state.nodes["r1"].data == state.nodes[state.primary].data


def test_manual_promotion_can_lose_acknowledged_write() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    result = group.client_write(b"paid", b"yes", AckLevel.LEADER)

    group.crash("primary")
    group.promote("r1")
    group.run_until_idle()

    assert result.accepted
    assert group.client_read(b"paid", ReadLevel.LEADER).value is None
    assert group.probe().primary == "r1"
