"""Experiment 6: measure every ReadLevel across all four protocols."""

from dataclasses import dataclass

from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
from minidist.protocols.async_primary import AsyncPrimaryGroup
from minidist.protocols.isr import IsrGroup
from minidist.protocols.raft import RaftGroup
from minidist.protocols.wal_shipping import WalShippingGroup


@dataclass(frozen=True, slots=True)
class ReadObservation:
    status: str
    value: bytes | None


def _observe(group: object, level: ReadLevel, node: str | None = None) -> ReadObservation:
    try:
        result = group.client_read(b"k", level, node=node)  # type: ignore[attr-defined]
    except UnsupportedLevelError:
        return ReadObservation("unsupported", None)
    except RuntimeError:
        return ReadObservation("blocked", None)
    return ReadObservation("fresh" if result.value == b"v" else "stale", result.value)


def run_experiment(*, verbose: bool = True) -> dict[str, dict[str, ReadObservation]]:
    matrix: dict[str, dict[str, ReadObservation]] = {}

    async_group = AsyncPrimaryGroup(replica_ids=("lagging",), replication_delay=2)
    async_group.client_write(b"k", b"v", AckLevel.LEADER)
    matrix["async"] = {
        "LOCAL": _observe(async_group, ReadLevel.LOCAL, "lagging"),
        "LEADER": _observe(async_group, ReadLevel.LEADER),
        "LINEARIZABLE": _observe(async_group, ReadLevel.LINEARIZABLE),
    }

    wal_group = WalShippingGroup(standby_ids=("lagging",), replication_delay=2)
    wal_group.client_write(b"k", b"v", AckLevel.LEADER)
    matrix["wal"] = {
        "LOCAL": _observe(wal_group, ReadLevel.LOCAL, "lagging"),
        "LEADER": _observe(wal_group, ReadLevel.LEADER),
        "LINEARIZABLE": _observe(wal_group, ReadLevel.LINEARIZABLE),
    }

    isr_group = IsrGroup(replication_delay=2)
    follower = next(node for node in isr_group.probe().nodes if node != isr_group.probe().leader)
    isr_group.client_write(b"k", b"v", AckLevel.LEADER)
    matrix["isr"] = {
        "LOCAL": _observe(isr_group, ReadLevel.LOCAL, follower),
        "LEADER": _observe(isr_group, ReadLevel.LEADER),
        "LINEARIZABLE": _observe(isr_group, ReadLevel.LINEARIZABLE),
    }

    raft_group = RaftGroup(
        seed=606,
        election_timeout_range=(4, 7),
        heartbeat_interval=1,
    )
    raft_group.run_until_leader()
    follower = next(node for node in raft_group.probe().nodes if node != raft_group.probe().leader)
    raft_group.isolate(follower)
    raft_group.client_write(b"k", b"v", AckLevel.QUORUM)
    matrix["raft"] = {
        "LOCAL": _observe(raft_group, ReadLevel.LOCAL, follower),
        "LEADER": _observe(raft_group, ReadLevel.LEADER),
        "LINEARIZABLE": _observe(raft_group, ReadLevel.LINEARIZABLE),
    }

    if verbose:
        print("实验 6：ReadLevel × 协议")
        for protocol, row in matrix.items():
            summary = ", ".join(f"{level}={result.status}" for level, result in row.items())
            print(f"{protocol}: {summary}")
    return matrix


if __name__ == "__main__":
    run_experiment()
