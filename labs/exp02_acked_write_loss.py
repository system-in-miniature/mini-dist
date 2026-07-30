"""Experiment 2: contrast async loss with Raft quorum durability."""

import argparse
from dataclasses import dataclass

from minidist.protocols import AckLevel, ReadLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup
from minidist.protocols.raft import RaftGroup


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    acknowledged: bool
    value_before_failover: bytes | None
    value_after_failover: bytes | None
    old_primary: str
    new_primary: str


def run_experiment(
    *,
    protocol: str = "async",
    verbose: bool = True,
) -> ExperimentResult:
    """Ack, immediately crash the leader, fail over, then read the same key."""

    if protocol == "async":
        group = AsyncPrimaryGroup(
            replica_ids=("replica-1",),
            replication_delay=2,
            seed=202,
        )
        old_primary = group.primary
        write = group.client_write(b"order:42", b"confirmed", AckLevel.LEADER)
        before = group.client_read(b"order:42", ReadLevel.LEADER).value

        # No tick occurs here: the client has an ack while the replica is stale.
        group.crash(old_primary)
        group.promote("replica-1")
        group.run_until_idle()
        new_primary = group.primary
        after = group.client_read(b"order:42", ReadLevel.LEADER).value
        title = "异步主从"
        conclusion = "已确认写丢失"
    elif protocol == "raft":
        group = RaftGroup(
            node_ids=("node-1", "node-2", "node-3"),
            seed=202,
            election_timeout_range=(4, 7),
            heartbeat_interval=1,
        )
        old_primary = group.run_until_leader()
        write = group.client_write(b"order:42", b"confirmed", AckLevel.QUORUM)
        before = group.client_read(b"order:42", ReadLevel.LEADER).value

        # QUORUM returned only after a majority stored the entry.
        group.crash(old_primary)
        new_primary = group.run_until_leader(exclude=old_primary)
        after = group.client_read(b"order:42", ReadLevel.LINEARIZABLE).value
        title = "Raft"
        conclusion = "多数派确认的写在换主后仍在"
    else:
        raise ValueError("protocol must be 'async' or 'raft'")

    result = ExperimentResult(
        acknowledged=write.accepted,
        value_before_failover=before,
        value_after_failover=after,
        old_primary=old_primary,
        new_primary=new_primary,
    )

    if verbose:
        print(f"实验 2：ack 后立即 crash leader（{title}）")
        print(
            "1) client 写入 order:42=confirmed；"
            f"leader 在 offset={write.offset} 返回 ack={write.accepted}。"
        )
        print(f"2) 故障前从 leader 读取：{before!r}。")
        print(f"3) ack 后立即 crash {old_primary}，随后完成 failover。")
        print(
            f"4) 新 leader={result.new_primary}，读取 order:42：{after!r}。"
        )
        print(f"结论：{conclusion}。")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=("async", "raft"), default="async")
    args = parser.parse_args()
    run_experiment(protocol=args.protocol)
