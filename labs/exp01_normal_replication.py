"""Experiment 1: compare normal async and Raft replication convergence."""

import argparse
from dataclasses import dataclass

from minidist.protocols import AckLevel, ReadLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup
from minidist.protocols.raft import RaftGroup


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    before_tick: bytes | None
    after_tick: bytes | None
    primary_offset: int
    replica_offset: int


def run_experiment(
    *,
    protocol: str = "async",
    verbose: bool = True,
) -> ExperimentResult:
    """Run one protocol using only its public replication-group API."""

    if protocol == "async":
        group = AsyncPrimaryGroup(
            replica_ids=("replica-1",),
            replication_delay=1,
            seed=101,
        )
        write = group.client_write(b"project", b"MiniDist", AckLevel.LEADER)
        before = group.client_read(
            b"project", ReadLevel.LOCAL, node="replica-1"
        ).value
        group.tick()
        after = group.client_read(
            b"project", ReadLevel.LOCAL, node="replica-1"
        ).value
        state = group.probe()
        primary_offset = state.nodes[state.primary].offset
        replica_offset = state.nodes["replica-1"].offset
        title = "异步主从"
        ack_note = "primary 立即 ack，复制仍在途中"
    elif protocol == "raft":
        group = RaftGroup(
            node_ids=("node-1", "node-2", "node-3"),
            seed=101,
            election_timeout_range=(4, 7),
            heartbeat_interval=1,
        )
        group.run_until_leader()
        write = group.client_write(b"project", b"MiniDist", AckLevel.QUORUM)
        state = group.probe()
        follower = next(node for node in state.nodes if node != state.leader)
        before = state.nodes[follower].data.get(b"project")
        group.run_until_converged()
        state = group.probe()
        after = state.nodes[follower].data.get(b"project")
        primary_offset = state.nodes[state.leader].commit_index
        replica_offset = state.nodes[follower].commit_index
        title = "Raft"
        ack_note = "多数派复制后才 ack，随后 leaderCommit 传播到全部 follower"
    else:
        raise ValueError("protocol must be 'async' or 'raft'")

    result = ExperimentResult(
        before_tick=before,
        after_tick=after,
        primary_offset=primary_offset,
        replica_offset=replica_offset,
    )

    if verbose:
        print(f"实验 1：正常复制（{title}）")
        print(f"1) 写入在 offset={write.offset} 返回 ack；{ack_note}。")
        print(f"2) ack 边界处，观察 follower 状态机：{before!r}。")
        print(f"3) 推进至收敛后，观察 follower 状态机：{after!r}。")
        print(
            "4) 最终 offset："
            f"leader={result.primary_offset}, "
            f"follower={result.replica_offset}；副本已收敛。"
        )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=("async", "raft"), default="async")
    args = parser.parse_args()
    run_experiment(protocol=args.protocol)
