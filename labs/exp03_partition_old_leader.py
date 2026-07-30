"""Experiment 3: contrast async split brain with Raft term fencing."""

import argparse
from dataclasses import dataclass

from minidist.protocols import AckLevel, ReadLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup
from minidist.protocols.raft import RaftGroup


@dataclass(frozen=True, slots=True)
class AsyncExperimentResult:
    old_primary: str
    new_primary: str
    old_primary_write_accepted: bool
    new_primary_write_accepted: bool
    old_primary_value_during_partition: bytes | None
    old_primary_new_value_during_partition: bytes | None
    new_primary_value_during_partition: bytes | None
    new_primary_old_value_during_partition: bytes | None
    converged_after_heal: bool
    old_dirty_value_after_heal: bytes | None
    new_dirty_value_after_heal: bytes | None


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    old_leader: str
    new_leader: str
    old_term: int
    new_term: int
    old_leader_write_committed: bool
    new_leader_write_committed: bool
    old_leader_role_after_heal: str
    logs_converged: bool
    minority_value: bytes | None
    majority_value: bytes | None


def run_experiment(
    *,
    protocol: str = "raft",
    verbose: bool = True,
) -> AsyncExperimentResult | ExperimentResult:
    """Run one partition protocol using only its public group API."""

    if protocol == "async":
        return _run_async_experiment(verbose=verbose)
    if protocol != "raft":
        raise ValueError("protocol must be 'async' or 'raft'")
    return _run_raft_experiment(verbose=verbose)


def _run_async_experiment(*, verbose: bool) -> AsyncExperimentResult:
    group = AsyncPrimaryGroup(
        replica_ids=("replica-1", "replica-2"),
        replication_delay=1,
        seed=303,
    )
    old_primary = group.primary
    group.isolate(old_primary)

    old_write = group.client_write(
        b"old-primary-dirty",
        b"old-primary",
        AckLevel.LEADER,
    )
    group.promote("replica-1")
    new_primary = group.primary
    new_write = group.client_write(
        b"new-primary-dirty",
        b"new-primary",
        AckLevel.LEADER,
    )
    group.run_until_idle()

    old_old_value = group.client_read(
        b"old-primary-dirty",
        ReadLevel.LOCAL,
        node=old_primary,
    ).value
    old_new_value = group.client_read(
        b"new-primary-dirty",
        ReadLevel.LOCAL,
        node=old_primary,
    ).value
    new_new_value = group.client_read(
        b"new-primary-dirty",
        ReadLevel.LEADER,
    ).value
    new_old_value = group.client_read(
        b"old-primary-dirty",
        ReadLevel.LEADER,
    ).value

    group.heal(old_primary)
    group.run_until_idle()
    state = group.probe()
    reference = state.nodes[new_primary]
    result = AsyncExperimentResult(
        old_primary=old_primary,
        new_primary=new_primary,
        old_primary_write_accepted=old_write.accepted,
        new_primary_write_accepted=new_write.accepted,
        old_primary_value_during_partition=old_old_value,
        old_primary_new_value_during_partition=old_new_value,
        new_primary_value_during_partition=new_new_value,
        new_primary_old_value_during_partition=new_old_value,
        converged_after_heal=all(
            node.data == reference.data
            and node.offset == reference.offset
            and node.replication_id == reference.replication_id
            for node in state.nodes.values()
        ),
        old_dirty_value_after_heal=reference.data.get(b"old-primary-dirty"),
        new_dirty_value_after_heal=reference.data.get(b"new-primary-dirty"),
    )

    if verbose:
        print("实验 3：异步主从的双主脏写")
        print(
            f"1) 隔离旧 primary {old_primary}；其本地写 ack="
            f"{result.old_primary_write_accepted}。"
        )
        print(
            f"2) 显式 promote {new_primary}；新 primary 本地写 ack="
            f"{result.new_primary_write_accepted}。"
        )
        print(
            "3) 分区中两侧状态："
            f"旧侧 old={old_old_value!r}, new={old_new_value!r}；"
            f"新侧 old={new_old_value!r}, new={new_new_value!r}。"
        )
        print(
            "4) 愈合后按新 primary 世代全量收敛："
            f"converged={result.converged_after_heal}, "
            f"old={result.old_dirty_value_after_heal!r}, "
            f"new={result.new_dirty_value_after_heal!r}。"
        )
        print("结论：协议 1 没有任期 fencing；分区中两侧都可确认本地脏写。")
    return result


def _run_raft_experiment(*, verbose: bool) -> ExperimentResult:
    """Run the minority-old-leader partition and return assertion-friendly facts."""

    group = RaftGroup(
        node_ids=("node-1", "node-2", "node-3"),
        seed=303,
        election_timeout_range=(4, 7),
        heartbeat_interval=1,
    )
    old_leader = group.run_until_leader()
    old_term = group.probe().nodes[old_leader].current_term
    group.isolate(old_leader)

    minority_write = group.client_write(
        b"minority-only",
        b"must-not-commit",
        AckLevel.QUORUM,
    )
    new_leader = group.run_until_leader(exclude=old_leader)
    majority_write = group.client_write(
        b"majority",
        b"continues",
        AckLevel.QUORUM,
    )
    new_term = group.probe().nodes[new_leader].current_term

    group.heal(old_leader)
    group.run_until_converged()
    state = group.probe()
    reference_log = state.nodes[new_leader].log
    result = ExperimentResult(
        old_leader=old_leader,
        new_leader=new_leader,
        old_term=old_term,
        new_term=new_term,
        old_leader_write_committed=minority_write.accepted,
        new_leader_write_committed=majority_write.accepted,
        old_leader_role_after_heal=state.nodes[old_leader].role.value,
        logs_converged=all(node.log == reference_log for node in state.nodes.values()),
        minority_value=state.nodes[new_leader].data.get(b"minority-only"),
        majority_value=state.nodes[new_leader].data.get(b"majority"),
    )

    if verbose:
        print("实验 3：旧 leader 位于少数派的 Raft 网络分区")
        print(
            f"1) term={old_term} 的旧 leader {old_leader} 被双向隔离；"
            f"其新写 QUORUM ack={minority_write.accepted}。"
        )
        print(
            f"2) 多数派在 term={new_term} 选出 {new_leader}；"
            f"继续写入的 QUORUM ack={majority_write.accepted}。"
        )
        print(
            f"3) 分区愈合后旧 leader 角色={result.old_leader_role_after_heal}；"
            f"日志收敛={result.logs_converged}。"
        )
        print(
            "4) 最终状态："
            f"minority-only={result.minority_value!r}, "
            f"majority={result.majority_value!r}。"
        )
        print("结论：高任期完成 fencing，少数派未提交分叉被新 leader 日志覆盖。")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        choices=("async", "raft", "both"),
        default="both",
    )
    args = parser.parse_args()
    protocols = ("async", "raft") if args.protocol == "both" else (args.protocol,)
    for index, selected_protocol in enumerate(protocols):
        if index:
            print()
        run_experiment(protocol=selected_protocol)
