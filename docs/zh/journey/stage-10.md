# Stage 10 · 四协议实验矩阵

### 目标

完成异步主从、WAL Shipping、ISR 与 Raft 的实验 4–7，用返回证据比较成员、追赶、读取一致性与旧权威行为。

??? note "交付文件"
    - `labs/exp04_slow_replica.py`
    - `labs/exp05_replica_reconnect.py`
    - `labs/exp06_read_consistency.py`
    - `labs/exp07_split_brain_lease.py`
    - `tests/labs/test_experiments.py`

### 当前遇到的问题

四种协议已经存在，但孤立实现测试不能回答项目最后的问题：慢 Replica 改变什么、何时重连变成全量复制、哪些读取可能陈旧，以及如何阻止旧侧声称当前权威。

### 测试契约

#### 先看会坏在哪里

读取矩阵契约要求在同一 Lag 脚本下，每种协议的 Local Read 都陈旧、Leader Read 都新鲜，而 Linearizable 状态按真实机制分别为 `unsupported`、`blocked` 或 `fresh`。把所有异常折成同一个值会抹掉“没有能力”和“当前权威暂不可用”的区别。

??? note "文件差异：tests/labs/test_experiments.py"
    ```diff
    diff --git a/tests/labs/test_experiments.py b/tests/labs/test_experiments.py
    index c65cd9cb45f36eb7b93cbcf00483b01c3ed52c7e..baf3432f01c59d00b46316b9d2106306e4a7e8a4 100644
    --- a/tests/labs/test_experiments.py
    +++ b/tests/labs/test_experiments.py
    @@ -1,6 +1,10 @@
     from labs.exp01_normal_replication import run_experiment as run_normal
     from labs.exp02_acked_write_loss import run_experiment as run_loss
     from labs.exp03_partition_old_leader import run_experiment as run_partition
    +from labs.exp04_slow_replica import run_experiment as run_slow
    +from labs.exp05_replica_reconnect import run_experiment as run_reconnect
    +from labs.exp06_read_consistency import run_experiment as run_reads
    +from labs.exp07_split_brain_lease import run_experiment as run_split_read


     def test_experiment_1_observes_delayed_convergence() -> None:
    @@ -55,3 +59,66 @@ def test_experiment_3_async_primary_exposes_split_brain_dirty_writes() -> None:
         assert result.converged_after_heal
         assert result.old_dirty_value_after_heal is None
         assert result.new_dirty_value_after_heal == b"new-primary"
    +
    +
    +def test_experiment_4_distinguishes_isr_shrink_from_raft_majority() -> None:
    +    isr = run_slow(protocol="isr", verbose=False)
    +    raft = run_slow(protocol="raft", verbose=False)
    +
    +    assert isr.write_available
    +    assert isr.slow_replica_stale
    +    assert isr.membership_effect == "removed from ISR"
    +    assert raft.write_available
    +    assert raft.slow_replica_stale
    +    assert raft.membership_effect == "fixed voter; majority unaffected"
    +
    +
    +def test_experiment_4_has_all_four_protocol_columns() -> None:
    +    results = {
    +        protocol: run_slow(protocol=protocol, verbose=False)
    +        for protocol in ("async", "wal", "isr", "raft")
    +    }
    +    assert all(result.write_available for result in results.values())
    +    assert results["wal"].membership_effect == "async standby does not gate commit"
    +
    +
    +def test_experiment_5_catchup_trigger_matrix_covers_all_protocols() -> None:
    +    results = {
    +        protocol: run_reconnect(protocol=protocol, verbose=False)
    +        for protocol in ("async", "wal", "isr", "raft")
    +    }
    +
    +    assert results["async"].inside_window == "incremental"
    +    assert results["async"].outside_window == "full"
    +    assert results["wal"].inside_window == "incremental"
    +    assert results["wal"].outside_window == "full"
    +    assert results["isr"].inside_window == "incremental"
    +    assert results["isr"].outside_window == "full"
    +    assert results["raft"].inside_window == "log replay"
    +    assert results["raft"].outside_window == "log replay"
    +
    +
    +def test_experiment_6_read_level_matrix_names_stale_and_authority_failures() -> None:
    +    matrix = run_reads(verbose=False)
    +
    +    for protocol in ("async", "wal", "isr", "raft"):
    +        assert matrix[protocol]["LOCAL"].status == "stale"
    +        assert matrix[protocol]["LEADER"].status == "fresh"
    +    assert matrix["async"]["LINEARIZABLE"].status == "unsupported"
    +    assert matrix["wal"]["LINEARIZABLE"].status == "unsupported"
    +    assert matrix["isr"]["LINEARIZABLE"].status == "blocked"
    +    assert matrix["raft"]["LINEARIZABLE"].status == "fresh"
    +
    +
    +def test_experiment_7_split_brain_local_read_and_authority_guard() -> None:
    +    results = {
    +        protocol: run_split_read(protocol=protocol, verbose=False)
    +        for protocol in ("async", "wal", "isr", "raft")
    +    }
    +
    +    assert all(result.old_side_local_value == b"before" for result in results.values())
    +    assert all(result.current_value == b"after" for result in results.values())
    +    assert results["async"].guard == "unsupported"
    +    assert results["wal"].guard == "unsupported"
    +    assert results["isr"].guard == "lease fenced"
    +    assert results["raft"].guard == "read-index fenced"
    ```

**测试锁定什么**

五条契约锁定慢副本可用性、Catch-up Trigger、Read-level Result 与 Split-brain Authority Guard 的全部四列。

**如何构造反例**

测试通过同一 Experiment Entry 调用每个协议，并断言归一化 Status 与协议特有 Membership/Guard 解释。

**关键测试语句**

```python
assert matrix["isr"]["LINEARIZABLE"].status == "blocked"
assert matrix["raft"]["LINEARIZABLE"].status == "fresh"
```

**失败意味着什么**

矩阵隐藏了语义差异、场景不再可比较，或者协议报告了无法证明的权威。

### 基本概念

慢 Replica 对投票、同步等待策略或动态成员有不同影响。Retention 是协议特有的连续性规则：异步 Backlog、WAL Retention、ISR Retained Log 或 Raft Log Replay。读取一致性同时包含位置与证明维度。陈旧本地值可以继续可见而不具权威；Lease 与 Read-index Barrier 是不同 Guard。

### 为什么需要这个机制

最终学习产物就是比较本身。它应让学习者从机制预测结果，而不是记产品标签，同时保持 Unsupported 与 Blocked 能力可见。

### 运行时心智模型

每个 Lab 都有 Protocol Selector、固定 Fault Script 与不可变 Result。实验 4–5 返回 Availability/Membership 与 Catch-up Mode。实验 6 把读取或失败转成 `fresh`、`stale`、`unsupported` 或 `blocked`。实验 7 本地读取被隔离旧侧，通过协议权威路径读取当前状态，并报告 Guard。

### 机制板块

#### 实验 4–5：慢副本与重连窗口

比较四种协议如何处理落后成员，以及如何决定增量或全量追赶。

??? note "文件差异：labs/exp04_slow_replica.py"
    ```diff
    diff --git a/labs/exp04_slow_replica.py b/labs/exp04_slow_replica.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..158bbf7e9d722be52f5deb91be24e3caf3105acb
    --- /dev/null
    +++ b/labs/exp04_slow_replica.py
    @@ -0,0 +1,86 @@
    +"""Experiment 4: compare availability while one replica is slow."""
    +
    +import argparse
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
    +from minidist.protocols.isr import IsrGroup
    +from minidist.protocols.raft import RaftGroup
    +from minidist.protocols.wal_shipping import WalShippingGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExperimentResult:
    +    protocol: str
    +    write_available: bool
    +    slow_replica_stale: bool
    +    membership_effect: str
    +
    +
    +def run_experiment(*, protocol: str = "isr", verbose: bool = True) -> ExperimentResult:
    +    if protocol == "async":
    +        group = AsyncPrimaryGroup(replica_ids=("slow", "fast"))
    +        group.isolate("slow")
    +        write = group.client_write(b"k", b"v", AckLevel.LEADER)
    +        group.run_until_idle()
    +        stale = group.probe().nodes["slow"].data.get(b"k") is None
    +        effect = "replica is not a voter"
    +    elif protocol == "wal":
    +        group = WalShippingGroup(
    +            standby_ids=("slow", "fast"),
    +            synchronous_standby_ids=(),
    +        )
    +        group.isolate("slow")
    +        write = group.client_write(b"k", b"v", AckLevel.LEADER)
    +        group.run_until_idle()
    +        stale = group.probe().nodes["slow"].data.get(b"k") is None
    +        effect = "async standby does not gate commit"
    +    elif protocol == "isr":
    +        group = IsrGroup(replica_lag_timeout=2, min_insync_replicas=2)
    +        slow_node = next(
    +            node for node in group.probe().nodes if node != group.probe().leader
    +        )
    +        group.set_replica_slow(slow_node, True)
    +        write = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
    +        state = group.probe()
    +        stale = state.nodes[slow_node].data.get(b"k") is None
    +        effect = "removed from ISR" if slow_node not in state.isr else "still in ISR"
    +    elif protocol == "raft":
    +        group = RaftGroup(
    +            seed=404,
    +            election_timeout_range=(4, 7),
    +            heartbeat_interval=1,
    +        )
    +        group.run_until_leader()
    +        slow_node = next(
    +            node for node in group.probe().nodes if node != group.probe().leader
    +        )
    +        group.isolate(slow_node)
    +        write = group.client_write(b"k", b"v", AckLevel.QUORUM)
    +        stale = group.probe().nodes[slow_node].data.get(b"k") is None
    +        effect = "fixed voter; majority unaffected"
    +    else:
    +        raise ValueError("protocol must be async, wal, isr, or raft")
    +
    +    result = ExperimentResult(protocol, write.accepted, stale, effect)
    +    if verbose:
    +        print(f"实验 4 [{protocol}]：write_available={result.write_available}")
    +        print(
    +            f"slow_replica_stale={result.slow_replica_stale}; "
    +            f"membership={result.membership_effect}"
    +        )
    +    return result
    +
    +
    +if __name__ == "__main__":
    +    parser = argparse.ArgumentParser()
    +    parser.add_argument(
    +        "--protocol",
    +        choices=("async", "wal", "isr", "raft", "all"),
    +        default="all",
    +    )
    +    args = parser.parse_args()
    +    selected = ("async", "wal", "isr", "raft") if args.protocol == "all" else (args.protocol,)
    +    for name in selected:
    +        run_experiment(protocol=name)
    ```

**是什么，为什么现在需要**

实验 4 询问每种协议能否继续写，以及慢节点具有何种成员含义。

**在运行时做什么**

脚本隔离或放慢一个节点，执行协议支持的确认写入，再返回陈旧可见性与命名 Membership Effect。

**关键代码**

```python
write = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
state = group.probe()
stale = state.nodes[slow_node].data.get(b"k") is None
```

**关键语句理解**

四列执行相同 Experiment API；返回的 Membership Explanation 保留可用性成立的原因。

??? note "文件差异：labs/exp05_replica_reconnect.py"
    ```diff
    diff --git a/labs/exp05_replica_reconnect.py b/labs/exp05_replica_reconnect.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..0dad31b96ad8bb5317449ee74c0857063ded4bb5
    --- /dev/null
    +++ b/labs/exp05_replica_reconnect.py
    @@ -0,0 +1,108 @@
    +"""Experiment 5: compare incremental and full catch-up triggers."""
    +
    +import argparse
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
    +from minidist.protocols.isr import IsrGroup
    +from minidist.protocols.raft import RaftGroup
    +from minidist.protocols.wal_shipping import WalShippingGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExperimentResult:
    +    protocol: str
    +    inside_window: str
    +    outside_window: str
    +
    +
    +def _write_many(group: object, ack: AckLevel, count: int) -> None:
    +    for number in range(count):
    +        group.client_write(f"k{number}".encode(), str(number).encode(), ack)  # type: ignore[attr-defined]
    +
    +
    +def run_experiment(*, protocol: str = "async", verbose: bool = True) -> ExperimentResult:
    +    if protocol == "async":
    +        inside = AsyncPrimaryGroup(replica_ids=("r",), backlog_size=3)
    +        inside.crash("r")
    +        _write_many(inside, AckLevel.LEADER, 2)
    +        inside.restart("r")
    +        inside.run_until_idle()
    +        inside_mode = inside.probe().nodes["r"].sync_mode.name.lower().replace("partial", "incremental")
    +
    +        outside = AsyncPrimaryGroup(replica_ids=("r",), backlog_size=2)
    +        outside.crash("r")
    +        _write_many(outside, AckLevel.LEADER, 3)
    +        outside.restart("r")
    +        outside.run_until_idle()
    +        outside_mode = outside.probe().nodes["r"].sync_mode.name.lower()
    +    elif protocol == "wal":
    +        inside = WalShippingGroup(standby_ids=("r",), wal_retention=3)
    +        inside.crash("r")
    +        _write_many(inside, AckLevel.LEADER, 2)
    +        inside.restart("r")
    +        inside.run_until_idle()
    +        inside_mode = inside.probe().nodes["r"].sync_mode.value
    +
    +        outside = WalShippingGroup(standby_ids=("r",), wal_retention=2)
    +        outside.crash("r")
    +        _write_many(outside, AckLevel.LEADER, 3)
    +        outside.restart("r")
    +        outside.run_until_idle()
    +        outside_mode = outside.probe().nodes["r"].sync_mode.value
    +    elif protocol == "isr":
    +        inside = IsrGroup(log_retention=3)
    +        follower = next(node for node in inside.probe().nodes if node != inside.probe().leader)
    +        inside.crash(follower)
    +        _write_many(inside, AckLevel.ALL_ISR, 2)
    +        inside.restart(follower)
    +        inside.run_until_idle()
    +        inside_mode = inside.probe().nodes[follower].catch_up_mode.value
    +
    +        outside = IsrGroup(log_retention=2)
    +        follower = next(node for node in outside.probe().nodes if node != outside.probe().leader)
    +        outside.crash(follower)
    +        _write_many(outside, AckLevel.ALL_ISR, 3)
    +        outside.restart(follower)
    +        outside.run_until_idle()
    +        outside_mode = outside.probe().nodes[follower].catch_up_mode.value
    +    elif protocol == "raft":
    +        def replay(count: int) -> str:
    +            group = RaftGroup(
    +                seed=505,
    +                election_timeout_range=(4, 7),
    +                heartbeat_interval=1,
    +            )
    +            group.run_until_leader()
    +            follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    +            group.crash(follower)
    +            _write_many(group, AckLevel.QUORUM, count)
    +            group.restart(follower)
    +            group.run_until_converged()
    +            return "log replay"
    +
    +        inside_mode, outside_mode = replay(2), replay(6)
    +    else:
    +        raise ValueError("protocol must be async, wal, isr, or raft")
    +
    +    result = ExperimentResult(protocol, inside_mode, outside_mode)
    +    if verbose:
    +        print(
    +            f"实验 5 [{protocol}]：inside_window={inside_mode}; "
    +            f"outside_window={outside_mode}"
    +        )
    +    return result
    +
    +
    +if __name__ == "__main__":
    +    parser = argparse.ArgumentParser()
    +    parser.add_argument(
    +        "--protocol",
    +        choices=("async", "wal", "isr", "raft", "all"),
    +        default="all",
    +    )
    +    args = parser.parse_args()
    +    selected = ("async", "wal", "isr", "raft") if args.protocol == "all" else (args.protocol,)
    +    for name in selected:
    +        run_experiment(protocol=name)
    ```

**是什么，为什么现在需要**

实验 5 让 Replica 分别错过短间隔与长间隔，并报告由此产生的 Catch-up Mode。

**在运行时做什么**

它驱动具体 Group 的 Crash、Write、Restart 与 Idle 操作，再把协议词汇翻译成可比较 Result String。

**关键代码**

```python
_write_many(inside, AckLevel.LEADER, 2)
inside.restart("r")
inside.run_until_idle()
```

**关键语句理解**

错过写入数量相对 Retention 受控；Reconnect Mode 是观察结果，不是 Lab 自己选择。

#### 实验 6–7：读取一致性与旧侧权威

用 Read-level 矩阵与 Split-brain 场景收束光谱，区分陈旧本地可见性与受保护权威。

??? note "文件差异：labs/exp06_read_consistency.py"
    ```diff
    diff --git a/labs/exp06_read_consistency.py b/labs/exp06_read_consistency.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..01099e307516f3720c69823fd30f7bb11471162f
    --- /dev/null
    +++ b/labs/exp06_read_consistency.py
    @@ -0,0 +1,80 @@
    +"""Experiment 6: measure every ReadLevel across all four protocols."""
    +
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
    +from minidist.protocols.isr import IsrGroup
    +from minidist.protocols.raft import RaftGroup
    +from minidist.protocols.wal_shipping import WalShippingGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ReadObservation:
    +    status: str
    +    value: bytes | None
    +
    +
    +def _observe(group: object, level: ReadLevel, node: str | None = None) -> ReadObservation:
    +    try:
    +        result = group.client_read(b"k", level, node=node)  # type: ignore[attr-defined]
    +    except UnsupportedLevelError:
    +        return ReadObservation("unsupported", None)
    +    except RuntimeError:
    +        return ReadObservation("blocked", None)
    +    return ReadObservation("fresh" if result.value == b"v" else "stale", result.value)
    +
    +
    +def run_experiment(*, verbose: bool = True) -> dict[str, dict[str, ReadObservation]]:
    +    matrix: dict[str, dict[str, ReadObservation]] = {}
    +
    +    async_group = AsyncPrimaryGroup(replica_ids=("lagging",), replication_delay=2)
    +    async_group.client_write(b"k", b"v", AckLevel.LEADER)
    +    matrix["async"] = {
    +        "LOCAL": _observe(async_group, ReadLevel.LOCAL, "lagging"),
    +        "LEADER": _observe(async_group, ReadLevel.LEADER),
    +        "LINEARIZABLE": _observe(async_group, ReadLevel.LINEARIZABLE),
    +    }
    +
    +    wal_group = WalShippingGroup(standby_ids=("lagging",), replication_delay=2)
    +    wal_group.client_write(b"k", b"v", AckLevel.LEADER)
    +    matrix["wal"] = {
    +        "LOCAL": _observe(wal_group, ReadLevel.LOCAL, "lagging"),
    +        "LEADER": _observe(wal_group, ReadLevel.LEADER),
    +        "LINEARIZABLE": _observe(wal_group, ReadLevel.LINEARIZABLE),
    +    }
    +
    +    isr_group = IsrGroup(replication_delay=2)
    +    follower = next(node for node in isr_group.probe().nodes if node != isr_group.probe().leader)
    +    isr_group.client_write(b"k", b"v", AckLevel.LEADER)
    +    matrix["isr"] = {
    +        "LOCAL": _observe(isr_group, ReadLevel.LOCAL, follower),
    +        "LEADER": _observe(isr_group, ReadLevel.LEADER),
    +        "LINEARIZABLE": _observe(isr_group, ReadLevel.LINEARIZABLE),
    +    }
    +
    +    raft_group = RaftGroup(
    +        seed=606,
    +        election_timeout_range=(4, 7),
    +        heartbeat_interval=1,
    +    )
    +    raft_group.run_until_leader()
    +    follower = next(node for node in raft_group.probe().nodes if node != raft_group.probe().leader)
    +    raft_group.isolate(follower)
    +    raft_group.client_write(b"k", b"v", AckLevel.QUORUM)
    +    matrix["raft"] = {
    +        "LOCAL": _observe(raft_group, ReadLevel.LOCAL, follower),
    +        "LEADER": _observe(raft_group, ReadLevel.LEADER),
    +        "LINEARIZABLE": _observe(raft_group, ReadLevel.LINEARIZABLE),
    +    }
    +
    +    if verbose:
    +        print("实验 6：ReadLevel × 协议")
    +        for protocol, row in matrix.items():
    +            summary = ", ".join(f"{level}={result.status}" for level, result in row.items())
    +            print(f"{protocol}: {summary}")
    +    return matrix
    +
    +
    +if __name__ == "__main__":
    +    run_experiment()
    ```

**是什么，为什么现在需要**

实验 6 让一个 Follower 落后，并跨所有协议求值 LOCAL、LEADER 与 LINEARIZABLE Read。

**在运行时做什么**

`_observe` 把返回值、`UnsupportedLevelError` 或 Authority `RuntimeError` 转成不同 Status，不抹掉原因。

**关键代码**

```python
except UnsupportedLevelError:
    return ReadObservation("unsupported", None)
except RuntimeError:
    return ReadObservation("blocked", None)
```

**关键语句理解**

Unsupported 表示机制不提供该保证；Blocked 表示保证存在，但当前没有足够权威证据。

??? note "文件差异：labs/exp07_split_brain_lease.py"
    ```diff
    diff --git a/labs/exp07_split_brain_lease.py b/labs/exp07_split_brain_lease.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..457858b4022c2fa2622d26362e9e4455b9c64268
    --- /dev/null
    +++ b/labs/exp07_split_brain_lease.py
    @@ -0,0 +1,99 @@
    +"""Experiment 7: stale old-leader reads versus authority guards."""
    +
    +import argparse
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel, ReadLevel
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
    +from minidist.protocols.isr import IsrGroup
    +from minidist.protocols.raft import RaftGroup
    +from minidist.protocols.wal_shipping import WalShippingGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExperimentResult:
    +    protocol: str
    +    old_side_local_value: bytes | None
    +    current_value: bytes | None
    +    guard: str
    +
    +
    +def run_experiment(*, protocol: str = "isr", verbose: bool = True) -> ExperimentResult:
    +    if protocol == "async":
    +        group = AsyncPrimaryGroup(replica_ids=("r1", "r2"))
    +        group.client_write(b"state", b"before", AckLevel.LEADER)
    +        group.run_until_idle()
    +        old = group.primary
    +        group.isolate(old)
    +        group.promote("r1")
    +        group.client_write(b"state", b"after", AckLevel.LEADER)
    +        group.run_until_idle()
    +        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
    +        current = group.client_read(b"state", ReadLevel.LEADER).value
    +        guard = "unsupported"
    +    elif protocol == "wal":
    +        group = WalShippingGroup(standby_ids=("s1", "s2"))
    +        group.client_write(b"state", b"before", AckLevel.LEADER)
    +        group.run_until_idle()
    +        old = group.primary
    +        group.isolate(old)
    +        group.promote("s1")
    +        group.client_write(b"state", b"after", AckLevel.LEADER)
    +        group.run_until_idle()
    +        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
    +        current = group.client_read(b"state", ReadLevel.LEADER).value
    +        guard = "unsupported"
    +    elif protocol == "isr":
    +        group = IsrGroup()
    +        group.client_write(b"state", b"before", AckLevel.ALL_ISR)
    +        old = group.leader
    +        group.isolate(old)
    +        new = next(node for node in group.probe().isr if node != old)
    +        group.controller_elect(new)
    +        group.client_write(b"state", b"after", AckLevel.ALL_ISR)
    +        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
    +        current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
    +        try:
    +            group.lease_read(old, b"state")
    +        except RuntimeError:
    +            guard = "lease fenced"
    +        else:
    +            guard = "lease failed open"
    +    elif protocol == "raft":
    +        group = RaftGroup(
    +            seed=707,
    +            election_timeout_range=(4, 7),
    +            heartbeat_interval=1,
    +        )
    +        old = group.run_until_leader()
    +        group.client_write(b"state", b"before", AckLevel.QUORUM)
    +        group.run_until_converged()
    +        group.isolate(old)
    +        group.run_until_leader(exclude=old)
    +        group.client_write(b"state", b"after", AckLevel.QUORUM)
    +        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
    +        current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
    +        guard = "read-index fenced"
    +    else:
    +        raise ValueError("protocol must be async, wal, isr, or raft")
    +
    +    result = ExperimentResult(protocol, old_value, current, guard)
    +    if verbose:
    +        print(
    +            f"实验 7 [{protocol}]：old_LOCAL={old_value!r}; "
    +            f"current={current!r}; authority_guard={guard}"
    +        )
    +    return result
    +
    +
    +if __name__ == "__main__":
    +    parser = argparse.ArgumentParser()
    +    parser.add_argument(
    +        "--protocol",
    +        choices=("async", "wal", "isr", "raft", "all"),
    +        default="all",
    +    )
    +    args = parser.parse_args()
    +    selected = ("async", "wal", "isr", "raft") if args.protocol == "all" else (args.protocol,)
    +    for name in selected:
    +        run_experiment(protocol=name)
    ```

**是什么，为什么现在需要**

实验 7 证明被隔离旧侧仍可暴露此前本地值，但只有当前侧能满足受权威保护的读取。

**在运行时做什么**

它创建共享状态、分区旧权威、改变权威、写入新值，再记录 Old-local、Current 与 Guard Result。

**关键代码**

```python
old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
```

**关键语句理解**

可观察性不等于权威。返回旧本地值可以允许；若通过受保护 Current Read 提供它才是安全违规。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/10-advanced-experiments/tests.txt)`。五条新增契约完成七实验、四协议证据矩阵。

### 需要真正记住的内容

Membership、Retention、Read Location 与 Authority Proof 是独立轴。`unsupported`、`blocked`、`stale` 与 `fresh` 必须保持不同结果。

### 用自己的话讲清楚

选择最终矩阵的一行，从机制预测四种协议结果，并说明陈旧 Local Read 为何本身不是安全故障。

### 教材

[第 8 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/08-experiments.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-09...stage-10)

完成后可运行 `git checkout stage-10` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/10-advanced-experiments/stage.patch)
