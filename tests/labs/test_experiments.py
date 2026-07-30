from labs.exp01_normal_replication import run_experiment as run_normal
from labs.exp02_acked_write_loss import run_experiment as run_loss
from labs.exp03_partition_old_leader import run_experiment as run_partition
from labs.exp04_slow_replica import run_experiment as run_slow
from labs.exp05_replica_reconnect import run_experiment as run_reconnect
from labs.exp06_read_consistency import run_experiment as run_reads
from labs.exp07_split_brain_lease import run_experiment as run_split_read


def test_experiment_1_observes_delayed_convergence() -> None:
    result = run_normal(verbose=False)
    assert result.before_tick is None
    assert result.after_tick == b"MiniDist"
    assert result.replica_offset == result.primary_offset == 1


def test_experiment_2_proves_acknowledged_write_can_be_lost() -> None:
    result = run_loss(protocol="async", verbose=False)
    assert result.acknowledged
    assert result.value_before_failover == b"confirmed"
    assert result.value_after_failover is None
    assert result.new_primary == "replica-1"


def test_experiment_1_raft_converges_after_quorum_ack() -> None:
    result = run_normal(protocol="raft", verbose=False)
    assert result.before_tick is None
    assert result.after_tick == b"MiniDist"
    assert result.replica_offset == result.primary_offset


def test_experiment_2_raft_acknowledged_write_survives_failover() -> None:
    result = run_loss(protocol="raft", verbose=False)
    assert result.acknowledged
    assert result.value_before_failover == b"confirmed"
    assert result.value_after_failover == b"confirmed"
    assert result.new_primary != result.old_primary


def test_experiment_3_old_leader_is_fenced_and_cluster_converges() -> None:
    result = run_partition(protocol="raft", verbose=False)
    assert not result.old_leader_write_committed
    assert result.new_leader_write_committed
    assert result.new_leader != result.old_leader
    assert result.old_leader_role_after_heal == "follower"
    assert result.logs_converged
    assert result.majority_value == b"continues"
    assert result.minority_value is None


def test_experiment_3_async_primary_exposes_split_brain_dirty_writes() -> None:
    result = run_partition(protocol="async", verbose=False)
    assert result.old_primary_write_accepted
    assert result.new_primary_write_accepted
    assert result.old_primary_value_during_partition == b"old-primary"
    assert result.old_primary_new_value_during_partition is None
    assert result.new_primary_value_during_partition == b"new-primary"
    assert result.new_primary_old_value_during_partition is None
    assert result.converged_after_heal
    assert result.old_dirty_value_after_heal is None
    assert result.new_dirty_value_after_heal == b"new-primary"


def test_experiment_4_distinguishes_isr_shrink_from_raft_majority() -> None:
    isr = run_slow(protocol="isr", verbose=False)
    raft = run_slow(protocol="raft", verbose=False)

    assert isr.write_available
    assert isr.slow_replica_stale
    assert isr.membership_effect == "removed from ISR"
    assert raft.write_available
    assert raft.slow_replica_stale
    assert raft.membership_effect == "fixed voter; majority unaffected"


def test_experiment_4_has_all_four_protocol_columns() -> None:
    results = {
        protocol: run_slow(protocol=protocol, verbose=False)
        for protocol in ("async", "wal", "isr", "raft")
    }
    assert all(result.write_available for result in results.values())
    assert results["wal"].membership_effect == "async standby does not gate commit"


def test_experiment_5_catchup_trigger_matrix_covers_all_protocols() -> None:
    results = {
        protocol: run_reconnect(protocol=protocol, verbose=False)
        for protocol in ("async", "wal", "isr", "raft")
    }

    assert results["async"].inside_window == "incremental"
    assert results["async"].outside_window == "full"
    assert results["wal"].inside_window == "incremental"
    assert results["wal"].outside_window == "full"
    assert results["isr"].inside_window == "incremental"
    assert results["isr"].outside_window == "full"
    assert results["raft"].inside_window == "log replay"
    assert results["raft"].outside_window == "log replay"


def test_experiment_6_read_level_matrix_names_stale_and_authority_failures() -> None:
    matrix = run_reads(verbose=False)

    for protocol in ("async", "wal", "isr", "raft"):
        assert matrix[protocol]["LOCAL"].status == "stale"
        assert matrix[protocol]["LEADER"].status == "fresh"
    assert matrix["async"]["LINEARIZABLE"].status == "unsupported"
    assert matrix["wal"]["LINEARIZABLE"].status == "unsupported"
    assert matrix["isr"]["LINEARIZABLE"].status == "blocked"
    assert matrix["raft"]["LINEARIZABLE"].status == "fresh"


def test_experiment_7_split_brain_local_read_and_authority_guard() -> None:
    results = {
        protocol: run_split_read(protocol=protocol, verbose=False)
        for protocol in ("async", "wal", "isr", "raft")
    }

    assert all(result.old_side_local_value == b"before" for result in results.values())
    assert all(result.current_value == b"after" for result in results.values())
    assert results["async"].guard == "unsupported"
    assert results["wal"].guard == "unsupported"
    assert results["isr"].guard == "lease fenced"
    assert results["raft"].guard == "read-index fenced"
