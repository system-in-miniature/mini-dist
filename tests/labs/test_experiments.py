from labs.exp01_normal_replication import run_experiment as run_normal
from labs.exp02_acked_write_loss import run_experiment as run_loss
from labs.exp03_partition_old_leader import run_experiment as run_partition


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
    result = run_partition(verbose=False)
    assert not result.old_leader_write_committed
    assert result.new_leader_write_committed
    assert result.new_leader != result.old_leader
    assert result.old_leader_role_after_heal == "follower"
    assert result.logs_converged
    assert result.majority_value == b"continues"
    assert result.minority_value is None
