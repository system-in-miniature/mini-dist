# Chapter 4: Hands-on Experiments

[中文版](zh/labs-guide.md)

Run commands from the repository root after `uv sync --dev`. Each script uses
the public replication-group API and a fixed seed, so its event ordering and
observations are reproducible.

## Experiment 1: normal replication

Source:
[exp01_normal_replication.py](https://github.com/system-in-miniature/mini-dist/blob/main/labs/exp01_normal_replication.py)

```bash
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

Expected: before progress, the observed follower value is `None`; after
convergence it is `b'MiniDist'`. Async reports offset `1`, while Raft reports
commit index `2`. Watch the more important difference in the text: immediate
leader acknowledgement versus majority acknowledgement.

## Experiment 2: acknowledged write and failover

Source:
[exp02_acked_write_loss.py](https://github.com/system-in-miniature/mini-dist/blob/main/labs/exp02_acked_write_loss.py)

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
uv run python labs/exp02_acked_write_loss.py --protocol raft
```

Expected: both writes return `ack=True`. After immediate leader failure, async
reads `None` from the promoted stale replica, while Raft reads
`b'confirmed'`. Watch what each protocol actually promises at its ack boundary.

## Experiment 3: partition and old-leader fencing

Source:
[exp03_partition_old_leader.py](https://github.com/system-in-miniature/mini-dist/blob/main/labs/exp03_partition_old_leader.py)

```bash
uv run python labs/exp03_partition_old_leader.py --protocol async
uv run python labs/exp03_partition_old_leader.py --protocol raft
```

Expected: both asynchronous sides accept local dirty writes during the
partition; after healing, the promoted primary's generation wins and the old
dirty value disappears. Raft rejects the isolated old leader's quorum write,
elects a higher-term leader, and converges with only
`majority=b'continues'`.

Use the [experiment matrix](experiments.md) to connect these observations to
supported read/ack levels and declared limitations. Simulation ticks encode
ordering, not milliseconds or performance.
