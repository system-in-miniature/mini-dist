# Chapter 7: Raft II — Log Replication and Commit

Election chooses a leader; it does not make every entry safe. Raft must first
make logs agree, prove that a majority stores an entry under the correct commit
rule, and apply only the committed prefix to the state machine. This chapter
traces that pipeline and then crashes a follower to expose the boundary between
persistent and volatile state.

## Learning objectives

By the end of this chapter, you will be able to:

- trace a client write through append, replication, commit, and application;
- explain the `prevLogIndex`/`prevLogTerm` consistency check;
- predict how `nextIndex` repairs a conflicting follower suffix;
- justify Raft's “commit by counting only current-term entries” rule;
- explain how a restarted node rebuilds its state machine from its durable log.

## 1. A client write begins as an uncommitted entry

`RaftGroup.client_write()` in
`src/minidist/protocols/raft/group.py` accepts only `AckLevel.QUORUM`. It finds
the current leader and appends a `LogEntry` containing the next index, current
term, key, and value. It records the leader's own `match_index`, broadcasts
AppendEntries, and calls `_wait_for_commit()`.

Appending is not committing. While the entry exists only on the leader, it may
be overwritten if another leader is elected. `_wait_for_commit()` advances
logical ticks and returns true only while the same node remains an alive leader
in the entry's term and `commit_index` reaches the entry's index. A timeout
returns `accepted=False`; MiniDist does not silently weaken QUORUM to a local
ack.

The log contains a sentinel at index 0. A newly elected leader also appends a
no-op in its current term in `RaftGroup._become_leader()`. Therefore the first
client entry commonly appears at index 2: sentinel, election no-op, client
command.

## 2. AppendEntries proves a shared prefix

`RaftGroup._send_append()` constructs an RPC for each follower from the
leader's per-follower `next_index`. The payload includes:

- leader term and ID;
- `prev_log_index` and `prev_log_term`;
- every entry beginning at `next_index`;
- the leader's `commit_index`;
- an optional read context.

The previous-entry coordinates are a compact consistency proof. In
`RaftGroup._receive_append_entries()`, the follower rejects an older-term
leader immediately. Otherwise it steps down when necessary and resets its
election deadline.

The follower then checks whether `prev_log_index` exists and whether the term at
that index equals `prev_log_term`. If either check fails, it sends an
unsuccessful response with a conflict index. It does not append the proposed
suffix over a prefix it cannot verify.

When the prefix matches, the follower walks the incoming entries. If an entry
index already exists with a different term, it deletes its log from that index
onward and appends the leader's entry:

```python
if target.log[index].term != entry.term:
    del target.log[index:]
    target.log.append(entry)
```

The agreed prefix remains untouched. Entries beyond the first conflict are not
independently authoritative; they were created on a history that the current
leader has superseded. New entries are appended normally.

The success response reports the matched index. In
`RaftGroup._receive_append_response()`, the leader raises that follower's
`match_index` and sets `next_index = match_index + 1`. On failure, it moves
`next_index` back to the returned conflict index and immediately retries
`_send_append()`. MiniDist returns the first position of the conflicting term,
which avoids repeated single-index retries but is not the paper's complete
conflict-term jump optimization.

## 3. Replicated is not always committed

The leader calls `RaftGroup._advance_commit()` after every successful append
response. It scans candidate indexes from the end of the log down to the
existing commit index and counts `match_index` values at or beyond each
candidate. Yet majority storage alone is deliberately insufficient:

```python
if (replicated >= self._quorum
        and leader.log[index].term == leader.current_term):
    leader.commit_index = index
```

Why require the current term? Raft paper §5.4.2 constructs a history in which
an entry from an older term is stored on a majority but can still be
overwritten after another election. Counting copies of that old entry directly
would label it committed too early.

The safe rule is:

1. use replica counting to commit an entry from the leader's **current** term;
2. because logs are prefixes, committing that entry commits every preceding
   entry indirectly, including entries from older terms.

This is why the election no-op matters. A new leader with no client traffic can
replicate and commit its current-term no-op, thereby establishing the safety of
the inherited prefix. MiniDist's why-comment in `_advance_commit()` cites this
exact counterexample rather than presenting the term check as an arbitrary
condition.

After advancing, the leader calls `_apply_committed()` and broadcasts another
AppendEntries so followers learn the new `leader_commit`.

## 4. Application is a separate monotonic cursor

Each node tracks `commit_index` and `last_applied`.
`RaftGroup._apply_committed()` increments `last_applied` one entry at a time
until it reaches `commit_index`. A no-op has `key is None` and changes no user
data; a command writes its key and value into the node's dictionary.

Separating these cursors states two invariants:

- an entry must never be applied before it is committed;
- each committed entry must be applied in log order and at most once per
  in-memory recovery cycle.

On the follower, `_receive_append_entries()` first bounds the received
`leader_commit` by its own last log index, then applies. A leader cannot make a
follower apply an index it has not received.

The simulator applies synchronously inside message handling. Production systems
often have a separate apply loop, batching, durable state-machine snapshots,
and waiting clients. MiniDist preserves the ordering boundary without modeling
those scheduling and storage costs.

## 5. Higher terms repair a partitioned old leader

During a minority partition, an old leader can append an entry locally.
`client_write()` waits for a majority, so that operation returns
`accepted=False`. Meanwhile, the connected majority elects a higher-term
leader and commits a different suffix.

After healing, AppendEntries from the higher term reaches the old leader.
`_receive_append_entries()` calls `_step_down()`. The consistency check
eventually finds the fork, and the retry path backs up `next_index`. The old
uncommitted suffix is deleted and replaced with the current leader's suffix.

Term fencing alone does not rewrite the log. It establishes authority.
`prevLogIndex`/`prevLogTerm`, conflict deletion, and `nextIndex` retry perform
the actual convergence. Chapter 8 observes the whole transition in experiment
3.

## 6. Crash recovery: durable log, rebuilt machine

MiniDist's `_Node` comments mark `current_term`, `voted_for`, and `log` as
persistent. `RaftGroup.crash()` calls `_clear_volatile()`, which resets role,
commit and apply indexes, dictionary data, timers, votes, replication cursors,
and read acknowledgements. `RaftGroup.restart()` restores the node as a
follower with a new election deadline.

Immediately after restart, the node can therefore have the complete durable log
but `commit_index == 0` and an empty dictionary. This is not data loss in the
modeled protocol. A leader heartbeat carries `leader_commit`; the follower
advances its commit index up to the available log and `_apply_committed()`
replays the committed prefix.

This model assumes that updates to the persistent fields survive atomically.
There is no file, fsync, torn record, checksum, snapshot, or log compaction.
The model verifies Raft's state classification and replay behavior, not a
production storage engine.

## 7. Comparison with textbook and production Raft

The [repository behavior
matrix](../mapping.md#mapping-protocol-4-to-the-raft-paper) labels log matching,
conflicting-suffix deletion, majority commitment, and the current-term rule as
equivalent to Raft §§5.3 and 5.4.2. It also documents the intentional gaps:

- the log is unbounded; snapshot installation and compaction from §7 are not
  implemented;
- membership is static; there is no joint consensus;
- persistent fields live in an in-process durable mapping, without disk
  failure or fsync timing;
- AppendEntries transports structured Python values rather than encoded RPCs;
- conflict repair uses a conflict index but not the complete conflict-term
  optimization;
- the client call synchronously drives ticks and has no session, retry ID, or
  duplicate-request table;
- state-machine application is immediate and single-threaded.

Production Raft implementations commonly batch log entries, pipeline
AppendEntries, limit inflight requests, snapshot state, compact logs, persist
hard state and entries through a storage abstraction, and separate committed
from applied notifications. MiniDist's claim is semantic: prefix checking,
term-based conflict repair, current-term commitment, and ordered application
are directly inspectable here.

## 8. Hands-on experiment: restart and replay

Run from the repository root:

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel
from minidist.protocols.raft import RaftGroup

g = RaftGroup(node_ids=("n1", "n2", "n3"), seed=23,
              election_timeout_range=(4, 7), heartbeat_interval=1)
leader = g.run_until_leader()
r = g.client_write(b"k", b"v", AckLevel.QUORUM)
g.run_until_converged()
follower = next(n for n in g.probe().nodes if n != leader)
before = g.probe().nodes[follower]
g.crash(follower); g.restart(follower)
after = g.probe().nodes[follower]
print("write:", r.accepted, "index:", r.offset)
print("log terms:", [e.term for e in before.log])
print("before restart:", before.commit_index, before.data.get(b"k"))
print("after restart:", after.commit_index, after.data.get(b"k"))
g.run_until_converged()
recovered = g.probe().nodes[follower]
print("after catch-up:", recovered.commit_index, recovered.data.get(b"k"))
PY
```

Measured output:

```text
write: True index: 2
log terms: [0, 1, 1]
before restart: 2 b'v'
after restart: 0 None
after catch-up: 2 b'v'
```

The sentinel has term 0; the leader no-op and client command have term 1. The
restart clears the follower's volatile commit/application state but preserves
those log entries. Convergence delivers `leader_commit=2`, and replay rebuilds
`k=v`.

Run the focused protocol suite:

```bash
uv run pytest -q tests/protocols/test_raft.py
```

Measured output:

```text
..........                                                               [100%]
10 passed in 0.05s
```

This covers quorum commit, crash recovery, partition repair,
linearizable-read failure without quorum, and deterministic replay. Duration
may vary; ten passes and zero failures are the acceptance facts.

## 9. Exercises

### Understanding

1. Why may a follower delete a conflicting suffix but never the matching
   prefix established by `prevLogIndex`/`prevLogTerm`?
2. Why can a current-term no-op safely commit older-term entries indirectly
   when counting those older entries directly would be unsafe?

??? note "Reference answers"

    1. The matching coordinate proves the leader and follower share that prefix
       under Raft's log-matching property. Entries after the first different
       term belong to a superseded branch and are not protected merely by being
       present.
    2. A majority that stores the current-term entry establishes a prefix that
       future leaders must contain through the election restriction. Its older
       prefix is therefore committed as part of it; replica count for the old
       entry alone lacks that proof.

### Hands-on

3. Extend the temporary replay script to print `last_applied` before crash,
   immediately after restart, and after convergence. Acceptance: it follows
   `2 -> 0 -> 2` together with the dictionary result.
4. Draft, but do not apply, a one-line faulty diff removing
   `leader.log[index].term == leader.current_term` from `_advance_commit()`.
   Then write a regression-test acceptance statement based on Raft §5.4.2:
   an old-term entry stored on a majority must not advance `commit_index` until
   a current-term entry is committed.

??? note "Reference answers"

    3. Read `before.last_applied`, `after.last_applied`, and
       `recovered.last_applied` from the immutable snapshots. The expected
       sequence matches `commit_index` in this synchronous apply model.
    4. The intentionally wrong proposed diff would reduce the `if` to only
       `replicated >= self._quorum`. The test must construct the paper's
       old-term-majority state (a temporary test may arrange private state),
       invoke the advancement path, and assert no direct commit. It should then
       append and replicate a current-term no-op and assert that the full prefix
       becomes committed. Keep this as an exercise diff; do not edit `src/`.

## Summary

Raft turns a leader's local append into a safe state-machine update through
four distinct stages: verify a common prefix, replicate and repair suffixes,
commit a current-term entry on a majority, then apply the committed prefix in
order. Crash recovery keeps term, vote, and log while rebuilding volatile
commit and application state from later leader messages. Chapter 8 now uses
the same simulator to compare these semantics against asynchronous
primary-replica behavior under normal operation, failover, and partition.
