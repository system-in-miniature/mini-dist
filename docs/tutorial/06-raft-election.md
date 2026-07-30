# Chapter 6: Raft I — Election

The asynchronous protocol delegates promotion to the experiment. Raft makes
authority a protocol outcome. Servers advance terms, ask peers for votes, and
become leader only after collecting a majority. This chapter follows those
transitions in MiniDist and separates election safety from eventual election.

## Learning objectives

By the end of this chapter, you will be able to:

- identify Raft's persistent and volatile election state in MiniDist;
- explain how randomized logical-time deadlines reduce repeated split votes;
- apply the one-vote-per-term and up-to-date-log voting rules;
- trace a higher term from an RPC to leader demotion;
- distinguish “one leader was observed” from “all queued RPCs have settled.”

## 1. Terms are logical eras of authority

In `src/minidist/protocols/raft/group.py`, the `_Node` dataclass divides state
using Raft Figure 2. `current_term`, `voted_for`, and `log` are persistent in
the simulation's crash model. Role, deadlines, collected votes, replication
cursors, commit indices, and the state machine are volatile.

A term is a monotonically increasing election era. It is not a wall-clock
timestamp and does not identify a particular node. When a follower's election
deadline expires, `RaftGroup._start_election()` performs one atomic local
transition:

```python
candidate.role = Role.CANDIDATE
candidate.current_term += 1
candidate.voted_for = candidate.node_id
candidate.votes = {candidate.node_id}
```

The candidate resets its deadline and sends `request_vote` through `SimNet` to
every peer. Each request includes the term and the candidate's last log index
and term. RPCs are scheduled messages, not direct Python method calls, so
partitions and delay affect elections at the same boundary as replication.

Terms provide fencing. In `RaftGroup._receive_request_vote()`, a receiver that
sees a higher term calls `_step_down()`. The same rule appears in
`_receive_vote_response()` and `_receive_append_response()`. In
`_receive_append_entries()`, a current candidate or leader also steps down when
it accepts AppendEntries authority for the same or higher term. `_step_down()`
updates the term when necessary, clears `voted_for` for the new term, changes
the role to follower, discards volatile leader/candidate bookkeeping, and
chooses a new deadline.

A stale leader may still believe it is leader while partitioned. That local
belief is harmless only if operations requiring authority cannot complete.
After healing, a higher-term RPC forces the stale leader to step down.

## 2. Randomized timeouts make progress reproducible

Every non-leader is driven by `RaftGroup.tick()`. The scheduler first delivers
due messages. Then each alive follower or candidate whose election deadline
has arrived calls `_start_election()`. Leaders instead send heartbeats when
`heartbeat_due` arrives.

`RaftGroup._reset_election_deadline()` draws an integer from the configured
range and adds it to the current logical tick:

```python
timeout = self._rng.randint(self._timeout_low, self._timeout_high)
node.election_deadline = self.clock.now + timeout
```

The random generator is `random.Random(seed)` owned by the group. MiniDist does
not read module-global randomness or wall time. With the same seed and command
sequence, the timeout draws and resulting trace are repeatable.

Randomization does not prove election safety. Its job is liveness: different
deadlines make it likely that one candidate asks for votes before the others.
Safety comes from quorum intersection and voting rules. MiniDist also validates
that the lowest election timeout is strictly greater than twice the maximum
configured network delay plus the heartbeat interval. The two delay legs
budget a heartbeat round trip; the additional interval lets the next regular
heartbeat start before a correctly connected follower times out. Configurations
that omit this network-delay budget do not receive a liveness claim.

A simulation tick is only an ordering unit. “The timeout is 5 ticks” does not
mean five milliseconds, and this model provides no latency benchmark.

## 3. A vote is constrained by term and log

`RaftGroup._receive_request_vote()` computes whether the candidate's log is at
least as up to date as the voter's:

```python
up_to_date = (
    candidate_last_term,
    candidate_last_index,
) >= (voter_last_term, voter_last_index)
```

The lexicographic comparison intentionally checks last term before last index.
A longer log whose final term is older is not more up to date. This voting rule
prevents a candidate missing committed entries from winning with votes from a
majority that has those entries.

The receiver grants the vote only when all three conditions hold:

1. the request term equals its current term;
2. `voted_for` is empty or already names this candidate;
3. the candidate's log is at least as up to date.

On a grant, the voter persists `voted_for` and resets its deadline. The response
contains the voter's current term and the decision. The trace event
`raft_vote_decided` records the voter, candidate, term, `granted`, and
`up_to_date` values.

`RaftGroup._receive_vote_response()` counts a grant only if the target is still
a candidate and the response term equals its current term. A delayed grant from
an older campaign cannot elect a candidate in a later term. Once the set of
unique votes reaches `_quorum`, `_become_leader()` runs.

For three nodes, quorum is two. Any two majorities intersect, so two different
candidates cannot each gather a majority of one-vote-per-term servers in the
same term. This is election safety: at most one leader per term. It does not
mean that elections always finish immediately; simultaneous candidates,
partitions, or repeatedly split votes can delay progress.

## 4. Becoming leader starts replication work

`RaftGroup._become_leader()` changes the role, initializes `next_index` and
`match_index`, schedules the heartbeat deadline, and appends a no-op entry in
the new term. It then broadcasts AppendEntries.

The no-op is not needed to win the election. It provides a current-term entry
that the new leader can safely commit. Once committed, the whole preceding
prefix becomes committed too. This connects election to the §5.4.2 commit rule
studied in Chapter 7.

The public helper `run_until_leader()` repeatedly checks for an alive leader,
then advances a tick. It returns as soon as it observes one, choosing the
highest-term leader if several local role views temporarily exist. It is a lab
driver, not an extra Raft RPC and not a proof that every queued response has
already arrived.

That distinction explains why a snapshot taken immediately on return can still
show another node as candidate. Election safety concerns leaders in the same
term; a candidate is not a second leader. A few more ticks deliver the new
leader's AppendEntries, which resets followers' deadlines and makes candidates
step down.

## 5. Crash and restart election state

`RaftGroup.crash()` marks the server dead and calls `_clear_volatile()`. The
term, vote, and log remain. `RaftGroup.restart()` marks it alive and calls
`_reset_volatile()`, restoring follower role and selecting a new deadline.

Persisting `voted_for` matters. If a crash erased it while preserving the term,
a server could vote for candidate A, restart, and vote for candidate B in the
same term. Two candidates might then each assemble a majority across different
moments. MiniDist preserves the field in memory rather than on disk, so it
models the state transition but not fsync, torn writes, or storage corruption.

## 6. Comparison with textbook and production Raft

The repository's [Raft mapping and behavior
differences](../mapping.md#differences-between-this-implementation-and-textbook-raft)
ties these functions to Raft §§5.1, 5.2, 5.4.1, and 5.6. The election state and
RequestVote rules are equivalent at the teaching boundary, but MiniDist is
intentionally narrower:

- membership is a fixed group of at least three nodes; joint-consensus
  reconfiguration from §6 is absent;
- persistent state is an in-process durable mapping, not a file, WAL, storage
  engine, or fsync protocol;
- RPCs are Python payload dictionaries in deterministic `SimNet`, not encoded
  network messages with authentication, backpressure, retransmission, or
  process scheduling;
- the synchronous lab helper drives ticks inside calls; production clients,
  futures, retries, request IDs, and duplicate suppression are absent;
- `probe()`, `run_until_leader()`, `isolate()`, and `heal()` are observability
  and fault-injection APIs, not Raft wire operations.

Real Raft-derived systems also make engineering choices beyond the paper:
pre-vote to avoid disruptive term increases, leadership transfer, snapshots,
membership administration, durable storage formats, and batching. MiniDist
does not claim those features. Its useful claim is that terms, majority votes,
log freshness, and step-down behavior preserve the election mechanism.

## 7. Hands-on experiment: observe a seeded election

Run:

```bash
uv run python - <<'PY'
from minidist.protocols.raft import RaftGroup

g = RaftGroup(node_ids=("n1", "n2", "n3"), seed=11,
              election_timeout_range=(4, 7), heartbeat_interval=1)
leader = g.run_until_leader()
for _ in range(3):
    g.tick()
s = g.probe()
print("leader:", leader)
print("term:", s.nodes[leader].current_term)
print("roles:", {n: x.role.value for n, x in s.nodes.items()})
print("granted votes:", [(e.details["voter"], e.details["candidate"],
                         e.details["term"])
                        for e in g.trace.events
                        if e.kind == "raft_vote_decided"
                        and e.details["granted"]])
PY
```

Measured output:

```text
leader: n1
term: 2
roles: {'n1': 'leader', 'n2': 'follower', 'n3': 'follower'}
granted votes: [('n3', 'n1', 2)]
```

`n1` includes its self-vote, so the displayed grant from `n3` reaches the
two-node quorum. The election took two terms because the seeded first campaigns
split their votes. Three settling ticks make the stable roles easy to read;
they are not required to manufacture the leader.

Verify the broader election behavior with:

```bash
uv run pytest -q tests/protocols/test_raft.py -k 'leader or replay'
```

Measured output:

```text
.....                                                                    [100%]
5 passed, 5 deselected in 0.04s
```

The selected tests cover exactly one observed leader and event-for-event replay
with the same seed. Duration may vary; five selected passes and zero failures
are the acceptance facts.

## 8. Exercises

### Understanding

1. Why does Raft compare `(lastLogTerm, lastLogIndex)` rather than log length
   alone when voting?
2. Can an isolated old leader remain locally in `LEADER` role without violating
   election safety? What must it be unable to do?

??? note "Reference answers"

    1. Entries from a newer term dominate entries from an older term. Length
       alone could prefer a longer but obsolete history and elect a candidate
       missing a committed prefix.
    2. Yes, until it hears a higher term. Safety requires it to be unable to
       gather a current majority and commit; after healing, a higher-term RPC
       must demote it.

### Hands-on

3. Run the election script twice and compare `g.trace.as_dicts()`. Acceptance:
   the lists are equal for seed 11; changing only the seed is allowed to produce
   a different trace.
4. Propose a test-only experiment for one-vote-per-term persistence across
   restart. Acceptance: after a node grants a vote, crash and restart it, send a
   same-term request from a different candidate, and observe `granted=False`.
   Do not modify `src/`.

??? note "Reference answers"

    3. Put construction and `run_until_leader()` in a function returning
       `trace.as_dicts()`, then assert `run(11) == run(11)`. This is the pattern
       used by `test_same_seed_replays_election_and_partition_heal_exactly()` in
       `tests/protocols/test_raft.py`.
    4. A focused test may deliver RequestVote dictionaries through
       `group.network.send()` and advance ticks. Record the original term and
       `voted_for` through `probe()`, call `crash()` and `restart()`, then send
       the competing request. The acceptance assertion belongs in a temporary
       test or proposed diff, not in repository source.

## Summary

Raft elections turn authority into a majority decision. A candidate advances
its term, votes for itself, advertises its last-log coordinates, and wins only
with a quorum of valid same-term votes. Randomized deterministic deadlines help
the simulation make progress; persistent term and vote state plus higher-term
step-down rules provide safety. The new leader then appends a current-term
no-op and begins AppendEntries. Chapter 7 follows that RPC into log matching,
conflict repair, commitment, application, and crash recovery.
