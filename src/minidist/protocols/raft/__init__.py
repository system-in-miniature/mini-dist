"""Public surface of MiniDist's textbook Raft replication group.

The implementation covers leader election, log replication, commitment, crash
recovery, and a small read-index barrier.  Snapshots and membership changes
are deliberately out of scope: the log is unbounded and membership is static
for the lifetime of a group.
"""

from .group import GroupState, LogEntry, NodeState, RaftGroup, Role

__all__ = [
    "GroupState",
    "LogEntry",
    "NodeState",
    "RaftGroup",
    "Role",
]
