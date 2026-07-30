"""A logical clock like those used by deterministic simulation tests.

Wall-clock time would make failure boundaries and timeout races depend on
machine load.  ``SimClock`` moves only when the harness asks it to, so a tick
number is a reproducible part of an experiment rather than a duration claim.
"""


class SimClock:
    """Monotonic integer time advanced explicitly by the simulation."""

    def __init__(self) -> None:
        self._now = 0

    @property
    def now(self) -> int:
        return self._now

    def advance(self, steps: int = 1) -> int:
        """Advance by positive logical steps and return the new tick."""

        if steps <= 0:
            raise ValueError("clock steps must be positive")
        self._now += steps
        return self._now
