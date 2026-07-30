from minidist.protocols import (
    AckLevel,
    ReadLevel,
    ReadResult,
    UnsupportedLevelError,
    WriteResult,
)


def test_replication_api_levels_and_results_are_explicit() -> None:
    assert [level.name for level in AckLevel] == [
        "NONE",
        "LEADER",
        "QUORUM",
        "ALL_ISR",
    ]
    assert [level.name for level in ReadLevel] == [
        "LOCAL",
        "LEADER",
        "LINEARIZABLE",
    ]
    assert WriteResult(accepted=True, offset=3).offset == 3
    assert ReadResult(value=b"value", node="n1", offset=2).value == b"value"
    assert issubclass(UnsupportedLevelError, ValueError)
