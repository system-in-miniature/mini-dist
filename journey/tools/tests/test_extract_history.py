"""Contracts for content-driven MiniDist Stage reconstruction."""

from pathlib import Path

from journey.tools import extract_history


ROOT = Path(__file__).resolve().parents[3]


def test_manifest_is_ten_contiguous_content_driven_stages() -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")

    assert [stage.number for stage in manifest.stages] == list(range(1, 11))
    assert manifest.stages[5].source == "3826aa7"
    assert manifest.stages[-1].slug == "advanced-experiments"


def test_real_fencing_delta_replaces_the_earlier_async_snapshot() -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")
    stage_five = extract_history.snapshot_for_stage(manifest, 5, root=ROOT)
    stage_six = extract_history.snapshot_for_stage(manifest, 6, root=ROOT)
    path = "src/minidist/protocols/async_primary/group.py"

    assert stage_five[path] != stage_six[path]
    assert stage_six[path] == extract_history.git_file(ROOT, "3826aa7", path)


def test_final_snapshot_matches_every_owned_reference_byte() -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")
    final = extract_history.snapshot_for_stage(manifest, 10, root=ROOT)
    expected = extract_history.owned_tree(ROOT, manifest)

    assert final == expected
