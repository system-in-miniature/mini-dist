#!/usr/bin/env python3
"""Resolve MiniDist's content-driven Journey snapshots from Git evidence."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, slots=True)
class StageSpec:
    number: int
    slug: str
    chapter: int
    source: str
    files: tuple[str, ...]
    tests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HistoryManifest:
    name: str
    package: str
    repository_url: str
    owned_roots: tuple[str, ...]
    owned_files: tuple[str, ...]
    stages: tuple[StageSpec, ...]


def _string_tuple(value: object, *, label: str, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ValueError(f"{label} must be a list of non-empty strings")
    if not allow_empty and not value:
        raise ValueError(f"{label} must not be empty")
    result = tuple(value)
    if len(result) != len(set(result)):
        raise ValueError(f"{label} contains duplicates")
    return result


def load_manifest(path: Path) -> HistoryManifest:
    data = tomllib.loads(path.read_text())
    project = data.get("project")
    if not isinstance(project, dict):
        raise ValueError("manifest requires [project]")
    raw_stages = data.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise ValueError("manifest requires [[stages]]")

    stages: list[StageSpec] = []
    for index, raw in enumerate(raw_stages, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"stage {index} must be a table")
        number = raw.get("number")
        chapter = raw.get("chapter")
        slug = raw.get("slug")
        source = raw.get("source")
        if number != index or not isinstance(chapter, int) or chapter < 1:
            raise ValueError(f"stage {index} has invalid number or chapter")
        if not isinstance(slug, str) or not slug:
            raise ValueError(f"stage {index} requires a slug")
        if not isinstance(source, str) or not source:
            raise ValueError(f"stage {index} requires a source revision")
        stages.append(
            StageSpec(
                number=number,
                slug=slug,
                chapter=chapter,
                source=source,
                files=_string_tuple(raw.get("files"), label=f"stage {index} files"),
                tests=_string_tuple(raw.get("tests"), label=f"stage {index} tests"),
            )
        )

    return HistoryManifest(
        name=str(project["name"]),
        package=str(project["package"]),
        repository_url=str(project["repository_url"]),
        owned_roots=_string_tuple(project.get("owned_roots"), label="owned_roots"),
        owned_files=_string_tuple(project.get("owned_files"), label="owned_files"),
        stages=tuple(stages),
    )


def git_file(root: Path, revision: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.decode(errors="replace").strip()
        raise ValueError(f"cannot read {revision}:{path}: {message}")
    return result.stdout


def snapshot_for_stage(
    manifest: HistoryManifest,
    number: int,
    *,
    root: Path,
) -> dict[str, bytes]:
    if not 0 <= number <= len(manifest.stages):
        raise ValueError(f"stage number must be between 0 and {len(manifest.stages)}")
    snapshot: dict[str, bytes] = {}
    for stage in manifest.stages[:number]:
        for path in stage.files:
            snapshot[path] = git_file(root, stage.source, path)
    return snapshot


def owned_tree(root: Path, manifest: HistoryManifest) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for relative in manifest.owned_roots:
        base = root / relative
        if not base.is_dir():
            raise ValueError(f"missing owned root: {relative}")
        for path in sorted(item for item in base.rglob("*") if item.is_file()):
            if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            result[path.relative_to(root).as_posix()] = path.read_bytes()
    for relative in manifest.owned_files:
        path = root / relative
        if not path.is_file():
            raise ValueError(f"missing owned file: {relative}")
        result[relative] = path.read_bytes()
    return result


def _write_snapshot(directory: Path, snapshot: dict[str, bytes]) -> None:
    for child in directory.iterdir():
        if child.name == ".git":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    for relative, payload in snapshot.items():
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


def patch_for_stage(
    manifest: HistoryManifest,
    number: int,
    *,
    root: Path,
) -> bytes:
    """Render one exact Git patch between consecutive content snapshots."""

    previous = snapshot_for_stage(manifest, number - 1, root=root)
    current = snapshot_for_stage(manifest, number, root=root)
    with tempfile.TemporaryDirectory(prefix=f"minidist-patch-{number:02d}-") as raw:
        repository = Path(raw)
        subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
        _write_snapshot(repository, previous)
        subprocess.run(["git", "add", "-A"], cwd=repository, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=MiniDist Journey",
                "-c",
                "user.email=journey@example.invalid",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                f"stage-{number - 1:02d}",
            ],
            cwd=repository,
            check=True,
        )
        _write_snapshot(repository, current)
        subprocess.run(["git", "add", "-A"], cwd=repository, check=True)
        result = subprocess.run(
            ["git", "diff", "--cached", "--binary", "--full-index", "--no-ext-diff"],
            cwd=repository,
            stdout=subprocess.PIPE,
            check=True,
        )
        if not result.stdout:
            raise ValueError(f"stage {number} generated an empty patch")
        return result.stdout


def write_stage_sources(
    manifest: HistoryManifest,
    *,
    root: Path,
    stages_root: Path,
) -> None:
    stages_root.mkdir(parents=True, exist_ok=True)
    expected: set[str] = set()
    for stage in manifest.stages:
        name = f"{stage.number:02d}-{stage.slug}"
        expected.add(name)
        directory = stages_root / name
        directory.mkdir(parents=True, exist_ok=True)
        directory.joinpath("stage.patch").write_bytes(
            patch_for_stage(manifest, stage.number, root=root)
        )
        directory.joinpath("tests.txt").write_text("\n".join(stage.tests) + "\n")
    for directory in stages_root.iterdir():
        if directory.is_dir() and directory.name not in expected:
            raise ValueError(f"unexpected Stage directory: {directory}")


def main() -> int:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    write_stage_sources(
        manifest,
        root=ROOT,
        stages_root=ROOT / "journey" / "stages",
    )
    print(f"wrote {len(manifest.stages)} MiniDist Stage patch/test pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
