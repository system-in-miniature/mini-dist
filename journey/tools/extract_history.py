#!/usr/bin/env python3
"""Resolve MiniDist's content-driven Journey snapshots from Git evidence."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import subprocess
import tomllib


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
