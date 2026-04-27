"""Extraction planning/runtime helpers for ``gza extract``."""

from __future__ import annotations

import json
import posixpath
import re
import shlex
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .db import SqliteTaskStore, Task
from .git import Git

EXTRACTIONS_DIR = Path(".gza") / "extractions"
MANIFEST_FILENAME = "manifest.json"
PATCH_FILENAME = "selected.patch"
PROMPT_FILENAME = "prompt.md"

_CODE_TASK_TYPES = {"task", "implement", "improve", "fix", "rebase"}
_DIFF_RE = re.compile(r"^diff --git a/(.+) b/(.+)$")


class ExtractionError(ValueError):
    """Raised when extraction inputs/artifacts are invalid."""


@dataclass(frozen=True)
class SourceSelection:
    """Resolved source identity for extraction planning."""

    source_task_id: str | None
    source_branch: str
    source_base_ref: str


@dataclass(frozen=True)
class FileDiffSummary:
    """Diff metadata for a selected file path."""

    status: str
    selected_path: str
    old_path: str | None
    new_path: str | None
    additions: int | None
    deletions: int | None
    binary: bool


@dataclass(frozen=True)
class ExtractionDraft:
    """Computed extraction payload before task creation."""

    source: SourceSelection
    selected_paths: tuple[str, ...]
    touched_paths: tuple[str, ...]
    file_summaries: tuple[FileDiffSummary, ...]
    patch: str
    prompt: str


def extraction_root(project_dir: Path) -> Path:
    """Return extraction artifact root under the project."""
    return project_dir / EXTRACTIONS_DIR


def extraction_bundle_path(project_dir: Path, task_slug: str) -> Path:
    """Return extraction bundle path for a specific task slug."""
    return extraction_root(project_dir) / task_slug


def read_paths_file(path: Path) -> list[str]:
    """Read newline-delimited paths file for ``--files-from``."""
    if not path.exists():
        raise ExtractionError(f"--files-from path not found: {path}")

    selected: list[str] = []
    try:
        content = path.read_text()
    except (OSError, UnicodeError) as exc:
        raise ExtractionError(f"Unable to read --files-from path {path}: {exc}") from exc

    for line in content.splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        selected.append(candidate)
    return selected


def normalize_selected_paths(paths: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Normalize/validate selected repo-relative paths."""
    normalized: list[str] = []
    seen: set[str] = set()

    for raw in paths:
        candidate = raw.strip()
        if not candidate:
            raise ExtractionError("Selected file paths must not be empty")

        posix_candidate = candidate.replace("\\", "/")
        normalized_path = posixpath.normpath(posix_candidate)

        if normalized_path in ("", "."):
            raise ExtractionError(f"Invalid selected path: {raw!r}")
        if normalized_path.startswith("../") or normalized_path == "..":
            raise ExtractionError(
                f"Selected path must stay within the repository root: {raw!r}",
            )
        if normalized_path.startswith("/"):
            raise ExtractionError(f"Selected path must be repo-relative: {raw!r}")

        if normalized_path not in seen:
            seen.add(normalized_path)
            normalized.append(normalized_path)

    return tuple(normalized)


def resolve_source_selection(
    store: SqliteTaskStore,
    git: Git,
    *,
    source_task_id: str | None,
    source_branch: str | None,
    base_branch_override: str | None,
) -> SourceSelection:
    """Resolve extraction source from task ID or explicit branch selector."""
    if bool(source_task_id) == bool(source_branch):
        raise ExtractionError("Specify exactly one source selector: SOURCE task ID or --branch")

    if source_task_id:
        task = store.get(source_task_id)
        if task is None:
            raise ExtractionError(f"Task {source_task_id} not found")
        _validate_task_source(task)
        assert task.branch is not None
        branch = task.branch
        base_ref = base_branch_override or task.base_branch or git.default_branch()
        if not git.ref_exists(branch):
            raise ExtractionError(f"Source task branch not found: {branch}")
        if not git.ref_exists(base_ref):
            raise ExtractionError(f"Source base ref not found: {base_ref}")
        return SourceSelection(
            source_task_id=task.id,
            source_branch=branch,
            source_base_ref=base_ref,
        )

    assert source_branch is not None
    branch = source_branch
    base_ref = base_branch_override or git.default_branch()
    if not git.ref_exists(branch):
        raise ExtractionError(f"Source branch not found: {branch}")
    if not git.ref_exists(base_ref):
        raise ExtractionError(f"Source base ref not found: {base_ref}")
    return SourceSelection(source_task_id=None, source_branch=branch, source_base_ref=base_ref)


def plan_extraction(
    git: Git,
    source: SourceSelection,
    selected_paths: tuple[str, ...],
    *,
    operator_prompt: str | None,
) -> ExtractionDraft:
    """Build extraction patch + manifest metadata + drafted prompt."""
    if not selected_paths:
        raise ExtractionError("Select at least one file path for extraction")

    revision_range = f"{source.source_base_ref}...{source.source_branch}"
    name_status_text = git.get_diff_name_status(revision_range, selected_paths)
    summaries = _parse_file_summaries(name_status_text, selected_paths)
    _ensure_every_selected_path_changed(summaries, selected_paths)

    patch = git.get_diff_patch_for_paths(revision_range, selected_paths, binary=True)
    if not patch.strip():
        raise ExtractionError(
            "No extractable diff for the selected file set on the source branch",
        )

    touched_paths = parse_patch_touched_paths(patch)
    if not touched_paths:
        raise ExtractionError("Generated patch has no file entries")

    allowed_paths = set(selected_paths)
    for summary in summaries:
        if summary.old_path:
            allowed_paths.add(summary.old_path)
        if summary.new_path:
            allowed_paths.add(summary.new_path)

    extra_touched = sorted(set(touched_paths) - allowed_paths)
    if extra_touched:
        raise ExtractionError(
            "Generated patch touched files outside selected set: " + ", ".join(extra_touched),
        )

    prompt = draft_extraction_prompt(
        source=source,
        selected_paths=selected_paths,
        summaries=summaries,
        operator_prompt=operator_prompt,
    )

    return ExtractionDraft(
        source=source,
        selected_paths=selected_paths,
        touched_paths=tuple(touched_paths),
        file_summaries=tuple(summaries),
        patch=patch,
        prompt=prompt,
    )


def draft_extraction_prompt(
    *,
    source: SourceSelection,
    selected_paths: tuple[str, ...],
    summaries: list[FileDiffSummary],
    operator_prompt: str | None,
) -> str:
    """Draft the implement prompt for an extracted task."""
    if source.source_task_id:
        source_label = f"task {source.source_task_id}"
    else:
        source_label = f"branch {source.source_branch}"

    lines = [
        "Finish and validate the extracted change set.",
        "",
        f"Source: {source_label} (branch `{source.source_branch}` vs `{source.source_base_ref}`).",
        "",
        f"Selected files ({len(selected_paths)}):",
    ]

    for path in selected_paths:
        lines.append(f"- {path}")

    lines.extend(["", "Observed source diff metadata:"])
    for summary in summaries:
        stat_suffix = ""
        if summary.binary:
            stat_suffix = " [binary]"
        elif summary.additions is not None and summary.deletions is not None:
            stat_suffix = f" (+{summary.additions}/-{summary.deletions})"

        if summary.status in {"R", "C"} and summary.old_path and summary.new_path:
            path_repr = f"{summary.old_path} -> {summary.new_path}"
        else:
            path_repr = summary.selected_path

        lines.append(f"- {summary.status}: {path_repr}{stat_suffix}")

    lines.extend(
        [
            "",
            "Validate the seeded patch in this worktree, finish any missing integration, run required verification, and produce the normal task summary.",
        ]
    )

    if operator_prompt:
        lines.extend(["", "Operator intent:", operator_prompt.strip()])

    return "\n".join(lines).strip() + "\n"


def write_extraction_bundle(
    *,
    project_dir: Path,
    task: Task,
    draft: ExtractionDraft,
) -> Path:
    """Persist extraction manifest/patch/prompt under `.gza/extractions/<slug>/`."""
    if not task.id or not task.slug:
        raise ExtractionError("Target task must have both id and slug before writing extraction bundle")

    bundle_dir = extraction_bundle_path(project_dir, task.slug)
    manifest_path = bundle_dir / MANIFEST_FILENAME
    if bundle_dir.exists() and manifest_path.exists():
        existing_manifest = load_manifest(manifest_path)
        existing_target_id = existing_manifest.get("target_task_id")
        if existing_target_id != task.id:
            raise ExtractionError(
                f"Extraction bundle path already reserved for task {existing_target_id}: {bundle_dir}",
            )
    bundle_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "version": 1,
        "source_task_id": draft.source.source_task_id,
        "source_branch": draft.source.source_branch,
        "source_base_ref": draft.source.source_base_ref,
        "target_task_id": task.id,
        "target_slug": task.slug,
        "selected_paths": list(draft.selected_paths),
        "touched_paths": list(draft.touched_paths),
        "patch_path": PATCH_FILENAME,
        "prompt_path": PROMPT_FILENAME,
        "created_at": datetime.now(UTC).isoformat(),
        "file_summaries": [
            {
                "status": summary.status,
                "selected_path": summary.selected_path,
                "old_path": summary.old_path,
                "new_path": summary.new_path,
                "additions": summary.additions,
                "deletions": summary.deletions,
                "binary": summary.binary,
            }
            for summary in draft.file_summaries
        ],
    }

    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (bundle_dir / PATCH_FILENAME).write_text(draft.patch)
    (bundle_dir / PROMPT_FILENAME).write_text(draft.prompt)
    return bundle_dir


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load and minimally validate an extraction manifest file."""
    if not manifest_path.exists():
        raise ExtractionError(f"Extraction manifest missing: {manifest_path}")

    try:
        manifest_text = manifest_path.read_text()
    except (OSError, UnicodeError) as exc:
        raise ExtractionError(f"Unable to read extraction manifest: {manifest_path}: {exc}") from exc

    try:
        manifest = json.loads(manifest_text)
    except json.JSONDecodeError as exc:
        raise ExtractionError(f"Invalid extraction manifest JSON: {manifest_path}") from exc

    required = [
        "version",
        "target_task_id",
        "target_slug",
        "selected_paths",
        "patch_path",
    ]
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ExtractionError(
            f"Extraction manifest missing required keys ({', '.join(missing)}): {manifest_path}",
        )

    return manifest


def load_patch_text(patch_path: Path) -> str:
    """Load extraction patch text and normalize read/decode failures."""
    if not patch_path.exists():
        raise ExtractionError(f"Extraction patch missing: {patch_path}")

    try:
        return patch_path.read_text()
    except (OSError, UnicodeError) as exc:
        raise ExtractionError(f"Unable to read extraction patch: {patch_path}: {exc}") from exc


def copy_bundle_to_worktree(project_bundle_dir: Path, worktree_path: Path) -> Path:
    """Copy a project extraction bundle into a task worktree."""
    if not project_bundle_dir.exists():
        raise ExtractionError(f"Extraction bundle not found: {project_bundle_dir}")

    expected = {MANIFEST_FILENAME, PATCH_FILENAME, PROMPT_FILENAME}
    worktree_bundle_dir = worktree_path / ".gza" / "extractions" / project_bundle_dir.name
    worktree_bundle_dir.parent.mkdir(parents=True, exist_ok=True)

    if worktree_bundle_dir.exists():
        existing = {entry.name for entry in worktree_bundle_dir.iterdir()}
        unexpected = sorted(existing - expected)
        if unexpected:
            raise ExtractionError(
                "Extraction bundle destination has unexpected files: " + ", ".join(unexpected),
            )

    shutil.copytree(project_bundle_dir, worktree_bundle_dir, dirs_exist_ok=True)
    return worktree_bundle_dir


def parse_patch_touched_paths(patch_text: str) -> list[str]:
    """Extract touched file paths from a unified patch text."""
    touched: set[str] = set()
    for line in patch_text.splitlines():
        header_paths = _parse_diff_git_header_paths(line)
        if not header_paths:
            continue

        for raw_path in header_paths:
            if raw_path == "/dev/null":
                continue
            touched.add(raw_path)

    return sorted(touched)


def resolve_manifest_patch_path(bundle_dir: Path, patch_path_value: Any) -> Path:
    """Resolve/validate manifest patch path as a bundle-contained relative path."""
    if not isinstance(patch_path_value, str) or not patch_path_value.strip():
        raise ExtractionError("Extraction manifest patch_path must be a non-empty string")

    normalized = posixpath.normpath(patch_path_value.strip().replace("\\", "/"))
    if normalized in {"", ".", ".."}:
        raise ExtractionError("Extraction manifest patch_path must be a bundle-relative file path")
    if normalized.startswith("/") or normalized.startswith("../"):
        raise ExtractionError(f"Extraction manifest patch_path is not bundle-relative: {patch_path_value!r}")

    bundle_resolved = bundle_dir.resolve()
    patch_path = (bundle_dir / normalized).resolve()
    try:
        patch_path.relative_to(bundle_resolved)
    except ValueError as exc:
        raise ExtractionError(
            f"Extraction manifest patch_path escapes extraction bundle: {patch_path_value!r}",
        ) from exc
    return patch_path


def _parse_diff_git_header_paths(line: str) -> tuple[str, str] | None:
    if line.startswith("diff --git "):
        header = line[len("diff --git ") :]
        try:
            parts = shlex.split(header, posix=True)
        except ValueError:
            parts = []
        if len(parts) == 2:
            left = _normalize_diff_header_path(parts[0], "a/")
            right = _normalize_diff_header_path(parts[1], "b/")
            if left is not None and right is not None:
                return left, right

    match = _DIFF_RE.match(line)
    if match:
        return match.group(1), match.group(2)
    return None


def _normalize_diff_header_path(path_with_prefix: str, prefix: str) -> str | None:
    if path_with_prefix == "/dev/null":
        return "/dev/null"
    if not path_with_prefix.startswith(prefix):
        return None
    return path_with_prefix[len(prefix) :]


def _validate_task_source(task: Task) -> None:
    if task.task_type not in _CODE_TASK_TYPES:
        raise ExtractionError(
            f"Task {task.id} is a {task.task_type} task. Source task must be a code task.",
        )
    if task.status not in {"completed", "failed"}:
        raise ExtractionError(
            f"Task {task.id} is {task.status}. Source task must be completed or failed.",
        )
    if not task.branch:
        raise ExtractionError(f"Task {task.id} has no branch to extract from")


def _parse_file_summaries(name_status_text: str, selected_paths: tuple[str, ...]) -> list[FileDiffSummary]:
    selected = set(selected_paths)
    out: list[FileDiffSummary] = []

    for line in name_status_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        parts = stripped.split("\t")
        status_raw = parts[0]
        status = status_raw[0]

        old_path: str | None = None
        new_path: str | None = None

        if status in {"R", "C"} and len(parts) >= 3:
            old_path = parts[1]
            new_path = parts[2]
        elif len(parts) >= 2:
            new_path = parts[1]

        match_paths = [p for p in (old_path, new_path) if p and p in selected]
        if not match_paths:
            continue

        selected_path = match_paths[0]
        out.append(
            FileDiffSummary(
                status=status,
                selected_path=selected_path,
                old_path=old_path,
                new_path=new_path,
                additions=None,
                deletions=None,
                binary=False,
            )
        )

    if not out:
        raise ExtractionError("Selected files have no diff entries on the chosen source/base")

    return out


def _ensure_every_selected_path_changed(
    summaries: list[FileDiffSummary],
    selected_paths: tuple[str, ...],
) -> None:
    matched: set[str] = set()
    for summary in summaries:
        matched.add(summary.selected_path)
        if summary.old_path:
            matched.add(summary.old_path)
        if summary.new_path:
            matched.add(summary.new_path)

    missing = [path for path in selected_paths if path not in matched]
    if missing:
        raise ExtractionError(
            "Selected paths are not changed on source branch: " + ", ".join(missing),
        )
