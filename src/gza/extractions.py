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
from .git import Git, _split_rename_paths, _unquote_c_style_path
from .task_slug import get_task_slug

EXTRACTIONS_DIR = Path(".gza") / "extractions"
MANIFEST_FILENAME = "manifest.json"
PATCH_FILENAME = "selected.patch"
PROMPT_FILENAME = "prompt.md"
RUNTIME_PATCH_FILENAMES = frozenset({"selected.runtime.patch"})

_CODE_TASK_TYPES = {"task", "implement", "improve", "fix", "rebase"}
_DIFF_RE = re.compile(r"^diff --git a/(.+) b/(.+)$")
_PROMPT_SECTION_HEADERS = frozenset(
    {
        "source",
        "operator intent",
        "selected files",
        "observed source diff metadata",
        "original task prompt",
        "source commit subjects",
    }
)
_NON_OBJECTIVE_PROMPT_SECTIONS = frozenset(
    {
        "source",
        "selected files",
        "observed source diff metadata",
        "source commit subjects",
        "operator intent",
    }
)


class ExtractionError(ValueError):
    """Raised when extraction inputs/artifacts are invalid."""


@dataclass(frozen=True)
class SourceSelection:
    """Resolved source identity for extraction planning."""

    source_task_id: str | None
    source_branch: str | None = None
    source_base_ref: str | None = None
    source_commits: tuple[str, ...] = ()
    source_commit_subjects: tuple[str, ...] = ()
    source_task_prompt: str | None = None
    source_task_slug: str | None = None

    @property
    def mode(self) -> str:
        return "commit" if self.source_commits else "branch"


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
    source_commits: tuple[str, ...] = (),
    base_branch_override: str | None,
) -> SourceSelection:
    """Resolve extraction source from task ID or explicit branch selector."""
    selectors = int(bool(source_task_id)) + int(bool(source_branch)) + int(bool(source_commits))
    if selectors != 1:
        raise ExtractionError("Specify exactly one source selector: SOURCE task ID, --branch, or --commit")

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
            source_task_prompt=task.prompt,
            source_task_slug=task.slug,
        )

    if source_commits:
        resolved_commits: list[str] = []
        commit_subjects: list[str] = []
        seen: set[str] = set()
        for commit_ref in source_commits:
            if not git.ref_exists(commit_ref):
                raise ExtractionError(f"Source commit not found: {commit_ref}")
            resolved = git.rev_parse(commit_ref)
            if resolved in seen:
                raise ExtractionError(f"Duplicate source commit selected: {commit_ref}")
            seen.add(resolved)
            resolved_commits.append(resolved)
            commit_subjects.append(git.get_commit_subject(resolved))
        return SourceSelection(
            source_task_id=None,
            source_commits=tuple(resolved_commits),
            source_commit_subjects=tuple(commit_subjects),
        )

    assert source_branch is not None
    branch = source_branch
    base_ref = base_branch_override or git.default_branch()
    if not git.ref_exists(branch):
        raise ExtractionError(f"Source branch not found: {branch}")
    if not git.ref_exists(base_ref):
        raise ExtractionError(f"Source base ref not found: {base_ref}")
    return SourceSelection(
        source_task_id=None,
        source_branch=branch,
        source_base_ref=base_ref,
    )


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

    name_status_text, numstat_text, patch = _collect_source_diff(git, source, selected_paths)
    summaries = _parse_file_summaries(name_status_text, numstat_text, selected_paths)
    _ensure_every_selected_path_changed(summaries, selected_paths)
    if not patch.strip():
        raise ExtractionError(_no_extractable_diff_message(source, selected=True))

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


def infer_selected_paths(git: Git, source: SourceSelection) -> tuple[str, ...]:
    """Infer the full changed path set for a source diff."""
    _name_status_text, _numstat_text, patch = _collect_source_diff(git, source, ())
    selected_paths = tuple(parse_patch_touched_paths(patch))
    if not selected_paths:
        raise ExtractionError(_no_extractable_diff_message(source, selected=False))
    return selected_paths


def draft_extraction_prompt(
    *,
    source: SourceSelection,
    selected_paths: tuple[str, ...],
    summaries: list[FileDiffSummary],
    operator_prompt: str | None,
) -> str:
    """Draft the implement prompt for an extracted task."""
    objective = _describe_source_objective(
        source=source,
        selected_paths=selected_paths,
        summaries=summaries,
    )
    if source.source_task_id:
        source_label = f"task {source.source_task_id}"
    elif source.mode == "commit":
        source_label = _describe_commit_source_label(source.source_commits)
    else:
        assert source.source_branch is not None
        source_label = f"branch {source.source_branch}"

    source_detail = _describe_prompt_source_detail(source)

    lines = [
        f"Carry over: {objective}",
        "",
        f"Source: {source_label} ({source_detail}).",
        "",
    ]

    if source.source_task_prompt:
        lines.extend(["", "Original task prompt:", source.source_task_prompt.strip()])

    if source.source_commit_subjects:
        lines.extend(["", "Source commit subjects:"])
        for commit_ref, subject in zip(source.source_commits, source.source_commit_subjects, strict=False):
            label = subject or "(empty subject)"
            lines.append(f"- {commit_ref[:12]}: {label}")

    lines.extend(["", f"Selected files ({len(selected_paths)}):"])

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


def _describe_source_objective(
    *,
    source: SourceSelection,
    selected_paths: tuple[str, ...],
    summaries: list[FileDiffSummary],
) -> str:
    """Derive a concise extract title from source metadata plus diff context."""
    prompt_summary = _summarize_prompt_line(source.source_task_prompt)
    if _is_specific_objective(prompt_summary):
        assert prompt_summary is not None
        return prompt_summary

    if source.mode == "commit":
        commit_summary = _describe_commit_subject_objective(source.source_commit_subjects)
        if _is_specific_objective(commit_summary):
            assert commit_summary is not None
            return commit_summary

    if source.source_branch and source.source_task_id is None:
        branch_summary = _humanize_branch_name(source.source_branch)
        if branch_summary:
            assert branch_summary is not None
            return branch_summary

    topic_hint = _best_topic_hint(source)
    diff_objective = _describe_diff_objective(
        selected_paths=selected_paths,
        summaries=summaries,
        topic_hint=topic_hint,
    )
    if _is_specific_objective(diff_objective):
        return diff_objective

    if source.source_task_slug:
        slug_text = get_task_slug(source.source_task_slug) or source.source_task_slug
        humanized_slug = _humanize_identifier(slug_text)
        if _is_specific_objective(humanized_slug):
            assert humanized_slug is not None
            return humanized_slug

    if source.source_branch:
        branch_summary = _humanize_branch_name(source.source_branch)
        if _is_specific_objective(branch_summary):
            assert branch_summary is not None
            return branch_summary

    if source.mode == "commit":
        return _describe_commit_objective(source)

    return diff_objective or "selected source changes"


def _summarize_prompt_line(prompt: str | None) -> str | None:
    """Extract the most specific single-line summary from a task prompt."""
    if not prompt:
        return None

    best_line: str | None = None
    best_score = float("-inf")
    for line in _iter_prompt_objective_candidates(prompt):
        if not line:
            continue
        score = _objective_specificity_score(line)
        if score > best_score:
            best_score = score
            best_line = line
    return best_line


def _iter_prompt_objective_candidates(prompt: str) -> list[str]:
    """Return prompt lines that can legitimately describe the extraction objective."""
    candidates: list[str] = []
    current_section = "title"

    for raw_line in prompt.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue

        normalized_header = _normalize_prompt_header(stripped)
        if normalized_header:
            current_section = normalized_header
            continue

        if current_section in _NON_OBJECTIVE_PROMPT_SECTIONS:
            continue

        line = _normalize_prompt_candidate(raw_line)
        if not line or _looks_like_prompt_section_header(line) or _looks_like_prompt_boilerplate(line):
            continue

        candidates.append(line)

    return candidates


def _normalize_prompt_header(value: str) -> str | None:
    lowered = value.lower().rstrip(":")
    if lowered in _PROMPT_SECTION_HEADERS:
        return lowered
    if lowered.startswith("selected files ("):
        return "selected files"
    return None


def _humanize_identifier(value: str) -> str:
    """Convert slug/branch fragments into readable text."""
    normalized = value.replace("/", " ").replace("_", " ").replace("-", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _normalize_prompt_candidate(value: str) -> str:
    line = value.strip()
    if not line:
        return ""
    line = re.sub(r"^[*\-+]\s+", "", line)
    line = re.sub(r"^\d+\.\s+", "", line)
    line = re.sub(
        r"^(task|prompt|title|summary|objective|goal)\s*:\s*",
        "",
        line,
        flags=re.IGNORECASE,
    )
    line = re.sub(r"^carry\s+over\s*:\s*", "", line, flags=re.IGNORECASE)
    line = re.sub(r"\s+", " ", line).strip(" -:.")
    return line


def _looks_like_prompt_section_header(line: str) -> bool:
    lowered = line.lower().rstrip(":")
    return lowered in _PROMPT_SECTION_HEADERS


def _looks_like_prompt_boilerplate(line: str) -> bool:
    lowered = line.lower()
    return lowered.startswith(
        (
            "source:",
            "validate the seeded patch in this worktree",
        )
    )


def _is_specific_objective(text: str | None) -> bool:
    return _objective_specificity_score(text) >= 10


def _objective_specificity_score(text: str | None) -> int:
    if not text:
        return -100

    lowered = text.lower().strip()
    if not lowered:
        return -100

    words = re.findall(r"[a-z0-9]+", lowered)
    if not words:
        return -100

    score = min(len(words), 8)
    if 3 <= len(words) <= 12:
        score += 6
    elif len(words) <= 2:
        score -= 6

    action_verbs = {
        "add",
        "allow",
        "build",
        "copy",
        "create",
        "derive",
        "ensure",
        "fix",
        "generate",
        "handle",
        "implement",
        "improve",
        "prevent",
        "preserve",
        "remove",
        "rename",
        "restore",
        "seed",
        "support",
        "update",
        "validate",
    }
    if words[0] in action_verbs:
        score += 12
    elif any(word in action_verbs for word in words[:2]):
        score += 8

    if lowered.startswith("carry over"):
        score -= 12

    generic_words = {"changes", "work", "task", "tasks", "source", "branch", "commit", "commits"}
    if len(words) <= 3 and words[-1] in generic_words:
        score -= 8

    if lowered.endswith(":"):
        score -= 8

    return score


def _best_topic_hint(source: SourceSelection) -> str | None:
    if source.source_task_slug:
        slug_text = get_task_slug(source.source_task_slug) or source.source_task_slug
        humanized_slug = _humanize_identifier(slug_text)
        if humanized_slug:
            return humanized_slug
    if source.source_branch:
        return _humanize_branch_name(source.source_branch)
    return None


def _humanize_branch_name(branch: str) -> str:
    return _humanize_identifier(branch.rsplit("/", 1)[-1])


def _describe_commit_subject_objective(subjects: tuple[str, ...]) -> str | None:
    if len(subjects) != 1:
        return None
    subject = _normalize_prompt_candidate(subjects[0])
    return subject or None


def _describe_diff_objective(
    *,
    selected_paths: tuple[str, ...],
    summaries: list[FileDiffSummary],
    topic_hint: str | None,
) -> str:
    if len(summaries) == 1:
        summary = summaries[0]
        if summary.status == "R" and summary.old_path and summary.new_path:
            return f"rename {_describe_path_focus(summary.old_path)} to {_describe_path_focus(summary.new_path)}"
        if summary.status == "C" and summary.old_path and summary.new_path:
            return f"copy {_describe_path_focus(summary.old_path)} to {_describe_path_focus(summary.new_path)}"

    action = _describe_diff_action(summaries)
    focus = _describe_selected_focus(selected_paths)
    if focus in {"module", "config", "doc", "asset", "test", "tests", "file", "files"} and topic_hint:
        if topic_hint.lower() not in focus.lower():
            focus = f"{topic_hint} {focus}"
    if focus == "selected files" and topic_hint:
        focus = f"{topic_hint} files"
    return f"{action} {focus}"


def _describe_diff_action(summaries: list[FileDiffSummary]) -> str:
    statuses = {summary.status for summary in summaries}
    if statuses == {"A"}:
        return "add"
    if statuses == {"D"}:
        return "remove"
    if statuses == {"R"}:
        return "rename"
    if statuses == {"C"}:
        return "copy"
    return "update"


def _describe_selected_focus(selected_paths: tuple[str, ...]) -> str:
    if len(selected_paths) == 1:
        return _describe_path_focus(selected_paths[0])
    if len(selected_paths) == 2:
        return " and ".join(_describe_path_focus(path) for path in selected_paths)

    token_phrase = _common_path_token_phrase(selected_paths)
    if token_phrase:
        return f"{token_phrase} files"

    shared_dir = _shared_path_directory(selected_paths)
    if shared_dir:
        return f"{shared_dir} files"

    return f"{len(selected_paths)} files"


def _describe_path_focus(path: str) -> str:
    path_obj = Path(path)
    path_parts = [part for part in path_obj.parts[:-1] if part not in {"src", "lib", "app", "tests", "docs", "assets"}]
    stem = path_obj.stem

    if stem in {"__init__", "index", "main", "module", "config", "settings", "test"} and path_parts:
        tokens = _tokenize_identifier(path_parts[-1])
    else:
        tokens = _tokenize_identifier(stem)

    label = " ".join(tokens).strip()
    if not label:
        label = path_obj.name

    suffix = _path_kind_suffix(path_obj)
    if suffix and suffix not in label.split():
        label = f"{label} {suffix}"
    return label


def _path_kind_suffix(path: Path) -> str:
    path_text = path.as_posix()
    if "/tests/" in f"/{path_text}" or path.name.startswith("test_"):
        return "tests"
    if path.suffix in {".md", ".rst"}:
        return "doc"
    if path.suffix in {".yml", ".yaml", ".json", ".toml", ".ini"}:
        return "config"
    if path.suffix in {".png", ".jpg", ".jpeg", ".gif", ".svg", ".bin"}:
        return "asset"
    if path.suffix in {".py", ".js", ".ts", ".tsx", ".jsx", ".rb", ".go", ".rs"}:
        return "module"
    return ""


def _tokenize_identifier(value: str) -> list[str]:
    if not value:
        return []
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    normalized = re.sub(r"[^A-Za-z0-9]+", " ", normalized)
    tokens = [token.lower() for token in normalized.split() if token]
    stopwords = {"src", "lib", "app", "file", "files"}
    return [token for token in tokens if token not in stopwords]


def _common_path_token_phrase(selected_paths: tuple[str, ...]) -> str | None:
    counts: dict[str, int] = {}
    for path in selected_paths:
        seen = set(_tokenize_identifier(Path(path).stem))
        for token in seen:
            counts[token] = counts.get(token, 0) + 1

    if not counts:
        return None

    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    shared = [token for token, count in ranked if count >= 2]
    chosen = shared[:2] or [ranked[0][0]]
    phrase = " ".join(chosen).strip()
    return phrase or None


def _shared_path_directory(selected_paths: tuple[str, ...]) -> str | None:
    parents = [Path(path).parent.as_posix() for path in selected_paths]
    common = posixpath.commonpath(parents)
    if common in {"", "."}:
        return None

    parts = [part for part in common.split("/") if part and part not in {"src", "lib", "app", "tests", "docs", "assets"}]
    if not parts:
        return None
    return _humanize_identifier(parts[-1])


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
        "source_commits": list(draft.source.source_commits),
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
    allowed_existing = expected | set(RUNTIME_PATCH_FILENAMES)
    worktree_bundle_dir = worktree_path / ".gza" / "extractions" / project_bundle_dir.name
    worktree_bundle_dir.parent.mkdir(parents=True, exist_ok=True)

    if worktree_bundle_dir.exists():
        existing = {entry.name for entry in worktree_bundle_dir.iterdir()}
        unexpected = sorted(existing - allowed_existing)
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


def _collect_source_diff(
    git: Git,
    source: SourceSelection,
    selected_paths: tuple[str, ...],
) -> tuple[str, str, str]:
    if source.mode == "commit":
        return _collect_commit_source_diff(git, source.source_commits, selected_paths)

    assert source.source_base_ref is not None
    assert source.source_branch is not None
    revision_range = f"{source.source_base_ref}...{source.source_branch}"
    return (
        git.get_diff_name_status(revision_range, selected_paths),
        git.get_diff_numstat(revision_range, selected_paths),
        git.get_diff_patch_for_paths(revision_range, selected_paths, binary=True),
    )


def _collect_commit_source_diff(
    git: Git,
    source_commits: tuple[str, ...],
    selected_paths: tuple[str, ...],
) -> tuple[str, str, str]:
    name_status_parts: list[str] = []
    numstat_parts: list[str] = []
    patch_parts: list[str] = []

    for commit_ref in source_commits:
        name_status_text = git.get_commit_name_status(commit_ref, selected_paths)
        if name_status_text:
            name_status_parts.append(name_status_text)
        numstat_text = git.get_commit_numstat(commit_ref, selected_paths)
        if numstat_text:
            numstat_parts.append(numstat_text)
        patch_text = git.get_commit_patch_for_paths(commit_ref, selected_paths, binary=True)
        if patch_text.strip():
            patch_parts.append(patch_text.rstrip("\n"))

    return (
        "\n".join(name_status_parts),
        "\n".join(numstat_parts),
        ("\n".join(patch_parts) + ("\n" if patch_parts else "")),
    )


def _no_extractable_diff_message(source: SourceSelection, *, selected: bool) -> str:
    if source.mode == "commit":
        if selected:
            return "No extractable diff for the selected file set on the chosen source commit set"
        return "Source commit set has no extractable diff for the selected revisions"
    if selected:
        return "No extractable diff for the selected file set on the source branch"
    return "Source branch has no extractable diff against the chosen base"


def _describe_commit_source_label(source_commits: tuple[str, ...]) -> str:
    if len(source_commits) == 1:
        return f"commit {source_commits[0][:12]}"
    return f"{len(source_commits)} commits"


def _describe_prompt_source_detail(source: SourceSelection) -> str:
    if source.mode == "commit":
        if len(source.source_commits) == 1:
            return f"commit `{source.source_commits[0]}`"
        commits_text = ", ".join(f"`{commit}`" for commit in source.source_commits)
        return f"commits in extraction order: {commits_text}"

    assert source.source_branch is not None
    assert source.source_base_ref is not None
    return f"branch `{source.source_branch}` vs `{source.source_base_ref}`"


def _describe_commit_objective(source: SourceSelection) -> str:
    if len(source.source_commits) == 1:
        if source.source_commit_subjects:
            subject = _normalize_prompt_candidate(source.source_commit_subjects[0])
            if subject:
                return subject
        return f"commit {source.source_commits[0][:12]}"
    return f"selected changes from {len(source.source_commits)} commits"


def _parse_file_summaries(
    name_status_text: str,
    numstat_text: str,
    selected_paths: tuple[str, ...],
) -> list[FileDiffSummary]:
    selected = set(selected_paths)
    stats_by_path = _parse_numstat_by_path(numstat_text)
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
        additions, deletions, binary = stats_by_path.get(selected_path, (None, None, False))
        if (additions, deletions, binary) == (None, None, False):
            if new_path and new_path in stats_by_path:
                additions, deletions, binary = stats_by_path[new_path]
            elif old_path and old_path in stats_by_path:
                additions, deletions, binary = stats_by_path[old_path]

        out.append(
            FileDiffSummary(
                status=status,
                selected_path=selected_path,
                old_path=old_path,
                new_path=new_path,
                additions=additions,
                deletions=deletions,
                binary=binary,
            )
        )

    if not out:
        raise ExtractionError("Selected files have no diff entries on the chosen source/base")

    return out


def _parse_numstat_by_path(numstat_text: str) -> dict[str, tuple[int | None, int | None, bool]]:
    stats: dict[str, tuple[int | None, int | None, bool]] = {}

    for line in numstat_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        parts = stripped.split("\t", 2)
        if len(parts) != 3:
            continue

        raw_additions, raw_deletions, pathspec = parts
        pathspec = pathspec.strip()
        if not pathspec:
            continue

        path_keys = _numstat_path_keys(pathspec)
        if not path_keys:
            continue

        if raw_additions == "-" and raw_deletions == "-":
            for path_key in path_keys:
                stats[path_key] = (None, None, True)
            continue

        try:
            additions = int(raw_additions)
            deletions = int(raw_deletions)
        except ValueError:
            continue

        for path_key in path_keys:
            stats[path_key] = (additions, deletions, False)

    return stats


def _numstat_path_keys(pathspec: str) -> tuple[str, ...]:
    brace_rename_paths = _split_braced_rename_paths(pathspec)
    if brace_rename_paths is not None:
        return brace_rename_paths

    rename_paths = _split_rename_paths(pathspec)
    if rename_paths is None:
        return (_unquote_c_style_path(pathspec),)

    old_raw, new_raw = rename_paths
    old_path = _unquote_c_style_path(old_raw.strip())
    new_path = _unquote_c_style_path(new_raw.strip())
    if not old_path and not new_path:
        return ()
    if old_path == new_path:
        return (old_path,)
    if old_path and new_path:
        return (old_path, new_path)
    return (old_path or new_path,)


def _split_braced_rename_paths(pathspec: str) -> tuple[str, str] | None:
    start = pathspec.find("{")
    end = pathspec.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    inner = pathspec[start + 1 : end]
    marker = " => "
    if marker not in inner:
        return None

    old_inner, new_inner = inner.split(marker, 1)
    prefix = pathspec[:start]
    suffix = pathspec[end + 1 :]
    old_path = _unquote_c_style_path((prefix + old_inner + suffix).strip())
    new_path = _unquote_c_style_path((prefix + new_inner + suffix).strip())
    return (old_path, new_path)


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
