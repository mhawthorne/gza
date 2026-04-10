"""Helpers for consistent git commit message formatting."""

from .console import MAX_PR_TITLE_LENGTH


def _normalize_subject_text(text: str) -> str:
    """Normalize any whitespace/newlines in prompt text to single spaces."""
    return " ".join((text or "").split())


def _truncate_word_boundary(text: str, max_len: int, suffix: str = "...") -> str:
    """Truncate text at word boundaries so subjects avoid mid-word cuts."""
    if len(text) <= max_len:
        return text
    if max_len <= len(suffix):
        return suffix[:max_len]

    candidate = text[: max_len - len(suffix)].rstrip()
    last_space = candidate.rfind(" ")
    if last_space > 0:
        candidate = candidate[:last_space].rstrip()

    # If there is no usable boundary (e.g. one giant token), avoid mid-word cuts.
    if not candidate:
        return suffix
    return f"{candidate}{suffix}"


def format_commit_subject(prompt: str, max_len: int = MAX_PR_TITLE_LENGTH, prefix: str = "") -> str:
    """Build a normalized commit subject line within max_len chars."""
    safe_prefix = prefix or ""
    available = max_len - len(safe_prefix)
    if available <= 0:
        return safe_prefix[:max_len]

    subject = _normalize_subject_text(prompt)
    subject = _truncate_word_boundary(subject, available)
    return f"{safe_prefix}{subject}"


def format_task_trailers(task_id: str, task_slug: str | None, review_task_id: str | None = None) -> str:
    """Build canonical trailer lines for task-linked commits."""
    lines = [f"Task #{task_id}"]
    if task_slug:
        lines.append(f"Slug: {task_slug}")
    if review_task_id is not None:
        lines.append(f"Gza-Review: #{review_task_id}")
    return "\n".join(lines)


def build_task_commit_message(
    prompt: str,
    task_id: str,
    task_slug: str | None,
    review_task_id: str | None = None,
    subject_prefix: str = "",
    subject_max_len: int = MAX_PR_TITLE_LENGTH,
) -> str:
    """Build a full commit message with subject and canonical trailers."""
    subject = format_commit_subject(prompt, max_len=subject_max_len, prefix=subject_prefix)
    trailers = format_task_trailers(task_id, task_slug, review_task_id=review_task_id)
    return f"{subject}\n\n{trailers}"
