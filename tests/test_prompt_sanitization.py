"""Tests for provider-facing prompt sanitization."""

from gza.prompt_sanitization import sanitize_provider_prompt


def test_sanitize_bypass_only_with_safety_context() -> None:
    prompt = "Do not bypass sandbox restrictions. Use normal flows."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert "bypass" not in result.lower()
    assert "work within sandbox restrictions" in result


def test_sanitize_leaves_unrelated_bypass_unchanged() -> None:
    prompt = "Use the network bypass mode in this legacy proxy setup."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert result == prompt


def test_sanitize_kill_in_execution_context() -> None:
    prompt = "If needed, kill the stuck process and continue."
    result = sanitize_provider_prompt(prompt, task_type="improve")
    assert "kill" not in result.lower()
    assert "terminate the stuck process" in result


def test_sanitize_skips_code_fences() -> None:
    prompt = "Avoid bypassing sandbox.\n```bash\nkill -9 1234\n```"
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert "work within sandbox" in result
    assert "kill -9 1234" in result


def test_sanitize_disabled_for_other_task_types() -> None:
    prompt = "bypass sandbox and kill process"
    result = sanitize_provider_prompt(prompt, task_type="implement")
    assert result == prompt


def test_sanitize_does_not_replace_when_context_is_far_away() -> None:
    prompt = "bypass " + ("x" * 220) + " sandbox restriction"
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert result == prompt


def test_sanitize_replaces_when_context_is_within_nearby_window() -> None:
    prompt = "Please bypass " + ("x" * 80) + " sandbox restriction for this review."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert "work within" in result
    assert "bypass" not in result.lower()
