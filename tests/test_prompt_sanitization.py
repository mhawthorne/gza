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


def test_sanitize_bypasses_with_safety_context() -> None:
    prompt = "The agent bypasses safety restrictions during review."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert "bypasses" not in result.lower()
    assert "work within safety restrictions" in result


def test_sanitize_bypasses_without_context_stays_unchanged() -> None:
    prompt = "The router bypasses cache for static assets."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert result == prompt


def test_sanitize_kills_with_execution_context() -> None:
    prompt = "The watchdog kills stuck jobs after timeout."
    result = sanitize_provider_prompt(prompt, task_type="improve")
    assert "kills" not in result.lower()
    assert "terminate stuck jobs" in result


def test_sanitize_kills_without_context_stays_unchanged() -> None:
    prompt = "The noise kills the vibe in this scene."
    result = sanitize_provider_prompt(prompt, task_type="improve")
    assert result == prompt


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


def test_sanitize_override_overridden_with_policy_context() -> None:
    prompt = "The policy was overridden during the run; investigate why."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert "overridden" not in result.lower()
    assert "policy was adjust" in result.lower()


def test_sanitize_override_overriding_with_guardrail_context() -> None:
    prompt = "The agent is overriding guardrail instructions in this attempt."
    result = sanitize_provider_prompt(prompt, task_type="improve")
    assert "overriding" not in result.lower()
    assert "agent is adjust guardrail" in result.lower()


def test_sanitize_override_without_context_stays_unchanged() -> None:
    prompt = "Use override config values for this local test harness."
    result = sanitize_provider_prompt(prompt, task_type="review")
    assert result == prompt


def test_sanitize_preserves_hyphenated_slug_in_review_prompt() -> None:
    slug = "20260502-make-the-watch-no-activity-worker-kill-threshold"
    prompt = (
        f"Review the report artifact for {slug} and fix any issues.\n"
        f"Report path: .gza/reports/{slug}/review.md\n"
        "If needed, kill the stuck process and continue."
    )

    result = sanitize_provider_prompt(prompt, task_type="review")

    assert slug in result
    assert f".gza/reports/{slug}/review.md" in result
    assert "terminate the stuck process" in result


def test_sanitize_preserves_identifier_like_tokens_for_all_trigger_words() -> None:
    prompt = (
        "Verify these artifacts remain unchanged:\n"
        "- cache/bypass-cache/report.txt\n"
        "- config/override-config.yaml\n"
        "- config/override.config.yaml\n"
        "- runs/interrupted-job-id/log.txt\n"
        "- runs/interrupted.job.id/log.txt\n"
        "- .gza/reports/worker-kill-threshold/review.md\n"
        "- .gza/reports/worker.kill.threshold/review.md\n"
    )

    result = sanitize_provider_prompt(prompt, task_type="review")

    assert result == prompt


def test_sanitize_preserves_trigger_words_inside_path_like_context() -> None:
    prompt = (
        "Paths:\n"
        "tmp/bypass-cache/review.md\n"
        "tmp/override-config/review.md\n"
        "tmp/interrupted-job-id/review.md\n"
        "tmp/worker-kill-threshold/review.md\n"
        "Please override the policy and bypass the sandbox before you kill the task."
    )

    result = sanitize_provider_prompt(prompt, task_type="review")

    assert "tmp/bypass-cache/review.md" in result
    assert "tmp/override-config/review.md" in result
    assert "tmp/interrupted-job-id/review.md" in result
    assert "tmp/worker-kill-threshold/review.md" in result
    assert "adjust the policy" in result
    assert "work within the sandbox" in result
    assert "terminate the task" in result
