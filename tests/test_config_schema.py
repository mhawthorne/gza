"""Parity tests for discoverable configuration key registry."""

import py_compile
from dataclasses import fields
from pathlib import Path

import pytest

from gza.config import (
    Config,
    ConfigError,
    DEFAULT_QUIET_PERIOD_SECONDS,
    DEFAULT_WATCH_DISPATCH_START_TIMEOUT,
    DEFAULT_WATCH_MAIN_VERIFY_REMEDIATION_MAX_ATTEMPTS,
)
from gza.config_schema import (
    CONFIG_KEY_REGISTRY,
    NON_CONFIG_ROOT_KEYS,
    RUNTIME_ONLY_CONFIG_FIELDS,
)


def test_config_registry_covers_all_config_fields() -> None:
    """All user-configurable Config dataclass fields should be discoverable in the key registry."""
    config_fields = {f.name for f in fields(Config)}
    user_fields = config_fields - set(RUNTIME_ONLY_CONFIG_FIELDS)
    registry_roots = {spec.key.split(".", 1)[0] for spec in CONFIG_KEY_REGISTRY}

    missing = user_fields - registry_roots
    assert not missing, f"Missing config roots in registry: {sorted(missing)}"


def test_config_registry_has_no_stale_keys() -> None:
    """Registry roots should match Config fields (plus explicit non-dataclass compatibility keys)."""
    config_fields = {f.name for f in fields(Config)}
    allowed_roots = (config_fields - set(RUNTIME_ONLY_CONFIG_FIELDS)) | set(NON_CONFIG_ROOT_KEYS)
    registry_roots = {spec.key.split(".", 1)[0] for spec in CONFIG_KEY_REGISTRY}

    stale = registry_roots - allowed_roots
    assert not stale, f"Stale/unknown config roots in registry: {sorted(stale)}"


def test_configuration_doc_mentions_all_registered_keys() -> None:
    """Operator docs should include every discoverable key listed by the registry."""
    docs_text = (Path(__file__).resolve().parents[1] / "docs" / "configuration.md").read_text()
    missing = [spec.key for spec in CONFIG_KEY_REGISTRY if spec.key not in docs_text]
    assert not missing, f"Missing config keys in docs/configuration.md: {missing}"


def test_config_registry_keys_are_unique() -> None:
    """Discoverable config keys should not be registered twice."""
    keys = [spec.key for spec in CONFIG_KEY_REGISTRY]
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    assert not duplicates, f"Duplicate config keys in registry: {duplicates}"


def test_config_module_compiles() -> None:
    """Config source should stay syntactically loadable after new fields are threaded through."""
    config_path = Path(__file__).resolve().parents[1] / "src" / "gza" / "config.py"
    py_compile.compile(str(config_path), doraise=True)


def test_docker_setup_command_registry_description_mentions_prewarm_execution() -> None:
    """Discoverable key metadata should describe docker_setup_command pre-warm semantics."""
    docker_setup_spec = next(spec for spec in CONFIG_KEY_REGISTRY if spec.key == "docker_setup_command")
    assert "Pre-warm" in docker_setup_spec.description
    assert "synchronously" in docker_setup_spec.description
    assert "before provider CLI starts" in docker_setup_spec.description


def test_advance_create_reviews_registry_description_matches_manual_refresh_semantics() -> None:
    """Config metadata should explain that review creation follows review-gating rules."""
    advance_reviews_spec = next(spec for spec in CONFIG_KEY_REGISTRY if spec.key == "advance_create_reviews")
    assert "review gating still requires them" in advance_reviews_spec.description
    assert "manual attention" in advance_reviews_spec.description


def test_code_task_diff_timeout_cap_registry_description_matches_hard_cap_docs() -> None:
    """Config metadata should describe the cap as a hard maximum, not an override bypass."""
    cap_spec = next(spec for spec in CONFIG_KEY_REGISTRY if spec.key == "code_task_diff_timeout_cap_minutes")
    assert "Hard maximum" in cap_spec.description
    assert "explicit task-type overrides can still be higher" not in cap_spec.description


def test_config_load_parses_pr_integration_false(tmp_path) -> None:
    """Explicit project opt-out should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "pr_integration: false\n"
    )

    config = Config.load(tmp_path)

    assert config.pr_integration is False


def test_config_load_parses_plan_review_lifecycle_keys(tmp_path) -> None:
    """Plan-review lifecycle controls should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "advance_create_plan_reviews: false\n"
        "require_plan_review_before_implement: false\n"
        "max_plan_review_cycles: 4\n"
        "max_failed_plan_review_retries: 5\n"
        "max_plan_slices: 7\n"
        "plan_slice_target_timeout_minutes: 25\n"
    )

    config = Config.load(tmp_path)

    assert config.advance_create_plan_reviews is False
    assert config.require_plan_review_before_implement is False
    assert config.max_plan_review_cycles == 4
    assert config.max_failed_plan_review_retries == 5
    assert config.max_plan_slices == 7
    assert config.plan_slice_target_timeout_minutes == 25
    assert config.get_plan_slice_target_timeout_minutes() == 25


def test_config_load_parses_advance_off_topic_verify_unblock(tmp_path) -> None:
    """The off-topic verify unblock policy knob should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "advance_off_topic_verify_unblock: true\n"
    )

    config = Config.load(tmp_path)

    assert config.advance_off_topic_verify_unblock is True


def test_plan_slice_target_timeout_defaults_from_code_task_timeout_cap(tmp_path) -> None:
    """Unset plan slice timeout should derive from the code-task timeout cap."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "code_task_diff_timeout_cap_minutes: 62\n"
    )

    config = Config.load(tmp_path)

    assert config.plan_slice_target_timeout_minutes is None
    assert config.get_plan_slice_target_timeout_minutes() == 62


def test_config_load_parses_docker_startup_timeout(tmp_path) -> None:
    """docker_startup_timeout should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "docker_startup_timeout: 60\n"
    )

    config = Config.load(tmp_path)

    assert config.docker_startup_timeout == 60


def test_config_load_defaults_watch_dispatch_start_timeout(tmp_path) -> None:
    """watch.dispatch_start_timeout should default when omitted."""
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")

    config = Config.load(tmp_path)

    assert config.watch.dispatch_start_timeout == DEFAULT_WATCH_DISPATCH_START_TIMEOUT


def test_config_load_defaults_watch_main_verify_remediation_max_attempts(tmp_path) -> None:
    """watch.main_verify_remediation_max_attempts should default when omitted."""
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")

    config = Config.load(tmp_path)

    assert (
        config.watch.main_verify_remediation_max_attempts
        == DEFAULT_WATCH_MAIN_VERIFY_REMEDIATION_MAX_ATTEMPTS
    )


def test_config_load_parses_watch_main_verify_remediation_max_attempts(tmp_path) -> None:
    """watch.main_verify_remediation_max_attempts should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "watch:\n"
        "  main_verify_remediation_max_attempts: 4\n"
    )

    config = Config.load(tmp_path)

    assert config.watch.main_verify_remediation_max_attempts == 4


def test_config_load_parses_watch_dispatch_start_timeout(tmp_path) -> None:
    """watch.dispatch_start_timeout should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "watch:\n"
        "  dispatch_start_timeout: 7\n"
    )

    config = Config.load(tmp_path)

    assert config.watch.dispatch_start_timeout == 7


def test_config_load_defaults_watch_parked_auto_rearm(tmp_path) -> None:
    """watch.parked_auto_rearm should default to the conservative blind-policy settings."""
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")

    config = Config.load(tmp_path)

    assert config.watch.parked_auto_rearm.enabled is False
    assert config.watch.parked_auto_rearm.budget == 2
    assert config.watch.parked_auto_rearm.cooldown_hours == 12
    assert config.watch.parked_auto_rearm.require_target_advanced is True


def test_config_load_parses_watch_parked_auto_rearm(tmp_path) -> None:
    """watch.parked_auto_rearm should round-trip through Config.load."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "watch:\n"
        "  parked_auto_rearm:\n"
        "    enabled: true\n"
        "    budget: 3\n"
        "    cooldown_hours: 6\n"
        "    require_target_advanced: false\n"
    )

    config = Config.load(tmp_path)

    assert config.watch.parked_auto_rearm.enabled is True
    assert config.watch.parked_auto_rearm.budget == 3
    assert config.watch.parked_auto_rearm.cooldown_hours == 6
    assert config.watch.parked_auto_rearm.require_target_advanced is False


def test_config_load_defaults_quiet_period_seconds(tmp_path) -> None:
    """quiet_period_seconds should default when omitted."""
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")

    config = Config.load(tmp_path)

    assert config.quiet_period_seconds == DEFAULT_QUIET_PERIOD_SECONDS


def test_config_load_accepts_zero_quiet_period_seconds(tmp_path) -> None:
    """quiet_period_seconds should accept zero as the disable sentinel."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "quiet_period_seconds: 0\n"
    )

    config = Config.load(tmp_path)

    assert config.quiet_period_seconds == 0


def test_quiet_period_registry_description_matches_display_only_quiet_lane_behavior() -> None:
    """Discoverable metadata should describe the quiet lane without overstating pickup behavior."""
    quiet_spec = next(spec for spec in CONFIG_KEY_REGISTRY if spec.key == "quiet_period_seconds")

    assert "Quiet lane" in quiet_spec.description
    assert "do not change worker pickup eligibility" in quiet_spec.description
    assert "0" in quiet_spec.description


def test_quiet_period_docs_match_display_only_scope() -> None:
    """Operator docs should describe the shipped quiet-lane display semantics."""
    docs_text = (Path(__file__).resolve().parents[1] / "docs" / "configuration.md").read_text()

    assert "quiet_period_seconds" in docs_text
    assert "Quiet lane of `gza queue` / `gza next`" in docs_text
    assert "do not change worker pickup eligibility" in docs_text


@pytest.mark.parametrize("value, expected", [
    ("-1", "'quiet_period_seconds' must be non-negative"),
    ("1.5", "'quiet_period_seconds' must be an integer"),
    ("true", "'quiet_period_seconds' must be an integer"),
])
def test_config_validation_rejects_invalid_quiet_period_seconds(
    tmp_path,
    value: str,
    expected: str,
) -> None:
    """Load and validate should reject invalid quiet_period_seconds values."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        f"quiet_period_seconds: {value}\n"
    )

    is_valid, errors, warnings = Config.validate(tmp_path)

    assert not is_valid
    assert expected in errors
    assert warnings == []

    with pytest.raises(ConfigError, match=expected):
        Config.load(tmp_path)


@pytest.mark.parametrize("value", ["0", "-1", "1.5", '"60"'])
def test_config_load_rejects_invalid_docker_startup_timeout(tmp_path, value: str) -> None:
    """Load should reject non-positive and non-integer docker_startup_timeout values."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        f"docker_startup_timeout: {value}\n"
    )

    expected = (
        "'docker_startup_timeout' must be positive"
        if value in {"0", "-1"}
        else "'docker_startup_timeout' must be an integer"
    )
    with pytest.raises(ConfigError, match=expected):
        Config.load(tmp_path)


@pytest.mark.parametrize("value, expected", [
    ("0", "'docker_startup_timeout' must be positive"),
    ("-1", "'docker_startup_timeout' must be positive"),
    ("1.5", "'docker_startup_timeout' must be an integer"),
    ('"60"', "'docker_startup_timeout' must be an integer"),
])
def test_config_validate_rejects_invalid_docker_startup_timeout(tmp_path, value: str, expected: str) -> None:
    """Validate should report shared positive-int wording for docker_startup_timeout."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        f"docker_startup_timeout: {value}\n"
    )

    is_valid, errors, warnings = Config.validate(tmp_path)

    assert not is_valid
    assert expected in errors
    assert warnings == []


@pytest.mark.parametrize("value, expected", [
    ("0", "'watch.dispatch_start_timeout' must be positive"),
    ("-1", "'watch.dispatch_start_timeout' must be positive"),
    ("1.5", "'watch.dispatch_start_timeout' must be an integer"),
    ("true", "'watch.dispatch_start_timeout' must be an integer"),
])
def test_config_watch_dispatch_start_timeout_validation(tmp_path, value: str, expected: str) -> None:
    """Load and validate should reject invalid watch.dispatch_start_timeout values."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "watch:\n"
        f"  dispatch_start_timeout: {value}\n"
    )

    is_valid, errors, warnings = Config.validate(tmp_path)

    assert not is_valid
    assert expected in errors
    assert warnings == []

    with pytest.raises(ConfigError, match=expected):
        Config.load(tmp_path)
