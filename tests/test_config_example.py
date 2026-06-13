"""Drift tests and behavior regressions for generated config example artifacts."""

from pathlib import Path

from gza.config import Config
from gza.config_examples import render_config_example
from gza.config_schema import CONFIG_KEY_REGISTRY, ConfigKeySpec


def test_committed_config_examples_match_generated_registry_output() -> None:
    # DELIBERATE exception to AGENTS.md "No config-value pinning":
    # these files are generated artifacts, and lockstep failure is the intended
    # reminder to regenerate and commit them when the registry changes.
    assert render_config_example() == Path("src/gza/gza.yaml.example").read_text(encoding="utf-8")
    assert render_config_example(local=True) == Path("src/gza/gza.local.yaml.example").read_text(encoding="utf-8")


def test_config_example_drift_guard_detects_registry_add_remove_and_default_changes() -> None:
    """The example snapshot should change when registry shape or defaults change."""
    committed = Path("src/gza/gza.yaml.example").read_text(encoding="utf-8")

    added_registry = CONFIG_KEY_REGISTRY + (
        ConfigKeySpec("z_generated_probe", "bool", False, "Synthetic probe for drift coverage."),
    )
    removed_registry = tuple(spec for spec in CONFIG_KEY_REGISTRY if spec.key != "watch.poll")
    changed_default_registry = tuple(
        ConfigKeySpec(spec.key, spec.value_type, 301, spec.description, spec.required)
        if spec.key == "watch.poll"
        else spec
        for spec in CONFIG_KEY_REGISTRY
    )

    assert render_config_example(registry=added_registry) != committed
    assert render_config_example(registry=removed_registry) != committed
    assert render_config_example(registry=changed_default_registry) != committed


def test_full_config_example_branch_strategy_default_is_valid_when_uncommented(tmp_path: Path) -> None:
    rendered = render_config_example()
    uncommented = rendered.replace("# branch_strategy: project_date_slug", "branch_strategy: project_date_slug", 1)

    (tmp_path / "gza.yaml").write_text(uncommented, encoding="utf-8")

    is_valid, errors, warnings = Config.validate(tmp_path)
    assert is_valid
    assert errors == []
    assert warnings == []

    config = Config.load(tmp_path)
    assert config.branch_strategy is not None
    assert config.branch_strategy.pattern == "{project}/{date}-{slug}"
    assert config.branch_strategy.default_type == "feature"


def test_full_config_example_groups_code_task_diff_timeout_keys_under_execution() -> None:
    rendered = render_config_example()

    execution_start = rendered.index("# --- Execution ---")
    branching_start = rendered.index("# --- Branching ---")
    timeout_key = "# code_task_diff_timeout_medium_threshold: 400"

    timeout_index = rendered.index(timeout_key)

    assert execution_start < timeout_index < branching_start
    _, other_header, other_section = rendered.partition("# --- Other ---")
    assert not other_header or timeout_key not in other_section


def test_full_config_example_groups_review_verify_timeout_grace_under_review() -> None:
    rendered = render_config_example()

    review_start = rendered.index("# --- Review ---")
    learnings_start = rendered.index("# --- Learnings ---")
    grace_key = "# review_verify_timeout_grace_seconds:"

    grace_index = rendered.index(grace_key)

    assert review_start < grace_index < learnings_start
    _, other_header, other_section = rendered.partition("# --- Other ---")
    assert not other_header or grace_key not in other_section
