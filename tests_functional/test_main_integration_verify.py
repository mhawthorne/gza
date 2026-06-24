from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tests.cli.conftest import make_store, setup_config
from tests_functional.git_helpers import init_basic_repo

from gza.config import Config
from gza.main_integration_verify import (
    check_main_integration_verify,
    current_main_integration_verify_alert,
    load_main_integration_verify_state,
)
from gza.runner import _make_review_verify_result


def test_configured_main_integration_verify_unavailable_halts_and_persists_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/missing-verify"
    git = init_basic_repo(tmp_path)
    head_sha = git.rev_parse("HEAD")
    captured_at = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)

    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="unavailable",
            exit_status="launch failed",
            captured_at=captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
            failure="launcher missing",
        ),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="test",
        )

    assert check.performed_verify is True
    assert check.state.gate_enabled is True
    assert check.state.verify_status == "unavailable"
    assert check.state.alert_message == (
        f"main verify RED at `{head_sha[:12]}` - merges halted; verify status `unavailable`"
    )
    assert check.merges_halted is True

    persisted = load_main_integration_verify_state(store)
    assert persisted is not None
    assert persisted.gate_enabled is True
    assert persisted.alert_message == check.state.alert_message


def test_main_integration_verify_without_configured_gate_does_not_halt_or_emit_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    git = init_basic_repo(tmp_path)

    check = check_main_integration_verify(
        config,
        store,
        git,
        reason="test",
    )

    assert check.performed_verify is True
    assert check.state.gate_enabled is False
    assert check.state.verify_status == "unavailable"
    assert check.state.alert_message is None
    assert check.merges_halted is False
    assert current_main_integration_verify_alert(store, git, config) is None


def test_same_tree_no_gate_checkpoint_reruns_when_verify_command_is_added(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    git = init_basic_repo(tmp_path)
    head_sha = git.rev_parse("HEAD")

    initial_check = check_main_integration_verify(
        config,
        store,
        git,
        reason="seed-no-gate",
    )

    assert initial_check.performed_verify is True
    assert initial_check.state.gate_enabled is False
    assert initial_check.state.verify_command is None
    initial_fingerprint = initial_check.state.tree_fingerprint

    config.verify_command = "./bin/tests"
    captured_at = datetime(2026, 6, 23, 12, 10, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="passed",
            exit_status="0",
            captured_at=captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
        ),
    ) as run_verify:
        refreshed = check_main_integration_verify(
            config,
            store,
            git,
            reason="enable-gate",
        )

    run_verify.assert_called_once()
    assert refreshed.performed_verify is True
    assert refreshed.merges_halted is False
    assert refreshed.state.gate_enabled is True
    assert refreshed.state.verify_command == config.verify_command
    assert refreshed.state.head_sha == head_sha
    assert refreshed.state.verify_status == "passed"
    assert refreshed.state.tree_fingerprint is not None
    assert initial_fingerprint is not None
    assert refreshed.state.alert_message is None
    assert current_main_integration_verify_alert(store, git, config) is None


def test_changed_main_head_reruns_verify_and_persists_red_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    git = init_basic_repo(tmp_path)

    initial_head = git.rev_parse("HEAD")
    first_captured_at = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)

    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="passed",
            exit_status="0",
            captured_at=first_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=initial_head,
            working_directory=str(git.repo_dir),
        ),
    ) as first_verify:
        initial_check = check_main_integration_verify(
            config,
            store,
            git,
            reason="initial-seed",
        )

    assert initial_check.performed_verify is True
    first_verify.assert_called_once()
    persisted_green = load_main_integration_verify_state(store)
    assert persisted_green is not None
    assert persisted_green.head_sha == initial_head
    assert persisted_green.tree_fingerprint is not None
    assert persisted_green.verify_status == "passed"

    (tmp_path / "README.md").write_text("direct main commit")
    git._run("add", "README.md")
    git._run("commit", "-m", "Direct main commit")
    new_head = git.rev_parse("HEAD")
    assert new_head != initial_head

    second_captured_at = datetime(2026, 6, 23, 12, 5, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="failed",
            exit_status="failed",
            captured_at=second_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=new_head,
            working_directory=str(git.repo_dir),
            failure="tests failed",
        ),
    ) as second_verify:
        refreshed_check = check_main_integration_verify(
            config,
            store,
            git,
            reason="direct-main-commit",
        )

    second_verify.assert_called_once()
    assert refreshed_check.performed_verify is True
    assert refreshed_check.merges_halted is True
    assert refreshed_check.state.head_sha == new_head
    assert refreshed_check.state.tree_fingerprint is not None
    assert refreshed_check.state.verify_status == "failed"
    assert refreshed_check.state.alert_message == f"main verify RED at `{new_head[:12]}` - merges halted"

    persisted_red = load_main_integration_verify_state(store)
    assert persisted_red is not None
    assert persisted_red.head_sha == new_head
    assert persisted_red.tree_fingerprint is not None
    assert persisted_red.tree_fingerprint != persisted_green.tree_fingerprint
    assert persisted_red.verify_status == "failed"
    assert persisted_red.alert_message == refreshed_check.state.alert_message

    assert refreshed_check.state.alert_message.startswith(f"main verify RED at `{new_head[:12]}`")


def test_same_tree_red_checkpoint_reruns_when_verify_command_changes_and_recovers(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/broken-verify"
    git = init_basic_repo(tmp_path)
    head_sha = git.rev_parse("HEAD")

    red_captured_at = datetime(2026, 6, 23, 12, 15, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="failed",
            exit_status="failed",
            captured_at=red_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
            failure="tests failed",
        ),
    ) as first_verify:
        red_check = check_main_integration_verify(
            config,
            store,
            git,
            reason="seed-red",
        )

    first_verify.assert_called_once()
    assert red_check.performed_verify is True
    assert red_check.merges_halted is True
    assert red_check.state.gate_enabled is True
    assert red_check.state.verify_command == "./bin/broken-verify"
    assert red_check.state.verify_status == "failed"
    assert red_check.state.alert_message == f"main verify RED at `{head_sha[:12]}` - merges halted"
    original_fingerprint = red_check.state.tree_fingerprint

    config.verify_command = "./bin/tests"
    green_captured_at = datetime(2026, 6, 23, 12, 20, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="passed",
            exit_status="0",
            captured_at=green_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
        ),
    ) as second_verify:
        recovered = check_main_integration_verify(
            config,
            store,
            git,
            reason="repair-gate",
        )

    second_verify.assert_called_once()
    assert recovered.performed_verify is True
    assert recovered.merges_halted is False
    assert recovered.state.gate_enabled is True
    assert recovered.state.verify_command == "./bin/tests"
    assert recovered.state.head_sha == head_sha
    assert recovered.state.verify_status == "passed"
    assert recovered.state.tree_fingerprint is not None
    assert original_fingerprint is not None
    assert recovered.state.alert_message is None
    assert current_main_integration_verify_alert(store, git, config) is None


def test_same_tree_red_checkpoint_reruns_after_ttl_and_recovers(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.main_integration_verify_red_ttl_minutes = 30
    git = init_basic_repo(tmp_path)
    head_sha = git.rev_parse("HEAD")

    red_captured_at = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="failed",
            exit_status="failed",
            captured_at=red_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
            failure="tests failed",
        ),
    ) as first_verify:
        seeded = check_main_integration_verify(
            config,
            store,
            git,
            reason="seed-red-ttl",
        )

    first_verify.assert_called_once()
    assert seeded.merges_halted is True
    original_fingerprint = seeded.state.tree_fingerprint

    green_captured_at = datetime(2026, 6, 23, 12, 31, tzinfo=UTC)
    with (
        patch("gza.main_integration_verify._run_review_verify_command", return_value=_make_review_verify_result(
            config.verify_command,
            status="passed",
            exit_status="0",
            captured_at=green_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
        )) as second_verify,
        patch("gza.main_integration_verify.datetime") as mocked_datetime,
    ):
        mocked_datetime.now.return_value = green_captured_at
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        recovered = check_main_integration_verify(
            config,
            store,
            git,
            reason="red-ttl-refresh",
        )

    second_verify.assert_called_once()
    assert recovered.performed_verify is True
    assert recovered.merges_halted is False
    assert recovered.state.verify_status == "passed"
    assert original_fingerprint is not None
    assert recovered.state.tree_fingerprint is not None
    assert current_main_integration_verify_alert(store, git, config) is None


def test_current_main_integration_verify_alert_suppresses_same_head_red_checkpoint_when_gate_removed(
    tmp_path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    git = init_basic_repo(tmp_path)
    head_sha = git.rev_parse("HEAD")

    red_captured_at = datetime(2026, 6, 23, 12, 25, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="failed",
            exit_status="failed",
            captured_at=red_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
            failure="tests failed",
        ),
    ):
        seeded = check_main_integration_verify(
            config,
            store,
            git,
            reason="seed-red-no-gate-regression",
        )

    assert seeded.merges_halted is True
    live_git = MagicMock()
    live_git.default_branch.return_value = "main"
    live_git.current_branch.return_value = "topic"
    live_git.rev_parse_if_exists.side_effect = lambda ref: head_sha if ref == "main" else "topic-sha"

    assert current_main_integration_verify_alert(store, live_git, config) is not None

    config.verify_command = None

    assert current_main_integration_verify_alert(store, live_git, config) is None


def test_current_main_integration_verify_alert_suppresses_same_head_red_checkpoint_when_gate_identity_changes(
    tmp_path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/broken-verify"
    git = init_basic_repo(tmp_path)
    head_sha = git.rev_parse("HEAD")

    red_captured_at = datetime(2026, 6, 23, 12, 30, tzinfo=UTC)
    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        return_value=_make_review_verify_result(
            config.verify_command,
            status="failed",
            exit_status="failed",
            captured_at=red_captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
            failure="tests failed",
        ),
    ):
        seeded = check_main_integration_verify(
            config,
            store,
            git,
            reason="seed-red-gate-change-regression",
        )

    assert seeded.merges_halted is True
    live_git = MagicMock()
    live_git.default_branch.return_value = "main"
    live_git.current_branch.return_value = "topic"
    live_git.rev_parse_if_exists.side_effect = lambda ref: head_sha if ref == "main" else "topic-sha"

    assert current_main_integration_verify_alert(store, live_git, config) is not None

    config.verify_command = "./bin/tests"

    assert current_main_integration_verify_alert(store, live_git, config) is None
