from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.off_topic_verify import (
    MAX_OFF_TOPIC_STRESS_RUNS,
    LocalTargetBaselinePlan,
    build_failing_nodes,
    build_local_target_pytest_command,
    classify_failure_diff_scope,
    detached_local_target_worktree,
    extract_assertion_signatures,
    extract_pytest_failing_nodeids,
    is_shared_global_or_concurrency_sensitive_path,
    parse_pytest_verify_failure,
    parse_review_verify_failure_set,
    run_local_target_baseline_plan,
    select_local_target_baseline_plan,
)
from gza.runner import ReviewVerifyResult


def test_parse_pytest_verify_failure_extracts_multiple_failing_nodes() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=31.25
_______________________________ test_alpha _______________________________

    def test_alpha():
>       assert left == right
E       AssertionError: assert left == right

tests/test_alpha.py:11: AssertionError
________________________________ test_beta ________________________________

    def test_beta():
>       raise ValueError("boom")
E       ValueError: boom

src/gza/workers.py:45: ValueError
=========================== short test summary info ============================
FAILED tests/test_alpha.py::test_alpha - AssertionError: assert left == right
FAILED tests/test_beta.py::test_beta - ValueError: boom
========================= 2 failed, 5 passed in 31.25s =========================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is True
    assert parsed.exit_status.code == 1
    assert parsed.pass_fail_counts.failed == 2
    assert parsed.pass_fail_counts.passed == 5
    assert [node.nodeid for node in parsed.failing_nodes] == [
        "tests/test_alpha.py::test_alpha",
        "tests/test_beta.py::test_beta",
    ]
    assert parsed.failing_nodes[0].assertion_signature == "AssertionError: assert left == right"
    assert parsed.failing_nodes[0].failure_path == "tests/test_alpha.py"
    assert parsed.failing_nodes[1].assertion_signature == "ValueError: boom"
    assert parsed.failing_nodes[1].traceback_paths == ("src/gza/workers.py",)


def test_parse_pytest_verify_failure_refuses_fail_fast_output() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=44.72
FAILED tests/test_runner.py::test_noop_improve_verify_only_auto_clear
!!!!!!!!!!!!!!!!!!!!!!!!!! stopping after 1 failures !!!!!!!!!!!!!!!!!!!!!!!!!!!
=========================== short test summary info ============================
FAILED tests/test_runner.py::test_noop_improve_verify_only_auto_clear
============================== 1 failed in 44.72s ==============================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="./bin/tests -o faulthandler_timeout=120",
        output=output,
        exit_status="1",
    )

    assert parsed.available is False
    assert parsed.unavailable is not None
    assert parsed.unavailable.reason == "fail_fast_enabled"
    assert parsed.failing_nodes == ()
    assert parsed.pass_fail_counts.failed == 1


def test_parse_pytest_verify_failure_refuses_bundled_fail_fast_flag() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=3.10
_______________________________ test_alpha _______________________________

    def test_alpha():
>       assert left == right
E       AssertionError: assert left == right

tests/test_alpha.py:11: AssertionError
=========================== short test summary info ============================
FAILED tests/test_alpha.py::test_alpha - AssertionError: assert left == right
========================= 1 failed, 2 passed in 3.10s =========================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest -qx tests/",
        output=output,
        exit_status="1",
    )

    assert parsed.available is False
    assert parsed.unavailable is not None
    assert parsed.unavailable.reason == "fail_fast_enabled"
    assert parsed.failing_nodes == ()
    assert parsed.pass_fail_counts.failed == 1


def test_parse_pytest_verify_failure_allows_maxfail_zero() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=3.10
_______________________________ test_alpha _______________________________

    def test_alpha():
>       assert left == right
E       AssertionError: assert left == right

tests/test_alpha.py:11: AssertionError
=========================== short test summary info ============================
FAILED tests/test_alpha.py::test_alpha - AssertionError: assert left == right
========================= 1 failed, 2 passed in 3.10s =========================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is True
    assert [node.nodeid for node in parsed.failing_nodes] == ["tests/test_alpha.py::test_alpha"]


def test_parse_pytest_verify_failure_refuses_short_summary_without_terminal_counts() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=3.10
_______________________________ test_alpha _______________________________

    def test_alpha():
>       assert left == right
E       AssertionError: assert left == right

tests/test_alpha.py:11: AssertionError
________________________________ test_beta ________________________________

    def test_beta():
>       raise ValueError("boom")
E       ValueError: boom

tests/test_beta.py:19: ValueError
=========================== short test summary info ============================
FAILED tests/test_alpha.py::test_alpha - AssertionError: assert left == right
FAILED tests/test_beta.py::test_beta - ValueError: boom
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is False
    assert parsed.unavailable is not None
    assert parsed.unavailable.reason == "incomplete_enumeration"
    assert parsed.failing_nodes == ()
    assert parsed.pass_fail_counts.total_failures == 0


def test_parse_pytest_verify_failure_refuses_pre_summary_failure_like_line() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=0.20
ERROR tests/test_log.py::test_not_real - logged before pytest summary
============================== 1 failed in 0.20s ==============================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is False
    assert parsed.unavailable is not None
    assert parsed.unavailable.reason == "no_failing_nodes"
    assert parsed.failing_nodes == ()
    assert parsed.pass_fail_counts.total_failures == 1


def test_parse_pytest_verify_failure_preserves_parametrized_nodeid_with_spaces() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=3.10
__________________________ test_value[value with space] __________________________

    def test_value():
>       raise AssertionError("boom")
E       AssertionError: boom

tests/test_param.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_param.py::test_value[value with space] - AssertionError: boom
========================= 1 failed, 2 passed in 3.10s =========================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is True
    assert [node.nodeid for node in parsed.failing_nodes] == [
        "tests/test_param.py::test_value[value with space]"
    ]
    assert parsed.failing_nodes[0].assertion_signature == "AssertionError: boom"


def test_build_failing_nodes_preserves_parametrized_nodeid_with_spaces_without_signature() -> None:
    output = """
__________________________ test_value[value with space] __________________________

    def test_value():
>       assert 1 == 2
E       assert 1 == 2

tests/test_param.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_param.py::test_value[value with space]
============================== 1 failed in 0.20s ==============================
""".strip()

    nodes = build_failing_nodes(output)

    assert [node.nodeid for node in nodes] == ["tests/test_param.py::test_value[value with space]"]
    assert nodes[0].assertion_signature == "assert 1 == 2"


def test_build_failing_nodes_preserves_parametrized_nodeid_with_hyphen_without_signature() -> None:
    output = """
__________________________ test_value[a - b] __________________________

    def test_value():
>       assert 1 == 2
E       assert 1 == 2

tests/test_param.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_param.py::test_value[a - b]
============================== 1 failed in 0.20s ==============================
""".strip()

    nodes = build_failing_nodes(output)

    assert [node.nodeid for node in nodes] == ["tests/test_param.py::test_value[a - b]"]
    assert nodes[0].assertion_signature == "assert 1 == 2"


def test_parse_pytest_verify_failure_preserves_parametrized_nodeid_with_hyphen_without_signature() -> None:
    output = """
gza-verify phase=failed name=unit duration_seconds=0.20
__________________________ test_value[a - b] __________________________

    def test_value():
>       assert 1 == 2
E       assert 1 == 2

tests/test_param.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_param.py::test_value[a - b]
============================== 1 failed in 0.20s ==============================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is True
    assert [node.nodeid for node in parsed.failing_nodes] == ["tests/test_param.py::test_value[a - b]"]
    assert parsed.failing_nodes[0].assertion_signature == "assert 1 == 2"


def test_build_failing_nodes_falls_back_to_assertion_signature_for_single_failure() -> None:
    output = """
_____________________ test_assertion_signature _____________________

    def test_assertion_signature():
>       assert 1 == 2
E       assert 1 == 2

tests/test_example.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_example.py::test_assertion_signature
============================== 1 failed in 0.20s ==============================
""".strip()

    nodes = build_failing_nodes(output)

    assert [node.nodeid for node in nodes] == ["tests/test_example.py::test_assertion_signature"]
    assert nodes[0].assertion_signature == "assert 1 == 2"
    assert nodes[0].failure_path == "tests/test_example.py"
    assert extract_pytest_failing_nodeids(output) == ("tests/test_example.py::test_assertion_signature",)
    assert extract_assertion_signatures(output) == ("assert 1 == 2",)


def test_build_failing_nodes_preserves_helper_traceback_paths_for_node() -> None:
    output = """
_________________________________ test_one __________________________________

    def test_one():
>       run_api()

tests/test_api.py:10:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
src/gza/changed_module.py:42: in run_api
    raise ValueError("boom")
E   ValueError: boom

=========================== short test summary info ============================
FAILED tests/test_api.py::test_one - ValueError: boom
============================== 1 failed in 0.20s ==============================
""".strip()

    nodes = build_failing_nodes(output)

    assert [node.nodeid for node in nodes] == ["tests/test_api.py::test_one"]
    assert nodes[0].failure_path == "tests/test_api.py"
    assert nodes[0].traceback_paths == ("tests/test_api.py", "src/gza/changed_module.py")


def test_build_failing_nodes_attributes_traceback_paths_per_failure_section() -> None:
    output = """
_________________________________ test_one __________________________________

    def test_one():
>       run_api()

tests/test_api.py:10:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
src/gza/changed_module.py:42: in run_api
    raise ValueError("boom")
E   ValueError: boom

_________________________________ test_two __________________________________

    def test_two():
>       call_helper()

tests/test_other.py:20:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
src/gza/other_module.py:7: in call_helper
    raise RuntimeError("nope")
E   RuntimeError: nope

=========================== short test summary info ============================
FAILED tests/test_api.py::test_one - ValueError: boom
FAILED tests/test_other.py::test_two - RuntimeError: nope
============================== 2 failed in 0.20s ==============================
""".strip()

    nodes = build_failing_nodes(output)

    assert [node.nodeid for node in nodes] == [
        "tests/test_api.py::test_one",
        "tests/test_other.py::test_two",
    ]
    assert nodes[0].traceback_paths == ("tests/test_api.py", "src/gza/changed_module.py")
    assert nodes[1].traceback_paths == ("tests/test_other.py", "src/gza/other_module.py")


def test_parse_pytest_verify_failure_captures_xdist_metadata() -> None:
    output = """
plugins: xdist-3.8.0, timeout-2.4.0
[gw0] [ 50%] FAILED tests/test_parallel.py::test_worker_a
[gw1] [100%] PASSED tests/test_parallel.py::test_worker_b
=========================== short test summary info ============================
FAILED tests/test_parallel.py::test_worker_a - AssertionError: worker mismatch
========================= 1 failed, 1 passed in 2.00s =========================
""".strip()

    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -n 4 --dist=loadfile --maxfail=0",
        output=output,
        exit_status="1",
    )

    assert parsed.available is True
    assert parsed.xdist.enabled is True
    assert parsed.xdist.worker_count == 4
    assert parsed.xdist.worker_count_raw == "4"
    assert parsed.xdist.dist_mode == "loadfile"
    assert parsed.xdist.plugin_version == "3.8.0"
    assert parsed.xdist.worker_ids == ("gw0", "gw1")


def test_parse_review_verify_failure_set_uses_persisted_review_verify_fields() -> None:
    result = ReviewVerifyResult(
        command="uv run pytest tests/test_example.py -q --maxfail=0",
        status="failed",
        exit_status="1",
        captured_at=datetime.now(UTC),
        output=(
            "=========================== short test summary info ============================\n"
            "FAILED tests/test_example.py::test_parse - AssertionError: assert 2 == 3\n"
            "============================== 1 failed in 0.12s ==============================\n"
        ),
    )

    parsed = parse_review_verify_failure_set(result)

    assert parsed.available is True
    assert [node.nodeid for node in parsed.failing_nodes] == ["tests/test_example.py::test_parse"]
    assert parsed.failing_nodes[0].assertion_signature == "AssertionError: assert 2 == 3"
    assert parsed.failing_nodes[0].trustworthy_attribution is False


def test_classify_failure_diff_scope_fails_closed_for_summary_only_tb_no_output() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest --tb=no --maxfail=0 tests/test_api.py -q",
        output="""
=========================== short test summary info ============================
FAILED tests/test_api.py::test_one - ValueError: boom
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/changed_module.py",),
    )

    assert parsed.available is True
    assert parsed.failing_nodes[0].trustworthy_attribution is False
    assert classification.outcome == "unavailable"
    assert classification.baseline_mode is None
    assert classification.node_classifications[0].outcome == "unavailable"
    assert classification.node_classifications[0].detail == "node had no attributable repo-relative paths"


def test_parse_review_verify_failure_set_cannot_bypass_summary_only_scope_guard() -> None:
    result = ReviewVerifyResult(
        command="uv run pytest --tb=no --maxfail=0 tests/test_api.py -q",
        status="failed",
        exit_status="1",
        captured_at=datetime.now(UTC),
        output=(
            "=========================== short test summary info ============================\n"
            "FAILED tests/test_api.py::test_one - ValueError: boom\n"
            "============================== 1 failed in 0.20s ==============================\n"
        ),
    )

    classification = classify_failure_diff_scope(
        parse_review_verify_failure_set(result),
        changed_paths=("src/gza/changed_module.py",),
    )

    assert classification.outcome == "unavailable"
    assert classification.baseline_mode is None
    assert classification.node_classifications[0].outcome == "unavailable"


def test_parse_pytest_verify_failure_fails_closed_for_non_pytest_commands() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run mypy src",
        output="src/gza/off_topic_verify.py:1: error: incompatible type",
        exit_status="1",
    )

    assert parsed.available is False
    assert parsed.unavailable is not None
    assert parsed.unavailable.reason == "not_pytest_command"


def test_classify_failure_diff_scope_marks_outside_diff_nodes_off_topic() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_worker_registry __________________________________

    def test_worker_registry():
>       assert worker.status == "completed"
E       AssertionError: assert 'running' == 'completed'

tests/cli/test_query.py:10:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
src/gza/workers.py:42: in get_status
    return registry.status()
E   AssertionError: assert 'running' == 'completed'

=========================== short test summary info ============================
FAILED tests/cli/test_query.py::test_worker_registry - AssertionError: assert 'running' == 'completed'
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/git.py", "src/gza/cli/git_ops.py"),
    )

    assert classification.outcome == "off_topic"
    assert classification.baseline_mode == "deterministic_once"
    assert classification.shared_global_paths == ()
    assert [node.outcome for node in classification.node_classifications] == ["outside_diff"]


def test_classify_failure_diff_scope_blocks_inside_diff_node() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_runner __________________________________

    def test_runner():
>       assert behavior() == "ok"
E       AssertionError: assert 'bad' == 'ok'

tests/test_runner.py:10:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
src/gza/cli/git_ops.py:42: in behavior
    return "bad"
E   AssertionError: assert 'bad' == 'ok'

=========================== short test summary info ============================
FAILED tests/test_runner.py::test_runner - AssertionError: assert 'bad' == 'ok'
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/cli/git_ops.py",),
    )

    assert classification.outcome == "branch_introduced"
    assert classification.baseline_mode is None
    assert classification.node_classifications[0].outcome == "inside_diff"
    assert classification.node_classifications[0].matched_changed_paths == ("src/gza/cli/git_ops.py",)


def test_classify_failure_diff_scope_normalizes_absolute_repo_traceback_paths() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_runner __________________________________

    def test_runner():
>       helper()
E       AssertionError: assert 'bad' == 'ok'

tests/test_runner.py:10:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
/workspace/src/gza/changed_module.py:42: in helper
    return "bad"
E   AssertionError: assert 'bad' == 'ok'

=========================== short test summary info ============================
FAILED tests/test_runner.py::test_runner - AssertionError: assert 'bad' == 'ok'
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/changed_module.py",),
        repo_root=Path("/workspace"),
    )

    assert classification.outcome == "branch_introduced"
    assert classification.baseline_mode is None
    assert classification.node_classifications[0].outcome == "inside_diff"
    assert classification.node_classifications[0].matched_changed_paths == ("src/gza/changed_module.py",)


def test_classify_failure_diff_scope_fails_closed_for_absolute_traceback_path_outside_repo() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_runner __________________________________

    def test_runner():
>       helper()
E       AssertionError: assert 'bad' == 'ok'

tests/test_runner.py:10:
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
/tmp/outside/src/gza/changed_module.py:42: in helper
    return "bad"
E   AssertionError: assert 'bad' == 'ok'

=========================== short test summary info ============================
FAILED tests/test_runner.py::test_runner - AssertionError: assert 'bad' == 'ok'
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/changed_module.py",),
        repo_root=Path("/workspace"),
    )

    assert classification.outcome == "unavailable"
    assert classification.baseline_mode is None
    assert classification.node_classifications[0].outcome == "unavailable"


def test_classify_failure_diff_scope_routes_shared_global_changes_to_stress_baseline() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_query __________________________________

    def test_query():
>       assert 1 == 2
E       assert 1 == 2

tests/cli/test_query.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/cli/test_query.py::test_query
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/workers.py",),
    )

    assert classification.outcome == "off_topic"
    assert classification.baseline_mode == "stress"
    assert classification.shared_global_paths == ("src/gza/workers.py",)


def test_classify_failure_diff_scope_routes_config_source_changes_to_stress_baseline() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_query __________________________________

    def test_query():
>       assert 1 == 2
E       assert 1 == 2

tests/cli/test_query.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/cli/test_query.py::test_query
============================== 1 failed in 0.20s ==============================
""".strip(),
        exit_status="1",
    )

    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/config.py",),
    )

    assert classification.outcome == "off_topic"
    assert classification.baseline_mode == "stress"
    assert classification.shared_global_paths == ("src/gza/config.py",)


def test_is_shared_global_or_concurrency_sensitive_path_is_named_conservative_policy() -> None:
    assert is_shared_global_or_concurrency_sensitive_path("src/gza/config.py") is True
    assert is_shared_global_or_concurrency_sensitive_path("src/gza/config_schema.py") is True
    assert is_shared_global_or_concurrency_sensitive_path("src/gza/workers.py") is True
    assert is_shared_global_or_concurrency_sensitive_path("tests/conftest.py") is True
    assert is_shared_global_or_concurrency_sensitive_path("src/gza/git.py") is False


def test_select_local_target_baseline_plan_builds_deterministic_one_shot_rerun() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_parse __________________________________

    def test_parse():
>       assert 2 == 3
E       assert 2 == 3

tests/test_example.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_example.py::test_parse - AssertionError: assert 2 == 3
============================== 1 failed in 0.12s ==============================
""".strip(),
        exit_status="1",
    )
    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/git.py",),
    )

    selection = select_local_target_baseline_plan(
        parsed,
        classification,
        target_branch="main",
        target_head_sha="abc123",
        target_tree_fingerprint="f" * 64,
    )

    assert selection.available is True
    assert selection.plan is not None
    assert selection.plan.mode == "deterministic_once"
    assert selection.plan.run_count == 1
    assert selection.plan.command == "uv run pytest -q --maxfail=0 tests/test_example.py::test_parse"


def test_select_local_target_baseline_plan_routes_shared_global_scope_to_bounded_stress_runs() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q -n 4 --dist=loadfile --maxfail=0",
        output="""
_________________________________ test_parse __________________________________

    def test_parse():
>       assert 2 == 3
E       assert 2 == 3

tests/test_example.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_example.py::test_parse - AssertionError: assert 2 == 3
============================== 1 failed in 0.12s ==============================
""".strip(),
        exit_status="1",
    )
    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/workers.py",),
    )

    selection = select_local_target_baseline_plan(
        parsed,
        classification,
        target_branch="main",
        target_head_sha="abc123",
        target_tree_fingerprint="f" * 64,
        stress_runs=99,
    )

    assert selection.available is True
    assert selection.plan is not None
    assert selection.plan.mode == "stress"
    assert selection.plan.run_count == 20
    assert selection.plan.command == (
        "uv run pytest -q -n 4 --dist=loadfile --maxfail=0 tests/test_example.py::test_parse"
    )


def test_select_local_target_baseline_plan_fails_closed_when_provenance_is_incomplete() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ -q --maxfail=0",
        output="""
_________________________________ test_parse __________________________________

    def test_parse():
>       assert 2 == 3
E       assert 2 == 3

tests/test_example.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_example.py::test_parse - AssertionError: assert 2 == 3
============================== 1 failed in 0.12s ==============================
""".strip(),
        exit_status="1",
    )
    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/git.py",),
    )

    selection = select_local_target_baseline_plan(
        parsed,
        classification,
        target_branch="main",
        target_head_sha=None,
        target_tree_fingerprint="f" * 64,
    )

    assert selection.available is False
    assert selection.plan is None
    assert selection.unavailable is not None
    assert selection.unavailable.reason == "missing_target_head_sha"


def test_select_local_target_baseline_plan_fails_closed_when_targeted_pytest_command_is_unsafe() -> None:
    parsed = parse_pytest_verify_failure(
        command="uv run pytest tests/ --unknown-flag value -q --maxfail=0",
        output="""
_________________________________ test_parse __________________________________

    def test_parse():
>       assert 2 == 3
E       assert 2 == 3

tests/test_example.py:14: AssertionError
=========================== short test summary info ============================
FAILED tests/test_example.py::test_parse - AssertionError: assert 2 == 3
============================== 1 failed in 0.12s ==============================
""".strip(),
        exit_status="1",
    )
    classification = classify_failure_diff_scope(
        parsed,
        changed_paths=("src/gza/git.py",),
    )

    selection = select_local_target_baseline_plan(
        parsed,
        classification,
        target_branch="main",
        target_head_sha="abc123",
        target_tree_fingerprint="f" * 64,
    )

    assert selection.available is False
    assert selection.plan is None
    assert selection.unavailable is not None
    assert selection.unavailable.reason == "unsafe_pytest_command"


def test_build_local_target_pytest_command_returns_none_for_wrapped_non_pytest_command() -> None:
    command = build_local_target_pytest_command(
        "./bin/tests -o faulthandler_timeout=120",
        nodeids=("tests/test_example.py::test_parse",),
    )

    assert command is None


class _RecordingGit:
    operations: list[tuple[str, str]]
    removed_paths: list[Path]
    fail_checkout_ref: str | None = None

    def __init__(self, repo_dir: Path) -> None:
        self.repo_dir = repo_dir
        if not hasattr(type(self), "operations"):
            type(self).operations = []
        if not hasattr(type(self), "removed_paths"):
            type(self).removed_paths = []

    @classmethod
    def reset(cls) -> None:
        cls.operations = []
        cls.removed_paths = []
        cls.fail_checkout_ref = None

    def worktree_add_existing(self, path: Path, ref: str, *, detach: bool = False) -> Path:
        type(self).operations.append(("worktree_add_existing", ref))
        path.mkdir(parents=True, exist_ok=True)
        return path

    def checkout_detached(self, ref: str) -> None:
        type(self).operations.append(("checkout_detached", ref))
        if ref == type(self).fail_checkout_ref:
            raise RuntimeError(f"cannot checkout {ref}")

    def reset_hard(self, ref: str) -> None:
        type(self).operations.append(("reset_hard", ref))

    def clean_force(self) -> None:
        type(self).operations.append(("clean_force", ""))

    def worktree_remove(self, path: Path, *, force: bool = False) -> None:
        type(self).operations.append(("worktree_remove", str(path)))
        type(self).removed_paths.append(path)


def test_run_local_target_baseline_plan_uses_immutable_target_sha(tmp_path: Path) -> None:
    _RecordingGit.reset()
    repo_git = _RecordingGit(tmp_path / "repo")
    plan = LocalTargetBaselinePlan(
        mode="deterministic_once",
        command="uv run pytest -q --maxfail=0 tests/test_example.py::test_parse",
        nodeids=("tests/test_example.py::test_parse",),
        target_branch="main",
        target_head_sha="abc123def456",
        target_tree_fingerprint="f" * 64,
        run_count=1,
        relative_cwd=".",
    )

    def _verify_runner(
        command: str,
        *,
        cwd: Path,
        reviewed_branch: str,
        reviewed_head_sha: str,
        timeout_seconds: int,
        timeout_grace_seconds: float,
    ) -> ReviewVerifyResult:
        assert command == plan.command
        assert cwd.is_dir()
        assert reviewed_branch == "main"
        assert reviewed_head_sha == "abc123def456"
        assert timeout_seconds == 30
        assert timeout_grace_seconds == 5.0
        return ReviewVerifyResult(
            command=command,
            status="failed",
            exit_status="1",
            captured_at=datetime.now(UTC),
            output="",
        )

    run = run_local_target_baseline_plan(
        plan,
        repo_git=repo_git,
        worktree_root=tmp_path / "worktrees",
        timeout_seconds=30,
        timeout_grace_seconds=5.0,
        run_verify_command=_verify_runner,
    )

    assert run.plan == plan
    assert [operation[:2] for operation in _RecordingGit.operations[:4]] == [
        ("worktree_add_existing", "abc123def456"),
        ("checkout_detached", "abc123def456"),
        ("reset_hard", "abc123def456"),
        ("clean_force", ""),
    ]


@pytest.mark.parametrize(
    ("relative_cwd", "message"),
    [
        ("/tmp", "must be relative"),
        ("../../..", "escapes detached target worktree"),
    ],
)
def test_run_local_target_baseline_plan_rejects_unsafe_cwd(
    tmp_path: Path,
    relative_cwd: str,
    message: str,
) -> None:
    _RecordingGit.reset()
    repo_git = _RecordingGit(tmp_path / "repo")
    plan = LocalTargetBaselinePlan(
        mode="deterministic_once",
        command="uv run pytest -q --maxfail=0 tests/test_example.py::test_parse",
        nodeids=("tests/test_example.py::test_parse",),
        target_branch="main",
        target_head_sha="abc123def456",
        target_tree_fingerprint="f" * 64,
        run_count=1,
        relative_cwd=relative_cwd,
    )

    with pytest.raises(ValueError, match=message):
        run_local_target_baseline_plan(
            plan,
            repo_git=repo_git,
            worktree_root=tmp_path / "worktrees",
            timeout_seconds=30,
            timeout_grace_seconds=5.0,
        )


@pytest.mark.parametrize("run_count", [0, MAX_OFF_TOPIC_STRESS_RUNS + 1])
def test_run_local_target_baseline_plan_rejects_out_of_bounds_run_count(
    tmp_path: Path,
    run_count: int,
) -> None:
    _RecordingGit.reset()
    repo_git = _RecordingGit(tmp_path / "repo")
    plan = LocalTargetBaselinePlan(
        mode="stress",
        command="uv run pytest -q --maxfail=0 tests/test_example.py::test_parse",
        nodeids=("tests/test_example.py::test_parse",),
        target_branch="main",
        target_head_sha="abc123def456",
        target_tree_fingerprint="f" * 64,
        run_count=run_count,
        relative_cwd=".",
    )

    with pytest.raises(ValueError, match="run_count must be between 1 and"):
        run_local_target_baseline_plan(
            plan,
            repo_git=repo_git,
            worktree_root=tmp_path / "worktrees",
            timeout_seconds=30,
            timeout_grace_seconds=5.0,
        )


def test_run_local_target_baseline_plan_allows_safe_subdirectory(tmp_path: Path) -> None:
    _RecordingGit.reset()
    repo_git = _RecordingGit(tmp_path / "repo")
    plan = LocalTargetBaselinePlan(
        mode="deterministic_once",
        command="uv run pytest -q --maxfail=0 tests/test_example.py::test_parse",
        nodeids=("tests/test_example.py::test_parse",),
        target_branch="main",
        target_head_sha="abc123def456",
        target_tree_fingerprint="f" * 64,
        run_count=1,
        relative_cwd="subdir",
    )
    observed_cwds: list[Path] = []

    def _verify_runner(
        command: str,
        *,
        cwd: Path,
        reviewed_branch: str,
        reviewed_head_sha: str,
        timeout_seconds: int,
        timeout_grace_seconds: float,
    ) -> ReviewVerifyResult:
        observed_cwds.append(cwd)
        assert cwd.is_dir()
        return ReviewVerifyResult(
            command=command,
            status="failed",
            exit_status="1",
            captured_at=datetime.now(UTC),
            output="",
        )

    original_add_existing = _RecordingGit.worktree_add_existing

    def _worktree_add_existing(self: _RecordingGit, path: Path, ref: str, *, detach: bool = False) -> Path:
        created = original_add_existing(self, path, ref, detach=detach)
        (created / "subdir").mkdir(parents=True, exist_ok=True)
        return created

    _RecordingGit.worktree_add_existing = _worktree_add_existing
    try:
        run = run_local_target_baseline_plan(
            plan,
            repo_git=repo_git,
            worktree_root=tmp_path / "worktrees",
            timeout_seconds=30,
            timeout_grace_seconds=5.0,
            run_verify_command=_verify_runner,
        )
    finally:
        _RecordingGit.worktree_add_existing = original_add_existing

    assert len(run.results) == 1
    assert len(observed_cwds) == 1
    assert observed_cwds[0].name == "subdir"


def test_detached_local_target_worktree_fails_closed_when_target_sha_cannot_be_checked_out(
    tmp_path: Path,
) -> None:
    _RecordingGit.reset()
    _RecordingGit.fail_checkout_ref = "abc123def456"
    repo_git = _RecordingGit(tmp_path / "repo")

    try:
        with detached_local_target_worktree(
            repo_git=repo_git,
            worktree_root=tmp_path / "worktrees",
            target_branch="main",
            target_head_sha="abc123def456",
        ):
            raise AssertionError("unreachable")
    except RuntimeError as exc:
        assert str(exc) == "cannot checkout abc123def456"
    else:
        raise AssertionError("expected checkout failure")

    assert ("worktree_add_existing", "abc123def456") in _RecordingGit.operations
    assert ("checkout_detached", "abc123def456") in _RecordingGit.operations
    assert _RecordingGit.removed_paths
