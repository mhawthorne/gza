"""Regression coverage for shared task-type admission and filtering."""

import argparse
from pathlib import Path
from unittest.mock import patch

from gza.cli._common import _add_query_filter_args
from gza.cli.execution import cmd_add
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.query import HistoryFilter, query_history


def _write_config(project_dir: Path) -> None:
    (project_dir / "gza.yaml").write_text("project_name: demo\n", encoding="utf-8")


def _load_store(project_dir: Path) -> SqliteTaskStore:
    config = Config.load(project_dir)
    return SqliteTaskStore(
        config.db_path,
        prefix=config.project_prefix,
        project_id=config.project_id,
        project_root=config.project_dir,
        config_path=Config.config_path(config.project_dir),
        project_name=config.project_name,
    )


def test_cmd_add_accepts_well_formed_plan_review_plan_improve_and_verify_fix_task_types(tmp_path: Path) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    source_plan = store.add("Draft the plan", task_type="plan")
    source_review = store.add("Review the plan", task_type="plan_review", depends_on=source_plan.id)
    source_impl = store.add("Implement the plan", task_type="implement")

    args_by_type = {
        "plan_review": argparse.Namespace(
            project_dir=tmp_path,
            prompt="plan_review prompt",
            prompt_file=None,
            edit=False,
            type="plan_review",
            explore=False,
            depends_on=source_plan.id,
            based_on=None,
            review=False,
            hold_for_review=False,
            create_pr=False,
            same_branch=False,
            spec=None,
            review_scope=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
            tags=None,
        ),
        "plan_improve": argparse.Namespace(
            project_dir=tmp_path,
            prompt="plan_improve prompt",
            prompt_file=None,
            edit=False,
            type="plan_improve",
            explore=False,
            depends_on=source_review.id,
            based_on=source_plan.id,
            review=False,
            hold_for_review=False,
            create_pr=False,
            same_branch=False,
            spec=None,
            review_scope=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
            tags=None,
        ),
        "verify_fix": argparse.Namespace(
            project_dir=tmp_path,
            prompt="verify_fix prompt",
            prompt_file=None,
            edit=False,
            type="verify_fix",
            explore=False,
            depends_on=None,
            based_on=source_impl.id,
            review=False,
            hold_for_review=False,
            create_pr=False,
            same_branch=True,
            spec=None,
            review_scope=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
            tags=None,
        ),
    }

    for task_type in ("plan_review", "plan_improve", "verify_fix"):
        with patch("gza.cli.execution.set_task_urgency", return_value=True):
            rc = cmd_add(args_by_type[task_type])

        assert rc == 0

    pending = store.get_pending()
    assert [task.task_type for task in pending] == [
        "plan",
        "plan_review",
        "implement",
        "plan_review",
        "plan_improve",
        "verify_fix",
    ]


def test_cmd_add_rejects_verify_fix_without_lineage_anchor(tmp_path: Path, capsys) -> None:
    _write_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="verify_fix prompt",
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=None,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=False,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "verify_fix tasks require --based-on" in capsys.readouterr().out


def test_cmd_add_accepts_same_branch_verify_fix_anchored_to_code_lineage(tmp_path: Path) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    impl = store.add("Implement the plan", task_type="implement")
    improve = store.add("Improve the plan", task_type="improve", based_on=impl.id, same_branch=True)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="verify_fix prompt",
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=improve.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=True,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    with patch("gza.cli.execution.set_task_urgency", return_value=True):
        rc = cmd_add(args)

    assert rc == 0
    verify_fix = next(task for task in store.get_pending() if task.task_type == "verify_fix")
    assert verify_fix.based_on == improve.id
    assert verify_fix.same_branch is True


def test_cmd_add_rejects_plan_review_without_plan_source_dependency(tmp_path: Path, capsys) -> None:
    _write_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="Review the plan",
        prompt_file=None,
        edit=False,
        type="plan_review",
        explore=False,
        depends_on=None,
        based_on=None,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=False,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "plan_review tasks require --depends-on" in capsys.readouterr().out


def test_cmd_add_rejects_plan_improve_without_matching_plan_review_dependency(
    tmp_path: Path,
    capsys,
) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    plan = store.add("Draft the plan", task_type="plan")
    other_plan = store.add("Different plan", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=other_plan.id)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="Revise the plan",
        prompt_file=None,
        edit=False,
        type="plan_improve",
        explore=False,
        depends_on=review.id,
        based_on=plan.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=False,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "must be based on the same plan source" in capsys.readouterr().out


def test_query_history_filters_accept_plan_review_plan_improve_and_verify_fix(tmp_path: Path) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)

    review_task = store.add("Review the plan", task_type="plan_review")
    review_task.status = "completed"
    store.update(review_task)
    improve_task = store.add("Revise the plan", task_type="plan_improve")
    improve_task.status = "completed"
    store.update(improve_task)
    verify_fix_task = store.add("Fix verify", task_type="verify_fix")
    verify_fix_task.status = "completed"
    store.update(verify_fix_task)

    review_rows = query_history(store, HistoryFilter(limit=None, task_type="plan_review"))
    improve_rows = query_history(store, HistoryFilter(limit=None, task_type="plan_improve"))
    verify_fix_rows = query_history(store, HistoryFilter(limit=None, task_type="verify_fix"))

    assert [task.task_type for task in review_rows] == ["plan_review"]
    assert [task.task_type for task in improve_rows] == ["plan_improve"]
    assert [task.task_type for task in verify_fix_rows] == ["verify_fix"]


def test_query_filter_flags_include_new_plan_review_task_types() -> None:
    parser = argparse.ArgumentParser()
    _add_query_filter_args(parser)

    choices_by_dest = {
        action.dest: set(action.choices or [])
        for action in parser._actions
        if action.dest in {"type", "type_not"}
    }

    for dest in ("type", "type_not"):
        assert "plan_review" in choices_by_dest[dest]
        assert "plan_improve" in choices_by_dest[dest]
        assert "verify_fix" in choices_by_dest[dest]
