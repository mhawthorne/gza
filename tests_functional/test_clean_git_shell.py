"""Functional wrappers for clean-command tests that require a real git repo."""

from tests.cli.test_config_cmds import TestCleanCommand as _UnitTestCleanCommand


def test_clean_dry_run(tmp_path) -> None:
    _UnitTestCleanCommand()._functional_test_clean_dry_run(tmp_path)


def test_clean_keep_unmerged_logs(tmp_path) -> None:
    _UnitTestCleanCommand()._functional_test_clean_keep_unmerged_logs(tmp_path)


def test_clean_lineage_aware_preserves_recent(tmp_path) -> None:
    _UnitTestCleanCommand()._functional_test_clean_lineage_aware_preserves_recent(tmp_path)


def test_clean_lineage_aware_removes_old(tmp_path) -> None:
    _UnitTestCleanCommand()._functional_test_clean_lineage_aware_removes_old(tmp_path)


def test_clean_force_skips_prompt(tmp_path) -> None:
    _UnitTestCleanCommand()._functional_test_clean_force_skips_prompt(tmp_path)


def test_clean_no_force_denies_removal(tmp_path) -> None:
    _UnitTestCleanCommand()._functional_test_clean_no_force_denies_removal(tmp_path)
