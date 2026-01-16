"""Tests for branch naming functionality."""

import pytest

from gza.branch_naming import generate_branch_name, infer_type_from_prompt
from gza.config import BranchStrategy, Config, ConfigError


class TestTypeInference:
    """Test type inference from prompts."""

    def test_infer_fix_type(self):
        """Test inference of 'fix' type."""
        assert infer_type_from_prompt("Fix the login bug") == "fix"
        assert infer_type_from_prompt("This is broken and needs fixing") == "fix"
        assert infer_type_from_prompt("Error in authentication") == "fix"
        assert infer_type_from_prompt("Crash on startup") == "fix"

    def test_infer_feature_type(self):
        """Test inference of 'feature' type."""
        assert infer_type_from_prompt("Add user authentication") == "feature"
        assert infer_type_from_prompt("Implement dark mode") == "feature"
        assert infer_type_from_prompt("Create new dashboard") == "feature"
        assert infer_type_from_prompt("New feature for exports") == "feature"

    def test_infer_refactor_type(self):
        """Test inference of 'refactor' type."""
        assert infer_type_from_prompt("Refactor the auth module") == "refactor"
        assert infer_type_from_prompt("Restructure the codebase") == "refactor"
        assert infer_type_from_prompt("Clean up the database layer") == "refactor"

    def test_infer_docs_type(self):
        """Test inference of 'docs' type."""
        assert infer_type_from_prompt("Update documentation") == "docs"
        assert infer_type_from_prompt("Write docs for API") == "docs"
        assert infer_type_from_prompt("Update README") == "docs"

    def test_infer_test_type(self):
        """Test inference of 'test' type."""
        assert infer_type_from_prompt("Add tests for auth") == "test"
        assert infer_type_from_prompt("Write test coverage") == "test"
        assert infer_type_from_prompt("Create test specs") == "test"

    def test_infer_chore_type(self):
        """Test inference of 'chore' type."""
        assert infer_type_from_prompt("Update dependencies") == "chore"
        assert infer_type_from_prompt("Upgrade to Node 20") == "chore"
        assert infer_type_from_prompt("Bump package versions") == "chore"

    def test_infer_perf_type(self):
        """Test inference of 'perf' type."""
        assert infer_type_from_prompt("Optimize query performance") == "perf"
        assert infer_type_from_prompt("Improve speed of rendering") == "perf"

    def test_no_inference(self):
        """Test when no type can be inferred."""
        assert infer_type_from_prompt("Do something") is None
        assert infer_type_from_prompt("Random task") is None

    def test_word_boundaries(self):
        """Test that word boundaries are respected."""
        # "perforce" should not match "perf"
        assert infer_type_from_prompt("Setup perforce integration") is None
        # "fixing" should match "fix"
        assert infer_type_from_prompt("We need fixing this") == "fix"


class TestBranchNameGeneration:
    """Test branch name generation."""

    def test_monorepo_pattern(self):
        """Test monorepo pattern: {project}/{task_id}."""
        name = generate_branch_name(
            pattern="{project}/{task_id}",
            project_name="myproj",
            task_id="20260107-add-auth",
            prompt="Add authentication",
            default_type="feature",
        )
        assert name == "myproj/20260107-add-auth"

    def test_conventional_pattern(self):
        """Test conventional pattern: {type}/{slug}."""
        name = generate_branch_name(
            pattern="{type}/{slug}",
            project_name="myproj",
            task_id="20260107-add-auth",
            prompt="Add authentication",
            default_type="feature",
        )
        assert name == "feature/add-auth"

    def test_conventional_pattern_with_inference(self):
        """Test conventional pattern with type inference."""
        name = generate_branch_name(
            pattern="{type}/{slug}",
            project_name="myproj",
            task_id="20260107-fix-login-bug",
            prompt="Fix login bug",
            default_type="feature",
        )
        assert name == "fix/fix-login-bug"

    def test_conventional_pattern_with_explicit_type(self):
        """Test conventional pattern with explicit type."""
        name = generate_branch_name(
            pattern="{type}/{slug}",
            project_name="myproj",
            task_id="20260107-update-deps",
            prompt="Update dependencies",
            default_type="feature",
            explicit_type="chore",
        )
        assert name == "chore/update-deps"

    def test_simple_pattern(self):
        """Test simple pattern: {slug}."""
        name = generate_branch_name(
            pattern="{slug}",
            project_name="myproj",
            task_id="20260107-add-auth",
            prompt="Add authentication",
            default_type="feature",
        )
        assert name == "add-auth"

    def test_custom_pattern_with_date(self):
        """Test custom pattern with date: {type}/{date}-{slug}."""
        name = generate_branch_name(
            pattern="{type}/{date}-{slug}",
            project_name="myproj",
            task_id="20260107-add-auth",
            prompt="Add authentication",  # Infers to "feature" from "add"
            default_type="feat",
        )
        assert name == "feature/20260107-add-auth"

    def test_custom_pattern_complex(self):
        """Test complex custom pattern."""
        name = generate_branch_name(
            pattern="user/{project}-{type}-{slug}",
            project_name="myproj",
            task_id="20260107-fix-bug",
            prompt="Fix bug in login",
            default_type="feature",
        )
        assert name == "user/myproj-fix-fix-bug"

    def test_explicit_type_overrides_inference(self):
        """Test that explicit type overrides inference."""
        name = generate_branch_name(
            pattern="{type}/{slug}",
            project_name="myproj",
            task_id="20260107-update-readme",
            prompt="Fix the README",  # Would infer "fix"
            default_type="feature",
            explicit_type="docs",  # But explicit type overrides
        )
        assert name == "docs/update-readme"

    def test_default_type_used_when_no_inference(self):
        """Test that default type is used when inference fails."""
        name = generate_branch_name(
            pattern="{type}/{slug}",
            project_name="myproj",
            task_id="20260107-do-something",
            prompt="Do something",  # Cannot infer type
            default_type="task",
        )
        assert name == "task/do-something"


class TestBranchStrategyValidation:
    """Test BranchStrategy validation."""

    def test_valid_patterns(self):
        """Test valid patterns."""
        BranchStrategy(pattern="{project}/{task_id}")
        BranchStrategy(pattern="{type}/{slug}")
        BranchStrategy(pattern="{slug}")
        BranchStrategy(pattern="{type}/{date}-{slug}")
        BranchStrategy(pattern="prefix/{project}-{slug}")

    def test_invalid_space(self):
        """Test that spaces are rejected."""
        with pytest.raises(ConfigError, match="Invalid character ' '"):
            BranchStrategy(pattern="{type} {slug}")

    def test_invalid_tilde(self):
        """Test that tilde is rejected."""
        with pytest.raises(ConfigError, match="Invalid character '~'"):
            BranchStrategy(pattern="~{slug}")

    def test_invalid_consecutive_dots(self):
        """Test that consecutive dots are rejected."""
        with pytest.raises(ConfigError, match="consecutive dots"):
            BranchStrategy(pattern="{type}..{slug}")

    def test_invalid_consecutive_slashes(self):
        """Test that consecutive slashes are rejected."""
        with pytest.raises(ConfigError, match="consecutive slashes"):
            BranchStrategy(pattern="{type}//{slug}")

    def test_invalid_start_with_dot(self):
        """Test that starting with dot is rejected."""
        with pytest.raises(ConfigError, match="cannot start with a dot"):
            BranchStrategy(pattern=".{slug}")

    def test_invalid_start_with_slash(self):
        """Test that starting with slash is rejected."""
        with pytest.raises(ConfigError, match="cannot start with a slash"):
            BranchStrategy(pattern="/{slug}")

    def test_invalid_end_with_slash(self):
        """Test that ending with slash is rejected."""
        with pytest.raises(ConfigError, match="cannot end with a slash"):
            BranchStrategy(pattern="{slug}/")

    def test_invalid_end_with_lock(self):
        """Test that ending with .lock is rejected."""
        with pytest.raises(ConfigError, match="cannot end with .lock"):
            BranchStrategy(pattern="{slug}.lock")


class TestConfigBranchStrategy:
    """Test Config loading with branch_strategy."""

    def test_preset_monorepo(self, tmp_path):
        """Test loading monorepo preset."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy: monorepo
""")
        config = Config.load(tmp_path)
        assert config.branch_strategy.pattern == "{project}/{task_id}"
        assert config.branch_strategy.default_type == "feature"

    def test_preset_conventional(self, tmp_path):
        """Test loading conventional preset."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy: conventional
""")
        config = Config.load(tmp_path)
        assert config.branch_strategy.pattern == "{type}/{slug}"
        assert config.branch_strategy.default_type == "feature"

    def test_preset_simple(self, tmp_path):
        """Test loading simple preset."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy: simple
""")
        config = Config.load(tmp_path)
        assert config.branch_strategy.pattern == "{slug}"
        assert config.branch_strategy.default_type == "feature"

    def test_custom_pattern(self, tmp_path):
        """Test loading custom pattern."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy:
  pattern: "{type}/{date}-{slug}"
  default_type: feat
""")
        config = Config.load(tmp_path)
        assert config.branch_strategy.pattern == "{type}/{date}-{slug}"
        assert config.branch_strategy.default_type == "feat"

    def test_default_when_not_specified(self, tmp_path):
        """Test that default branch_strategy is used when not specified."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
""")
        config = Config.load(tmp_path)
        # Default should be monorepo pattern
        assert config.branch_strategy.pattern == "{project}/{task_id}"
        assert config.branch_strategy.default_type == "feature"

    def test_invalid_preset_name(self, tmp_path):
        """Test that invalid preset name raises error."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy: invalid_preset
""")
        with pytest.raises(ConfigError, match="Unknown branch_strategy preset"):
            Config.load(tmp_path)

    def test_custom_without_pattern(self, tmp_path):
        """Test that custom dict without pattern raises error."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy:
  default_type: feat
""")
        with pytest.raises(ConfigError, match="must have a 'pattern' key"):
            Config.load(tmp_path)

    def test_invalid_pattern_validation(self, tmp_path):
        """Test that invalid pattern in config is caught."""
        config_file = tmp_path / "gza.yaml"
        config_file.write_text("""
project_name: test
branch_strategy:
  pattern: "{type} {slug}"
""")
        with pytest.raises(ConfigError, match="Invalid character"):
            Config.load(tmp_path)
