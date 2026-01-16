"""Branch naming utilities for Gza."""

import re
from datetime import datetime


def infer_type_from_prompt(prompt: str) -> str | None:
    """Infer the branch type from keywords in the prompt.

    Args:
        prompt: The task prompt/description

    Returns:
        The inferred type string, or None if no match found

    Priority order ensures more specific types are checked before generic ones.
    For example, "test" is checked before "feature" since "Add tests" should
    match "test" not "feature" (from "add").
    """
    # Normalize prompt to lowercase for keyword matching
    prompt_lower = prompt.lower()

    # Type inference rules - ordered by specificity (most specific first)
    # This ensures "Add tests" matches "test" before "feature"
    # and "Update documentation" matches "docs" before "chore"
    #
    # Each entry is (type_name, [(keyword, allow_prefix), ...])
    # allow_prefix=True means "fixing" matches "fix", allow_prefix=False requires exact word boundary
    type_keywords = [
        # Highly specific types first
        ("docs", [("documentation", False), ("document", False), ("doc", False), ("docs", False), ("readme", False)]),
        ("test", [("tests", False), ("test", False), ("spec", False), ("coverage", False)]),
        ("perf", [("performance", False), ("optimize", False), ("speed", False)]),  # "perf" removed to avoid "perforce" match
        ("refactor", [("refactor", False), ("restructure", False), ("reorganize", False), ("clean", False)]),
        # Fix-related (should come before feature since "fix" is more specific)
        ("fix", [("fix", True), ("bug", False), ("error", False), ("crash", False), ("broken", False), ("issue", False)]),
        # Chore - "update" needs special handling (allow prefix for "update" -> "updating")
        ("chore", [("chore", False), ("update", True), ("upgrade", False), ("bump", False), ("deps", False), ("dependencies", False)]),
        # Feature is most generic - should be last
        ("feature", [("feat", False), ("feature", False), ("add", False), ("implement", False), ("create", False), ("new", False)]),
    ]

    # Check each type's keywords in priority order
    for type_name, keywords in type_keywords:
        for keyword, allow_prefix in keywords:
            if allow_prefix:
                # Allow word stems (e.g., "fixing" matches "fix")
                pattern = r'\b' + re.escape(keyword)
            else:
                # Require exact word boundary
                pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, prompt_lower):
                return type_name

    return None


def generate_branch_name(
    pattern: str,
    project_name: str,
    task_id: str,
    prompt: str,
    default_type: str = "feature",
    explicit_type: str | None = None,
) -> str:
    """Generate a branch name from a pattern and task information.

    Args:
        pattern: The branch name pattern with variables (e.g. "{type}/{slug}")
        project_name: The project name
        task_id: The task ID in format YYYYMMDD-slug
        prompt: The task prompt (used for type inference)
        default_type: The default type to use if inference fails
        explicit_type: Explicitly provided type (overrides inference)

    Returns:
        The generated branch name

    Raises:
        ValueError: If the pattern is invalid
    """
    # Determine the type to use
    if explicit_type:
        branch_type = explicit_type
    else:
        # Try to infer from prompt
        inferred = infer_type_from_prompt(prompt)
        branch_type = inferred if inferred else default_type

    # Parse task_id into date and slug
    if "-" in task_id:
        date_part, slug_part = task_id.split("-", 1)
    else:
        # Fallback if task_id doesn't have expected format
        date_part = datetime.now().strftime("%Y%m%d")
        slug_part = task_id

    # Variable substitution
    branch_name = pattern
    branch_name = branch_name.replace("{project}", project_name)
    branch_name = branch_name.replace("{task_id}", task_id)
    branch_name = branch_name.replace("{date}", date_part)
    branch_name = branch_name.replace("{slug}", slug_part)
    branch_name = branch_name.replace("{type}", branch_type)

    return branch_name
