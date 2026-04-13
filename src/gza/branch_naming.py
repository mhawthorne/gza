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
    task_slug: str,
    prompt: str,
    default_type: str = "feature",
    explicit_type: str | None = None,
    task_id: str = "",
    project_prefix: str = "",
) -> str:
    if not isinstance(project_prefix, str):
        project_prefix = ""
    if not isinstance(task_id, str):
        task_id = ""
    """Generate a branch name from a pattern and task information.

    Args:
        pattern: The branch name pattern with variables (e.g. "{type}/{slug}")
        project_name: The project name
        task_slug: The task slug in format ``YYYYMMDD-{prefix}-{slug}``
            (corresponds to ``Task.slug``)
        prompt: The task prompt (used for type inference)
        default_type: The default type to use if inference fails
        explicit_type: Explicitly provided type (overrides inference)
        task_id: The short task id (e.g. ``gza-1234``, corresponds to
            ``Task.id``). Empty string when unknown (e.g. during pre-creation
            collision checks).

    Supported pattern variables: ``{project}``, ``{task_id}``, ``{task_slug}``,
    ``{date}``, ``{slug}``, ``{type}``.

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

    # Parse task_slug into date, (optional) prefix, and bare slug.
    # Canonical task_slug format: ``YYYYMMDD-{project_prefix}-{slug_text}``.
    # When ``project_prefix`` is known we strip it so ``{slug}`` doesn't
    # duplicate the project name in patterns that also include ``{project}``
    # or ``{prefix}``.
    if "-" in task_slug:
        date_part, rest = task_slug.split("-", 1)
    else:
        date_part = datetime.now().strftime("%Y%m%d")
        rest = task_slug

    if project_prefix and rest.startswith(f"{project_prefix}-"):
        slug_part = rest[len(project_prefix) + 1:]
    elif project_prefix and rest == project_prefix:
        slug_part = ""
    else:
        slug_part = rest

    # Variable substitution
    branch_name = pattern
    branch_name = branch_name.replace("{project}", project_name)
    branch_name = branch_name.replace("{task_slug}", task_slug)
    branch_name = branch_name.replace("{task_id}", task_id)
    branch_name = branch_name.replace("{prefix}", project_prefix)
    branch_name = branch_name.replace("{date}", date_part)
    branch_name = branch_name.replace("{slug}", slug_part)
    branch_name = branch_name.replace("{type}", branch_type)

    return branch_name
