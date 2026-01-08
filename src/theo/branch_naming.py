"""Branch naming utilities for Theo."""

import re
from datetime import datetime


def infer_type_from_prompt(prompt: str) -> str | None:
    """Infer the branch type from keywords in the prompt.

    Args:
        prompt: The task prompt/description

    Returns:
        The inferred type string, or None if no match found
    """
    # Normalize prompt to lowercase for keyword matching
    prompt_lower = prompt.lower()

    # Type inference rules from spec
    type_keywords = {
        "fix": ["fix", "bug", "error", "crash", "broken", "issue"],
        "feature": ["feat", "feature", "add", "implement", "create", "new"],
        "refactor": ["refactor", "restructure", "reorganize", "clean"],
        "docs": ["doc", "docs", "document", "readme"],
        "test": ["test", "spec", "coverage"],
        "chore": ["chore", "update", "upgrade", "bump", "deps"],
        "perf": ["perf", "performance", "optimize", "speed"],
    }

    # Check each type's keywords
    for type_name, keywords in type_keywords.items():
        for keyword in keywords:
            # Use word boundary matching to avoid partial matches
            if re.search(r'\b' + re.escape(keyword) + r'\b', prompt_lower):
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
