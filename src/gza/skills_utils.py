"""Utilities for managing Claude Code skills.

Skill Naming Convention:
    All skills must use SKILL.md (uppercase) as the skill definition filename.
    This ensures consistency across all skills and simplifies discovery.
"""

import importlib.resources
import shutil
import tempfile
from pathlib import Path
from typing import List, Tuple, Optional


def get_skills_source_path() -> Path:
    """Get the path to bundled skills directory.

    Returns:
        Path to the skills directory in the gza package.
    """
    # Use importlib.resources to get the skills directory
    ref = importlib.resources.files('gza').joinpath('skills')
    # Convert Traversable to Path
    return Path(str(ref))


def get_available_skills() -> List[str]:
    """List available skill names from the bundled skills.

    A skill is valid if it's a directory containing a SKILL.md file (uppercase).

    Returns:
        Sorted list of skill names.
    """
    skills_path = get_skills_source_path()
    skills = []

    if not skills_path.exists():
        return skills

    for item in skills_path.iterdir():
        if item.is_dir():
            # Only check for SKILL.md (uppercase) - this is the convention
            if (item / 'SKILL.md').exists():
                skills.append(item.name)

    return sorted(skills)


def _parse_skill_frontmatter(skill_name: str, field: str) -> str:
    """Extract a field from a skill's SKILL.md frontmatter.

    Args:
        skill_name: Name of the skill.
        field: Field name to extract from frontmatter.

    Returns:
        The field value from the SKILL.md frontmatter, or empty string if not found.
    """
    skills_path = get_skills_source_path()
    skill_path = skills_path / skill_name
    skill_file = skill_path / 'SKILL.md'

    if not skill_file.exists():
        return ""

    try:
        content = skill_file.read_text()
        # Parse frontmatter for field
        if content.startswith('---'):
            lines = content.split('\n')
            for line in lines[1:]:
                if line.strip() == '---':
                    break
                if line.startswith(f'{field}:'):
                    return line.split(f'{field}:', 1)[1].strip()
    except Exception:
        pass

    return ""


def get_skill_description(skill_name: str) -> str:
    """Extract the description from a skill's SKILL.md file.

    Args:
        skill_name: Name of the skill.

    Returns:
        The description line from the SKILL.md frontmatter, or empty string if not found.
    """
    return _parse_skill_frontmatter(skill_name, 'description')


def get_skill_version(skill_name: str) -> Optional[str]:
    """Extract the version from a skill's SKILL.md file.

    Args:
        skill_name: Name of the skill.

    Returns:
        The version from the SKILL.md frontmatter, or None if not found.
    """
    version = _parse_skill_frontmatter(skill_name, 'version')
    return version if version else None


def copy_skill(skill_name: str, target_dir: Path, force: bool = False) -> Tuple[bool, str]:
    """Copy a skill from the package to the target directory atomically.

    Uses a temporary directory and atomic rename to prevent partial state
    if the copy operation fails mid-operation.

    Args:
        skill_name: Name of the skill to copy.
        target_dir: Target directory (should be .claude/skills/).
        force: If True, overwrite existing skills.

    Returns:
        Tuple of (success: bool, message: str).
        - On success: (True, "installed")
        - On skip: (False, "already exists, use --force to overwrite")
        - On error: (False, error message)
    """
    skills_path = get_skills_source_path()
    source = skills_path / skill_name
    target = target_dir / skill_name

    # Check if source skill exists
    if not source.exists() or not source.is_dir():
        return False, f"Skill '{skill_name}' not found"

    # Check if target already exists
    if target.exists() and not force:
        return False, "already exists, use --force to overwrite"

    # Use a temporary directory in the same parent directory as target
    # to ensure atomic rename works (must be on same filesystem)
    temp_dir = None
    try:
        # Create temp directory in the target's parent directory
        temp_dir = tempfile.mkdtemp(dir=target_dir, prefix=f".tmp_{skill_name}_")
        temp_path = Path(temp_dir) / skill_name

        # Copy to temporary location first
        shutil.copytree(source, temp_path)

        # Remove existing target if force is True
        if target.exists() and force:
            try:
                shutil.rmtree(target)
            except Exception as e:
                return False, f"failed to remove existing: {e}"

        # Atomically rename temp to target
        # This is atomic on most filesystems (POSIX rename)
        temp_path.rename(target)

        return True, "installed"
    except Exception as e:
        return False, f"copy failed: {e}"
    finally:
        # Clean up temp directory if it still exists
        if temp_dir and Path(temp_dir).exists():
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass  # Best effort cleanup
