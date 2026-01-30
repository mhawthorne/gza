"""Utilities for managing Claude Code skills."""

import importlib.resources
import shutil
from pathlib import Path
from typing import List, Tuple


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

    A skill is valid if it's a directory containing a SKILL.md or skill.md file.

    Returns:
        Sorted list of skill names.
    """
    skills_path = get_skills_source_path()
    skills = []

    if not skills_path.exists():
        return skills

    for item in skills_path.iterdir():
        if item.is_dir():
            # Check for SKILL.md or skill.md
            if (item / 'SKILL.md').exists() or (item / 'skill.md').exists():
                skills.append(item.name)

    return sorted(skills)


def get_skill_description(skill_name: str) -> str:
    """Extract the description from a skill's SKILL.md file.

    Args:
        skill_name: Name of the skill.

    Returns:
        The description line from the SKILL.md frontmatter, or empty string if not found.
    """
    skills_path = get_skills_source_path()
    skill_path = skills_path / skill_name

    # Try both SKILL.md and skill.md
    for filename in ['SKILL.md', 'skill.md']:
        skill_file = skill_path / filename
        if skill_file.exists():
            try:
                content = skill_file.read_text()
                # Parse frontmatter for description
                if content.startswith('---'):
                    lines = content.split('\n')
                    for line in lines[1:]:
                        if line.strip() == '---':
                            break
                        if line.startswith('description:'):
                            return line.split('description:', 1)[1].strip()
            except Exception:
                pass

    return ""


def copy_skill(skill_name: str, target_dir: Path, force: bool = False) -> Tuple[bool, str]:
    """Copy a skill from the package to the target directory.

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

    # Remove target if it exists and force is True
    if target.exists() and force:
        try:
            shutil.rmtree(target)
        except Exception as e:
            return False, f"failed to remove existing: {e}"

    # Copy the skill
    try:
        shutil.copytree(source, target)
        return True, "installed"
    except Exception as e:
        return False, f"copy failed: {e}"
