#!/usr/bin/env python3
"""Validate Python syntax of modified files."""

import py_compile
import sys
from pathlib import Path

def validate_file(filepath: Path) -> bool:
    """Validate a Python file's syntax."""
    try:
        py_compile.compile(filepath, doraise=True)
        print(f"✓ {filepath}")
        return True
    except py_compile.PyCompileError as e:
        print(f"✗ {filepath}: {e}")
        return False

def main():
    """Validate all Python files in src/gza and tests."""
    files_to_check = [
        Path("src/gza/runner.py"),
        Path("src/gza/db.py"),
        Path("tests/test_db.py"),
        Path("tests/test_cli.py"),
    ]

    all_valid = True
    for filepath in files_to_check:
        if filepath.exists():
            if not validate_file(filepath):
                all_valid = False
        else:
            print(f"⚠ {filepath} does not exist")
            all_valid = False

    if all_valid:
        print("\n✓ All files have valid syntax!")
        return 0
    else:
        print("\n✗ Some files have syntax errors")
        return 1

if __name__ == "__main__":
    sys.exit(main())
