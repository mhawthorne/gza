#!/usr/bin/env python3
"""Simple syntax verification script."""

import ast
import sys
from pathlib import Path

def verify_syntax(filepath):
    """Verify Python syntax by parsing AST."""
    try:
        with open(filepath) as f:
            code = f.read()
        ast.parse(code)
        print(f"✓ {filepath}")
        return True
    except SyntaxError as e:
        print(f"✗ {filepath}: {e}")
        return False

if __name__ == "__main__":
    files = [
        "src/theo/db.py",
        "src/theo/runner.py",
        "test_plan_persistence.py"
    ]

    all_ok = True
    for f in files:
        if Path(f).exists():
            if not verify_syntax(f):
                all_ok = False
        else:
            print(f"⚠ {f} not found")
            all_ok = False

    if all_ok:
        print("\n✓ All syntax checks passed!")
        sys.exit(0)
    else:
        print("\n✗ Syntax errors found")
        sys.exit(1)
