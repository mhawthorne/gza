#!/usr/bin/env python3
"""Check syntax of cli.py."""

import py_compile
import sys
from pathlib import Path

try:
    py_compile.compile("src/theo/cli.py", doraise=True)
    print("✓ src/theo/cli.py has valid syntax")
    sys.exit(0)
except py_compile.PyCompileError as e:
    print(f"✗ src/theo/cli.py has syntax error: {e}")
    sys.exit(1)
