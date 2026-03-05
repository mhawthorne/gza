#!/usr/bin/env python3
"""Count the number of lines per function in Python files.

Output format: line_count module.class.function
Pipe to `sort -rn` or use --sort to control ordering.
"""

import argparse
import ast
import sys
from pathlib import Path


def collect_functions(filepath: Path, base: Path) -> list[tuple[str, int]]:
    """Return [(qualified_name, line_count), ...] for a file."""
    source = filepath.read_text()
    tree = ast.parse(source)

    # Derive module name from path
    try:
        rel = filepath.relative_to(base)
    except ValueError:
        rel = filepath
    module = str(rel.with_suffix("")).replace("/", ".")

    results = []

    def visit(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                class_prefix = f"{prefix}.{child.name}" if prefix else child.name
                visit(child, class_prefix)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"{prefix}.{child.name}" if prefix else child.name
                lines = (child.end_lineno or child.lineno) - child.lineno + 1
                results.append((f"{module}.{name}", lines))
                # Also visit nested functions/classes
                visit(child, name if prefix else f"{prefix}.{child.name}" if prefix else child.name)

    visit(tree, "")
    return results


def class_name(qualified: str) -> str:
    """Extract class from module.class.func or '' for module-level."""
    parts = qualified.rsplit(".", 2)
    return parts[-2] if len(parts) >= 3 else ""


def main():
    parser = argparse.ArgumentParser(description="Count lines per Python function.")
    parser.add_argument("paths", nargs="+", help="Files or directories to scan")
    parser.add_argument(
        "--sort",
        choices=["lines", "class-size", "class-alpha"],
        default=None,
        help="Sort order: lines (largest first), class-size (largest class first, "
        "then by line count), class-alpha (class name, then by line count). "
        "Default: no sort (use `| sort -rn`).",
    )
    parser.add_argument(
        "--base",
        default=None,
        help="Base directory for computing module paths (default: common ancestor of paths)",
    )
    args = parser.parse_args()

    files: list[Path] = []
    dirs: list[Path] = []
    for p in args.paths:
        path = Path(p)
        if path.is_dir():
            dirs.append(path)
            files.extend(path.rglob("*.py"))
        else:
            files.append(path)

    if args.base:
        base = Path(args.base)
    elif dirs:
        base = dirs[0]
    elif files:
        base = files[0].parent
    else:
        base = Path(".")

    all_funcs: list[tuple[str, int]] = []
    for filepath in files:
        try:
            all_funcs.extend(collect_functions(filepath, base))
        except SyntaxError:
            print(f"# skipping {filepath} (syntax error)", file=sys.stderr)

    if args.sort == "lines":
        all_funcs.sort(key=lambda x: -x[1])
    elif args.sort == "class-size":
        # Sum lines per class, then sort by class total desc, then func lines desc
        class_totals: dict[str, int] = {}
        for name, lines in all_funcs:
            cls = class_name(name)
            class_totals[cls] = class_totals.get(cls, 0) + lines
        all_funcs.sort(key=lambda x: (-class_totals[class_name(x[0])], -x[1]))
    elif args.sort == "class-alpha":
        all_funcs.sort(key=lambda x: (class_name(x[0]), -x[1]))

    for name, lines in all_funcs:
        print(f"{lines:6d} {name}")


if __name__ == "__main__":
    main()
