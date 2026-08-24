"""CLI adapter for operator-triggered landing."""

import argparse


def cmd_land(args: argparse.Namespace) -> int:
    """Placeholder command surface for the landing coordinator implementation slice."""
    print(
        "Error: gza land is specified but its landing coordinator is not implemented in this slice."
    )
    return 1
