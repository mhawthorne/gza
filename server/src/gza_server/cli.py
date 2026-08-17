"""CLI entry point: gza-server start/stop/status/open."""

import argparse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="gza-server", description=__doc__)
    parser.add_argument("command", choices=["start", "stop", "status", "open"])
    parser.parse_args(argv)
    raise SystemExit("gza-server is scaffolding only; see specs/features/server.md")


if __name__ == "__main__":
    raise SystemExit(main())
