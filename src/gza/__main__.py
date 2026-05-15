"""Allow running gza as a module: python -m gza."""

import sys

from gza.cli import main

if __name__ == "__main__":
    sys.exit(main())
