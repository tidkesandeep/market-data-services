"""Backward-compatible entry point — delegates to main."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from main import run  # noqa: E402

if __name__ == "__main__":
    run()
