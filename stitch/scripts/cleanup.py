from __future__ import annotations

import argparse
from pathlib import Path
import shutil

from . import paths

USAGE = "spark-submit stitch.py cleanup"


def clear_data(root: Path | None = None) -> int:
    # Empty the data directory, keeping the directory itself.
    #
    # A run is one crawl on one date, and the next one starts from an empty
    # directory: rows are told apart from an earlier crawl's by date alone, so
    # two crawls left side by side would be read as one. Run this only once the
    # data is safely in Snowflake - nothing brings it back but another crawl.
    root = paths.OUTPUT_ROOT if root is None else root
    if not root.is_dir():
        return 0
    removed = 0
    for path in sorted(root.iterdir()):
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()
        removed += 1
    return removed


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog=USAGE, description="Empty this stage's data directory.")
    parser.add_argument("--data", type=Path, default=paths.OUTPUT_ROOT,
                        help="the directory to empty")
    args = parser.parse_args(argv)
    removed = clear_data(args.data)
    print(f"removed {removed} entr{'y' if removed == 1 else 'ies'} "
          f"from {paths.relative(args.data)}")
