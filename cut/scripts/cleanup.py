from __future__ import annotations

import argparse
from pathlib import Path
import shutil

from . import paths

USAGE = "python cut.py cleanup"


def clear_data(root: Path) -> int:
    # Empty a data directory, keeping the directory itself.
    #
    # A run is one crawl on one date, and the next one starts from an empty
    # directory: rows are told apart from an earlier crawl's by date alone, so
    # two crawls left side by side would be read as one. Run this only once the
    # data is safely in Snowflake - nothing brings it back but another crawl.
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
    # This stage holds two directories, so both are emptied: the dismantled
    # intermediate nothing downstream reads, and the parquet tables once they
    # are in the PROCESSED stage.
    parser = argparse.ArgumentParser(
        prog=USAGE, description="Empty this stage's data directories.")
    parser.add_argument("--dismantled", type=Path, default=paths.DISMANTLED_ROOT,
                        help="the dismantled crawl directory to empty")
    parser.add_argument("--trimmed", type=Path, default=paths.TRIMMED_ROOT,
                        help="the parquet table directory to empty")
    parser.add_argument("--only", choices=("dismantled", "trimmed"),
                        help="empty just one of them")
    args = parser.parse_args(argv)

    targets = [("dismantled", args.dismantled), ("trimmed", args.trimmed)]
    if args.only:
        targets = [t for t in targets if t[0] == args.only]

    for label, root in targets:
        removed = clear_data(root)
        print(f"removed {removed} entr{'y' if removed == 1 else 'ies'} "
              f"from {label} ({paths.relative(root)})")
