from __future__ import annotations

import argparse
from pathlib import Path

from scripts import dismantle, paths


# What --help prints above the options.
USAGE = """\
python dismantling.py                     every crawl under ingestion/data/raw
python dismantling.py path/to/crawl.json  one crawl
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python dismantling.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", type=Path, nargs="?",
                        help="a single raw crawl; default is every crawl under --raw")
    parser.add_argument("--raw", type=Path, default=paths.RAW_ROOT,
                        help="directory holding the raw crawls")
    parser.add_argument("--staging", type=Path, default=paths.STAGING_ROOT,
                        help="directory to write the staged files into")
    args = parser.parse_args()

    if not args.input:
        raise SystemExit(1 if dismantle.dismantle_all(args.raw, args.staging) else 0)

    if not args.input.is_file():
        raise SystemExit(f"no such raw crawl: {args.input}")

    directory, products, variants = dismantle.dismantle(args.input, args.staging)
    print(f"{products:,} products and {variants:,} variants "
          f"-> {paths.relative(directory)}")


if __name__ == "__main__":
    main()
