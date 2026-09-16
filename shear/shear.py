from __future__ import annotations

import argparse
from pathlib import Path

from scripts import paths, split


# What --help prints above the options.
USAGE = """\
python shear.py                     the current crawl under gather/data
python shear.py path/to/crawl.json  one retailer's file
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python shear.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", type=Path, nargs="?",
                        help="a single raw crawl; default is every file in the current active crawl")
    parser.add_argument("--raw", type=Path, default=paths.RAW_ROOT,
                        help="directory holding the raw crawls")
    parser.add_argument("--staging", type=Path, default=paths.STAGING_ROOT,
                        help="directory to write the staged files into")
    args = parser.parse_args()

    targets = [args.input] if args.input else split.find_single_raw(args.raw)
    if args.input and args.input.is_dir():
        targets = sorted(args.input.glob("*.json"))

    for input_path in targets:
        if not input_path.is_file():
            raise SystemExit(f"no such raw crawl: {input_path}")

        directory, products, variants = split.shear(input_path, args.staging)
        print(f"{products:,} products and {variants:,} variants "
              f"-> {paths.relative(directory)}")


if __name__ == "__main__":
    main()
