from __future__ import annotations

import argparse

from connection import RAW_ROOT, STAGE, connect, datasets, put


# What --help prints above the options.
USAGE = """\
python upload_raw.py                every retailer
python upload_raw.py kith rickowens named retailers only
"""


def upload(only: list[str] | None = None) -> int:
    # Upload every retailer's crawls, or just the named ones. Returns failures.
    wanted = set(only or [])
    found = [(name, files) for name, files in datasets(RAW_ROOT, "*.json")
             if not wanted or name in wanted]

    missing = wanted - {name for name, _ in found}
    if missing:
        raise SystemExit(f"no such retailer(s): {', '.join(sorted(missing))}")
    if not found:
        raise SystemExit(f"no crawls under {RAW_ROOT}")

    connection = connect()
    try:
        cursor = connection.cursor()
        for name, files in found:
            if not files:
                print(f"  {name}: no crawls, skipped")
                continue
            print(f"  {name}: {len(files)} crawl(s) -> @{STAGE}/{name}/")
            for file in files:
                put(cursor, file, name)
    finally:
        connection.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python upload_raw.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("retailers", nargs="*",
                        help="retailer names; default is every retailer")
    args = parser.parse_args()
    raise SystemExit(upload(args.retailers or None))


if __name__ == "__main__":
    main()
