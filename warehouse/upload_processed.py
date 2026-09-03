"""Uploads the processed parquets to a Snowflake stage.

    python upload_processed.py                  every table
    python upload_processed.py products brands  named tables only

One stage prefix per table, so `@AMARA_STAGE/products/` holds exactly the part
files of `processing/data/processed/products/`. COPY INTO reads a whole prefix,
so the part count is Spark's business and nothing needs flattening first.
"""

from __future__ import annotations

import argparse

from connection import PROCESSED_ROOT, STAGE, connect, datasets, put


def upload(only: list[str] | None = None) -> int:
    """Upload every processed table, or just the named ones. Returns failures."""
    wanted = set(only or [])
    found = [(name, files) for name, files in datasets(PROCESSED_ROOT, "part-*.parquet")
             if not wanted or name in wanted]

    missing = wanted - {name for name, _ in found}
    if missing:
        raise SystemExit(f"no such table(s): {', '.join(sorted(missing))}")
    if not found:
        raise SystemExit(f"no parquet datasets under {PROCESSED_ROOT}")

    connection = connect()
    try:
        cursor = connection.cursor()
        for name, files in found:
            if not files:
                print(f"  {name}: no parquet files, skipped")
                continue
            print(f"  {name}: {len(files)} file(s) -> @{STAGE}/{name}/")
            for file in files:
                put(cursor, file, name)
    finally:
        connection.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python upload_processed.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("tables", nargs="*",
                        help="table names; default is every table")
    args = parser.parse_args()
    raise SystemExit(upload(args.tables or None))


if __name__ == "__main__":
    main()
