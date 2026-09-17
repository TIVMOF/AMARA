from __future__ import annotations

import argparse
from pathlib import Path

from . import paths
from .connection import connect, env


DEFAULT_PROCESSED_ROOT = paths.PROCESSED_ROOT


def datasets(root: Path) -> list[tuple[str, list[Path]]]:
    if not root.is_dir():
        raise SystemExit(f"Processed directory does not exist: {root}")

    result = []

    for directory in sorted(
        path for path in root.iterdir() if path.is_dir()
    ):
        files = sorted(directory.glob("part-*.parquet"))

        if files:
            result.append((directory.name, files))

    return result


def upload(processed_root: Path) -> None:
    found = datasets(processed_root)

    if not found:
        raise SystemExit(
            f"No processed Parquet datasets found in {processed_root}"
        )

    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")

    connection = connect(env("PROCESSED_SCHEMA"))

    try:
        with connection.cursor() as cursor:
            for dataset, files in found:
                stage = (
                    f"@{database}.{schema}.AMARA_STAGE/{dataset}/"
                )

                print(f"Uploading {dataset}...")

                # Clear the prefix first. Spark names every part file with a
                # fresh UUID, so a second run would sit alongside the first
                # rather than replace it, and COPY INTO reads the whole prefix -
                # loading both, which fails the MERGE as a duplicate row.
                cursor.execute(f"REMOVE {stage}")

                for file in files:
                    cursor.execute(
                        f"PUT 'file://{file.resolve()}' "
                        f"{stage} "
                        f"AUTO_COMPRESS=FALSE"
                    )

                    for row in cursor.fetchall():
                        name, _, status, *_ = row
                        print(f"    {name} -> {status}")

    finally:
        connection.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Upload the current AMARA processed datasets."
    )

    parser.add_argument(
        "processed_root",
        nargs="?",
        type=Path,
        default=DEFAULT_PROCESSED_ROOT,
        help="Current processed output directory.",
    )

    args = parser.parse_args(argv)

    upload(args.processed_root)
