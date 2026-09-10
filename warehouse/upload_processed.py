from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterator

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_ROOT = PROJECT_ROOT / "processing" / "data" / "processed"

ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)


def env(name: str) -> str:
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")

    if not value:
        raise SystemExit(
            f"AMARA_SNOWFLAKE_{name} is not set.\n"
            f"Expected it in {ENV_PATH}"
        )

    return value


def connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=env("PROCESSED_SCHEMA"),
    )


def datasets(root: Path) -> Iterator[tuple[str, list[Path]]]:
    if not root.is_dir():
        raise SystemExit(
            f"nothing to upload: {root} does not exist"
        )

    for directory in sorted(
        path for path in root.iterdir() if path.is_dir()
    ):
        yield directory.name, sorted(
            directory.glob("part-*.parquet")
        )


def put(cursor: snowflake.connector.cursor.SnowflakeCursor, file: Path, dataset: str) -> None:
    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")
    stage = "AMARA_STAGE"

    cursor.execute(
        f"PUT 'file://{file}' "
        f"@{database}.{schema}.{stage}/{dataset}/ "
        f"AUTO_COMPRESS=FALSE"
    )

    for row in cursor.fetchall():
        name, _, status, *_ = row
        print(f"    {name} -> {status}")


def upload(only: list[str] | None = None) -> int:
    wanted = set(only or [])

    found = [
        (name, files)
        for name, files in datasets(PROCESSED_ROOT)
        if not wanted or name in wanted
    ]

    found_names = {name for name, _ in found}
    missing = wanted - found_names

    if missing:
        raise SystemExit(
            f"no such dataset(s): {', '.join(sorted(missing))}"
        )

    if not found:
        raise SystemExit(
            f"no parquet datasets under {PROCESSED_ROOT}"
        )

    connection = connect()

    try:
        cursor = connection.cursor()

        for dataset, files in found:
            if not files:
                print(f"  {dataset}: no parquet files, skipped")
                continue

            print(
                f"  {dataset}: {len(files)} file(s) -> "
                f"@{env('DATABASE')}.{env('PROCESSED_SCHEMA')}."
                f"AMARA_STAGE/{dataset}/"
            )

            for file in files:
                put(cursor, file, dataset)

    finally:
        connection.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python upload_processed.py",
        description=(
            "Upload processed Parquet datasets to the "
            "Snowflake PROCESSED stage."
        ),
    )

    parser.add_argument(
        "tables",
        nargs="*",
        help="dataset names; default is every dataset",
    )

    args = parser.parse_args()

    raise SystemExit(upload(args.tables or None))


if __name__ == "__main__":
    main()