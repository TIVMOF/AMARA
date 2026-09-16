from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "gather" / "data"

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)


def env(name: str) -> str:
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")

    if not value:
        raise SystemExit(
            f"Missing environment variable: AMARA_SNOWFLAKE_{name}"
        )

    return value


def connect():
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=env("RAW_SCHEMA"),
    )


def crawl_files() -> list[tuple[str, Path]]:
    if not RAW_ROOT.is_dir():
        raise SystemExit(f"Raw directory does not exist: {RAW_ROOT}")

    files = sorted(RAW_ROOT.glob("*.json"))

    if not files:
        raise SystemExit(f"No retailer crawls found in {RAW_ROOT}")

    # One flat file per retailer, named <retailer>-<stamp>.json. The stamp
    # carries no hyphen, so the last one splits the two apart whatever the
    # retailer is called.
    return [(path.stem.rsplit("-", 1)[0], path) for path in files]


def upload() -> None:
    crawls = crawl_files()

    database = env("DATABASE")
    schema = env("RAW_SCHEMA")

    connection = connect()

    try:
        with connection.cursor() as cursor:
            for retailer, crawl in crawls:
                print(f"Uploading {retailer}: {crawl.name}")

                stage = (
                    f"@{database}.{schema}.AMARA_STAGE/{retailer}/"
                )

                cursor.execute(
                    f"PUT 'file://{crawl.resolve()}' "
                    f"{stage} "
                    f"AUTO_COMPRESS=FALSE"
                )

                for row in cursor.fetchall():
                    name, _, status, *_ = row
                    print(f"    {name} -> {status}")

    finally:
        connection.close()


if __name__ == "__main__":
    upload()