from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Each stage owns its own data directory. These are the two this stage reads.
RAW_ROOT = PROJECT_ROOT / "ingestion" / "data" / "raw"
PROCESSED_ROOT = PROJECT_ROOT / "processing" / "data" / "processed"

# The internal stage files are PUT into. Snowflake's COPY INTO then loads them
# from there into a table.
STAGE = os.getenv("AMARA_SNOWFLAKE_STAGE", "AMARA_STAGE")

ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

REQUIRED = ("ACCOUNT", "USER", "TOKEN", "WAREHOUSE", "DATABASE", "SCHEMA")


def env(name: str) -> str:
    # Read a required AMARA_SNOWFLAKE_* setting.
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")
    if not value:
        raise SystemExit(
            f"AMARA_SNOWFLAKE_{name} is not set.\n"
            f"  Expected it in {ENV_PATH}\n"
            f"  Fix: cp {PROJECT_ROOT}/.env.example {ENV_PATH}"
        )
    return value


def connect() -> snowflake.connector.SnowflakeConnection:
    # Open a connection using the programmatic access token in .env.
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=env("SCHEMA"),
    )


def put(cursor, file: Path, prefix: str) -> None:
    # Upload one file to `@STAGE/<prefix>/` and report what Snowflake said.
    #
    # AUTO_COMPRESS is off: the parquets are already compressed, and gzipping a
    # raw JSON here would only have to be undone by COPY INTO.
    #
    # The path is quoted rather than parameterised because PUT is a client-side
    # command - the connector rewrites it locally and does not accept binds.
    cursor.execute(f"PUT 'file://{file}' @{STAGE}/{prefix}/ AUTO_COMPRESS=FALSE")
    for name, _, status, *_ in (row for row in cursor.fetchall()):
        print(f"    {name} -> {status}")


def datasets(root: Path, pattern: str) -> Iterator[tuple[str, list[Path]]]:
    # Each subdirectory of `root` and the files in it matching `pattern`.
    #
    # Empty directories are yielded too, so a caller can say a dataset was
    # skipped rather than silently uploading nothing.
    if not root.is_dir():
        raise SystemExit(f"nothing to upload: {root} does not exist")
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        yield directory.name, sorted(directory.glob(pattern))
