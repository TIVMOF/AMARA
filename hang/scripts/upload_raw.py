from __future__ import annotations

from pathlib import Path

from . import paths
from .connection import connect, env


RAW_ROOT = paths.RAW_ROOT


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

    connection = connect(env("RAW_SCHEMA"))

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
