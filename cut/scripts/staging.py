from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from . import paths


# Only what the tables need. `listings`, `errors` and `vendors` are nested
# arrays nothing here reads.
CRAWL_SCHEMA = """
    site STRING, base_url STRING, country STRING, currency STRING,
    brand_override STRING, scraped_at STRING, products_received BIGINT,
    products_stored BIGINT, pages BIGINT, short_pages BIGINT,
    collections_crawled BIGINT
"""


@dataclass
class Staged:
    # The three dismantled tables, before anything is done to them.

    crawls: DataFrame
    products: DataFrame
    variants: DataFrame


def require_dismantled(dismantled_root: Path) -> None:
    # Fail before the JVM starts, with something actionable.
    #
    # Only --skip-dismantle reaches this: an ordinary run dismantles the raw
    # crawl itself, so there is always something here by the time Spark looks.
    if not dismantled_root.is_dir() or not any(dismantled_root.glob("*/*")):
        raise SystemExit(
            f"nothing dismantled under {paths.relative(dismantled_root)}\n"
            f"  --skip-dismantle reuses an earlier run's intermediate, and there\n"
            f"  is none. Drop the flag to dismantle the raw crawl first."
        )


def find_single_crawl(dismantled_root: Path) -> list[Path]:
    """Return all dismantled retailer directories in the only active crawl."""
    if not dismantled_root.is_dir():
        raise FileNotFoundError(
            f"no dismantled directory at {paths.relative(dismantled_root)}")

    crawls = sorted(path for path in dismantled_root.glob("*/*") if path.is_dir())
    if not crawls:
        raise FileNotFoundError(
            f"no dismantled crawl under {paths.relative(dismantled_root)}")

    timestamps = {path.name for path in crawls}
    if len(timestamps) > 1:
        raise ValueError(
            f"expected one active dismantled crawl under "
            f"{paths.relative(dismantled_root)}, found {len(timestamps)} distinct "
            f"timestamps: {', '.join(sorted(timestamps))}. One run is one crawl on "
            f"one date - run `python cut.py cleanup --only dismantled` and start over"
        )
    return crawls


def read(spark: SparkSession, crawl_directory: Path | list[Path] | tuple[Path, ...]) -> Staged:
    # The current crawl, across every retailer directory in the active stamp.
    directories = [Path(p) for p in (crawl_directory
                   if isinstance(crawl_directory, (list, tuple)) else [crawl_directory])]
    if not directories:
        raise FileNotFoundError("no staged crawl directories provided")

    def read_files(filename: str, *, multiline: bool = False,
                   schema: str | None = None) -> DataFrame:
        paths = [directory / filename for directory in directories]
        if missing := [path for path in paths if not path.is_file()]:
            raise FileNotFoundError(f"missing staged crawl file: {missing[0]}")
        reader = spark.read.option("multiLine", multiline)
        if schema:
            reader = reader.schema(schema)
        # One read over every retailer's file rather than one read each,
        # unioned. Spark infers a schema per read, and two stores disagree:
        # featured_image comes back a string from one and a struct from
        # another, which no union can reconcile. Reading them together infers
        # a single schema over the lot.
        return reader.json([str(path) for path in paths])

    return Staged(
        crawls=read_files("crawl.json", multiline=True, schema=CRAWL_SCHEMA),
        products=read_files("products.jsonl"),
        variants=read_files("variants.jsonl"),
    )
