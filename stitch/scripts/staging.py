from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


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
    # The three staged tables, before anything is done to them.

    crawls: DataFrame
    products: DataFrame
    variants: DataFrame


def find_single_crawl(staging_root: Path) -> list[Path]:
    """Return all staged retailer directories in the only active crawl."""
    if not staging_root.is_dir():
        raise FileNotFoundError(f"no staging directory at {staging_root}")

    crawls = sorted(path for path in staging_root.glob("*/*") if path.is_dir())
    if not crawls:
        raise FileNotFoundError(f"no staged crawl under {staging_root}")

    timestamps = {path.name for path in crawls}
    if len(timestamps) > 1:
        raise ValueError(
            f"expected one active staged crawl under {staging_root}, "
            f"found {len(timestamps)} distinct timestamps; pass its directory explicitly"
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
