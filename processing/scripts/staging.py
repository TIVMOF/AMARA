"""Reading what dismantling staged.

Three files per crawl, under `data/staging/<site>/<timestamp>/`. Two traps,
both handled here:

  crawl.json is one indented object, so it needs `multiLine`. Without it every
  line comes back as `_corrupt_record`.

  Its schema is declared rather than inferred. `listings` and `errors` are
  nested arrays this job has no use for, and inferring them across 50 crawls
  costs a pass over every file for columns nothing reads.
"""

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
    """The three staged tables, before anything is done to them."""

    crawls: DataFrame
    products: DataFrame
    variants: DataFrame


def read(spark: SparkSession, staging_root: Path) -> Staged:
    """Every staged crawl under `staging_root`, across every site.

    `recursiveFileLookup` walks the whole tree, so a site crawled twice
    contributes both timestamps without the caller knowing the depth.
    """
    def read_files(filename: str, *, multiline: bool = False,
                   schema: str | None = None) -> DataFrame:
        reader = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", filename)
            .option("multiLine", multiline)
        )
        if schema:
            reader = reader.schema(schema)
        return reader.json(str(staging_root))

    return Staged(
        crawls=read_files("crawl.json", multiline=True, schema=CRAWL_SCHEMA),
        products=read_files("products.jsonl"),
        variants=read_files("variants.jsonl"),
    )
