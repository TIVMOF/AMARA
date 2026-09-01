"""Turns staged crawls into clean parquet tables.

    python -m scripts.process              every staged crawl
    python -m scripts.process --dry-run    build and report, write nothing

Two kinds of output land in `data/processed/`.

Reference parquets - dim_brand, segment, tier, category, gender, country - are
the controlled vocabularies in `reference/*.yaml`. Each run appends whatever
the YAML has gained, so editing a YAML is how a vocabulary changes.

Model tables - dim_retailer, dim_date, dim_product, fact_product_observation
and size_to_product - are the data-bearing tables of
`img/amara-analystical-data-diagram.png`. The remaining lookups and every
primary and foreign key are Snowflake's, so these columns hold natural values
in upper case rather than surrogate ids.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from . import paths, reference, staging, tables


# How many unrecognised values to name per column before summarising the rest.
REPORT_LIMIT = 8


# ── the run ─────────────────────────────────────────────────────────────────

def build_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("AMARA")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def sync_reference(spark: SparkSession) -> dict[str, reference.Reference]:
    """Bring every reference parquet in step with its YAML."""
    print("Reference data")
    loaded = {}
    for vocabulary in reference.load_all():
        added, total = reference.sync(spark, vocabulary)
        loaded[vocabulary.name] = vocabulary
        note = f"+{added} new" if added else "unchanged"
        print(f"  {vocabulary.name:12} {total:>4} values  ({note})")
    return loaded


# ── reporting ───────────────────────────────────────────────────────────────

def report_unmatched(staged: DataFrame, column: str,
                     vocabulary: reference.Reference) -> None:
    """Name the values a vocabulary missed, so the YAML can grow."""
    rows = tables.unmatched(staged, column, vocabulary).limit(REPORT_LIMIT + 1).collect()
    if not rows:
        return
    named = rows[:REPORT_LIMIT]
    preview = ", ".join(f"{r['value']} ({r['products']:,})" for r in named)
    more = " ..." if len(rows) > REPORT_LIMIT else ""
    print(f"  unmatched {column}: {preview}{more}")


def write(frame: DataFrame, name: str, *, dry_run: bool) -> None:
    rows = frame.count()
    if dry_run:
        print(f"  {name:26} {rows:>10,} rows  (not written)")
        return
    path = paths.OUTPUT_ROOT / name
    frame.write.mode("overwrite").parquet(str(path))
    print(f"  {name:26} {rows:>10,} rows  -> {paths.relative(path)}")


def run(spark: SparkSession, *, dry_run: bool) -> None:
    vocabularies = sync_reference(spark)

    print(f"\nStaged crawls from {paths.relative(paths.STAGING_ROOT)}")
    staged = staging.read(spark, paths.STAGING_ROOT)

    dim_product = tables.products(
        staged.products,
        brands=vocabularies["dim_brand"],
        categories=vocabularies["category"],
        genders=vocabularies["gender"],
    ).cache()

    print("\nWhat the vocabularies did not recognise")
    report_unmatched(staged.products, "vendor", vocabularies["dim_brand"])
    report_unmatched(staged.products, "product_type", vocabularies["category"])
    report_unmatched(staged.crawls, "country", vocabularies["country"])

    print("\nModel tables")
    write(tables.retailers(staged.crawls, vocabularies["country"]),
          "dim_retailer", dry_run=dry_run)
    write(tables.dates(staged.crawls), "dim_date", dry_run=dry_run)
    write(dim_product.drop("date"), "dim_product", dry_run=dry_run)
    write(tables.observations(dim_product, staged.variants),
          "fact_product_observation", dry_run=dry_run)
    write(tables.sizes(staged.products, staged.variants, dim_product),
          "size_to_product", dry_run=dry_run)

    print("\nDone.")


# ── cli ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="build every table and report, but write no model tables")
    args = parser.parse_args()

    spark = build_session()
    try:
        run(spark, dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
