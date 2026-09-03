from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from scripts import paths, reference, staging, tables


# What --help prints above the options.
USAGE = """\
spark-submit process.py              every staged crawl
spark-submit process.py --dry-run    build and report, write nothing
"""


# How many unrecognised values to name per column before summarising the rest.
REPORT_LIMIT = 8


# ── the run ─────────────────────────────────────────────────────────────────

def build_session() -> SparkSession:
    # The session, configured by whoever submitted the job.
    #
    # Deliberately sets no master. A builder option overrides what spark-submit
    # was told, so a hardcoded `.master("local[*]")` turns every cluster
    # submission into one local JVM without saying so. Left alone, `--master`
    # wins, and a bare `python process.py` still falls back to local[*] because
    # that is already PySpark's own default.
    spark = SparkSession.builder.appName("AMARA").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def sync_reference(spark: SparkSession) -> dict[str, reference.Reference]:
    # Bring every reference parquet in step with its YAML.
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
    # Name the values a vocabulary missed, so the YAML can grow.
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
        print(f"  {name:12} {rows:>11,} rows  (not written)")
        return
    path = paths.OUTPUT_ROOT / name
    frame.write.mode("overwrite").parquet(str(path))
    print(f"  {name:12} {rows:>11,} rows  -> {paths.relative(path)}")


def run(spark: SparkSession, *, dry_run: bool) -> None:
    vocabularies = sync_reference(spark)

    print(f"\nStaged crawls from {paths.relative(paths.STAGING_ROOT)}")
    staged = staging.read(spark, paths.STAGING_ROOT)

    catalogue = tables.products(
        staged.products,
        brands=vocabularies["brands"],
        categories=vocabularies["categories"],
        genders=vocabularies["genders"],
    ).cache()

    print("\nWhat the vocabularies did not recognise")
    report_unmatched(staged.products, "vendor", vocabularies["brands"])
    report_unmatched(staged.products, "product_type", vocabularies["categories"])
    report_unmatched(staged.crawls, "country", vocabularies["countries"])
    report_unmatched(staged.crawls, "currency", vocabularies["currencies"])

    print("\nData tables")
    write(tables.crawls(staged.crawls, vocabularies["currencies"]),
          "crawls", dry_run=dry_run)
    write(tables.retailers(staged.crawls, vocabularies["countries"]),
          "retailers", dry_run=dry_run)
    write(tables.dates(staged.crawls), "dates", dry_run=dry_run)
    write(catalogue, "products", dry_run=dry_run)
    write(tables.variants(staged.products, staged.variants, staged.crawls,
                          catalogue, vocabularies["currencies"]),
          "variants", dry_run=dry_run)

    print("\nDone.")


# ── cli ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="spark-submit process.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="build every table and report, but write no data tables")
    args = parser.parse_args()

    spark = build_session()
    try:
        run(spark, dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
