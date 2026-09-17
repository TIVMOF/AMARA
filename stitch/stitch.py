from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from scripts import paths, reference, staging, tables, validate


# What --help prints above the options.
USAGE = """\
spark-submit stitch.py              the single staged crawl
spark-submit stitch.py --crawl PATH an explicit staged crawl directory
spark-submit stitch.py --dry-run    build and report, write nothing
spark-submit stitch.py validate     is the stitched output sound?
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
    # wins, and a bare `python stitch.py` still falls back to local[*] because
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


def run(spark: SparkSession, *, crawl: Path | list[Path] | None = None,
        dry_run: bool) -> None:
    vocabularies = sync_reference(spark)

    crawl = crawl or staging.find_single_crawl(paths.STAGING_ROOT)
    if isinstance(crawl, (list, tuple)) and crawl:
        print(f"\nStaged crawl {paths.relative(crawl[0])}")
    else:
        print(f"\nStaged crawl {paths.relative(crawl)}")
    staged = staging.read(spark, crawl)

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

def main(argv: list[str] | None = None) -> int:
    # `validate` is handed straight to the validator with the rest of the command
    # line intact, so it keeps its own arguments rather than having them
    # re-declared here and kept in step by hand.
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "validate":
        validate.main(argv[1:])
        return 0

    parser = argparse.ArgumentParser(
        prog="spark-submit stitch.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="build the table and report, but write no data tables")
    parser.add_argument("--crawl", type=Path,
                        help="explicit staged crawl directory for a retry")
    args = parser.parse_args(argv)

    spark = build_session()
    try:
        run(spark, crawl=args.crawl, dry_run=args.dry_run)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
