from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from scripts import (cleanup, paths, reference, staging, tables, upload_processed,
                     validate, validate_upload)


# What --help prints above the options.
USAGE = """\
spark-submit stitch.py              the single staged crawl
spark-submit stitch.py --crawl PATH an explicit staged crawl directory
spark-submit stitch.py --output DIR where to write the tables
spark-submit stitch.py --dry-run    build and report, write nothing
spark-submit stitch.py validate     is the stitched output sound?
spark-submit stitch.py upload       the parquets -> the PROCESSED stage
spark-submit stitch.py validate-upload   is every parquet in the stage?
spark-submit stitch.py cleanup      empty data/, once it is uploaded
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


def sync_reference(spark: SparkSession,
                   output_root: Path) -> dict[str, reference.Reference]:
    # Bring every reference parquet in step with its YAML.
    print("Reference data")
    loaded = {}
    for vocabulary in reference.load_all():
        added, total = reference.sync(spark, vocabulary, output_root)
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


def write(frame: DataFrame, name: str, *, output_root: Path, dry_run: bool) -> None:
    rows = frame.count()
    if dry_run:
        print(f"  {name:12} {rows:>11,} rows  (not written)")
        return
    path = output_root / name
    frame.write.mode("overwrite").parquet(str(path))
    print(f"  {name:12} {rows:>11,} rows  -> {paths.relative(path)}")


def run(spark: SparkSession, *, crawl: Path | list[Path] | None = None,
        staging_root: Path | None = None, output_root: Path | None = None,
        dry_run: bool) -> None:
    staging_root = paths.STAGING_ROOT if staging_root is None else staging_root
    output_root = paths.OUTPUT_ROOT if output_root is None else output_root

    vocabularies = sync_reference(spark, output_root)

    crawl = crawl or staging.find_single_crawl(staging_root)
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
          "crawls", output_root=output_root, dry_run=dry_run)
    write(tables.retailers(staged.crawls, vocabularies["countries"]),
          "retailers", output_root=output_root, dry_run=dry_run)
    write(tables.dates(staged.crawls), "dates", output_root=output_root, dry_run=dry_run)
    write(catalogue, "products", output_root=output_root, dry_run=dry_run)
    write(tables.variants(staged.products, staged.variants, staged.crawls,
                          catalogue, vocabularies["currencies"]),
          "variants", output_root=output_root, dry_run=dry_run)

    print("\nDone.")


# ── cli ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    # `validate` is handed straight to the validator with the rest of the command
    # line intact, so it keeps its own arguments rather than having them
    # re-declared here and kept in step by hand.
    argv = sys.argv[1:] if argv is None else argv
    for name, command in (("validate", validate.main), ("upload", upload_processed.main),
                          ("validate-upload", validate_upload.main),
                          ("cleanup", cleanup.main)):
        if argv and argv[0] == name:
            # `or 0` because these signal failure by raising SystemExit
            # rather than returning. Dropping the return value would turn a
            # validator that ever starts returning a code into a silent
            # success, which under Airflow is a green task on bad data.
            return command(argv[1:]) or 0

    parser = argparse.ArgumentParser(
        prog="spark-submit stitch.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="build the table and report, but write no data tables")
    parser.add_argument("--crawl", type=Path,
                        help="explicit staged crawl directory for a retry")
    parser.add_argument("--staging", type=Path, default=paths.STAGING_ROOT,
                        help="directory holding the sheared crawl")
    parser.add_argument("--output", type=Path, default=paths.OUTPUT_ROOT,
                        help="directory to write the parquet tables into")
    args = parser.parse_args(argv)

    spark = build_session()
    try:
        run(spark, crawl=args.crawl, staging_root=args.staging,
            output_root=args.output, dry_run=args.dry_run)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
