from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from scripts import (cleanup, dismantle, paths, reference, staging, tables,
                     upload_processed, validate, validate_upload)


# What --help prints above the options.
USAGE = """\
python cut.py                    dismantle the raw crawl, then build the tables
python cut.py --dry-run          build and report, write no tables
python cut.py --collected PATH   an explicit raw crawl directory or file
python cut.py --skip-dismantle   reuse what is already dismantled
python cut.py validate           is the output sound?
python cut.py upload             the parquets -> the PROCESSED stage
python cut.py validate-upload    is every parquet in the stage?
python cut.py cleanup            empty both data directories, once uploaded

One command does both halves, in order: the raw JSON has to be cut into the
shape Spark can read before Spark can read it. --skip-dismantle re-runs only
the second half, which is what the dismantled files are kept for.
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
    # wins, and a bare `python cut.py` still falls back to local[*] because
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


def run_dismantle(collected_root: Path, dismantled_root: Path) -> int:
    # The first half: raw JSON into the three flat files Spark can read.
    #
    # This runs before any Spark session exists, and it has to: `staging.read`
    # below reads what this writes. Standard library only - it is a change of
    # shape, not of meaning.
    raw_files = dismantle.find_raw_crawl(collected_root)
    print(f"Dismantling {len(raw_files)} raw crawl file(s) "
          f"from {paths.relative(collected_root)}")
    products = variants = 0
    for raw_path in raw_files:
        directory, sited_products, sited_variants = dismantle.dismantle(
            raw_path, dismantled_root)
        products += sited_products
        variants += sited_variants
        print(f"  {sited_products:>7,} products {sited_variants:>9,} variants "
              f"-> {paths.relative(directory)}")
    print(f"  {products:,} products and {variants:,} variants in total")
    return len(raw_files)


def run(spark: SparkSession, *, collected: Path | None = None,
        dismantled_root: Path | None = None, trimmed_root: Path | None = None,
        skip_dismantle: bool = False, dry_run: bool) -> None:
    collected = paths.COLLECTED_ROOT if collected is None else collected
    dismantled_root = paths.DISMANTLED_ROOT if dismantled_root is None else dismantled_root
    trimmed_root = paths.TRIMMED_ROOT if trimmed_root is None else trimmed_root

    if skip_dismantle:
        print(f"Reusing what is already dismantled "
              f"under {paths.relative(dismantled_root)}")
    else:
        run_dismantle(collected, dismantled_root)
        print()

    vocabularies = sync_reference(spark, trimmed_root)

    crawl = staging.find_single_crawl(dismantled_root)
    if isinstance(crawl, (list, tuple)) and crawl:
        print(f"\nDismantled crawl {paths.relative(crawl[0])}")
    else:
        print(f"\nDismantled crawl {paths.relative(crawl)}")
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
          "crawls", output_root=trimmed_root, dry_run=dry_run)
    write(tables.retailers(staged.crawls, vocabularies["countries"]),
          "retailers", output_root=trimmed_root, dry_run=dry_run)
    write(tables.dates(staged.crawls), "dates", output_root=trimmed_root, dry_run=dry_run)
    write(catalogue, "products", output_root=trimmed_root, dry_run=dry_run)
    write(tables.variants(staged.products, staged.variants, staged.crawls,
                          catalogue, vocabularies["currencies"]),
          "variants", output_root=trimmed_root, dry_run=dry_run)

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
        prog="python cut.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="build the table and report, but write no data tables")
    parser.add_argument("--collected", type=Path, default=paths.COLLECTED_ROOT,
                        help="the raw crawl to dismantle: a directory, or one file")
    parser.add_argument("--dismantled", type=Path, default=paths.DISMANTLED_ROOT,
                        help="directory for the dismantled intermediate")
    parser.add_argument("--trimmed", type=Path, default=paths.TRIMMED_ROOT,
                        help="directory to write the parquet tables into")
    parser.add_argument("--skip-dismantle", action="store_true",
                        help="reuse what is already dismantled, and only build the tables")
    args = parser.parse_args(argv)

    # Before the JVM starts, so a missing crawl costs nothing and says what to
    # do about it rather than surfacing as a Spark read failure two minutes in.
    if args.skip_dismantle:
        staging.require_dismantled(args.dismantled)
    else:
        dismantle.find_raw_crawl(args.collected)

    spark = build_session()
    try:
        run(spark, collected=args.collected, dismantled_root=args.dismantled,
            trimmed_root=args.trimmed, skip_dismantle=args.skip_dismantle,
            dry_run=args.dry_run)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
