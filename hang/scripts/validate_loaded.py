from __future__ import annotations

import argparse

from .connection import connect, env
from .findings import Finding, error, report, warn


USAGE = """\
python hang.py validate-loaded            the most recent crawl, both layers
python hang.py validate-loaded --date D   an earlier crawl date
"""

# Rebuilt whole on every load - TRUNCATE then COPY - so the table is exactly
# what the parquet holds.
REFERENCE = ("BRANDS", "CATEGORIES", "COUNTRIES", "CURRENCIES",
             "GENDERS", "RETAILERS", "SEGMENTS", "TIERS")

# Merged, never replaced: every crawl adds its date and earlier ones stay. Only
# the crawl under test is comparable to the parquet beside it.
HISTORICAL = ("CRAWLS", "DATES", "PRODUCTS", "VARIANTS")

# Which processed table each dimension is built from, and the business key the
# two agree on. The dimensions are cumulative like the historical tables, so the
# test is coverage - every processed value reached the dimension - not equality.
DIMENSIONS = {
    "DIM_BRAND": ("BRANDS", "NAME"), "DIM_CATEGORY": ("CATEGORIES", "NAME"),
    "DIM_COUNTRY": ("COUNTRIES", "CODE"), "DIM_CURRENCY": ("CURRENCIES", "NAME"),
    "DIM_GENDER": ("GENDERS", "NAME"), "DIM_RETAILER": ("RETAILERS", "NAME"),
    "DIM_SEGMENT": ("SEGMENTS", "NAME"), "DIM_TIER": ("TIERS", "NAME"),
    "DIM_DATE": ("DATES", "DATE"),
}


def scalar(cursor, sql: str):
    cursor.execute(sql)
    return cursor.fetchone()[0]


def check_processed(cursor, db: str, schema: str, date) -> list[Finding]:
    # Each table against the parquet it was loaded from, still in the stage.
    findings: list[Finding] = []
    fmt = f"{db}.{schema}.PARQUET_FORMAT"
    print("processed              table      parquet")

    for table in REFERENCE + HISTORICAL:
        staged = scalar(cursor, f"SELECT COUNT(*) FROM @{db}.{schema}.AMARA_STAGE/"
                                f"{table.lower()}/ (FILE_FORMAT => {fmt})")
        if table in REFERENCE:
            rows = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{schema}.{table}")
            note = ""
        else:
            rows = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{schema}.{table} "
                                  f"WHERE DATE = '{date}'")
            note = "  (this crawl)"
        print(f"  {table:16}{rows:>10,}{staged:>13,}{note}")
        if rows != staged:
            findings.append(error(table, f"{rows:,} row(s) loaded, parquet holds "
                                         f"{staged:,}"))
    return findings


def check_analytical(cursor, db: str, processed: str, analytical: str,
                     date) -> list[Finding]:
    findings: list[Finding] = []
    print("\nanalytical                  rows      source")

    for dim, (table, key) in DIMENSIONS.items():
        rows = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{analytical}.{dim}")
        source = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{processed}.{table}")
        # A value in the processed table with no row in the dimension is a
        # merge that did not happen; anything the dimension holds beyond that
        # is an earlier crawl's, which is the point of keeping it.
        missing = scalar(cursor, f"""
            SELECT COUNT(*) FROM {db}.{processed}.{table} AS p
            LEFT JOIN {db}.{analytical}.{dim} AS d ON d.{key} = p.{key}
            WHERE p.{key} IS NOT NULL AND d.{key} IS NULL""")
        print(f"  {dim:22}{rows:>8,}{source:>11,}  {table}")
        if missing:
            findings.append(error(dim, f"{missing:,} {table} {key.lower()}(s) have "
                                       f"no row in the dimension"))

    # The two big ones, for this crawl only. DIM_PRODUCT is keyed on
    # (product, retailer, date) and the fact on (variant, product, retailer,
    # date) - the same grain as the parquets they come from, so equality is the
    # test. The fact joins DIM_PRODUCT with an INNER JOIN, so a product that
    # never reached the dimension takes its variants with it silently; that is
    # what this catches.
    products = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{processed}.PRODUCTS "
                              f"WHERE DATE = '{date}'")
    variants = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{processed}.VARIANTS "
                              f"WHERE DATE = '{date}'")
    dim_products = scalar(cursor, f"""
        SELECT COUNT(*) FROM {db}.{analytical}.DIM_PRODUCT AS p
        JOIN {db}.{analytical}.DIM_DATE AS d ON d.DATE_KEY = p.DATE_KEY
        WHERE d.DATE = '{date}'""")
    facts = scalar(cursor, f"""
        SELECT COUNT(*) FROM {db}.{analytical}.FACT_PRODUCT_OBSERVATION AS f
        JOIN {db}.{analytical}.DIM_DATE AS d ON d.DATE_KEY = f.DATE_KEY
        WHERE d.DATE = '{date}'""")
    print(f"  {'DIM_PRODUCT':22}{dim_products:>8,}{products:>11,}  PRODUCTS (this crawl)")
    print(f"  {'FACT_PRODUCT_OBS':22}{facts:>8,}{variants:>11,}  VARIANTS (this crawl)")

    if dim_products != products:
        findings.append(error("DIM_PRODUCT", f"{dim_products:,} row(s) for {date}, "
                                             f"PRODUCTS holds {products:,}"))
    if facts != variants:
        findings.append(error("FACT_PRODUCT_OBSERVATION",
                              f"{facts:,} row(s) for {date}, VARIANTS holds "
                              f"{variants:,} - variants whose product is missing "
                              f"from DIM_PRODUCT are dropped by the inner join"))

    # These three are inner-joined by the loader and can never be null through
    # it; a null means the table was written by something else.
    for column in ("PRODUCT_KEY", "RETAILER_KEY", "DATE_KEY"):
        orphans = scalar(cursor, f"SELECT COUNT(*) FROM {db}.{analytical}."
                                 f"FACT_PRODUCT_OBSERVATION WHERE {column} IS NULL")
        if orphans:
            findings.append(error("FACT_PRODUCT_OBSERVATION",
                                  f"{orphans:,} row(s) with no {column}"))

    # These come from left joins, so a null is missing reference data rather
    # than a broken load - the same gap the stitch vocabularies report.
    if facts:
        print("\n  fact coverage, this crawl")
        for column in ("BRAND_KEY", "CATEGORY_KEY", "CURRENCY_KEY"):
            filled = scalar(cursor, f"""
                SELECT COUNT(*) FROM {db}.{analytical}.FACT_PRODUCT_OBSERVATION AS f
                JOIN {db}.{analytical}.DIM_DATE AS d ON d.DATE_KEY = f.DATE_KEY
                WHERE d.DATE = '{date}' AND f.{column} IS NOT NULL""")
            print(f"    {column:16}{filled / facts:>7.0%}")
            if not filled:
                findings.append(warn("FACT_PRODUCT_OBSERVATION",
                                     f"{column} is null in every row"))
    return findings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python hang.py validate-loaded", description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", help="crawl date to check; default is the latest")
    parser.add_argument("--processed-only", action="store_true")
    parser.add_argument("--analytical-only", action="store_true")
    args = parser.parse_args(argv)

    db = env("DATABASE")
    processed, analytical = env("PROCESSED_SCHEMA"), env("ANALYTICAL_SCHEMA")

    connection = connect(processed)
    try:
        with connection.cursor() as cursor:
            date = args.date or scalar(
                cursor, f"SELECT MAX(DATE) FROM {db}.{processed}.PRODUCTS")
            if date is None:
                raise SystemExit(f"error: {db}.{processed}.PRODUCTS is empty")
            print(f"Validating the crawl of {date}\n")

            findings: list[Finding] = []
            if not args.analytical_only:
                findings += check_processed(cursor, db, processed, date)
            if not args.processed_only:
                findings += check_analytical(cursor, db, processed, analytical, date)
    finally:
        connection.close()

    report(findings, f"the crawl of {date} is loaded")
