from __future__ import annotations

from pathlib import Path


# What cut uploads, and therefore what this stage expects to find waiting.
DATASETS = (
    "brands", "categories", "countries", "crawls", "currencies", "dates",
    "genders", "products", "retailers", "segments", "tiers", "variants",
)


def staged_datasets(cursor, database: str, schema: str) -> dict[str, int]:
    # Dataset name -> number of parquet files sitting under it in the stage.
    #
    # A staged path reads `amara_stage/products/part-0000-....parquet`, so the
    # dataset is the segment after the stage name.
    cursor.execute(f"LIST @{database}.{schema}.AMARA_STAGE")
    counts: dict[str, int] = {}
    for row in cursor.fetchall():
        parts = Path(str(row[0])).parts
        if len(parts) < 3 or not parts[-1].endswith(".parquet"):
            continue
        counts[parts[1]] = counts.get(parts[1], 0) + 1
    return counts


def require_uploaded_parquet(cursor, database: str, schema: str) -> None:
    # Stop before loading anything if cut has not put its output up.
    #
    # Without this the COPY INTO statements simply load nothing, every table
    # comes back empty, and the run reports success - which under Airflow is a
    # green task and an empty warehouse.
    stage = f"{database}.{schema}.AMARA_STAGE"
    counts = staged_datasets(cursor, database, schema)

    if not counts:
        raise SystemExit(
            f"no parquet files in @{stage}\n"
            f"  Nothing has been uploaded, so there is nothing to load. Run the\n"
            f"  cut stage first: `python cut.py` then `python cut.py upload`."
        )

    missing = [name for name in DATASETS if name not in counts]
    if missing:
        raise SystemExit(
            f"@{stage} is missing {len(missing)} of {len(DATASETS)} datasets: "
            f"{', '.join(missing)}\n"
            f"  The upload is incomplete. Re-run `python cut.py upload`, and\n"
            f"  `python cut.py validate-upload` to see what did not arrive."
        )

    total = sum(counts.values())
    print(f"Stage @{stage}: {total} parquet file(s) across "
          f"{len(counts)} dataset(s)\n")


def require_processed_rows(cursor, database: str, schema: str) -> None:
    # The analytical model is derived from the processed tables, so those have
    # to be loaded first. Empty ones mean load-processed has not run, and the
    # merges below would silently build a star schema out of nothing.
    empty = []
    for table in ("PRODUCTS", "VARIANTS"):
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
        if not cursor.fetchone()[0]:
            empty.append(table)

    if empty:
        raise SystemExit(
            f"{database}.{schema}.{' and '.join(empty)} "
            f"{'is' if len(empty) == 1 else 'are'} empty\n"
            f"  The analytical model is built from the processed tables, so\n"
            f"  those come first: `python sew.py load-processed`."
        )
