from __future__ import annotations

import argparse
from pathlib import Path

from . import paths
from .connection import connect, env
from .findings import Finding, error, report
from .stage import by_dataset, encrypted_size


USAGE = """\
python hang.py validate-processed          every parquet against the stage
python hang.py validate-processed products named tables only
"""


def local_tables(root: Path, wanted: set[str]) -> list[tuple[str, list[Path]]]:
    # One directory per table, each holding Spark's part files.
    found = []
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        if wanted and directory.name not in wanted:
            continue
        found.append((directory.name, sorted(directory.glob("part-*.parquet"))))
    return found


def check(tables: list[tuple[str, list[Path]]], held: dict[str, dict[str, int]],
          wanted: set[str]) -> list[Finding]:
    findings: list[Finding] = []

    for table, files in tables:
        staged = held.get(table, {})
        if not files:
            findings.append(error(table, "no part files on disk"))
            continue
        if not staged:
            findings.append(error(table, f"{len(files)} part file(s) on disk, "
                                         f"nothing staged"))
            continue

        for file in files:
            size = staged.get(file.name)
            if size is None:
                findings.append(error(table, f"{file.name} is not in the stage"))
            elif size != (expected := encrypted_size(local := file.stat().st_size)):
                findings.append(error(table, f"{file.name} is {size:,} bytes in "
                                             f"the stage, expected {expected:,} "
                                             f"for {local:,} on disk"))

        # COPY INTO reads the whole dataset directory, so a part file left from
        # an earlier run is loaded as data. upload_processed clears the stage
        # first; anything extra here means that did not happen.
        if extra := sorted(set(staged) - {f.name for f in files}):
            findings.append(error(table, f"{len(extra)} stale part file(s) still "
                                         f"staged and would be loaded: "
                                         f"{extra[0]}"))
        total = sum(staged.get(f.name, 0) for f in files)
        print(f"    ok  {table:16}{len(files):>4} part(s)  {total:>12,} bytes")

    # A dataset in the stage that no longer exists on disk is loaded by nothing,
    # but it is also a table this run did not write - worth saying out loud.
    if not wanted:
        if orphan := sorted(set(held) - {t for t, _ in tables}):
            findings.append(error("stage", f"{len(orphan)} staged dataset(s) with "
                                           f"no local table: {', '.join(orphan)}"))
    print(f"\n{len(tables)} table(s), "
          f"{sum(len(f) for _, f in tables)} part file(s)")
    return findings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python hang.py validate-processed", description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tables", nargs="*", help="default is every table")
    parser.add_argument("--processed", type=Path, default=paths.PROCESSED_ROOT)
    args = parser.parse_args(argv)

    if not args.processed.is_dir():
        raise SystemExit(f"error: no processed directory at {args.processed}")
    wanted = set(args.tables)
    tables = local_tables(args.processed, wanted)
    if not tables:
        raise SystemExit(f"error: no tables in {args.processed}"
                         + (f" for: {', '.join(sorted(wanted))}" if wanted else ""))
    if missing := wanted - {name for name, _ in tables}:
        raise SystemExit(f"error: no local table for: {', '.join(sorted(missing))}")

    database, schema = env("DATABASE"), env("PROCESSED_SCHEMA")
    stage = f"@{database}.{schema}.AMARA_STAGE"
    print(f"Validating {len(tables)} table(s) against {stage}\n")

    connection = connect(schema)
    try:
        with connection.cursor() as cursor:
            findings = check(tables, by_dataset(cursor, stage), wanted)
    finally:
        connection.close()

    report(findings, f"{len(tables)} table(s) uploaded")
