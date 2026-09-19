from __future__ import annotations

import argparse
from pathlib import Path

from .connection import connect, env
from .findings import Finding, error, report, warn
from .stage import by_name, encrypted_size
from .store import DATA_DIR as RAW_ROOT


USAGE = """\
python get.py validate-upload       every crawl file against the raw stage
python get.py validate-upload kith  named retailers only
"""


def local_crawls(root: Path, wanted: set[str]) -> list[tuple[str, Path]]:
    # get writes <retailer>-<stamp>.json, flat. The stamp carries no hyphen,
    # so the last one splits the two whatever the retailer is called.
    files = sorted(root.glob("*.json"))
    crawls = [(path.stem.rsplit("-", 1)[0], path) for path in files]
    return [(name, path) for name, path in crawls if not wanted or name in wanted]


def check(crawls: list[tuple[str, Path]], held: dict[str, tuple[str, int]],
          wanted: set[str]) -> list[Finding]:
    findings: list[Finding] = []
    uploaded = 0

    for retailer, path in crawls:
        entry = held.get(path.name)
        if entry is None:
            findings.append(error(retailer, f"{path.name} is not in the stage"))
            continue
        staged_path, size = entry
        uploaded += 1
        # The stage reports the encrypted size, not the file's - see stage.py.
        # A difference beyond that padding is a partial or superseded upload.
        if size != (expected := encrypted_size(local := path.stat().st_size)):
            findings.append(error(retailer, f"{path.name} is {size:,} bytes in the "
                                            f"stage, expected {expected:,} for "
                                            f"{local:,} on disk"))
        # The stage lays files out by retailer; a file under the wrong prefix
        # still lists, but nothing downstream would find it where it looks.
        elif f"/{retailer}/" not in staged_path:
            findings.append(error(retailer, f"{path.name} staged at {staged_path}, "
                                            f"not under {retailer}/"))
        print(f"    ok  {retailer:22}{size:>14,} bytes")

    # Anything in the stage that this crawl did not put there. upload_raw does
    # not clear the stage first, so an earlier crawl's files sit alongside
    # today's until cleaned out - storage, not correctness, but it grows.
    if not wanted:
        mine = {path.name for _, path in crawls}
        if stale := sorted(set(held) - mine):
            findings.append(warn("stage", f"{len(stale)} file(s) from an earlier "
                                          f"crawl: {', '.join(stale[:4])}"
                                          + (" ..." if len(stale) > 4 else "")))
    print(f"\n{uploaded} of {len(crawls)} crawl file(s) in the stage")
    return findings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python get.py validate-upload", description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("retailers", nargs="*", help="default is every crawl file")
    parser.add_argument("--raw", type=Path, default=RAW_ROOT)
    args = parser.parse_args(argv)

    if not args.raw.is_dir():
        raise SystemExit(f"error: no crawl directory at {args.raw}")
    wanted = set(args.retailers)
    crawls = local_crawls(args.raw, wanted)
    if not crawls:
        raise SystemExit(f"error: no crawl files in {args.raw}"
                         + (f" for: {', '.join(sorted(wanted))}" if wanted else ""))
    if missing := wanted - {name for name, _ in crawls}:
        raise SystemExit(f"error: no crawl file for: {', '.join(sorted(missing))}")

    database, schema = env("DATABASE"), env("RAW_SCHEMA")
    stage = f"@{database}.{schema}.AMARA_STAGE"
    print(f"Validating {len(crawls)} crawl file(s) against {stage}\n")

    connection = connect(schema)
    try:
        with connection.cursor() as cursor:
            findings = check(crawls, by_name(cursor, stage), wanted)
    finally:
        connection.close()

    report(findings, f"{len(crawls)} crawl file(s) uploaded")
