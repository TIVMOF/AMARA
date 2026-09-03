from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterator, NamedTuple

from scripts import paths
from scripts.raw import CRAWL_FIELDS

USAGE = """\
python validate_dismantling.py         every staged crawl
python validate_dismantling.py kith    named sites only
"""

STAGED_FILES = ("crawl.json", "products.jsonl", "variants.jsonl")
ERROR, WARN = "ERROR", "WARN"

class Finding(NamedTuple):
    level: str
    site: str
    message: str

def stamp(scraped_at) -> str:
    return (scraped_at or "").replace(":", "").replace("-", "")

def read_lines(path: Path) -> Iterator[dict]:
    # One record per line. Staging is 2 GB; nothing needs two in hand at once.
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)

def inspect(at: str, directory: Path) -> tuple[list[Finding], int, int]:
    # One staged crawl, both .jsonl files streamed once.
    meta = json.loads((directory / "crawl.json").read_text(encoding="utf-8"))
    site, when = directory.parent.name, directory.name
    ids: set[str] = set()
    vendors: Counter = Counter()
    products = duplicated = kept_variants = unidentified = misplaced = 0

    for product in read_lines(directory / "products.jsonl"):
        products += 1
        kept_variants += "variants" in product
        # Every row carries site and scraped_at so a table built from several
        # crawls knows which crawl each row came from.
        misplaced += (product.get("site") != site
                      or stamp(product.get("scraped_at")) != when)
        if (identifier := product.get("id")) is None:
            unidentified += 1
            continue
        duplicated += str(identifier) in ids
        ids.add(str(identifier))
        if product.get("vendor"):
            vendors[product["vendor"]] += 1

    variants = nameless = orphaned = reused = 0
    owners: dict[str, str] = {}
    parents: set[str] = set()
    for variant in read_lines(directory / "variants.jsonl"):
        variants += 1
        parent = variant.get("product_id")
        if parent is None:
            nameless += 1
        elif (parent := str(parent)) not in ids:
            orphaned += 1
        else:
            parents.add(parent)
            reused += owners.setdefault(str(variant.get("id")), parent) != parent

    listed = meta.get("vendors") if isinstance(meta.get("vendors"), list) else None
    tally = {r.get("vendor"): r.get("products") for r in listed or []}
    findings = [Finding(level, at, message) for level, count, message in (
        # The stage did its job: shape changed, nothing lost.
        (ERROR, misplaced, f"{misplaced:,} product(s) not from {site}/{when}"),
        (ERROR, kept_variants, f"{kept_variants:,} product(s) keep a variants key"),
        (ERROR, unidentified, f"{unidentified:,} product(s) with no id"),
        (ERROR, duplicated, f"{duplicated:,} duplicate product id(s)"),
        (ERROR, nameless, f"{nameless:,} variant(s) name no product_id"),
        (ERROR, orphaned, f"{orphaned:,} variant(s) name a product not in products.jsonl"),
        # crawl.json against the directory and the lines beside it.
        (ERROR, [k for k in CRAWL_FIELDS if k not in meta],
         f"crawl.json missing: {', '.join(k for k in CRAWL_FIELDS if k not in meta)}"),
        (ERROR, meta.get("site") != site, f"crawl.json site {meta.get('site')!r} != dir"),
        (ERROR, stamp(meta.get("scraped_at")) != when, f"crawl.json scraped_at != {when}"),
        # Bodies belong in the .jsonl files; either key back here and Spark
        # infers a column per product again.
        (ERROR, "products" in meta, "crawl.json still carries 'products'"),
        (ERROR, "responses" in meta, "crawl.json still carries 'responses'"),
        (ERROR, meta.get("products_stored") != products,
         f"crawl.json says {meta.get('products_stored')} products, jsonl holds {products}"),
        # vendors arrives keyed by name and is reshaped into records: two
        # spellings differing only in case collide under Spark's column names.
        (ERROR, listed is None, "crawl.json vendors is not a list of records"),
        (ERROR, listed is not None and tally != dict(vendors),
         "crawl.json vendors does not match products.jsonl"),
        # Not dismantling's fault - what the store served, carried faithfully.
        (WARN, not products, "no products"),
        (WARN, products - len(parents),
         f"{products - len(parents):,} product(s) with no variant, so no price or size"),
        (WARN, reused, f"{reused:,} variant id(s) reused across products; unique per "
                       f"product, not per store"),
    ) if count]
    return findings, products, variants

def check_crawl(directory: Path) -> tuple[list[Finding], int, int]:
    at = f"{directory.parent.name}/{directory.name}"
    for name in STAGED_FILES:
        path = directory / name
        if not path.is_file() or not path.stat().st_size:
            state = "missing" if not path.is_file() else "empty"
            return [Finding(ERROR, at, f"{name} is {state}")], 0, 0
    try:
        return inspect(at, directory)
    except json.JSONDecodeError as error:
        return [Finding(ERROR, at, f"invalid JSON: {error}")], 0, 0

def validate(only: list[str] | None, staging: Path, raw: Path) -> list[Finding]:
    if not staging.is_dir():
        raise SystemExit(f"error: no staging directory at {staging}")
    wanted = set(only or [])
    sites = sorted(p for p in staging.iterdir()
                   if p.is_dir() and (not wanted or p.name in wanted))
    if missing := wanted - {p.name for p in sites}:
        raise SystemExit(f"error: nothing staged for: {', '.join(sorted(missing))}")
    if not sites:
        raise SystemExit(f"error: no staged sites in {staging}")

    findings: list[Finding] = []
    crawls = products = variants = 0
    print(f"Validating {len(sites)} site(s) under {paths.relative(staging)}\n")

    for site in sites:
        directories = sorted(p for p in site.iterdir() if p.is_dir())
        if not directories:
            findings.append(Finding(ERROR, site.name, "no staged crawl directories"))
            continue
        found, seen_products, seen_variants = [], 0, 0
        for directory in directories:
            crawl_findings, p, v = check_crawl(directory)
            found += crawl_findings
            seen_products += p
            seen_variants += v
        crawls += len(directories)
        products += seen_products
        variants += seen_variants
        findings += found
        errors = sum(1 for f in found if f.level == ERROR)
        note = (f"{errors} error(s) " if errors else "") + (
            f"{len(found) - errors} warning(s)" if len(found) - errors else "")
        print(f"  {'FAIL' if errors else 'warn' if found else 'ok':>4}  {site.name:22}"
              f"{seen_products:>8,} products {seen_variants:>9,} variants  {note}")

    # A crawl never dismantled is not a small table to Spark, it is an absent one.
    if not wanted and not raw.is_dir():
        findings.append(Finding(WARN, "coverage", f"no raw directory at {raw}"))
    elif not wanted:
        done = {(p.parent.name, p.name) for p in staging.glob("*/*") if p.is_dir()}
        crawled = {(p.parent.name, p.stem) for p in raw.glob("*/*.json")}
        findings += [Finding(WARN, "coverage", f"{len(names)} {label}: "
                             + ", ".join(f"{s}/{w}" for s, w in sorted(names)[:6])
                             + (" ..." if len(names) > 6 else ""))
                     for label, names in (("never dismantled", crawled - done),
                                          ("staged with no raw file", done - crawled))
                     if names]
    print(f"\n{crawls} staged crawl(s), {products:,} products, {variants:,} variants")
    return findings

def main() -> None:
    parser = argparse.ArgumentParser(prog="python validate_dismantling.py",
        description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sites", nargs="*", help="default is every staged site")
    parser.add_argument("--staging", type=Path, default=paths.STAGING_ROOT)
    parser.add_argument("--raw", type=Path, default=paths.RAW_ROOT)
    args = parser.parse_args()
    findings = validate(args.sites or None, args.staging, args.raw)
    for level in (ERROR, WARN):
        if hits := [f for f in findings if f.level == level]:
            print(f"\n{level}S ({len(hits)})")
            print("\n".join(f"  {f.site}: {f.message}" for f in hits))
    errors = sum(1 for f in findings if f.level == ERROR)
    if errors:
        raise SystemExit(f"\nFAILED: {errors} error(s), "
                         f"{len(findings) - errors} warning(s)")
    print("\nOK: no errors" + (f", {len(findings)} warning(s)" if findings else ""))

if __name__ == "__main__":
    main()
