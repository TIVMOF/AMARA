from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, NamedTuple

from . import registry
from .adapters.shopify import PAGE_SIZE, UNFILTERED_LABEL

USAGE = ("python gather.py validate              every retailer file in data/\n"
         "python gather.py validate kith agjeans   named retailers only")

# parents[1] is the component root: this module lives in gather/scripts/.
RAW_ROOT = Path(__file__).resolve().parents[1] / "data"

# gather writes <retailer>-<stamp>.json. The stamp carries no hyphen, so the
# last one splits the two whatever the retailer is called.
def retailer_of(path: Path) -> str:
    return path.stem.rsplit("-", 1)[0]

def stamp_of(path: Path) -> str:
    return path.stem.rsplit("-", 1)[-1]
# Keys store.build() always writes. Without them every check below is guesswork.
REQUIRED_KEYS = ("site", "adapter", "base_url", "scraped_at", "products_received",
                 "products_stored", "pages", "short_pages", "collections_crawled",
                 "complete", "listings", "errors", "responses", "products", "vendors")
COLLECTIONS = (("products", dict), ("responses", list), ("listings", list),
               ("errors", list), ("vendors", dict))
ERROR, WARN = "ERROR", "WARN"
class Finding(NamedTuple):
    level: str
    site: str
    message: str
def check_structure(at: str, data: Any) -> list[Finding]:
    if not isinstance(data, dict) or not data:
        return [Finding(ERROR, at, f"root is not an object ({type(data).__name__})")]
    found = [Finding(ERROR, at, f"{k} is {type(v).__name__}, want {t.__name__}")
             for k, t in COLLECTIONS
             if (v := data.get(k)) is not None and not isinstance(v, t)]
    if gone := [k for k in REQUIRED_KEYS if k not in data]:
        found.append(Finding(ERROR, at, f"missing key(s): {', '.join(gone)}"))
    return found
def check_self(at: str, path: Path, data: dict) -> Iterator[Finding]:
    # Every summary number recomputed, and the file against where it sits.
    products, responses = data.get("products") or {}, data.get("responses") or []
    listings = data.get("listings") or []

    for key, actual in (
        ("products_stored", len(products)),
        ("products_received", sum(r.get("count", 0) for r in responses)),
        ("pages", len(responses)),
        ("short_pages", sum(len(l.get("short_pages") or []) for l in listings)),
        ("collections_crawled",
         sum(1 for l in listings if l.get("label") != UNFILTERED_LABEL))):
        if data.get(key) != actual:
            yield Finding(ERROR, at, f"{key} says {data.get(key)}, holds {actual}")

    # Bodies against the page trace, both ways: a traced id with no body is a
    # lost product, a body nothing traced is a product with no provenance.
    traced = {pid for r in responses for pid in (r.get("product_ids") or [])}
    if orphans := traced - set(products):
        yield Finding(ERROR, at, f"{len(orphans):,} id(s) on a page with no body")
    if loose := set(products) - traced:
        yield Finding(ERROR, at, f"{len(loose):,} body/ies no page claims")
    if wrong := [k for k, b in products.items() if str((b or {}).get("id")) != k]:
        yield Finding(ERROR, at, f"{len(wrong):,} body/ies keyed by a foreign id")
    tally = Counter(b["vendor"] for b in products.values()
                    if isinstance(b, dict) and b.get("vendor"))
    if dict(tally) != (data.get("vendors") or {}):
        yield Finding(ERROR, at, "vendors tally does not match the bodies")

    ended = all(l.get("stopped_reason") == "empty_page" for l in listings)
    if data.get("complete") is not ended:
        yield Finding(ERROR, at, f"complete is {data.get('complete')} but "
                                 f"{'every' if ended else 'not every'} listing ended "
                                 f"on an empty page")
    if data.get("site") != retailer_of(path):
        yield Finding(ERROR, at, f"site {data.get('site')!r} in a file named for "
                                 f"{retailer_of(path)}")
    try:
        datetime.strptime(data.get("scraped_at") or "", "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        yield Finding(ERROR, at, f"scraped_at not UTC: {data.get('scraped_at')!r}")
        return
    if data["scraped_at"].replace(":", "").replace("-", "") != stamp_of(path):
        yield Finding(ERROR, at, f"filename stamp != scraped_at")

def check_quality(at: str, data: dict) -> Iterator[Finding]:
    # Not wrong, thin. A table built on this is thin in the same places.
    listings, pages = data.get("listings") or [], data.get("pages") or 0
    errors = data.get("errors") or []

    if not data.get("products"):
        yield Finding(WARN, at, "no products at all")
    if cut := [l for l in listings if l.get("stopped_reason") != "empty_page"]:
        why = Counter(l.get("stopped_reason") for l in cut)
        yield Finding(WARN, at, f"{len(cut)}/{len(listings)} listing(s) cut off "
                                f"({', '.join(f'{n} on {r}' for r, n in why.items())})")
    if errors:
        yield Finding(WARN, at, f"{len(errors)} failed request(s), first on page "
                                f"{errors[0].get('page')}: {errors[0].get('error')[:160]}")
    if throttled := data.get("throttled") or 0:
        yield Finding(WARN, at, f"throttled {throttled}x, rate now "
                                f"{data.get('rate_limit_final')}/s")

    # A listing ends on one partial page, so that many short pages mean nothing.
    # Beyond it the store served less than it was asked for mid-listing.
    short = data.get("short_pages") or 0
    closing = sum(1 for l in listings if l.get("stopped_reason") == "empty_page")
    if short > closing and pages:
        yield Finding(WARN, at, f"{short} of {pages} page(s) under {PAGE_SIZE} items, "
                                f"{short - closing} beyond the {closing} closing page(s) "
                                f"- thinner than the store")
    if absent := [f for f in ("currency", "country") if not data.get(f)]:
        yield Finding(WARN, at, f"no {' or '.join(absent)}; /meta.json did not answer")

def check_file(path: Path) -> tuple[list[Finding], int]:
    # One crawl. Findings and product count together, so the file is read once -
    # a crawl can be 900 MB and reading it twice for a number is not free.
    at = path.name
    if not path.stat().st_size:
        return [Finding(ERROR, at, "file is empty")], 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        return [Finding(ERROR, at, f"invalid JSON: {error}")], 0
    if structural := check_structure(at, data):
        return structural, 0
    return ([*check_self(at, path, data), *check_quality(at, data)],
            len(data.get("products") or {}))

def validate(only: list[str] | None = None, root: Path = RAW_ROOT) -> list[Finding]:
    if not root.is_dir():
        raise SystemExit(f"error: no raw directory at {root}")
    wanted = set(only or [])
    files = sorted(p for p in root.glob("*.json")
                   if not wanted or retailer_of(p) in wanted)
    if missing := wanted - {retailer_of(p) for p in files}:
        raise SystemExit(f"error: no crawls for: {', '.join(sorted(missing))}")
    if not files:
        raise SystemExit(f"error: no retailer crawls in {root}")

    findings: list[Finding] = []
    crawls = products = 0
    print(f"Validating {len(files)} retailer file(s) in {root}\n")

    for file in files:
        found, stored = check_file(file)
        crawls, products = crawls + 1, products + stored
        findings += found
        errors = sum(1 for f in found if f.level == ERROR)
        note = (f"{errors} error(s) " if errors else "") + (
            f"{len(found) - errors} warning(s)" if len(found) - errors else "")
        print(f"  {'FAIL' if errors else 'warn' if found else 'ok':>4}  "
              f"{retailer_of(file):22} {stored:>7,} products  {note}")

    # One run is one crawl on one date: products and variants are told apart
    # from an earlier crawl's by date alone, so a second crawl left in data/raw
    # makes the same product look like two. cleanup.py clears it between runs.
    if not wanted and len(stamps := {stamp_of(p) for p in root.glob("*.json")}) > 1:
        dates = sorted({stamp[:8] for stamp in stamps})
        findings.append(Finding(ERROR, "crawl", f"{len(stamps)} crawl stamp(s) across "
                                f"{len(dates)} date(s) ({', '.join(dates)}); the "
                                f"workflow handles one crawl on one date - run "
                                f"cleanup.py before crawling again"))

    # A retailer never crawled is invisible downstream: not a small number, none.
    if not wanted:
        crawled = {retailer_of(p) for p in root.glob("*.json")}
        want = {site.name for site in registry.load_sites()}
        findings += [Finding(WARN, "coverage", f"{len(n)} {label}: {', '.join(sorted(n))}")
                     for label, n in (("enabled site(s) never crawled", want - crawled),
                                      ("crawl dir(s) with no enabled site", crawled - want))
                     if n]

    print(f"\n{crawls} crawl(s), {products:,} products stored")
    return findings

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python gather.py validate",
        description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("retailers", nargs="*", help="default is every retailer")
    parser.add_argument("--raw", type=Path, default=RAW_ROOT, help="the raw crawls")
    args = parser.parse_args(argv)
    findings = validate(args.retailers or None, args.raw)
    for level in (ERROR, WARN):
        if hits := [f for f in findings if f.level == level]:
            print(f"\n{level}S ({len(hits)})")
            print("\n".join(f"  {f.site}: {f.message}" for f in hits))
    errors = sum(1 for f in findings if f.level == ERROR)
    warnings = len(findings) - errors
    if errors:
        raise SystemExit(f"\nFAILED: {errors} error(s), {warnings} warning(s)")
    print("\nOK: no errors" + (f", {warnings} warning(s)" if warnings else ""))
