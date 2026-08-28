"""Entry point for the scraper.

Usage:
    python -m scripts crawl                      every enabled site
    python -m scripts crawl brownsfashion kith   named sites only
    python -m scripts crawl kith --max-pages 2   short run, for a look at the data
    python -m scripts probe example.com          can this domain be scraped?
    python -m scripts sites                      what is configured
    python -m scripts brands                     what the allowlist holds

Each subcommand maps to one function below. argparse reads sys.argv and calls
the matching function - nothing here shells out.
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import normalize, registry
from .fetch import ConfigError, FetchError, Fetcher
from .adapters.shopify import MAX_PAGE as PAGE_CEILING, PAGE_SIZE
from .probe import probe, suggest_yaml


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )


# ── subcommand handlers ────────────────────────────────────────────────────────

def run_crawl(args: argparse.Namespace) -> int:
    """Scrape each requested site and write one JSON file per site."""
    brands = registry.load_brands()
    sites = registry.load_sites(args.sites or None)
    if not sites:
        print("no sites matched; run `python -m scripts sites` to see what is configured")
        return 1

    print(f"{len(brands)} brands on the allowlist, {len(sites)} site(s) to crawl\n")
    failures = 0

    for site in sites:
        print(f"{site.name} ({site.base_url})")
        adapter = registry.get_adapter(site.adapter)
        try:
            result = adapter.crawl(site, brands, max_pages=args.max_pages)
        except FetchError as exc:
            print(f"  FAILED: {exc}\n")
            failures += 1
            continue

        path, raw_path = normalize.write(site, result)
        kept = len(result["products"])
        seen = result.get("seen_unique") or result["seen_raw"]
        pct = (kept / seen * 100) if seen else 0
        scope = (f" across {result['collections_crawled']} collections"
                 if result.get("collections_crawled") else "")
        print(f"  kept {kept} of {seen} products ({pct:.0f}%){scope} "
              f"-> {path.relative_to(normalize.ROOT)}")
        if raw_path:
            print(f"  raw: {result['seen_raw']} products as received "
                  f"-> {raw_path.relative_to(normalize.ROOT)}")

        for listing in result.get("listings", []):
            reason = listing["stopped_reason"]
            if reason == "page_ceiling":
                print(f"  INCOMPLETE [{listing['label']}] - hit Shopify's {PAGE_CEILING}-page "
                      f"ceiling; this catalogue continues past {PAGE_CEILING * PAGE_SIZE:,} "
                      f"products. Shard by collection to reach the rest (see #5).")
            elif reason == "max_pages":
                print(f"  INCOMPLETE [{listing['label']}] - stopped at --max-pages")
            elif reason == "error":
                print(f"  INCOMPLETE [{listing['label']}] - stopped by a failed request; "
                      f"everything collected before it was still written")

        for err in result.get("errors", []):
            print(f"    error on page {err['page']}: {err['error']}")
            failures += 1
        if result.get("short_pages"):
            print(f"  note: {result['short_pages']} of {result['pages_fetched']} pages came "
                  f"back under {PAGE_SIZE} items after retries")

        dropped = result["unmatched_vendors"]
        if dropped:
            top = list(dropped.items())[:8]
            preview = ", ".join(f"{vendor} ({count})" for vendor, count in top)
            more = f" +{len(dropped) - len(top)} more" if len(dropped) > len(top) else ""
            print(f"  dropped brands: {preview}{more}")
        print()

    return 1 if failures else 0


def run_probe(args: argparse.Namespace) -> int:
    """Classify domains and print a sites/*.yaml starting point for each."""
    fetcher = Fetcher()
    for domain in args.domains:
        result = probe(domain, fetcher)
        print(f"\n{'=' * 70}\n{domain}\n{'=' * 70}")
        if not result.get("adapter"):
            print(f"  no open JSON endpoint: {result.get('error')}")
            continue
        print(f"  adapter        {result['adapter']}")
        print(f"  kind           {result['kind']} ({result['vendor_count']} vendors on page 1)")
        print(f"  currency       {result['currency']} / {result['country']}")
        print(f"  vendors        {', '.join(result['vendors'][:15])}")
        print(f"\n  sites/{domain.split('.')[0]}.yaml:\n")
        print("    " + suggest_yaml(result).replace("\n", "\n    "))
    return 0


def run_renormalize(args: argparse.Namespace) -> int:
    """Rebuild normalized files from stored raw ones, without touching the network.

    Use after changing brands.yaml or the field mapping - the products a former
    allowlist discarded are still in raw, so they come back for free.
    """
    from .adapters.shopify import from_raw

    brands = registry.load_brands()
    sites = {s.name: s for s in registry.load_sites(include_disabled=True)}
    wanted = set(args.sites or [])
    raw_files = sorted(normalize.RAW_DIR.glob("*/*.json"))
    if wanted:
        raw_files = [f for f in raw_files if f.parent.name in wanted]
    if not raw_files:
        print(f"no raw files under {normalize.RAW_DIR.relative_to(normalize.ROOT)}")
        return 1

    print(f"{len(brands)} brands on the allowlist, {len(raw_files)} raw file(s)\n")
    for path in raw_files:
        raw = normalize.load_raw(path)
        site = sites.get(raw["site"])
        if site is None:
            print(f"  {raw['site']}: no site config, skipped")
            continue
        result = from_raw(raw, site, brands)
        out, _ = normalize.write(site, result)
        kept, seen = len(result["products"]), result["seen_unique"]
        pct = (kept / seen * 100) if seen else 0
        print(f"  {raw['site']:20} {kept:>6} of {seen:>6} ({pct:>3.0f}%) "
              f"-> {out.relative_to(normalize.ROOT)}")
    return 0


def list_collections(args: argparse.Namespace) -> int:
    """Show what collections a store publishes, largest first.

    Use it to decide `max_collections`, or to pick handles for an explicit
    `collections:` list in the site's YAML.
    """
    from .adapters.shopify import discover_collections
    for site in registry.load_sites(args.sites or None, include_disabled=True):
        found = discover_collections(Fetcher(rate_limit_rps=site.rate_limit_rps), site)
        print(f"\n{site.name}: {len(found)} non-empty collections")
        for collection in found[:args.limit]:
            print(f"  {collection['products_count']:>7}  {collection['handle']}")
        if len(found) > args.limit:
            tail = sum(c["products_count"] for c in found[args.limit:])
            print(f"  ... {len(found) - args.limit} smaller collections, {tail:,} products between them")
    return 0


def list_sites(args: argparse.Namespace) -> int:
    """Print what is configured in sites/."""
    sites = registry.load_sites(include_disabled=True)
    print(f"{len(sites)} site(s) in sites/\n")
    for site in sites:
        flag = " " if site.enabled else "-"
        scope = f"{len(site.collections)} collections" if site.collections else "whole catalogue"
        override = f"  brand_override={site.brand_override}" if site.brand_override else ""
        print(f" {flag} {site.name:22} {site.adapter:9} {scope:18}{override}")
        if site.notes:
            print(f"     {site.notes}")
    return 0


def list_brands(args: argparse.Namespace) -> int:
    """Print the allowlist grouped by whichever axis was asked for."""
    brands = registry.load_brands()
    axis = args.by
    grouped: dict[str, list] = {}
    for brand in brands.brands:
        # styles is a list, so a brand shows up under each style it carries.
        for value in (brand.styles if axis == "style" else [getattr(brand, axis)]):
            grouped.setdefault(value, []).append(brand)

    print(f"{len(brands)} brands, grouped by {axis}\n")
    for value, members in grouped.items():
        print(f"  {value} ({len(members)})")
        if args.detail:
            for brand in sorted(members, key=lambda b: b.name):
                suffix = (f"{brand.segment} · {brand.tier}" if axis == "style"
                          else " · ".join([a for a in (brand.segment, brand.tier)
                                           if a != value] + ["/".join(brand.styles)]))
                print(f"    {brand.name:32} {suffix}")
        else:
            print(f"    {', '.join(b.name for b in members)}")
        print()
    return 0


# ── argument parsing ───────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    crawl = sub.add_parser("crawl", help="scrape sites into data/")
    crawl.add_argument("sites", nargs="*", help="site names; default is all enabled")
    crawl.add_argument("--max-pages", type=int, help="stop after N pages per collection")
    crawl.set_defaults(handler=run_crawl)

    probe_cmd = sub.add_parser("probe", help="check whether a domain can be scraped")
    probe_cmd.add_argument("domains", nargs="+")
    probe_cmd.set_defaults(handler=run_probe)

    sub.add_parser("sites", help="list configured sites").set_defaults(handler=list_sites)

    renorm = sub.add_parser("renormalize",
                            help="rebuild normalized files from raw, without re-crawling")
    renorm.add_argument("sites", nargs="*", help="site names; default is every raw file")
    renorm.set_defaults(handler=run_renormalize)

    cols = sub.add_parser("collections", help="show a store's collections, largest first")
    cols.add_argument("sites", nargs="*", help="site names; default is all")
    cols.add_argument("--limit", type=int, default=30, help="how many to print (default 30)")
    cols.set_defaults(handler=list_collections)
    brands_cmd = sub.add_parser("brands", help="list the brand allowlist")
    brands_cmd.add_argument("--by", choices=["segment", "tier", "style"], default="segment",
                            help="axis to group by (default: segment)")
    brands_cmd.add_argument("--detail", action="store_true",
                            help="one line per brand showing the other two axes")
    brands_cmd.set_defaults(handler=list_brands)

    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    try:
        return args.handler(args)
    except ConfigError as exc:
        print(f"\nconfiguration error:\n{exc}", file=sys.stderr)
        return 2
    except (FileNotFoundError, ValueError) as exc:
        print(f"\nerror: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
