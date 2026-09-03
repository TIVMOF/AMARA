from __future__ import annotations

import argparse
import logging
import sys

from scripts import registry, store
from scripts.fetch import ConfigError, FetchError, Fetcher
from scripts.adapters.shopify import MAX_PAGE as PAGE_CEILING, PAGE_SIZE
from scripts.probe import probe, suggest_yaml


# What --help prints above the options.
USAGE = """\
python ingestion.py crawl                      every enabled site
python ingestion.py crawl brownsfashion kith   named sites only
python ingestion.py crawl kith --max-pages 2   short run, for a look at the data
python ingestion.py probe example.com          can this domain be scraped?
python ingestion.py sites                      what is configured
python ingestion.py collections kith           what a store publishes
"""


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )


# ── subcommand handlers ────────────────────────────────────────────────────────

def run_crawl(args: argparse.Namespace) -> int:
    # Scrape each requested site and write one JSON file per site.
    sites = registry.load_sites(args.sites or None)
    if not sites:
        print("no sites matched; run `python ingestion.py sites` to see what is configured")
        return 1

    print(f"{len(sites)} site(s) to crawl\n")
    failures = 0

    for site in sites:
        print(f"{site.name} ({site.base_url})")
        adapter = registry.get_adapter(site.adapter)
        try:
            result = adapter.crawl(site, max_pages=args.max_pages)
        except FetchError as exc:
            print(f"  FAILED: {exc}\n")
            failures += 1
            continue

        path = store.write(site, result)
        scope = (f" across {result['collections_crawled']} collections"
                 if result.get("collections_crawled") else "")
        print(f"  {result['seen_unique']} products from {result['seen_raw']} "
              f"deliveries{scope} -> {path.relative_to(store.ROOT)}")

        for listing in result.get("listings", []):
            reason = listing["stopped_reason"]
            if reason == "page_ceiling":
                print(f"  INCOMPLETE [{listing['label']}] - hit Shopify's {PAGE_CEILING}-page "
                      f"ceiling; this catalogue continues past {PAGE_CEILING * PAGE_SIZE:,} "
                      f"products. Sharded by collection to reach past it (see #5).")
            elif reason == "max_pages":
                print(f"  INCOMPLETE [{listing['label']}] - stopped at --max-pages")
            elif reason == "error":
                print(f"  INCOMPLETE [{listing['label']}] - stopped by a failed request; "
                      f"everything collected before it was still written")

        if result.get("throttled"):
            print(f"  throttled {result['throttled']}x - rate backed off from "
                  f"{result['rate_limit_start']}/s to {result['rate_limit_final']}/s")

        for err in result.get("errors", []):
            print(f"    error on page {err['page']}: {err['error']}")
            failures += 1
        if result.get("short_pages"):
            print(f"  note: {result['short_pages']} of {result['pages_fetched']} pages came "
                  f"back under {PAGE_SIZE} items after retries")

        seen_vendors = result["vendors"]
        if seen_vendors:
            top = list(seen_vendors.items())[:8]
            preview = ", ".join(f"{vendor} ({count})" for vendor, count in top)
            more = f" +{len(seen_vendors) - len(top)} more" if len(seen_vendors) > len(top) else ""
            print(f"  {len(seen_vendors)} vendors: {preview}{more}")
        print()

    return 1 if failures else 0


# ── probing ─────────────────────────────────────────────────────────────────

def run_probe(args: argparse.Namespace) -> int:
    # Classify domains and print a sites/*.yaml starting point for each.
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


# ── listing what is configured ──────────────────────────────────────────────

def list_collections(args: argparse.Namespace) -> int:
    # Show what collections a store publishes, largest first.
    #
    # Use it to decide `max_collections`, or to pick handles for an explicit
    # `collections:` list in the site's YAML.
    from scripts.adapters.shopify import discover_collections
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
    # Print what is configured in sites/.
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


# ── cli ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python ingestion.py",
        description=USAGE,
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

    cols = sub.add_parser("collections", help="show a store's collections, largest first")
    cols.add_argument("sites", nargs="*", help="site names; default is all")
    cols.add_argument("--limit", type=int, default=30, help="how many to print (default 30)")
    cols.set_defaults(handler=list_collections)

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
