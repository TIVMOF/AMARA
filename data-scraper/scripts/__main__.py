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

        path = normalize.write(site, result)
        kept, seen = len(result["products"]), result["seen_raw"]
        pct = (kept / seen * 100) if seen else 0
        print(f"  kept {kept} of {seen} products ({pct:.0f}%) -> {path.relative_to(normalize.ROOT)}")

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
