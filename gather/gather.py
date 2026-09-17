from __future__ import annotations

import argparse
import logging
import signal
import sys
from datetime import datetime, timezone

from scripts import (cleanup, registry, store, upload_raw, validate,
                     validate_upload)
from scripts.fetch import ConfigError, FetchError, Fetcher
from scripts.adapters.shopify import MAX_PAGE as PAGE_CEILING, PAGE_SIZE
from scripts.probe import probe, suggest_yaml


# What --help prints above the options.
USAGE = """\
python gather.py crawl                      every enabled site
python gather.py crawl brownsfashion kith   named sites only
python gather.py crawl kith --max-pages 2   short run, for a look at the data
python gather.py crawl kith --scraped-at STAMP   add a site to an earlier crawl
python gather.py probe example.com          can this domain be scraped?
python gather.py sites                      what is configured
python gather.py collections kith           what a store publishes
python gather.py validate                   is the crawl output sound?
python gather.py upload                     the crawl JSON -> the RAW stage
python gather.py validate-upload            is every crawl file in the stage?
python gather.py cleanup                    empty data/, once it is uploaded
"""

# The shape store._stamp turns into a filename, and validate reads a date back
# out of. Not cosmetic: a malformed stamp would split one crawl across two.
STAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def crawl_stamp(value: str) -> str:
    # A stamp from an earlier run, so a retry can crawl only the sites that are
    # missing and still land inside the same crawl.
    try:
        datetime.strptime(value, STAMP_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"expected a UTC stamp like 2026-09-17T05:59:45Z, got {value!r}")
    return value


def _interrupt(signum: int, frame: object) -> None:
    raise KeyboardInterrupt


def _stop_cleanly_on_sigterm() -> None:
    # A crawl runs for hours, and `docker stop` - or an Airflow task kill -
    # sends SIGTERM. Raising KeyboardInterrupt hands it to the handler Ctrl-C
    # already uses, which exits 130 with every finished retailer on disk:
    # store.write runs once per site, so only the site in flight is lost.
    # Measured: a stop mid-crawl returns in 0.2s with exit 130, against 10s
    # and a SIGKILL without this.
    #
    # This covers the run, not the boot. An exec-form ENTRYPOINT makes Python
    # PID 1, and the kernel disables PID 1's *default* signal actions - so in
    # the moment before this line runs, a SIGTERM is dropped silently and the
    # stop costs the full grace period (measured: 10.1s, exit 137). Closing
    # that window needs an init process, so run these images with `docker run
    # --init` / compose's `init: true`, which puts tini at PID 1 and leaves
    # Python as an ordinary child whose default action works from the start.
    signal.signal(signal.SIGTERM, _interrupt)


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )


# ── subcommand handlers ────────────────────────────────────────────────────────

def run_crawl(args: argparse.Namespace) -> int:
    """Scrape each requested site and write one JSON file per site."""
    sites = registry.load_sites(args.sites or None)
    if not sites:
        print("no sites matched; run `python gather.py sites` to see what is configured")
        return 1

    # One run is one crawl on one date. The timestamp is taken once here and
    # given to every site, so a run lasting hours - which a full crawl does -
    # cannot straddle midnight and split itself across two dates. Products and
    # variants are told apart from an earlier crawl's by date alone, so two
    # dates inside one run would make the same product look like two.
    #
    # --scraped-at passes an earlier run's stamp back in, which is what makes a
    # retry possible: crawl the sites that are missing, and they join the crawl
    # that is already on disk rather than starting a second one.
    scraped_at = args.scraped_at or datetime.now(timezone.utc).strftime(STAMP_FORMAT)

    print(f"{len(sites)} site(s) to crawl, stamped {scraped_at}\n")
    failures = 0

    for site in sites:
        print(f"{site.name} ({site.base_url})")
        adapter = registry.get_adapter(site.adapter)
        try:
            result = adapter.crawl(site, scraped_at=scraped_at,
                                   max_pages=args.max_pages)
        except FetchError as exc:
            print(f"  FAILED: {exc}\n")
            failures += 1
            continue

        path = store.write(site, result)
        scope = (f" across {result['collections_crawled']} collections"
                 if result.get("collections_crawled") else "")
        print(f"  {result['seen_unique']} products from {result['seen_raw']} "
              f"deliveries{scope} -> {store.relative(path)}")

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

        # Reported, but not counted: the site's file was written, holding
        # everything collected around the gap. Only a site that failed outright
        # - the FetchError above - fails the run. Counting pages here meant one
        # bad request in ~5,000 failed a whole crawl, which also disagreed with
        # the INCOMPLETE branch above, where a listing cut short by a failed
        # request is only ever reported.
        for err in result.get("errors", []):
            print(f"    error on page {err['page']}: {err['error']}")
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


# ── listing what is configured ──────────────────────────────────────────────

def list_collections(args: argparse.Namespace) -> int:
    """Show what collections a store publishes, largest first.

    Use it to decide `max_collections`, or to pick handles for an explicit
    `collections:` list in the site's YAML.
    """
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


def run_upload(args: argparse.Namespace) -> int:
    """PUT every crawl file into Snowflake's RAW stage."""
    upload_raw.upload()
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


# ── cli ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    # `validate` is handed straight to the validator with the rest of the command
    # line intact, so it keeps its own arguments rather than having them
    # re-declared here and kept in step by hand.
    argv = sys.argv[1:] if argv is None else argv
    for name, command in (("validate", validate.main),
                          ("validate-upload", validate_upload.main),
                          ("cleanup", cleanup.main)):
        if argv and argv[0] == name:
            # `or 0` because these signal failure by raising SystemExit rather
            # than returning. Dropping the return value would turn a validator
            # that ever starts returning a code into a silent success, which
            # under Airflow is a green task on bad data.
            return command(argv[1:]) or 0

    parser = argparse.ArgumentParser(
        prog="python gather.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    crawl = sub.add_parser("crawl", help="scrape sites into data/")
    crawl.add_argument("sites", nargs="*", help="site names; default is all enabled")
    crawl.add_argument("--max-pages", type=int, help="stop after N pages per collection")
    crawl.add_argument("--scraped-at", type=crawl_stamp, metavar="STAMP",
                       help="an earlier run's stamp, to add sites to that crawl "
                            "instead of starting a new one")
    crawl.set_defaults(handler=run_crawl)

    probe_cmd = sub.add_parser("probe", help="check whether a domain can be scraped")
    probe_cmd.add_argument("domains", nargs="+")
    probe_cmd.set_defaults(handler=run_probe)

    sub.add_parser("sites", help="list configured sites").set_defaults(handler=list_sites)

    sub.add_parser("upload", help="crawl JSON -> Snowflake's RAW stage").set_defaults(handler=run_upload)

    cols = sub.add_parser("collections", help="show a store's collections, largest first")
    cols.add_argument("sites", nargs="*", help="site names; default is all")
    cols.add_argument("--limit", type=int, default=30, help="how many to print (default 30)")
    cols.set_defaults(handler=list_collections)

    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    _stop_cleanly_on_sigterm()

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
