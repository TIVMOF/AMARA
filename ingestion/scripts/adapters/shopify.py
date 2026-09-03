"""Adapter for stores running Shopify.

Used ONLY by sites whose YAML says `adapter: shopify`. Every Shopify store
exposes the same two endpoints with the same payload shape, which is why one
module serves all of them - kith, brownsfashion, antonioli and the rest differ
only in the config file, never in the code:

    /products.json?limit=250&page=N               whole catalogue
    /collections/<handle>/products.json?limit=250 one collection
    /meta.json                                    shop currency + country

Nothing here interprets a product. The adapter's job is to reach every page a
store will serve and hand back the bodies exactly as they arrived; deciding
what a field means belongs to whatever reads the file later.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ..fetch import Fetcher, FetchError
from ..site_config import SiteConfig

log = logging.getLogger(__name__)

PAGE_SIZE = 250

# How many times to re-request a page that comes back short. These stores
# soft-throttle by serving fewer items instead of returning 429, so one short
# response is not evidence that the catalogue has ended.
PAGE_ATTEMPTS = 3

# Shopify stops paging products.json here - page 101 answers HTTP 400. That is
# a ceiling of MAX_PAGE * PAGE_SIZE products per listing endpoint, and it is
# the normal end of a large catalogue rather than a fault. See issue #3.
MAX_PAGE = 100

# The unfiltered listing needs a label that a Shopify collection handle cannot
# take. Handles are [a-z0-9-]+, so the asterisks make a collision impossible.
# Nine of 21 stores publish a real collection called `all`, which used to share
# this label and made per-listing diagnostics ambiguous - see issue #13.
UNFILTERED_LABEL = "*unfiltered*"

# Collection discovery sorts largest-first. Past site.max_collections the crawl
# keeps walking the list only while it is still finding products it has not
# already seen; this is the ceiling on how far it may walk. See issues #14, #17.
DEEP_MAX_COLLECTIONS = 400

# How many consecutive tail collections may come back barren before the tail is
# abandoned, and what counts as barren. Ceilings used to drive this decision and
# measured the wrong thing: extrabutterny truncated on two shards, dug to all
# 400 collections, and 468 extra pages produced 2 extra products. Yield is the
# thing that actually matters and is self-limiting on every store. See #17.
TAIL_PATIENCE = 5
TAIL_MIN_NEW = 25

# Hard stop on a single site, so a store with 3,000 collections cannot run
# unbounded. Only ever reached by a store that is already truncating.
PAGE_BUDGET = 4000

# ── crawl ──────────────────────────────────────────────────────────────────────

def _shop_meta(fetcher: Fetcher, site: SiteConfig) -> tuple[str | None, str | None]:
    """Currency and country for the store, from /meta.json.

    products.json does not carry a currency anywhere, so without this every
    price would be a bare number of unknown denomination.
    """
    if site.currency and site.country:
        return site.currency, site.country
    try:
        meta = fetcher.get_json(f"{site.base_url.rstrip('/')}/meta.json", allow_404=True) or {}
    except FetchError as exc:
        # Worth a null currency, not worth losing the crawl - see issue #2.
        log.warning("  [%s] /meta.json unavailable (%s); currency will be null", site.name, exc)
        meta = {}
    return site.currency or meta.get("currency"), site.country or meta.get("country")


def discover_collections(fetcher: Fetcher, site: SiteConfig) -> list[dict]:
    """Every collection the store publishes, largest first.

    `products_count` is a hint, not a contract - Browns claims 3,679 for
    womens-new-season and serves 3,661 - so it is used only for ordering and
    reporting, never as a page budget.
    """
    base = site.base_url.rstrip("/")
    found: list[dict] = []
    page = 1
    while page <= MAX_PAGE:
        batch, _ = _fetch_page(fetcher, f"{base}/collections.json", page, key="collections")
        if not batch:
            break
        found.extend(batch)
        page += 1

    non_empty = [c for c in found if c.get("handle") and (c.get("products_count") or 0) > 0]
    non_empty.sort(key=lambda c: -(c.get("products_count") or 0))
    log.info("  [%s] %d collections published, %d non-empty",
             site.name, len(found), len(non_empty))
    return non_empty


def _initial_targets(site: SiteConfig) -> tuple[list[tuple[str, str]], int]:
    """What to crawl before the store has told us anything.

    Always the unfiltered listing, plus any collections named outright in the
    site config - a hand-picked list is a deliberate instruction, not a guess.
    Discovered collections are NOT included: whether this store needs them at
    all is decided from what the unfiltered listing does. See issue #16.
    """
    base = site.base_url.rstrip("/")
    targets = [(UNFILTERED_LABEL, f"{base}/products.json")]
    targets += [(h, f"{base}/collections/{h}/products.json") for h in site.collections]
    return targets, len(targets)


def _shard_targets(fetcher: Fetcher, site: SiteConfig) -> tuple[list[tuple[str, str]], int]:
    """Collections to shard by, once the unfiltered listing has proved it needs them.

    An unfiltered /products.json cannot reach past MAX_PAGE * PAGE_SIZE
    products. Collection-scoped endpoints each get their own page budget, which
    is the only way past that ceiling - see issue #5.

    Collections are additional shards, never a replacement. The unfiltered
    listing is crawled first and kept: it is the only target guaranteed to
    reach a product that belongs to no collection, or to one outside the
    largest max_collections. bdgastore lost 874 products to the earlier
    either/or - see issue #12. Dedupe by product id makes the union a superset
    by construction.

    Returns (targets, guaranteed): the first `guaranteed` are crawled outright,
    the rest are the long tail entered only if something truncates - issue #14.
    """
    base = site.base_url.rstrip("/")
    discovered = discover_collections(fetcher, site)
    handles = [c["handle"] for c in discovered[:DEEP_MAX_COLLECTIONS]]
    if len(discovered) > site.max_collections:
        log.info("  [%s] sharding by the largest %d of %d collections, and up to "
                 "%d more if any of them truncates",
                 site.name, site.max_collections, len(discovered),
                 min(len(discovered), DEEP_MAX_COLLECTIONS) - site.max_collections)
    targets = [(h, f"{base}/collections/{h}/products.json") for h in handles]
    return targets, min(len(handles), site.max_collections)


def _fetch_page(fetcher: Fetcher, url: str, page: int,
                *, key: str = "products") -> tuple[list[dict], bool]:
    """Fetch one listing page, re-requesting it while it comes back short.

    A short page is NOT proof that a catalogue has ended. These stores
    soft-throttle by serving fewer items rather than returning 429, and because
    paging is offset-based, silently accepting a short page also drops the
    items it should have carried.

    Returns (items, was_short). Items are the raw objects exactly as the store
    sent them. `was_short` means every attempt came back under PAGE_SIZE with at
    least one item - the page is probably incomplete and is recorded as such,
    rather than being taken as the end of the store.
    """
    best: list[dict] = []

    for attempt in range(1, PAGE_ATTEMPTS + 1):
        payload = fetcher.get_json(f"{url}?limit={PAGE_SIZE}&page={page}") or {}
        batch = payload.get(key) or []
        if len(batch) > len(best):
            best = batch
        if len(best) == PAGE_SIZE:
            return best, False
        if attempt < PAGE_ATTEMPTS:
            log.debug("    page %d returned %d/%d, re-requesting (%d/%d)",
                      page, len(batch), PAGE_SIZE, attempt, PAGE_ATTEMPTS)

    return best, bool(best)


def crawl(site: SiteConfig, *, max_pages: int | None = None) -> dict:
    """Page through a Shopify store and return everything it served.

    No product is dropped and no field is interpreted. Vendors are tallied on
    the way past, which is how you see what a store actually sells without
    opening the file.
    """
    fetcher = Fetcher(rate_limit_rps=site.rate_limit_rps)
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    currency, country = _shop_meta(fetcher, site)
    page_cap = max_pages or site.max_pages

    seen_ids: set[str] = set()
    vendors: dict[str, int] = {}
    seen_raw = 0
    listings: list[dict] = []
    errors: list[dict] = []
    # Every product body as received, before any filtering or field selection -
    # keyed by id, so a product carried by twelve collections is stored once
    # rather than twelve times (issue #15). This is what makes the brand
    # allowlist re-runnable without re-crawling.
    raw_products: dict[str, dict] = {}
    # What each page carried, by id. No bodies, so the trace that diagnosed
    # #12 and #13 survives at a fraction of the size.
    raw_pages: list[dict] = []

    targets, guaranteed = _initial_targets(site)
    unfiltered_complete = False
    sharded = False
    skipped = 0
    barren = 0

    index = -1
    while index + 1 < len(targets):
        index += 1
        label, url = targets[index]
        # `/collections/all` is the whole catalogue in a different order. Once
        # the unfiltered listing has ended on its own it has already seen every
        # product, so this is a straight duplicate - but when it truncated, the
        # different ordering reaches products it never got to. See issue #13.
        if label == "all" and unfiltered_complete:
            log.info("  [%s] skipping the `all` collection; the unfiltered "
                     "listing already covered the catalogue", site.name)
            # Does not spend one of the guaranteed slots - max_collections
            # means that many real collections.
            skipped += 1
            continue

        if index >= guaranteed + skipped:
            # The long tail. Walked while it still yields, abandoned once it
            # does not - see issue #17.
            if barren >= TAIL_PATIENCE:
                log.info("  [%s] last %d collections added under %d new products "
                         "each; tail exhausted at %d collections",
                         site.name, TAIL_PATIENCE, TAIL_MIN_NEW, len(listings) - 1)
                break
            if sum(l["pages_fetched"] for l in listings) >= PAGE_BUDGET:
                log.info("  [%s] page budget (%d) reached; catalogue may continue "
                         "past this crawl", site.name, PAGE_BUDGET)
                break

        page = 1
        known = len(seen_ids)
        short_pages: list[int] = []
        stopped = "empty_page"

        while True:
            if page_cap and page > page_cap:
                log.info("  [%s/%s] page cap %d reached", site.name, label, page_cap)
                stopped = "max_pages"
                break

            if page > MAX_PAGE:
                # Reached before asking for the page that would 400, so the
                # request is never wasted.
                log.info("  [%s/%s] Shopify page ceiling (%d) reached - catalogue "
                         "continues past it, shard by collection to go deeper",
                         site.name, label, MAX_PAGE)
                stopped = "page_ceiling"
                break

            try:
                batch, was_short = _fetch_page(fetcher, url, page)
            except FetchError as exc:
                # Whatever has been collected so far is kept and written, rather
                # than thrown away with the site - see issue #2.
                if "HTTP 400" in str(exc) and page > 1:
                    # A store capping paging below MAX_PAGE. End of listing,
                    # not a fault.
                    log.info("  [%s/%s] listing ends at page %d (HTTP 400)",
                             site.name, label, page - 1)
                    stopped = "page_ceiling"
                else:
                    log.warning("  [%s/%s] stopping at page %d: %s", site.name, label, page, exc)
                    stopped = "error"
                    errors.append({"listing": label, "page": page, "error": str(exc)})
                break

            if not batch:
                # The only signal that a catalogue has actually ended.
                break

            if was_short:
                # Recorded rather than treated as the end - see issue #1.
                short_pages.append(page)
                log.warning("  [%s/%s] page %d short: %d/%d after %d attempts",
                            site.name, label, page, len(batch), PAGE_SIZE, PAGE_ATTEMPTS)

            seen_raw += len(batch)
            page_ids: list[str] = []
            for raw in batch:
                product_id = str(raw.get("id"))
                page_ids.append(product_id)
                if product_id in seen_ids:
                    continue  # same product can sit in several collections
                seen_ids.add(product_id)
                # First delivery of an id wins. A later one can differ - a price
                # can change mid-crawl - but keeping both would defeat the point.
                raw_products[product_id] = raw
                vendor = raw.get("vendor")
                if vendor:
                    vendors[vendor] = vendors.get(vendor, 0) + 1

            raw_pages.append({
                "listing": label,
                "page": page,
                "url": f"{url}?limit={PAGE_SIZE}&page={page}",
                "count": len(batch),
                "product_ids": page_ids,
            })

            log.info("  [%s/%s] page %d: %d products, %d collected so far",
                     site.name, label, page, len(batch), len(seen_ids))
            page += 1

        new = len(seen_ids) - known
        if index >= guaranteed + skipped:
            barren = barren + 1 if new < TAIL_MIN_NEW else 0
        if label == UNFILTERED_LABEL and stopped == "empty_page":
            unfiltered_complete = True

        listings.append({
            "label": label,
            "pages_fetched": page - 1,
            "stopped_reason": stopped,
            "short_pages": short_pages,
            # Products this listing contributed that nothing before it had.
            # This is what decides whether the tail is worth continuing (#17),
            # and it is the number to look at when tuning max_collections.
            "new_products": new,
        })

        # The sharding decision, made from what the store just did rather than
        # from config - see issue #16. A listing that ran out of products on
        # its own has shown the whole catalogue, and collections can only
        # re-deliver what is already held. One that hit the ceiling, or failed,
        # has not, and collections are the only way past it.
        if label == UNFILTERED_LABEL and not sharded and site.discover_collections:
            sharded = True
            if stopped in ("page_ceiling", "error"):
                log.info("  [%s] unfiltered listing stopped on %s at %d products - "
                         "sharding by collection to reach the rest",
                         site.name, stopped, len(seen_ids))
                try:
                    shards, allowed = _shard_targets(fetcher, site)
                except FetchError as exc:
                    # collections.json failing is not worth the products already
                    # in hand. Recorded and reported like any other listing
                    # error - see issue #2 - rather than raised, which would
                    # discard the whole crawl at run_crawl.
                    log.warning("  [%s] collection discovery failed (%s); keeping "
                                "the %d products already collected",
                                site.name, exc, len(seen_ids))
                    errors.append({"listing": "*collections*", "page": 0,
                                   "error": str(exc)})
                else:
                    targets += shards
                    guaranteed += allowed
            elif stopped == "empty_page":
                log.info("  [%s] unfiltered listing ended on its own at %d products; "
                         "no sharding needed", site.name, len(seen_ids))

    return {
        "scraped_at": scraped_at,
        "currency": currency,
        "country": country,
        "seen_raw": seen_raw,
        # Collections overlap, so seen_raw double-counts. seen_unique is how
        # many distinct products the crawl actually laid eyes on.
        "seen_unique": len(seen_ids),
        "collections_crawled": sum(1 for l in listings if l["label"] != UNFILTERED_LABEL),
        "pages_fetched": sum(l["pages_fetched"] for l in listings),
        "short_pages": sum(len(l["short_pages"]) for l in listings),
        # Complete is about termination only: every listing ran out of products
        # on its own rather than being cut off. Short pages are reported
        # separately - they are a caveat on density, not proof of truncation,
        # and folding them in here would mark almost every crawl incomplete and
        # make the flag worthless.
        "complete": all(l["stopped_reason"] == "empty_page" for l in listings),
        "errors": errors,
        # Whether the host pushed back, and what the rate ended up at. A crawl
        # that was slowed is still trustworthy; one that was slowed a lot is a
        # sign the configured rate is wrong for this store. See issue #10.
        "throttled": fetcher.throttled,
        "rate_limit_final": round(1 / fetcher.min_interval, 2) if fetcher.min_interval else None,
        "rate_limit_start": round(1 / fetcher.min_interval_initial, 2) if fetcher.min_interval_initial else None,
        "raw_pages": raw_pages,
        "raw_products": raw_products,
        "listings": listings,
        "vendors": dict(sorted(vendors.items(), key=lambda kv: -kv[1])),
    }
