"""Adapter for stores running Shopify.

Used ONLY by sites whose YAML says `adapter: shopify`. Every Shopify store
exposes the same two endpoints with the same payload shape, which is why one
module serves all of them - kith, brownsfashion, antonioli and the rest differ
only in the config file, never in the code:

    /products.json?limit=250&page=N               whole catalogue
    /collections/<handle>/products.json?limit=250 one collection
    /meta.json                                    shop currency + country

Field mapping is deliberately shallow. Where Shopify states a value outright it
is copied across verbatim; where it does not, the field is left null and the
raw evidence is parked under `source` for a parser to handle later.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from ..fetch import Fetcher, FetchError
from ..models.brand import BrandIndex
from ..models.product import Product
from ..models.site_config import SiteConfig
from ..models.size import Size

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

# Tag values that name a gender. Shopify has no gender field, so the tag list
# is the only place most stores state it.
GENDER_TAGS = {
    "women": "Women", "woman": "Women", "womens": "Women", "women's": "Women",
    "ladies": "Women", "female": "Women",
    "men": "Men", "man": "Men", "mens": "Men", "men's": "Men", "male": "Men",
    "kids": "Kids", "kid": "Kids", "children": "Kids", "child": "Kids",
    "boys": "Kids", "girls": "Kids", "junior": "Kids", "baby": "Kids",
    "unisex": "Unisex",
}

COLOR_OPTION_NAMES = {"color", "colour", "colorway", "colourway"}
SIZE_OPTION_NAMES = {"size", "sizes", "shoe size", "eu size", "uk size", "us size"}


# ── payload helpers ────────────────────────────────────────────────────────────

def _option_index(options: list[dict], wanted: set[str]) -> int | None:
    """Return the 1-based option position matching one of `wanted`, if any.

    Shopify variants carry option1/option2/option3 positionally, so the option
    list has to be consulted to know which slot holds sizes. Stores also add
    junk options - Browns exposes a 'VendorSKU' option - so position alone is
    never safe to assume.
    """
    for option in options:
        if (option.get("name") or "").strip().lower() in wanted:
            position = option.get("position")
            if isinstance(position, int) and 1 <= position <= 3:
                return position
    return None


def _bullets(body_html: str | None) -> list[str]:
    """Pull the <li> items out of a description.

    Most stores describe a product as a bullet list that mixes colour, fabric
    and features with no labels - Browns emits ['black', 'velvet', 'rhinestone
    embellishments', ...]. The list is kept as-is; deciding which bullet is the
    colour is a parsing job for later.
    """
    if not body_html:
        return []
    items = re.findall(r"<li[^>]*>(.*?)</li>", body_html, re.DOTALL | re.IGNORECASE)
    out = []
    for item in items:
        text = re.sub(r"<[^>]+>", " ", item)
        text = re.sub(r"\s+", " ", text).replace("&amp;", "&").strip()
        if text:
            out.append(text)
    return out


def _gender_from_tags(tags: list[str]) -> str | None:
    for tag in tags:
        hit = GENDER_TAGS.get(tag.strip().lower())
        if hit:
            return hit
    return None


def _color_from_options(options: list[dict]) -> str | None:
    """Only returns a colour when the store declares one as a variant option.

    Where colour lives in the title or a description bullet instead, this stays
    null rather than guessing.
    """
    index = _option_index(options, COLOR_OPTION_NAMES)
    if index is None:
        return None
    for option in options:
        if option.get("position") == index:
            values = [v for v in (option.get("values") or []) if v]
            return " / ".join(values) if values else None
    return None


def _sizes(product: dict) -> list[Size]:
    size_index = _option_index(product.get("options") or [], SIZE_OPTION_NAMES)
    sizes: list[Size] = []
    for variant in product.get("variants") or []:
        label = variant.get(f"option{size_index}") if size_index else variant.get("title")
        sizes.append(Size(
            size=label,
            available=variant.get("available"),
            price=variant.get("price"),
            sku=variant.get("sku") or None,
        ))
    return sizes


# ── mapping ────────────────────────────────────────────────────────────────────

def to_product(raw: dict, site: SiteConfig, *, brand: str | None, taxonomy: tuple,
               currency: str | None, scraped_at: str) -> Product:
    """Map one raw Shopify product onto the AMARA record."""
    variants = raw.get("variants") or []
    first = variants[0] if variants else {}
    tags = [t for t in (raw.get("tags") or []) if t]
    options = raw.get("options") or []
    handle = raw.get("handle")
    segment, tier, styles = taxonomy

    return Product(
        name=raw.get("title"),
        brand=brand,
        brand_segment=segment,
        brand_tier=tier,
        brand_styles=styles,
        category=raw.get("product_type") or None,
        gender=_gender_from_tags(tags),
        product_url=f"{site.base_url.rstrip('/')}/products/{handle}" if handle else None,
        price=first.get("price"),
        original_price=first.get("compare_at_price"),
        currency=currency,
        color=_color_from_options(options),
        material=None,  # lives unlabelled in source.description_bullets
        sizes=_sizes(raw),
        availability=any(v.get("available") for v in variants) if variants else None,

        retailer=site.name,
        product_id=str(raw["id"]) if raw.get("id") is not None else None,
        sku=first.get("sku") or None,
        images=[img["src"] for img in (raw.get("images") or []) if img.get("src")],
        scraped_at=scraped_at,

        source={
            "vendor": raw.get("vendor"),
            "product_type": raw.get("product_type"),
            "tags": tags,
            "handle": handle,
            "options": options,
            "description_bullets": _bullets(raw.get("body_html")),
            "body_html": raw.get("body_html"),
            "published_at": raw.get("published_at"),
            "updated_at": raw.get("updated_at"),
            "variant_count": len(variants),
        },
    )


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


def _listing_urls(site: SiteConfig) -> list[tuple[str, str]]:
    """(label, base URL) pairs to page through."""
    base = site.base_url.rstrip("/")
    if site.collections:
        return [(c, f"{base}/collections/{c}/products.json") for c in site.collections]
    return [("all", f"{base}/products.json")]


def _fetch_page(fetcher: Fetcher, url: str, page: int) -> tuple[list[dict], bool]:
    """Fetch one listing page, re-requesting it while it comes back short.

    A short page is NOT proof that a catalogue has ended. These stores
    soft-throttle by serving fewer items rather than returning 429, and because
    paging is offset-based, silently accepting a short page also drops the
    items it should have carried.

    Returns (products, was_short). `was_short` means every attempt came back
    under PAGE_SIZE with at least one item - the page is probably incomplete
    and is recorded as such, rather than being taken as the end of the store.
    """
    best: list[dict] = []

    for attempt in range(1, PAGE_ATTEMPTS + 1):
        batch = (fetcher.get_json(f"{url}?limit={PAGE_SIZE}&page={page}") or {}).get("products") or []
        if len(batch) > len(best):
            best = batch
        if len(best) == PAGE_SIZE:
            return best, False
        if attempt < PAGE_ATTEMPTS:
            log.debug("    page %d returned %d/%d, re-requesting (%d/%d)",
                      page, len(batch), PAGE_SIZE, attempt, PAGE_ATTEMPTS)

    return best, bool(best)


def crawl(site: SiteConfig, brands: BrandIndex, *, max_pages: int | None = None) -> dict:
    """Page through a Shopify store and return matched products plus a report.

    Products whose vendor is not on the allowlist are dropped, but their vendor
    strings are counted and returned - that tally is how you find brands worth
    adding, and spelling variants worth aliasing.
    """
    fetcher = Fetcher(rate_limit_rps=site.rate_limit_rps)
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    currency, country = _shop_meta(fetcher, site)
    page_cap = max_pages or site.max_pages

    products: list[Product] = []
    seen_ids: set[str] = set()
    unmatched: dict[str, int] = {}
    seen_raw = 0
    listings: list[dict] = []
    errors: list[dict] = []

    for label, url in _listing_urls(site):
        page = 1
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
            for raw in batch:
                product_id = str(raw.get("id"))
                if product_id in seen_ids:
                    continue  # same product can sit in several collections
                seen_ids.add(product_id)

                # A single-brand store's `vendor` is often a season or fabric
                # name rather than the brand, so config wins when it is set.
                vendor = raw.get("vendor")
                if site.brand_override:
                    brand_name = site.brand_override
                    hit = brands.match(site.brand_override)
                else:
                    hit = brands.match(vendor)
                    if not hit:
                        if vendor:
                            unmatched[vendor] = unmatched.get(vendor, 0) + 1
                        continue
                    brand_name = vendor

                taxonomy = (hit.segment, hit.tier, hit.styles) if hit else (None, None, [])
                products.append(to_product(
                    raw, site, brand=brand_name, taxonomy=taxonomy,
                    currency=currency, scraped_at=scraped_at,
                ))

            log.info("  [%s/%s] page %d: %d raw, %d kept so far",
                     site.name, label, page, len(batch), len(products))
            page += 1

        listings.append({
            "label": label,
            "pages_fetched": page - 1,
            "stopped_reason": stopped,
            "short_pages": short_pages,
        })

    return {
        "products": products,
        "scraped_at": scraped_at,
        "currency": currency,
        "country": country,
        "seen_raw": seen_raw,
        "pages_fetched": sum(l["pages_fetched"] for l in listings),
        "short_pages": sum(len(l["short_pages"]) for l in listings),
        # Complete is about termination only: every listing ran out of products
        # on its own rather than being cut off. Short pages are reported
        # separately - they are a caveat on density, not proof of truncation,
        # and folding them in here would mark almost every crawl incomplete and
        # make the flag worthless.
        "complete": all(l["stopped_reason"] == "empty_page" for l in listings),
        "errors": errors,
        "listings": listings,
        "unmatched_vendors": dict(sorted(unmatched.items(), key=lambda kv: -kv[1])),
    }
