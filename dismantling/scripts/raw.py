from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Copied onto crawl.json as they are. Everything else in the raw file is
# product data.
CRAWL_FIELDS = (
    "site", "adapter", "base_url", "currency", "country", "brand_override",
    "scraped_at", "products_received", "products_stored", "complete", "pages",
    "short_pages", "collections_crawled", "listings", "errors", "throttled",
    "rate_limit_start", "rate_limit_final",
)


def load(path: Path) -> dict[str, Any]:
    # One raw crawl, as ingestion wrote it.
    return json.loads(path.read_text(encoding="utf-8"))


def crawl_record(raw: dict[str, Any]) -> dict[str, Any]:
    # The crawl's own metadata.
    #
    # `vendors` arrives keyed by vendor name - the same map shape that makes the
    # product bodies unreadable - and two spellings differing only in case
    # collide outright under Spark's case-insensitive column names, which `032c`
    # and `032C` do. It becomes a list of records.
    record = {field: raw.get(field) for field in CRAWL_FIELDS}
    record["vendors"] = [
        {"vendor": vendor, "products": count}
        for vendor, count in (raw.get("vendors") or {}).items()
    ]
    return record


def product_records(raw: dict[str, Any]) -> tuple[list[dict], list[dict]]:
    # Split the product map into product rows and variant rows.
    #
    # Every row carries `site` and `scraped_at` so the two can be joined back
    # together, and so a table built from several crawls knows which crawl each
    # row came from.
    site = raw["site"]
    scraped_at = raw["scraped_at"]

    products: list[dict[str, Any]] = []
    variants: list[dict[str, Any]] = []

    for product_id, body in (raw.get("products") or {}).items():
        product = dict(body)
        # The map key is authoritative: it is what ingestion deduplicated on.
        product["id"] = body.get("id", product_id)
        product["site"] = site
        product["scraped_at"] = scraped_at

        for variant in product.pop("variants", None) or []:
            variants.append({**variant, "product_id": product["id"],
                             "site": site, "scraped_at": scraped_at})

        products.append(product)

    return products, variants
