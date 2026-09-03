from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .site_config import SiteConfig

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"


def _stamp(scraped_at: str) -> str:
    return scraped_at.replace(":", "").replace("-", "")


def build(site: SiteConfig, result: dict[str, Any]) -> dict[str, Any]:
    # The file's contents: what was collected, and how the crawl went.
    pages = result.get("raw_pages", [])
    products = result.get("raw_products", {})
    return {
        "site": site.name,
        "adapter": site.adapter,
        "base_url": site.base_url,
        "currency": result.get("currency"),
        "country": result.get("country"),
        # Config's claim about this store's brand, when its `vendor` field is a
        # season or a fabric rather than the label. Ingestion does not act on
        # it - it is recorded so processing does not have to read sites/*.yaml.
        "brand_override": site.brand_override,
        "scraped_at": result["scraped_at"],

        # Deliveries, counting a product once per page that carried it.
        "products_received": sum(p["count"] for p in pages),
        # Distinct bodies actually written.
        "products_stored": len(products),
        "vendors": result.get("vendors", {}),

        # How the crawl went. Without these a truncated crawl and a whole one
        # produce identical-looking files - see issue #4. `complete` covers
        # termination only; short pages are a separate caveat on density.
        "complete": result.get("complete"),
        "pages": len(pages),
        "short_pages": result.get("short_pages"),
        "collections_crawled": result.get("collections_crawled", 0),
        "listings": result.get("listings", []),
        # Non-empty when a listing stopped on a failed request. Everything
        # gathered before it is still here - see issue #2.
        "errors": result.get("errors", []),
        "throttled": result.get("throttled", 0),
        "rate_limit_start": result.get("rate_limit_start"),
        "rate_limit_final": result.get("rate_limit_final"),

        "responses": pages,
        "products": products,
    }


def write(site: SiteConfig, result: dict[str, Any]) -> Path:
    # Write the crawl to disk and return the path.
    path = RAW_DIR / site.name / f"{_stamp(result['scraped_at'])}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    # ensure_ascii=False keeps Alaïa, Chloé and Stüssy readable in the file.
    path.write_text(json.dumps(build(site, result), indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8")
    return path
