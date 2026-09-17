from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .site_config import SiteConfig

ROOT = Path(__file__).resolve().parent.parent


def _dir(variable: str, default: Path) -> Path:
    # This stage's output directory, overridable from the environment.
    #
    # The default is where the data sits in a checkout, so a local run needs no
    # setup. The variable is how a container is told where its own storage is:
    # it names a directory inside whatever filesystem this process can see and
    # says nothing about what backs it - a docker volume, a bind mount or the
    # container's own writable layer are all the same to this code.
    #
    # sites/ is deliberately not overridable: it is versioned config that ships
    # beside this code, not data that arrives from somewhere else.
    value = os.getenv(variable)
    return Path(value) if value else default


DATA_DIR = _dir("AMARA_GATHERED_DIR", ROOT / "data")


def relative(path: Path) -> str:
    # A path as it reads in the run log.
    #
    # The fallback is not decoration: once DATA_DIR can point outside the
    # component - which is the whole point of the variable - a bare
    # relative_to() raises ValueError and takes the run down after the file
    # has already been written.
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


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
        # it - it is recorded so stitch does not have to read sites/*.yaml.
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
    # One file per retailer per crawl, flat: a run is a single crawl on a
    # single date, and cleanup.py empties this directory once it is uploaded.
    path = DATA_DIR / f"{site.name}-{_stamp(result['scraped_at'])}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    # ensure_ascii=False keeps Alaïa, Chloé and Stüssy readable in the file.
    path.write_text(json.dumps(build(site, result), indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8")
    return path
