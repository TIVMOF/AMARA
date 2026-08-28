"""Shape, not content - and the split between what was received and what was kept.

Two layers are written for every crawl:

    data/raw/<site>/<timestamp>.json         every response body, untouched
    data/normalized/<site>/<timestamp>.json  the AMARA record shape

The raw layer exists because normalization is lossy in ways that cannot be
undone. The brand allowlist alone discards ~43% of what Browns serves, and that
allowlist changes constantly - every crawl's dropped-vendor report adds
candidates. Without raw, adding one brand means re-crawling the store; with it,
the same change is a re-normalize.

The normalized layer still does not clean values: prices stay as the strings
the site sent, brands stay however the retailer spelled them, categories stay
raw. It selects and shapes, nothing more.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .models.product import Product
from .models.site_config import SiteConfig

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
NORMALIZED_DIR = DATA_DIR / "normalized"


def _stamp(scraped_at: str) -> str:
    return scraped_at.replace(":", "").replace("-", "")


def _write(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    # ensure_ascii=False keeps Alaïa, Chloé and Stüssy readable in the file.
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


# ── raw layer ──────────────────────────────────────────────────────────────────

def build_raw(site: SiteConfig, result: dict[str, Any]) -> dict[str, Any]:
    """Response bodies exactly as received, with enough context to replay them.

    Nothing here is filtered, deduplicated or reshaped - products dropped by the
    brand allowlist are present, and so are products that appear in more than
    one collection.
    """
    pages = result.get("raw_pages", [])
    return {
        "site": site.name,
        "adapter": site.adapter,
        "base_url": site.base_url,
        "scraped_at": result["scraped_at"],
        "pages": len(pages),
        "products_received": sum(p["count"] for p in pages),
        "responses": pages,
    }


# ── normalized layer ───────────────────────────────────────────────────────────

def build_normalized(site: SiteConfig, result: dict[str, Any]) -> dict[str, Any]:
    """The AMARA record shape, plus the context needed to read it later.

    The counts and the unmatched-vendor tally are part of the record on
    purpose: a file that says 14,146 kept out of 25,000 seen tells you the brand
    allowlist is doing its job, and names the brands it turned away.
    """
    products: list[Product] = result["products"]
    return {
        "site": site.name,
        "adapter": site.adapter,
        "base_url": site.base_url,
        "currency": result.get("currency"),
        "country": result.get("country"),
        "scraped_at": result["scraped_at"],
        # Pairs this file with the raw responses it was derived from.
        "raw_file": f"raw/{site.name}/{_stamp(result['scraped_at'])}.json",

        # Completeness. Without these a truncated crawl and a whole one produce
        # identical-looking files - see issue #4. `complete` covers termination
        # only; short pages are a separate caveat on density.
        "complete": result.get("complete"),
        "pages_fetched": result.get("pages_fetched"),
        "short_pages": result.get("short_pages"),
        "listings": result.get("listings", []),
        # Non-empty when a listing stopped on a failed request. The products
        # gathered before it are still here - see issue #2.
        "errors": result.get("errors", []),

        "collections_crawled": result.get("collections_crawled", 0),
        "products_seen": result.get("seen_raw", 0),
        "products_seen_unique": result.get("seen_unique"),
        "products_kept": len(products),
        "unmatched_vendors": result.get("unmatched_vendors", {}),
        "products": [asdict(p) for p in products],
    }


# ── writing ────────────────────────────────────────────────────────────────────

def write(site: SiteConfig, result: dict[str, Any]) -> tuple[Path, Path | None]:
    """Write both layers. Returns (normalized_path, raw_path)."""
    stamp = _stamp(result["scraped_at"])
    normalized_path = _write(NORMALIZED_DIR / site.name / f"{stamp}.json",
                             build_normalized(site, result))
    raw_path = None
    if result.get("raw_pages"):
        raw_path = _write(RAW_DIR / site.name / f"{stamp}.json", build_raw(site, result))
    return normalized_path, raw_path


def load_raw(path: Path) -> dict[str, Any]:
    """Read a raw file back, for re-normalizing without re-crawling."""
    return json.loads(path.read_text(encoding="utf-8"))
