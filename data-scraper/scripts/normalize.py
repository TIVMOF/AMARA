"""Shape, not content.

This module gives every adapter's output the same envelope and writes it to
disk as JSON. It does not clean values: prices stay as the strings the site
sent, brands stay as the site spelled them, categories stay raw.

Deliberately thin for now. The parsing work - colour and material out of the
description bullets, categories onto a shared taxonomy, prices into numbers -
belongs here later, once the raw files have been eyeballed and the patterns
are known. Writing it before then would be guessing.
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


def build_envelope(site: SiteConfig, result: dict[str, Any]) -> dict[str, Any]:
    """Wrap one crawl's products with the context needed to read them later.

    The counts and the unmatched-vendor tally are part of the record on
    purpose: a file that says 250 kept out of 4,000 seen tells you the brand
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

        # Completeness. Without these a truncated crawl and a whole one produce
        # identical-looking files - see issue #4. `complete` is false whenever a
        # listing stopped for any reason other than genuinely running out, or
        # any page stayed short after retries.
        "complete": result.get("complete"),
        "pages_fetched": result.get("pages_fetched"),
        "short_pages": result.get("short_pages"),
        "listings": result.get("listings", []),

        "products_seen": result.get("seen_raw", 0),
        "products_kept": len(products),
        "unmatched_vendors": result.get("unmatched_vendors", {}),
        "products": [asdict(p) for p in products],
    }


def output_path(site: SiteConfig, scraped_at: str) -> Path:
    """data/<site>/<timestamp>.json - one file per site per run."""
    stamp = scraped_at.replace(":", "").replace("-", "")
    return DATA_DIR / site.name / f"{stamp}.json"


def write(site: SiteConfig, result: dict[str, Any]) -> Path:
    """Write one crawl to disk and return the path."""
    envelope = build_envelope(site, result)
    path = output_path(site, result["scraped_at"])
    path.parent.mkdir(parents=True, exist_ok=True)
    # ensure_ascii=False keeps Alaïa, Chloé and Stüssy readable in the file.
    path.write_text(
        json.dumps(envelope, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return path
