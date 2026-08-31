"""Takes a raw crawl apart into files Spark can read.

Ingestion writes one JSON object per crawl, with product bodies keyed by
product id so a product carried by twelve collections is stored once. That
shape is right for storage and wrong for Spark, which infers one column per
key and falls over on a catalogue of 44,000 products.

So each crawl becomes three files under `data/staging/<site>/<timestamp>/`:

    crawl.json       the crawl's own metadata, one object
    products.jsonl   one product per line, without its variants
    variants.jsonl   one variant per line, carrying its product id

Nothing is cleaned or renamed here - that is `processing/`. This stage only
changes the shape: maps become lines, and the nesting is flattened by one
level so products and variants can be read as two tables.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_ROOT = PROJECT_ROOT / "ingestion" / "data" / "raw"

STAGING_ROOT = PROJECT_ROOT / "dismantling" / "data" / "staging"

# Copied from the crawl envelope onto crawl.json. Everything else in the raw
# file is product data.
CRAWL_FIELDS = (
    "site", "adapter", "base_url", "currency", "country", "brand_override",
    "scraped_at", "products_received", "products_stored", "complete", "pages",
    "short_pages", "collections_crawled", "listings", "errors", "throttled",
    "rate_limit_start", "rate_limit_final",
)


# ── writing ────────────────────────────────────────────────────────────────────

def write_json(path: Path, record: dict[str, Any]) -> None:
    """One indented object. Read back with Spark's `multiLine` option."""
    with path.open("w", encoding="utf-8") as file:
        json.dump(record, file, ensure_ascii=False, indent=2)
        file.write("\n")


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """One compact object per line, which Spark splits and reads in parallel."""
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            json.dump(record, file, ensure_ascii=False, separators=(",", ":"))
            file.write("\n")


# ── reshaping ──────────────────────────────────────────────────────────────────

def build_crawl(raw: dict[str, Any]) -> dict[str, Any]:
    """The crawl's metadata.

    `vendors` arrives keyed by vendor name, the same map shape that makes the
    product bodies unreadable, and two spellings differing only in case - 032c
    and 032C both exist - collide outright under Spark's case-insensitive
    column names. It becomes a list of records instead.
    """
    crawl = {field: raw.get(field) for field in CRAWL_FIELDS}
    crawl["vendors"] = [
        {"vendor": vendor, "products": count}
        for vendor, count in (raw.get("vendors") or {}).items()
    ]
    return crawl


def build_products(raw: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split the product map into product rows and variant rows.

    Every row carries `site` and `scraped_at` so the two files can be joined
    back together, and so a table built from several crawls knows which crawl
    each row came from.
    """
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
            variants.append({
                **variant,
                "product_id": product["id"],
                "site": site,
                "scraped_at": scraped_at,
            })

        products.append(product)

    return products, variants


def dismantle(raw_path: Path, staging_root: Path) -> tuple[Path, int, int]:
    """Take one raw crawl apart. Returns (output directory, products, variants)."""
    raw = json.loads(raw_path.read_text(encoding="utf-8"))

    # The crawl timestamp names the output, so re-running overwrites the same
    # crawl rather than accumulating copies of it.
    timestamp = raw["scraped_at"].replace(":", "").replace("-", "")
    output_dir = staging_root / raw["site"] / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    products, variants = build_products(raw)

    write_json(output_dir / "crawl.json", build_crawl(raw))
    write_jsonl(output_dir / "products.jsonl", products)
    write_jsonl(output_dir / "variants.jsonl", variants)

    return output_dir, len(products), len(variants)


# ── running ────────────────────────────────────────────────────────────────────

def dismantle_all(raw_root: Path, staging_root: Path) -> int:
    """Every raw crawl under `raw_root`. Returns the number that failed."""
    raw_files = sorted(raw_root.glob("*/*.json"))
    if not raw_files:
        print(f"No raw crawls found in {raw_root}")
        return 0

    print(f"{len(raw_files)} raw crawl(s) to dismantle\n")
    failures = 0

    for raw_file in raw_files:
        try:
            output_dir, products, variants = dismantle(raw_file, staging_root)
        except (KeyError, json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            # One malformed crawl should not stop the other forty-nine.
            failures += 1
            print(f"  FAILED  {raw_file.parent.name:18} {type(exc).__name__}: {exc}")
            continue

        print(f"  {raw_file.parent.name:18} {products:>7,} products  {variants:>8,} variants"
              f"  -> {output_dir.relative_to(PROJECT_ROOT)}")

    print(f"\nDismantled {len(raw_files) - failures}/{len(raw_files)}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", type=Path, nargs="?",
                        help="a single raw crawl; default is every crawl under --raw")
    parser.add_argument("--raw", type=Path, default=RAW_ROOT,
                        help="directory holding the raw crawls")
    parser.add_argument("--staging", type=Path, default=STAGING_ROOT,
                        help="directory to write the staged files into")
    args = parser.parse_args()

    if not args.input:
        raise SystemExit(1 if dismantle_all(args.raw, args.staging) else 0)

    if not args.input.is_file():
        raise SystemExit(f"no such raw crawl: {args.input}")

    output_dir, products, variants = dismantle(args.input, args.staging)
    print(f"{products:,} products and {variants:,} variants "
          f"-> {output_dir.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
