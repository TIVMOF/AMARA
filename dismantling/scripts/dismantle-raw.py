from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_raw(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def prepare_raw(raw_path: Path, output_root: Path) -> Path:
    raw = load_raw(raw_path)

    site = raw["site"]
    scraped_at = raw["scraped_at"]

    # Keep the original timestamp as the crawl identifier.
    timestamp = scraped_at.replace(":", "").replace("-", "")
    output_dir = output_root / site / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Crawl metadata

    crawl = {
        "site": raw.get("site"),
        "adapter": raw.get("adapter"),
        "base_url": raw.get("base_url"),
        "currency": raw.get("currency"),
        "country": raw.get("country"),
        "brand_override": raw.get("brand_override"),
        "scraped_at": raw.get("scraped_at"),
        "products_received": raw.get("products_received"),
        "products_stored": raw.get("products_stored"),
        "vendors": raw.get("vendors"),
        "complete": raw.get("complete"),
        "pages": raw.get("pages"),
        "short_pages": raw.get("short_pages"),
        "collections_crawled": raw.get("collections_crawled"),
        "listings": raw.get("listings"),
        "errors": raw.get("errors"),
        "throttled": raw.get("throttled"),
        "rate_limit_start": raw.get("rate_limit_start"),
        "rate_limit_final": raw.get("rate_limit_final"),
    }

    write_json(output_dir / "crawl.json", crawl)

    # 2. Products and variants

    products_path = output_dir / "products.jsonl"
    variants_path = output_dir / "variants.jsonl"

    with (
        products_path.open("w", encoding="utf-8") as products_file,
        variants_path.open("w", encoding="utf-8") as variants_file,
    ):
        products = raw.get("products", {})

        for product_id, product in products.items():
            product_record = dict(product)

            # Guarantee an ID exists.
            product_record["id"] = product.get("id", product_id)

            # Add crawl context.
            product_record["site"] = site
            product_record["scraped_at"] = scraped_at

            # Variants are stored separately.
            variants = product_record.pop("variants", [])

            products_file.write(
                json.dumps(
                    product_record,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            )

            for variant in variants:
                variant_record = dict(variant)

                # Relationship to the product.
                variant_record["product_id"] = product_record["id"]

                # Crawl context.
                variant_record["site"] = site
                variant_record["scraped_at"] = scraped_at

                variants_file.write(
                    json.dumps(
                        variant_record,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )

    return output_dir


def write_json(path: Path, data: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")


def process_all(raw_root: Path, output_root: Path) -> None:
    raw_files = sorted(raw_root.glob("*/*.json"))

    if not raw_files:
        print(f"No raw JSON files found in {raw_root}")
        return

    print(f"Found {len(raw_files)} raw crawl(s)\n")

    failures = 0

    for raw_file in raw_files:
        try:
            output_dir = prepare_raw(raw_file, output_root)

            print(f"Prepared: {raw_file}")
            print(f"  -> {output_dir}")
            print(f"     crawl.json")
            print(f"     products.jsonl")
            print(f"     variants.jsonl")
            print()

        except (KeyError, json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            failures += 1
            print(f"FAILED: {raw_file}")
            print(f"  {type(exc).__name__}: {exc}")
            print()

    print(f"Processed: {len(raw_files) - failures}/{len(raw_files)}")
    if failures:
        print(f"Failed:    {failures}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare AMARA raw crawl data for PySpark."
    )

    parser.add_argument(
        "input",
        type=Path,
        nargs="?",
        help=(
            "Optional raw JSON file. If omitted, every JSON file under "
            "data/raw/<site>/ is processed."
        ),
    )

    parser.add_argument(
        "--raw",
        type=Path,
        default=Path(__file__).resolve().parents[2]
        / "ingestion"
        / "data"
        / "raw",
        help="Root directory containing raw crawl data.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "data"
        / "staging",
        help="Directory where staging data will be written.",
    )

    args = parser.parse_args()

    # If a specific file was supplied, process only that file.
    if args.input:
        if not args.input.is_file():
            raise FileNotFoundError(f"Raw file does not exist: {args.input}")

        output_dir = prepare_raw(args.input, args.output)

        print(f"Prepared: {args.input}")
        print(f"Output:   {output_dir}")
        print()
        print(f"  {output_dir / 'crawl.json'}")
        print(f"  {output_dir / 'products.jsonl'}")
        print(f"  {output_dir / 'variants.jsonl'}")

    # Otherwise process every site/crawl.
    else:
        process_all(args.raw, args.output)


if __name__ == "__main__":
    main()