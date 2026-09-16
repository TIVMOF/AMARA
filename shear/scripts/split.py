from __future__ import annotations

from pathlib import Path

from . import paths, raw, staging


def find_single_raw(raw_root: Path) -> list[Path]:
    """Return every raw retailer JSON in the current active crawl."""
    if raw_root.is_file():
        return [raw_root]
    if not raw_root.is_dir():
        raise SystemExit(f"no raw crawl directory at {paths.relative(raw_root)}")

    raw_files = sorted(raw_root.glob("*.json"))
    if not raw_files:
        raise SystemExit(f"no raw crawl found under {paths.relative(raw_root)}")

    # A single crawl is every retailer file currently in gather/data, named
    # <retailer>-<stamp>.json. One run stamps them all alike, and cleanup.py
    # empties the directory once they are uploaded.
    return raw_files


def shear(raw_path: Path, staging_root: Path) -> tuple[Path, int, int]:
    # Cut one raw crawl into three. Returns (directory, products, variants).
    document = raw.load(raw_path)
    directory = staging.crawl_directory(
        staging_root, document["site"], document["scraped_at"]
    )
    products, variants = raw.product_records(document)

    staging.write_json(directory / "crawl.json", raw.crawl_record(document))
    staging.write_jsonl(directory / "products.jsonl", products)
    staging.write_jsonl(directory / "variants.jsonl", variants)

    return directory, len(products), len(variants)
