from __future__ import annotations

import json
from pathlib import Path

from . import paths, raw, staging


def dismantle(raw_path: Path, staging_root: Path) -> tuple[Path, int, int]:
    # Take one raw crawl apart. Returns (directory, products, variants).
    document = raw.load(raw_path)
    directory = staging.crawl_directory(
        staging_root, document["site"], document["scraped_at"]
    )
    products, variants = raw.product_records(document)

    staging.write_json(directory / "crawl.json", raw.crawl_record(document))
    staging.write_jsonl(directory / "products.jsonl", products)
    staging.write_jsonl(directory / "variants.jsonl", variants)

    return directory, len(products), len(variants)


def dismantle_all(raw_root: Path, staging_root: Path) -> int:
    # Every raw crawl under `raw_root`. Returns how many failed.
    raw_files = sorted(raw_root.glob("*/*.json"))
    if not raw_files:
        print(f"No raw crawls found in {paths.relative(raw_root)}")
        return 0

    print(f"{len(raw_files)} raw crawl(s) to dismantle\n")
    failures = 0

    for raw_file in raw_files:
        try:
            directory, products, variants = dismantle(raw_file, staging_root)
        except (KeyError, json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            # One malformed crawl should not stop the other forty-nine.
            failures += 1
            print(f"  FAILED  {raw_file.parent.name:18} {type(exc).__name__}: {exc}")
            continue

        print(f"  {raw_file.parent.name:18} {products:>7,} products  "
              f"{variants:>8,} variants  -> {paths.relative(directory)}")

    print(f"\nDismantled {len(raw_files) - failures}/{len(raw_files)}")
    return failures
