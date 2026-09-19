from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import paths, raw


def find_raw_crawl(collected_root: Path) -> list[Path]:
    """Return every raw retailer JSON in the current active crawl."""
    if collected_root.is_file():
        return [collected_root]
    if not collected_root.is_dir():
        raise SystemExit(
            f"no raw crawl directory at {paths.relative(collected_root)}\n"
            f"  Nothing to cut. Run the get stage first: `python get.py crawl`,\n"
            f"  or in a container: `docker run --init -v amara_storage:/data amara-get crawl`"
        )

    raw_files = sorted(collected_root.glob("*.json"))
    if not raw_files:
        raise SystemExit(
            f"no raw crawl found under {paths.relative(collected_root)}\n"
            f"  The directory is there but empty, so the get stage has not run -\n"
            f"  or its output was already cleaned up. Run `python get.py crawl` first."
        )

    # A single crawl is every retailer file currently in the collected
    # directory, named <retailer>-<stamp>.json. One run stamps them all alike,
    # and cleanup empties the directory once they are uploaded.
    return raw_files


# ── writing what a dismantled crawl looks like ──────────────────────────────

def crawl_directory(dismantled_root: Path, site: str, scraped_at: str) -> Path:
    # Where one crawl's files go.
    #
    # Named by the crawl timestamp, so re-running overwrites that crawl rather
    # than accumulating copies of it.
    timestamp = scraped_at.replace(":", "").replace("-", "")
    directory = dismantled_root / site / timestamp
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_json(path: Path, record: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(record, file, ensure_ascii=False, indent=2)
        file.write("\n")


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            json.dump(record, file, ensure_ascii=False, separators=(",", ":"))
            file.write("\n")


def dismantle(raw_path: Path, dismantled_root: Path) -> tuple[Path, int, int]:
    # Cut one raw crawl into three. Returns (directory, products, variants).
    document = raw.load(raw_path)
    directory = crawl_directory(
        dismantled_root, document["site"], document["scraped_at"]
    )
    products, variants = raw.product_records(document)

    write_json(directory / "crawl.json", raw.crawl_record(document))
    write_jsonl(directory / "products.jsonl", products)
    write_jsonl(directory / "variants.jsonl", variants)

    return directory, len(products), len(variants)
