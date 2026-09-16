from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def crawl_directory(staging_root: Path, site: str, scraped_at: str) -> Path:
    # Where one crawl's files go.
    #
    # Named by the crawl timestamp, so re-running overwrites that crawl rather
    # than accumulating copies of it.
    timestamp = scraped_at.replace(":", "").replace("-", "")
    directory = staging_root / site / timestamp
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
