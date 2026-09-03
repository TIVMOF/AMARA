from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_ROOT = PROJECT_ROOT / "ingestion" / "data" / "raw"

STAGING_ROOT = PROJECT_ROOT / "dismantling" / "data" / "staging"


def relative(path: Path) -> str:
    # A path as it reads in the run log.
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)
