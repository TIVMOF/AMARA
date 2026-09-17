from __future__ import annotations

from pathlib import Path


# parents[2] is the repository root: this module lives in hang/scripts/.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPONENT_ROOT = Path(__file__).resolve().parents[1]

# What the other stages leave behind, and what this one uploads.
RAW_ROOT = PROJECT_ROOT / "gather" / "data"
PROCESSED_ROOT = PROJECT_ROOT / "stitch" / "data"

# Cleared once a crawl is safely in Snowflake.
DATA_ROOTS = (
    PROJECT_ROOT / "gather" / "data",
    PROJECT_ROOT / "shear" / "data",
    PROJECT_ROOT / "stitch" / "data",
)

ENV_PATH = COMPONENT_ROOT / ".env"


def relative(path: Path) -> str:
    # A path as written in the docs, when it is under the repository.
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)
