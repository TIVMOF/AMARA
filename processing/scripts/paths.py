"""Where everything lives on disk."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_ROOT = PROJECT_ROOT / "dismantling" / "data" / "staging"

REFERENCE_ROOT = PROJECT_ROOT / "processing" / "reference"

OUTPUT_ROOT = PROJECT_ROOT / "processing" / "data" / "processed"


def relative(path: Path) -> str:
    """A path as it reads in the run log."""
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)
