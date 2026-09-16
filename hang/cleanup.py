from __future__ import annotations

import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOTS = (
    PROJECT_ROOT / "gather" / "data",
    PROJECT_ROOT / "shear" / "data",
    PROJECT_ROOT / "stitch" / "data",
)


def clear_data() -> None:
    """Remove temporary crawl data while preserving its data directories."""
    for root in DATA_ROOTS:
        if not root.is_dir():
            continue
        for path in root.iterdir():
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()


if __name__ == "__main__":
    clear_data()