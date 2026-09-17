from __future__ import annotations

import shutil

from .paths import DATA_ROOTS


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
