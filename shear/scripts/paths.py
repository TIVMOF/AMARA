from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _dir(variable: str, default: Path) -> Path:
    # A data root, overridable from the environment.
    #
    # The default is where the data sits in a checkout, so a local run needs no
    # setup. The variable is how a container is told where its own storage is:
    # it names a directory inside whatever filesystem this process can see and
    # says nothing about what backs it - a docker volume, a bind mount or the
    # container's own writable layer are all the same to this code.
    value = os.getenv(variable)
    return Path(value) if value else default


# Written by gather, and only read here.
RAW_ROOT = _dir("AMARA_GATHERED_DIR", PROJECT_ROOT / "gather" / "data")

STAGING_ROOT = _dir("AMARA_SHEARED_DIR", PROJECT_ROOT / "shear" / "data")


def relative(path: Path) -> str:
    # A path as it reads in the run log.
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)
