from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# parents[1] is the component root: this module lives in cut/scripts/.
COMPONENT_ROOT = Path(__file__).resolve().parents[1]


def storage_dir(name: str, default: Path) -> Path:
    # A stage directory inside the one shared storage, overridable together.
    #
    # The whole pipeline shares a single volume, mounted at AMARA_STORAGE_DIR,
    # with a directory per stage inside it: get writes `collected`, cut reads
    # it and writes `dismantled` and `trimmed`. One variable rather than one
    # per directory, because there is one volume and they move together.
    #
    # It names a directory inside whatever filesystem this process can see and
    # says nothing about what backs it - a docker volume, a bind mount or the
    # container's own writable layer are all the same to this code. Unset -
    # which is how a checkout runs - each falls back to the path beside the
    # code, so a local run behaves exactly as it always has.
    root = os.getenv("AMARA_STORAGE_DIR")
    return Path(root) / name if root else default


# Written by get, and only read here.
COLLECTED_ROOT = storage_dir("collected", PROJECT_ROOT / "get" / "data")

# This stage's own intermediate: raw JSON cut into the shape Spark can read.
# Nothing downstream touches it - sew works from Snowflake - so it never
# leaves this stage, and `cut cleanup` empties it.
DISMANTLED_ROOT = storage_dir("dismantled", COMPONENT_ROOT / "data" / "dismantled")

# The parquet tables, which `cut upload` puts into the PROCESSED stage.
TRIMMED_ROOT = storage_dir("trimmed", COMPONENT_ROOT / "data" / "trimmed")

# The vocabularies, deliberately NOT part of the storage: they are versioned
# config that ships beside this code, not data that arrives from somewhere else.
REFERENCE_ROOT = COMPONENT_ROOT / "reference"


def relative(path: Path) -> str:
    # A path as it reads in the run log.
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)
