from __future__ import annotations

from pathlib import Path


# parents[1] is the component root: this module lives in sew/scripts/.
COMPONENT_ROOT = Path(__file__).resolve().parents[1]

# This stage's own credentials. It reads no other file from disk: everything it
# works on is already in Snowflake, put there by get and cut.
ENV_PATH = COMPONENT_ROOT / ".env"
