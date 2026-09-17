from __future__ import annotations

import os

import snowflake.connector
from dotenv import load_dotenv

from . import paths


# Real environment variables already set take precedence - load_dotenv does not
# override by default - so a one-off run can point at another account without
# editing .env.
load_dotenv(paths.ENV_PATH)


def env(name: str) -> str:
    # Read a required AMARA_SNOWFLAKE_* setting.
    #
    # There are no fallbacks on purpose: a missing token should stop the run
    # rather than connect as whoever the environment happens to describe.
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")
    if not value:
        raise SystemExit(
            f"Missing environment variable: AMARA_SNOWFLAKE_{name}\n"
            f"  Expected it in {paths.ENV_PATH}\n"
            f"  Fix: cp {paths.ENV_PATH.parent}/.env.example {paths.ENV_PATH}"
        )
    return value


def connect(schema: str):
    # A connection bound to one schema.
    #
    # The schema is passed rather than read here because the three layers live
    # in three of them, and a script that connected to the wrong one would still
    # run - it would just write somewhere unexpected.
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=schema,
    )
