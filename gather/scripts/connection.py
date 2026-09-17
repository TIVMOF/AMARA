from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


# This component's own .env, not a shared one. Each stage of AMARA carries its
# own credentials and requirements so it can be run - or deployed - on its own.
# The cost is that the access token exists in more than one file; rotating it
# means rotating all of them.
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

# Real environment variables already set take precedence - load_dotenv does not
# override by default - so a one-off run can point at another account without
# editing .env.
load_dotenv(ENV_PATH)


def env(name: str) -> str:
    # Read a required AMARA_SNOWFLAKE_* setting.
    #
    # There are no fallbacks on purpose: a missing token should stop the run
    # rather than connect as whoever the environment happens to describe.
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")
    if not value:
        raise SystemExit(
            f"Missing environment variable: AMARA_SNOWFLAKE_{name}\n"
            f"  Expected it in {ENV_PATH}\n"
            f"  Fix: cp {ENV_PATH.parent}/.env.example {ENV_PATH}"
        )
    return value


def connect(schema: str):
    # A connection bound to one schema, which is passed rather than read here:
    # a script that connected to the wrong one would still run and just write
    # somewhere unexpected.
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=schema,
    )
