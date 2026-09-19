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


def _where_to_set_it(env_path: Path) -> str:
    # Point at whatever is actually there. A container has no .env and no
    # .env.example - its credentials arrive through the environment - so
    # naming a file that does not exist sends you looking for the wrong thing.
    if env_path.exists():
        return f"  Set it in the environment, or add it to {env_path}"
    example = env_path.with_name(".env.example")
    if example.exists():
        return ("  Set it in the environment, or for a local run:\n"
                f"    cp {example} {env_path}")
    return "  Pass it at run time: docker run -e ..., or an Airflow secret"


def env(name: str) -> str:
    # Read a required AMARA_SNOWFLAKE_* setting.
    #
    # There are no fallbacks on purpose: a missing token should stop the run
    # rather than connect as whoever the environment happens to describe.
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")
    if not value:
        raise SystemExit(
            f"Missing environment variable: AMARA_SNOWFLAKE_{name}\n"
            + _where_to_set_it(ENV_PATH)
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
