from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"

load_dotenv(PROJECT_ROOT / ".env")

def upload_retailer(retailer: str) -> None:
    retailer_root = RAW_ROOT / retailer

    if not retailer_root.exists():
        raise FileNotFoundError(f"Retailer folder '{retailer_root}' does not exist.")

    files = sorted(retailer_root.glob("*.json"))

    if not files:
        raise FileNotFoundError(f"No JSON files found in '{retailer_root}'.")

    connection = snowflake.connector.connect(
        account=os.environ["AMARA_SNOWFLAKE_ACCOUNT"],
        user=os.environ["AMARA_SNOWFLAKE_USER"],
        token=os.environ["AMARA_SNOWFLAKE_TOKEN"],
        warehouse=os.environ["AMARA_SNOWFLAKE_WAREHOUSE"],
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        database=os.environ["AMARA_SNOWFLAKE_DATABASE"],
        schema=os.environ["AMARA_SNOWFLAKE_SCHEMA"],
    )

    try:
        cursor = connection.cursor()

        for file in files:
            print(f"Uploading {file} to Snowflake...")

            cursor.execute(
                f"PUT 'file://{file}' "
                f"@AMARA_STAGE/{retailer}/ "
                f"AUTO_COMPRESS=FALSE"
            )

            for row in cursor.fetchall():
                print(f"  {row[0]} -> {row[2]}")

    finally:
        connection.close()

if __name__ == "__main__":
    upload_retailer("rickowens")