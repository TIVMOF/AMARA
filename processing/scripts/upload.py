from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed"

load_dotenv(PROJECT_ROOT / ".env")


def upload_parquet() -> None:
    if not PROCESSED_ROOT.exists():
        raise FileNotFoundError(f"Processed directory '{PROCESSED_ROOT}' does not exist.")

    datasets = sorted(
        path for path in PROCESSED_ROOT.iterdir()
        if path.is_dir()
    )

    if not datasets:
        raise FileNotFoundError(
            f"No Parquet datasets found in '{PROCESSED_ROOT}'."
        )

    connection = snowflake.connector.connect(
        account=os.environ["AMARA_SNOWFLAKE_ACCOUNT"],
        user=os.environ["AMARA_SNOWFLAKE_USER"],
        token=os.environ["AMARA_SNOWFLAKE_TOKEN"],
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=os.environ["AMARA_SNOWFLAKE_WAREHOUSE"],
        database=os.environ["AMARA_SNOWFLAKE_DATABASE"],
        schema=os.environ["AMARA_SNOWFLAKE_SCHEMA"],
    )

    try:
        cursor = connection.cursor()

        for dataset in datasets:
            parquet_files = sorted(dataset.glob("part-*.parquet"))

            if not parquet_files:
                print(f"Skipping {dataset.name}: no Parquet files found.")
                continue

            print(f"\nUploading dataset: {dataset.name}")

            for file in parquet_files:
                print(f"  Uploading {file.name}...")

                cursor.execute(
                    f"PUT 'file://{file}' "
                    f"@AMARA_STAGE/{dataset.name}/ "
                    f"AUTO_COMPRESS=FALSE"
                )

                for row in cursor.fetchall():
                    print(f"    {row[0]} -> {row[2]}")

    finally:
        connection.close()


if __name__ == "__main__":
    upload_parquet()