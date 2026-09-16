from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)


def env(name: str) -> str:
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")

    if not value:
        raise SystemExit(
            f"Missing environment variable: AMARA_SNOWFLAKE_{name}"
        )

    return value


def connect():
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=env("PROCESSED_SCHEMA"),
    )


def column_list(columns: str) -> list[str]:
    # The column names, however the caller spaced them across lines.
    return [name.strip() for name in columns.split(",") if name.strip()]


def load_reference_table(
    cursor,
    table: str,
    columns: str,
    select: str,
    dataset: str,
) -> None:
    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")
    file_format = f"{database}.{schema}.PARQUET_FORMAT"

    print(f"Loading {table}...")

    cursor.execute(
        f"TRUNCATE TABLE {database}.{schema}.{table}"
    )

    cursor.execute(
        f"""
        COPY INTO {database}.{schema}.{table} ({columns})
        FROM (
            SELECT
                {select}
            FROM @{database}.{schema}.AMARA_STAGE/{dataset}/
            (FILE_FORMAT => {file_format})
        )
        """
    )


def load_historical_table(
    cursor,
    table: str,
    columns: str,
    select: str,
    dataset: str,
    merge_condition: str,
    update: str | None = None,
) -> None:
    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")
    file_format = f"{database}.{schema}.PARQUET_FORMAT"

    temp_table = f"{table}_LOAD"
    names = column_list(columns)
    insert = ", ".join(names)
    # Every value qualified with source. A bare name here is ambiguous: the
    # temp table is LIKE the target, so both sides carry the same columns.
    source_values = ", ".join(f"source.{name}" for name in names)

    print(f"Loading {table}...")

    cursor.execute(
        f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} "
        f"LIKE {database}.{schema}.{table}"
    )

    cursor.execute(
        f"""
        COPY INTO {temp_table} ({columns})
        FROM (
            SELECT
                {select}
            FROM @{database}.{schema}.AMARA_STAGE/{dataset}/
            (FILE_FORMAT => {file_format})
        )
        """

    )

    if update:
        cursor.execute(
            f"""
            MERGE INTO {database}.{schema}.{table} AS target
            USING {temp_table} AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET {update}
            WHEN NOT MATCHED THEN
                INSERT ({insert})
                VALUES ({source_values})
            """
        )
    else:
        cursor.execute(
            f"""
            MERGE INTO {database}.{schema}.{table} AS target
            USING {temp_table} AS source
            ON {merge_condition}
            WHEN NOT MATCHED THEN
                INSERT ({insert})
                VALUES ({source_values})
            """
        )


def create_file_format(cursor) -> None:
    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")

    cursor.execute(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS
            {database}.{schema}.PARQUET_FORMAT
            TYPE = PARQUET
        """
    )


def load() -> None:
    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")

    connection = connect()

    try:
        with connection.cursor() as cursor:
            create_file_format(cursor)

            # ---------------------------------------------------------
            # Reference / current-state tables
            # ---------------------------------------------------------

            load_reference_table(
                cursor,
                "BRANDS",
                "NAME, SEGMENT, TIER",
                "$1:name::VARCHAR, $1:segment::VARCHAR, $1:tier::VARCHAR",
                "brands",
            )

            load_reference_table(
                cursor,
                "CATEGORIES",
                "NAME",
                "$1:name::VARCHAR",
                "categories",
            )

            load_reference_table(
                cursor,
                "COUNTRIES",
                "CODE, NAME",
                "$1:code::VARCHAR, $1:name::VARCHAR",
                "countries",
            )

            load_reference_table(
                cursor,
                "CURRENCIES",
                "NAME",
                "$1:name::VARCHAR",
                "currencies",
            )

            load_reference_table(
                cursor,
                "GENDERS",
                "NAME",
                "$1:name::VARCHAR",
                "genders",
            )

            load_reference_table(
                cursor,
                "RETAILERS",
                "NAME, URL, COUNTRY",
                "$1:name::VARCHAR, $1:url::VARCHAR, $1:country::VARCHAR",
                "retailers",
            )

            load_reference_table(
                cursor,
                "SEGMENTS",
                "NAME",
                "$1:name::VARCHAR",
                "segments",
            )

            load_reference_table(
                cursor,
                "TIERS",
                "NAME",
                "$1:name::VARCHAR",
                "tiers",
            )

            # ---------------------------------------------------------
            # Historical tables
            # ---------------------------------------------------------

            load_historical_table(
                cursor,
                "CRAWLS",
                """
                SITE,
                BASE_URL,
                CURRENCY,
                NAME,
                DATE,
                PRODUCTS_RECEIVED,
                PRODUCTS_STORED,
                PAGES,
                SHORT_PAGES,
                COLLECTIONS_CRAWLED
                """,
                """
                $1:site::VARCHAR,
                $1:base_url::VARCHAR,
                $1:currency::VARCHAR,
                $1:name::VARCHAR,
                $1:date::DATE,
                $1:products_received::NUMBER(38,0),
                $1:products_stored::NUMBER(38,0),
                $1:pages::NUMBER(38,0),
                $1:short_pages::NUMBER(38,0),
                $1:collections_crawled::NUMBER(38,0)
                """,
                "crawls",
                """
                target.SITE = source.SITE
                AND target.DATE = source.DATE
                """,
            )

            load_historical_table(
                cursor,
                "DATES",
                """
                DATE,
                DAY,
                WEEK,
                MONTH,
                QUARTER,
                YEAR,
                SEASON
                """,
                """
                $1:date::DATE,
                $1:day::NUMBER(38,0),
                $1:week::NUMBER(38,0),
                $1:month::NUMBER(38,0),
                $1:quarter::NUMBER(38,0),
                $1:year::NUMBER(38,0),
                $1:season::VARCHAR
                """,
                "dates",
                "target.DATE = source.DATE",
            )

            load_historical_table(
                cursor,
                "PRODUCTS",
                """
                PRODUCT,
                RETAILER,
                NAME,
                BRAND,
                CATEGORY,
                GENDER,
                COLOR,
                MATERIAL,
                DATE
                """,
                """
                $1:product::VARCHAR,
                $1:retailer::VARCHAR,
                $1:name::VARCHAR,
                $1:brand::VARCHAR,
                $1:category::VARCHAR,
                $1:gender::VARCHAR,
                $1:color::VARCHAR,
                $1:material::VARCHAR,
                $1:date::DATE
                """,
                "products",
                """
                target.PRODUCT = source.PRODUCT
                AND target.RETAILER = source.RETAILER
                AND target.DATE = source.DATE
                """,
            )

            load_historical_table(
                cursor,
                "VARIANTS",
                """
                VARIANT,
                PRODUCT,
                RETAILER,
                DATE,
                SKU,
                SIZE,
                COLOR,
                PRICE,
                ORIGINAL_PRICE,
                CURRENCY,
                DISCOUNT,
                AVAILABLE
                """,
                """
                $1:variant::VARCHAR,
                $1:product::VARCHAR,
                $1:retailer::VARCHAR,
                $1:date::DATE,
                $1:sku::VARCHAR,
                $1:size::VARCHAR,
                $1:color::VARCHAR,
                $1:price::NUMBER(12,2),
                $1:original_price::NUMBER(12,2),
                $1:currency::VARCHAR,
                $1:discount::NUMBER(20,2),
                $1:available::BOOLEAN
                """,
                "variants",
                """
                target.VARIANT = source.VARIANT
                AND target.PRODUCT = source.PRODUCT
                AND target.RETAILER = source.RETAILER
                AND target.DATE = source.DATE
                """,
            )

            connection.commit()

    finally:
        connection.close()


if __name__ == "__main__":
    load()