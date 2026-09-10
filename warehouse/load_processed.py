from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)


def env(name: str) -> str:
    value = os.getenv(f"AMARA_SNOWFLAKE_{name}")

    if not value:
        raise SystemExit(
            f"AMARA_SNOWFLAKE_{name} is not set.\n"
            f"Expected it in {ENV_PATH}"
        )

    return value


def connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=env("ACCOUNT"),
        user=env("USER"),
        token=env("TOKEN"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=env("WAREHOUSE"),
        database=env("DATABASE"),
        schema=env("PROCESSED_SCHEMA"),
    )


def load_table(cursor: snowflake.connector.cursor.SnowflakeCursor, table: str, columns: str, select: str, dataset: str ) -> None:
    database = env("DATABASE")
    schema = env("PROCESSED_SCHEMA")
    file_format = f"{database}.{schema}.PARQUET_FORMAT"

    print(f"  Loading {table}...")

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

    results = cursor.fetchall()

    loaded = sum(
        int(row[3])
        for row in results
        if row[6] == "LOADED"
    )

    print(f"    ✓ {loaded:,} row(s) loaded")


def load() -> None:
    connection = connect()

    try:
        cursor = connection.cursor()

        database = env("DATABASE")
        schema = env("PROCESSED_SCHEMA")

        # The file format must already exist.
        cursor.execute(
            f"""
            USE DATABASE {database}
            """
        )

        cursor.execute(
            f"""
            CREATE FILE FORMAT IF NOT EXISTS
                {database}.{schema}.PARQUET_FORMAT
                TYPE = PARQUET
            """
        )

        # ------------------------------------------------------------------
        # BRANDS
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "BRANDS",
            """
            NAME,
            SEGMENT,
            TIER
            """,
            """
            $1:name::VARCHAR,
            $1:segment::VARCHAR,
            $1:tier::VARCHAR
            """,
            "brands",
        )

        # ------------------------------------------------------------------
        # CATEGORIES
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "CATEGORIES",
            """
            NAME
            """,
            """
            $1:name::VARCHAR
            """,
            "categories",
        )

        # ------------------------------------------------------------------
        # COUNTRIES
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "COUNTRIES",
            """
            CODE,
            NAME
            """,
            """
            $1:code::VARCHAR,
            $1:name::VARCHAR
            """,
            "countries",
        )

        # ------------------------------------------------------------------
        # CRAWLS
        # ------------------------------------------------------------------

        load_table(
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
            $1:products_received::INTEGER,
            $1:products_stored::INTEGER,
            $1:pages::INTEGER,
            $1:short_pages::INTEGER,
            $1:collections_crawled::INTEGER
            """,
            "crawls",
        )

        # ------------------------------------------------------------------
        # CURRENCIES
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "CURRENCIES",
            """
            NAME
            """,
            """
            $1:name::VARCHAR
            """,
            "currencies",
        )

        # ------------------------------------------------------------------
        # DATES
        # ------------------------------------------------------------------

        load_table(
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
            $1:day::INTEGER,
            $1:week::INTEGER,
            $1:month::INTEGER,
            $1:quarter::INTEGER,
            $1:year::INTEGER,
            $1:season::VARCHAR
            """,
            "dates",
        )

        # ------------------------------------------------------------------
        # GENDERS
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "GENDERS",
            """
            NAME
            """,
            """
            $1:name::VARCHAR
            """,
            "genders",
        )

        # ------------------------------------------------------------------
        # PRODUCTS
        # ------------------------------------------------------------------

        load_table(
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
            MATERIAL
            """,
            """
            $1:product::VARCHAR,
            $1:retailer::VARCHAR,
            $1:name::VARCHAR,
            $1:brand::VARCHAR,
            $1:category::VARCHAR,
            $1:gender::VARCHAR,
            $1:color::VARCHAR,
            $1:material::VARCHAR
            """,
            "products",
        )

        # ------------------------------------------------------------------
        # RETAILERS
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "RETAILERS",
            """
            NAME,
            URL,
            COUNTRY
            """,
            """
            $1:name::VARCHAR,
            $1:url::VARCHAR,
            $1:country::VARCHAR
            """,
            "retailers",
        )

        # ------------------------------------------------------------------
        # SEGMENTS
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "SEGMENTS",
            """
            NAME
            """,
            """
            $1:name::VARCHAR
            """,
            "segments",
        )

        # ------------------------------------------------------------------
        # TIERS
        # ------------------------------------------------------------------

        load_table(
            cursor,
            "TIERS",
            """
            NAME
            """,
            """
            $1:name::VARCHAR
            """,
            "tiers",
        )

        # ------------------------------------------------------------------
        # VARIANTS
        # ------------------------------------------------------------------

        load_table(
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
        )

        print("\n✓ Processed data loaded successfully.")

    finally:
        connection.close()


if __name__ == "__main__":
    load()