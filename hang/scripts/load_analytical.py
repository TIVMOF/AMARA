from __future__ import annotations

from .connection import connect, env


def merge_reference_dimensions(cursor) -> None:
    """
    Merge current processed reference data into persistent analytical
    dimensions.

    Existing dimension rows keep their surrogate keys.
    New business keys are inserted.

    Brands and retailers are also updated when their descriptive
    properties changed.
    """

    # ------------------------------------------------------------------
    # SEGMENT
    # ------------------------------------------------------------------

    cursor.execute(
        """
        MERGE INTO DIM_SEGMENT AS target
        USING (
            SELECT DISTINCT NAME
            FROM AMARA.PROCESSED.SEGMENTS
            WHERE NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN NOT MATCHED THEN
            INSERT (NAME)
            VALUES (source.NAME)
        """
    )

    # ------------------------------------------------------------------
    # TIER
    # ------------------------------------------------------------------

    cursor.execute(
        """
        MERGE INTO DIM_TIER AS target
        USING (
            SELECT DISTINCT NAME
            FROM AMARA.PROCESSED.TIERS
            WHERE NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN NOT MATCHED THEN
            INSERT (NAME)
            VALUES (source.NAME)
        """
    )

    # ------------------------------------------------------------------
    # CATEGORY
    # ------------------------------------------------------------------

    cursor.execute(
        """
        MERGE INTO DIM_CATEGORY AS target
        USING (
            SELECT DISTINCT NAME
            FROM AMARA.PROCESSED.CATEGORIES
            WHERE NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN NOT MATCHED THEN
            INSERT (NAME)
            VALUES (source.NAME)
        """
    )

    # ------------------------------------------------------------------
    # COUNTRY
    # ------------------------------------------------------------------

    cursor.execute(
        """
        MERGE INTO DIM_COUNTRY AS target
        USING (
            SELECT DISTINCT CODE, NAME
            FROM AMARA.PROCESSED.COUNTRIES
            WHERE CODE IS NOT NULL
        ) AS source
        ON target.CODE = source.CODE

        WHEN NOT MATCHED THEN
            INSERT (CODE, NAME)
            VALUES (source.CODE, source.NAME)
        """
    )

    # ------------------------------------------------------------------
    # CURRENCY
    # ------------------------------------------------------------------

    cursor.execute(
        """
        MERGE INTO DIM_CURRENCY AS target
        USING (
            SELECT DISTINCT NAME
            FROM AMARA.PROCESSED.CURRENCIES
            WHERE NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN NOT MATCHED THEN
            INSERT (NAME)
            VALUES (source.NAME)
        """
    )

    # ------------------------------------------------------------------
    # GENDER
    # ------------------------------------------------------------------

    cursor.execute(
        """
        MERGE INTO DIM_GENDER AS target
        USING (
            SELECT DISTINCT NAME
            FROM AMARA.PROCESSED.GENDERS
            WHERE NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN NOT MATCHED THEN
            INSERT (NAME)
            VALUES (source.NAME)
        """
    )

    # ------------------------------------------------------------------
    # BRAND
    # ------------------------------------------------------------------
    #
    # Brand identity is NAME.
    #
    # If the brand already exists, preserve BRAND_KEY but update its
    # SEGMENT_KEY and TIER_KEY in case the processed data changed.
    #

    cursor.execute(
        """
        MERGE INTO DIM_BRAND AS target
        USING (
            SELECT
                b.NAME,
                s.SEGMENT_KEY,
                t.TIER_KEY
            FROM AMARA.PROCESSED.BRANDS AS b

            LEFT JOIN DIM_SEGMENT AS s
                ON s.NAME = b.SEGMENT

            LEFT JOIN DIM_TIER AS t
                ON t.NAME = b.TIER

            WHERE b.NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN MATCHED THEN
            UPDATE SET
                target.SEGMENT_KEY = source.SEGMENT_KEY,
                target.TIER_KEY = source.TIER_KEY

        WHEN NOT MATCHED THEN
            INSERT (
                NAME,
                SEGMENT_KEY,
                TIER_KEY
            )
            VALUES (
                source.NAME,
                source.SEGMENT_KEY,
                source.TIER_KEY
            )
        """
    )

    # ------------------------------------------------------------------
    # RETAILER
    # ------------------------------------------------------------------
    #
    # Retailer identity is NAME.
    #
    # Existing retailer keeps RETAILER_KEY but URL and COUNTRY_KEY are
    # updated when the processed layer contains newer information.
    #

    cursor.execute(
        """
        MERGE INTO DIM_RETAILER AS target
        USING (
            SELECT
                r.NAME,
                r.URL,
                c.COUNTRY_KEY
            FROM AMARA.PROCESSED.RETAILERS AS r

            LEFT JOIN DIM_COUNTRY AS c
                ON c.CODE = r.COUNTRY

            WHERE r.NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME

        WHEN MATCHED THEN
            UPDATE SET
                target.URL = source.URL,
                target.COUNTRY_KEY = source.COUNTRY_KEY

        WHEN NOT MATCHED THEN
            INSERT (
                NAME,
                URL,
                COUNTRY_KEY
            )
            VALUES (
                source.NAME,
                source.URL,
                source.COUNTRY_KEY
            )
        """
    )


def merge_dates(cursor) -> None:
    """
    Append dates that do not already exist in DIM_DATE.
    """

    cursor.execute(
        """
        MERGE INTO DIM_DATE AS target
        USING (
            SELECT DISTINCT
                DATE,
                DAY,
                WEEK,
                MONTH,
                QUARTER,
                SEASON,
                YEAR
            FROM AMARA.PROCESSED.DATES
            WHERE DATE IS NOT NULL
        ) AS source
        ON target.DATE = source.DATE

        WHEN NOT MATCHED THEN
            INSERT (
                DATE,
                DAY,
                WEEK,
                MONTH,
                QUARTER,
                SEASON,
                YEAR
            )
            VALUES (
                source.DATE,
                source.DAY,
                source.WEEK,
                source.MONTH,
                source.QUARTER,
                source.SEASON,
                source.YEAR
            )
        """
    )


def merge_products(cursor) -> None:
    """
    Append new product observations.

    Product grain:
        PRODUCT_ID + RETAILER + DATE
    """

    cursor.execute(
        """
        MERGE INTO DIM_PRODUCT AS target
        USING (
            SELECT
                p.PRODUCT AS PRODUCT_ID,
                p.NAME,
                b.BRAND_KEY,
                r.RETAILER_KEY,
                c.CATEGORY_KEY,
                g.GENDER_KEY,
                p.COLOR,
                p.MATERIAL,
                d.DATE_KEY
            FROM AMARA.PROCESSED.PRODUCTS AS p

            LEFT JOIN DIM_BRAND AS b
                ON b.NAME = p.BRAND

            LEFT JOIN DIM_RETAILER AS r
                ON r.NAME = p.RETAILER

            LEFT JOIN DIM_CATEGORY AS c
                ON c.NAME = p.CATEGORY

            LEFT JOIN DIM_GENDER AS g
                ON g.NAME = p.GENDER

            LEFT JOIN DIM_DATE AS d
                ON d.DATE = p.DATE

            WHERE p.PRODUCT IS NOT NULL
              AND p.RETAILER IS NOT NULL
              AND p.DATE IS NOT NULL
        ) AS source
        ON target.PRODUCT_ID = source.PRODUCT_ID
        AND target.RETAILER_KEY = source.RETAILER_KEY
        AND target.DATE_KEY = source.DATE_KEY

        WHEN NOT MATCHED THEN
            INSERT (
                PRODUCT_ID,
                NAME,
                BRAND_KEY,
                RETAILER_KEY,
                CATEGORY_KEY,
                GENDER_KEY,
                COLOR,
                MATERIAL,
                DATE_KEY
            )
            VALUES (
                source.PRODUCT_ID,
                source.NAME,
                source.BRAND_KEY,
                source.RETAILER_KEY,
                source.CATEGORY_KEY,
                source.GENDER_KEY,
                source.COLOR,
                source.MATERIAL,
                source.DATE_KEY
            )
        """
    )


def merge_facts(cursor) -> None:
    """
    Append new variant observations.

    Fact grain:
        VARIANT + PRODUCT + RETAILER + DATE
    """

    cursor.execute(
        """
        MERGE INTO FACT_PRODUCT_OBSERVATION AS target

        USING (
            SELECT
                v.PRODUCT AS PRODUCT_ID,
                v.VARIANT AS VARIANT_ID,

                p.PRODUCT_KEY,

                p.BRAND_KEY,
                r.RETAILER_KEY,
                p.CATEGORY_KEY,
                d.DATE_KEY,

                v.SKU,
                v.SIZE,
                v.COLOR,
                v.PRICE,
                v.ORIGINAL_PRICE,
                cu.CURRENCY_KEY,
                v.DISCOUNT,
                v.AVAILABLE

            FROM AMARA.PROCESSED.VARIANTS AS v

            INNER JOIN DIM_RETAILER AS r
                ON r.NAME = v.RETAILER

            INNER JOIN DIM_DATE AS d
                ON d.DATE = v.DATE

            INNER JOIN DIM_PRODUCT AS p
                ON p.PRODUCT_ID = v.PRODUCT
                AND p.RETAILER_KEY = r.RETAILER_KEY
                AND p.DATE_KEY = d.DATE_KEY

            LEFT JOIN DIM_CURRENCY AS cu
                ON cu.NAME = v.CURRENCY

            WHERE v.VARIANT IS NOT NULL
              AND v.PRODUCT IS NOT NULL
              AND v.RETAILER IS NOT NULL
              AND v.DATE IS NOT NULL

        ) AS source

        ON target.VARIANT_ID = source.VARIANT_ID
        AND target.PRODUCT_ID = source.PRODUCT_ID
        AND target.RETAILER_KEY = source.RETAILER_KEY
        AND target.DATE_KEY = source.DATE_KEY

        WHEN NOT MATCHED THEN
            INSERT (
                PRODUCT_ID,
                VARIANT_ID,
                PRODUCT_KEY,
                BRAND_KEY,
                RETAILER_KEY,
                CATEGORY_KEY,
                DATE_KEY,
                SKU,
                SIZE,
                COLOR,
                PRICE,
                ORIGINAL_PRICE,
                CURRENCY_KEY,
                DISCOUNT,
                AVAILABLE
            )
            VALUES (
                source.PRODUCT_ID,
                source.VARIANT_ID,
                source.PRODUCT_KEY,
                source.BRAND_KEY,
                source.RETAILER_KEY,
                source.CATEGORY_KEY,
                source.DATE_KEY,
                source.SKU,
                source.SIZE,
                source.COLOR,
                source.PRICE,
                source.ORIGINAL_PRICE,
                source.CURRENCY_KEY,
                source.DISCOUNT,
                source.AVAILABLE
            )
        """
    )


def load() -> None:
    connection = connect(env("ANALYTICAL_SCHEMA"))

    try:
        with connection.cursor() as cursor:
            print("Loading analytical reference dimensions...")
            merge_reference_dimensions(cursor)

            print("Loading dates...")
            merge_dates(cursor)

            print("Loading products...")
            merge_products(cursor)

            print("Loading product observations...")
            merge_facts(cursor)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
