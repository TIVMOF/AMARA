"""The model's data-bearing tables.

Each function takes staged data plus the vocabularies and returns one table of
`img/amara-analystical-data-diagram.png`. Names are upper case throughout;
Snowflake derives the lookups and every key from these columns.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    col,
    concat_ws,
    dayofmonth,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    quarter,
    round as spark_round,
    to_date,
    to_timestamp,
    transform as array_transform,
    try_element_at,
    weekofyear,
    when,
    year,
)

from .clean import (
    clean_text,
    lookup,
    merge,
    name,
    one_of,
    positive,
    season_of,
)
from .reference import Reference


# Option names a store may use for the same thing, matched case-insensitively.
SIZE_OPTIONS = ("size", "sizes", "taille", "size (us)", "us size", "uk size",
                "eu size", "shoe size", "clothing size", "cup size")

COLOR_OPTIONS = ("color", "colour", "colorway", "colourway")

MATERIAL_OPTIONS = ("material", "materials", "fabric")

GENDER_OPTIONS = ("gender", "sex")

# A discount steeper than this is a data-entry artifact, not a sale: Stadium
# Goods lists a 190.00 sneaker against a compare_at_price of 25,542,668.00.
MAX_PLAUSIBLE_DISCOUNT = 95


# ── option helpers ─────────────────────────────────────────────────────────────

def option_values(options: Column, names: tuple[str, ...]) -> Column:
    """The values of the first option named one of `names`.

    Shopify options are an array of {name, position, values} and the name is
    the only reliable way in - which slot an option occupies varies by store.
    """
    from pyspark.sql.functions import filter as array_filter, lower, trim

    matching = array_filter(options, lambda o: lower(trim(o["name"])).isin(list(names)))
    return try_element_at(matching, lit(1))["values"]


def option_position(options: Column, names: tuple[str, ...]) -> Column:
    """The 1-based slot a named option occupies, for reading variant.optionN.

    Cast to int because JSON inference makes it a bigint, which `element_at`
    rejects as an index.
    """
    from pyspark.sql.functions import filter as array_filter, lower, trim

    matching = array_filter(options, lambda o: lower(trim(o["name"])).isin(list(names)))
    return try_element_at(matching, lit(1))["position"].cast("int")


def joined_values(options: Column, names: tuple[str, ...]) -> Column:
    """A named option's values as one string, or null when it has none.

    Colour and material are open vocabularies - 9,315 colours, and materials
    that are fabric compositions like `74%WOOL,26%SILK` - so they are cleaned
    and kept as written rather than mapped onto a reference list.
    """
    joined = concat_ws(" / ", array_transform(option_values(options, names), name))
    return when(joined == "", None).otherwise(joined)


# ── tables ─────────────────────────────────────────────────────────────────────

def retailers(crawls: DataFrame, countries: Reference) -> DataFrame:
    """dim_retailer - name, url, country."""
    return (
        crawls
        .select(
            name(col("site")).alias("name"),
            clean_text(col("base_url")).alias("url"),
            lookup(col("country"), countries.lookup).alias("country"),
        )
        .dropDuplicates(["name"])
    )


def dates(crawls: DataFrame) -> DataFrame:
    """dim_date - one row per date any crawl observed."""
    return (
        crawls
        .select(to_date(to_timestamp(col("scraped_at"))).alias("date"))
        .where(col("date").isNotNull())
        .dropDuplicates(["date"])
        .select(
            "date",
            dayofmonth("date").alias("day"),
            weekofyear("date").alias("week"),
            month("date").alias("month"),
            quarter("date").alias("quarter"),
            season_of(month("date")).alias("season"),
            year("date").alias("year"),
        )
    )


def products(staged: DataFrame, brands: Reference, categories: Reference,
             genders: Reference) -> DataFrame:
    """dim_product - name, brand, category, gender, color, material.

    Products whose vendor is not on the brand allowlist are dropped here, which
    is what makes brands.yaml the allowlist as well as the classification.

    Gender is stated three ways and often more than once: a `Gender` option, a
    `Gender: Women` tag and a bare `mens` tag. All three are folded together,
    and a product declaring more than one is Unisex rather than whichever came
    first.
    """
    return (
        staged
        .select(
            col("id").cast("string").alias("product"),
            name(col("site")).alias("retailer"),
            clean_text(col("title")).alias("name"),
            lookup(col("vendor"), brands.lookup).alias("brand"),
            lookup(col("product_type"), categories.lookup).alias("category"),
            one_of(
                merge(option_values(col("options"), GENDER_OPTIONS), col("tags")),
                genders.lookup,
                several="UNISEX",
            ).alias("gender"),
            joined_values(col("options"), COLOR_OPTIONS).alias("color"),
            joined_values(col("options"), MATERIAL_OPTIONS).alias("material"),
            to_date(to_timestamp(col("scraped_at"))).alias("date"),
        )
        .where(col("brand").isNotNull())
        .dropDuplicates(["product", "retailer", "date"])
    )


def observations(dim_products: DataFrame, staged_variants: DataFrame) -> DataFrame:
    """fact_product_observation - price and availability, at product grain.

    The model puts sizes in their own bridge, so this is one row per product
    per date rather than per variant. A product's variants are the same garment
    in different sizes and almost always share a price, so the row carries the
    lowest - the price a shopper is quoted.

    `original_price` is set only where a store is genuinely charging less than
    its stated normal price. Shopify stores routinely set `compare_at_price`
    equal to `price`, or to zero, when nothing is on sale.
    """
    priced = (
        staged_variants
        .select(
            col("product_id").cast("string").alias("product"),
            name(col("site")).alias("retailer"),
            to_date(to_timestamp(col("scraped_at"))).alias("date"),
            positive(col("price").cast("decimal(12,2)")).alias("price"),
            positive(col("compare_at_price").cast("decimal(12,2)")).alias("compare_at"),
            col("available").cast("boolean").alias("available"),
        )
        .groupBy("product", "retailer", "date")
        .agg(
            spark_min("price").alias("price"),
            spark_max("compare_at").alias("compare_at"),
            spark_max("available").alias("available"),
        )
    )

    discounting = (col("compare_at") > col("price")) & (
        col("compare_at") <= col("price") / (1 - MAX_PLAUSIBLE_DISCOUNT / 100)
    )
    original = when(discounting, col("compare_at"))

    return (
        dim_products.alias("p")
        .join(priced.alias("v"), ["product", "retailer", "date"], "inner")
        .select(
            "product",
            col("p.brand").alias("brand"),
            col("p.category").alias("category"),
            "retailer",
            "date",
            col("v.price").alias("price"),
            original.alias("original_price"),
            spark_round(
                when(original.isNotNull(), (original - col("v.price")) / original * 100), 2
            ).alias("discount"),
            col("v.available").alias("available"),
        )
    )


def sizes(staged_products: DataFrame, staged_variants: DataFrame,
          dim_products: DataFrame) -> DataFrame:
    """size_to_product - one row per product per size it is sold in.

    Which of option1/2/3 holds the size differs per store, so the slot is read
    from the product's own option list. Products with no size option -
    fragrance, homeware - contribute nothing rather than a null size.

    Sizes are not a controlled vocabulary: 2,533 distinct values mixing alpha,
    US, EU and UK scales. They are cleaned and kept as written.
    """
    positions = staged_products.select(
        col("id").cast("string").alias("product"),
        name(col("site")).alias("retailer"),
        to_date(to_timestamp(col("scraped_at"))).alias("date"),
        option_position(col("options"), SIZE_OPTIONS).alias("slot"),
    ).where(col("slot").isNotNull())

    slots = staged_variants.select(
        col("product_id").cast("string").alias("product"),
        name(col("site")).alias("retailer"),
        to_date(to_timestamp(col("scraped_at"))).alias("date"),
        array(col("option1"), col("option2"), col("option3")).alias("slots"),
    )

    return (
        positions
        .join(slots, ["product", "retailer", "date"], "inner")
        # Only sizes of products that survived the brand allowlist.
        .join(dim_products.select("product", "retailer", "date"),
              ["product", "retailer", "date"], "left_semi")
        .select(
            "product", "retailer", "date",
            name(try_element_at(col("slots"), col("slot"))).alias("size"),
        )
        .where(col("size").isNotNull())
        .dropDuplicates(["product", "retailer", "date", "size"])
    )


# ── what did not match ─────────────────────────────────────────────────────────

def unmatched(staged: DataFrame, column: str, reference: Reference) -> DataFrame:
    """Raw values a vocabulary did not recognise, most frequent first.

    This report is how a reference file grows - anything here that should be
    reported on is a missing entry or a missing alias.
    """
    from pyspark.sql.functions import count

    return (
        staged
        .select(clean_text(col(column)).alias("value"),
                lookup(col(column), reference.lookup).alias("matched"))
        .where(col("value").isNotNull() & col("matched").isNull())
        .groupBy("value")
        .agg(count("*").alias("products"))
        .orderBy(col("products").desc())
    )
