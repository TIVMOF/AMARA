from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    broadcast,
    col,
    concat_ws,
    dayofmonth,
    lit,
    month,
    quarter,
    round as spark_round,
    row_number,
    to_date,
    to_timestamp,
    transform as array_transform,
    try_element_at,
    weekofyear,
    when,
    year,
)
from pyspark.sql.window import Window

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
    # The values of the first option named one of `names`.
    #
    # Shopify options are an array of {name, position, values} and the name is
    # the only reliable way in - which slot an option occupies varies by store.
    from pyspark.sql.functions import filter as array_filter, lower, trim

    matching = array_filter(options, lambda o: lower(trim(o["name"])).isin(list(names)))
    return try_element_at(matching, lit(1))["values"]


def option_position(options: Column, names: tuple[str, ...]) -> Column:
    # The 1-based slot a named option occupies, for reading variant.optionN.
    #
    # Cast to int because JSON inference makes it a bigint, which `element_at`
    # rejects as an index.
    from pyspark.sql.functions import filter as array_filter, lower, trim

    matching = array_filter(options, lambda o: lower(trim(o["name"])).isin(list(names)))
    return try_element_at(matching, lit(1))["position"].cast("int")


def slot_value(slots: Column, position: Column) -> Column:
    # A variant's option1/2/3 at a given slot, cleaned.
    #
    # Null where the product declares no such option, which is most of what a
    # store sells that is not a garment.
    return when(position.isNotNull(), name(try_element_at(slots, position)))


def joined_values(options: Column, names: tuple[str, ...]) -> Column:
    # A named option's values as one string, or null when it has none.
    #
    # Colour and material are open vocabularies - 9,315 colours, and materials
    # that are fabric compositions like `74%WOOL,26%SILK` - so they are cleaned
    # and kept as written rather than mapped onto a reference list.
    joined = concat_ws(" / ", array_transform(option_values(options, names), name))
    return when(joined == "", None).otherwise(joined)


def crawl_date() -> Column:
    # The date a staged row was observed, from its `scraped_at` timestamp.
    return to_date(to_timestamp(col("scraped_at")))


# ── tables ─────────────────────────────────────────────────────────────────────

def crawls(staged_crawls: DataFrame, currencies: Reference) -> DataFrame:
    # crawls - one row per crawl run: what was collected, and in what currency.
    #
    # The provenance table. `products_received` against `products_stored` is what
    # deduplication removed, and `short_pages` against `pages` is how much of the
    # walk came back under-full - both are how you tell a retailer shrinking from
    # a crawl running short.
    #
    # `name` is the crawl's brand override, set only for the 29 single-brand
    # sites. It is null for a multi-brand retailer, which has no one brand to
    # name.
    return (
        staged_crawls
        .select(
            name(col("site")).alias("site"),
            clean_text(col("base_url")).alias("base_url"),
            lookup(col("currency"), currencies.lookup).alias("currency"),
            name(col("brand_override")).alias("name"),
            crawl_date().alias("date"),
            col("products_received").cast("int").alias("products_received"),
            col("products_stored").cast("int").alias("products_stored"),
            col("pages").cast("int").alias("pages"),
            col("short_pages").cast("int").alias("short_pages"),
            col("collections_crawled").cast("int").alias("collections_crawled"),
        )
        .dropDuplicates(["site", "date"])
    )


def retailers(staged_crawls: DataFrame, countries: Reference) -> DataFrame:
    # retailers - name, url, country.
    return (
        staged_crawls
        .select(
            name(col("site")).alias("name"),
            clean_text(col("base_url")).alias("url"),
            lookup(col("country"), countries.lookup).alias("country"),
        )
        .dropDuplicates(["name"])
    )


def dates(staged_crawls: DataFrame) -> DataFrame:
    # dates - one row per date any crawl observed.
    return (
        staged_crawls
        .select(crawl_date().alias("date"))
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
    # products - the catalogue: name, brand, category, gender, color, material.
    #
    # One row per product per retailer, describing what the garment is. What it
    # cost and whether it was in stock belongs to `variants`, which is where a
    # date makes sense - so a product crawled twice is one row here, taken from
    # the most recent crawl.
    #
    # Products whose vendor is not on the brand allowlist are dropped here, which
    # is what makes brands.yaml the allowlist as well as the classification.
    #
    # Gender is stated three ways and often more than once: a `Gender` option, a
    # `Gender: Women` tag and a bare `mens` tag. All three are folded together,
    # and a product declaring more than one is Unisex rather than whichever came
    # first.
    described = (
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
            crawl_date().alias("date"),
        )
        .where(col("brand").isNotNull())
    )

    newest = Window.partitionBy("product", "retailer").orderBy(col("date").desc_nulls_last())
    return (
        described
        .withColumn("_rank", row_number().over(newest))
        .where(col("_rank") == 1)
        .drop("_rank", "date")
    )


def variants(staged_products: DataFrame, staged_variants: DataFrame,
             staged_crawls: DataFrame, catalogue: DataFrame,
             known_currencies: Reference) -> DataFrame:
    # variants - one row per variant per crawl: size, colour, price, stock.
    #
    # The variant is what a store actually sells and prices, so this is where the
    # measures live. A product averages 10.7 variants and they do not agree:
    # 27.3% of products have some sizes in stock and others not, which is the
    # signal any size-level analysis in Snowflake needs.
    #
    # Which of option1/2/3 holds the size differs per store, so the slot is read
    # from the product's own option list rather than assumed. Variants of a
    # product with no size option - fragrance, homeware - keep a null size rather
    # than being dropped; they still carry a price.
    #
    # `original_price` is set only where a store is genuinely charging less than
    # its stated normal price. Shopify stores routinely set `compare_at_price`
    # equal to `price`, or to zero, when nothing is on sale.
    #
    # `currency` sits beside `price` because a price without it is just a number:
    # the 50 retailers quote in USD, EUR, GBP and SEK.
    slots = staged_products.select(
        col("id").cast("string").alias("product"),
        name(col("site")).alias("retailer"),
        option_position(col("options"), SIZE_OPTIONS).alias("size_slot"),
        option_position(col("options"), COLOR_OPTIONS).alias("color_slot"),
    ).dropDuplicates(["product", "retailer"])

    sold = staged_variants.select(
        col("id").cast("string").alias("variant"),
        col("product_id").cast("string").alias("product"),
        name(col("site")).alias("retailer"),
        crawl_date().alias("date"),
        clean_text(col("sku")).alias("sku"),
        array(col("option1"), col("option2"), col("option3")).alias("options"),
        positive(col("price").cast("decimal(12,2)")).alias("price"),
        positive(col("compare_at_price").cast("decimal(12,2)")).alias("compare_at"),
        col("available").cast("boolean").alias("available"),
    )

    discounting = (col("compare_at") > col("price")) & (
        col("compare_at") <= col("price") / (1 - MAX_PLAUSIBLE_DISCOUNT / 100)
    )
    original = when(discounting, col("compare_at"))

    # Shopify states the currency once per store, not per variant, so it comes
    # down from the crawl the variant was seen in.
    currencies = staged_crawls.select(
        name(col("site")).alias("retailer"),
        crawl_date().alias("date"),
        lookup(col("currency"), known_currencies.lookup).alias("currency"),
    ).dropDuplicates(["retailer", "date"])

    return (
        sold
        .join(slots, ["product", "retailer"], "left")
        .join(broadcast(currencies), ["retailer", "date"], "left")
        # Only variants of products that survived the brand allowlist.
        .join(catalogue.select("product", "retailer"), ["product", "retailer"], "left_semi")
        .select(
            "variant",
            "product",
            "retailer",
            "date",
            "sku",
            slot_value(col("options"), col("size_slot")).alias("size"),
            slot_value(col("options"), col("color_slot")).alias("color"),
            "price",
            "currency",
            original.alias("original_price"),
            spark_round(
                when(original.isNotNull(), (original - col("price")) / original * 100), 2
            ).alias("discount"),
            "available",
        )
        .dropDuplicates(["variant", "retailer", "date"])
    )


# ── what did not match ─────────────────────────────────────────────────────────

def unmatched(staged: DataFrame, column: str, reference: Reference) -> DataFrame:
    # Raw values a vocabulary did not recognise, most frequent first.
    #
    # This report is how a reference file grows - anything here that should be
    # reported on is a missing entry or a missing alias.
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
