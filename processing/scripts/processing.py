"""Turns staged crawls into clean parquet tables.

Reads every crawl under `dismantling/data/staging/` and writes the
data-bearing tables of the model in `img/amara-analystical-data-diagram.png`:

    dim_retailer              one row per retailer
    dim_date                  one row per observation date
    dim_product               one row per product per crawl
    fact_product_observation  price and availability, at product grain
    size_to_product           one row per product per size

The lookup tables - dim_brand, dim_category, size, style, tier, gender, color,
material, country - are not written here. They are distinct values of columns
this job already produces, and Snowflake derives them along with every primary
and foreign key. So the columns below hold natural values, not surrogate ids.

Cleaning happens here and nowhere else. Values arrive from the crawl exactly
as a store typed them: 39% of vendor strings are ALL CAPS, `product_type`
has 549 spellings across 50 stores, and gender appears as `men`, `Mens`,
`Male` and `Gender: Women` depending on the store.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    array_distinct,
    coalesce,
    concat,
    col,
    concat_ws,
    dayofmonth,
    try_element_at,
    filter as array_filter,
    initcap,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    month,
    quarter,
    regexp_replace,
    round as spark_round,
    size as array_size,
    to_date,
    to_timestamp,
    transform as array_transform,
    trim,
    upper,
    weekofyear,
    when,
    year,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_ROOT = PROJECT_ROOT / "dismantling" / "data" / "staging"

OUTPUT_ROOT = PROJECT_ROOT / "processing" / "data" / "processed"

# Option names a store may use for the same thing. Matched case-insensitively
# against the trimmed option name.
SIZE_OPTIONS = (
    "size", "sizes", "taille", "size (us)", "us size", "uk size", "eu size",
    "shoe size", "clothing size", "cup size",
)

COLOR_OPTIONS = ("color", "colour", "colorway", "colourway")

MATERIAL_OPTIONS = ("material", "materials", "fabric")

GENDER_OPTIONS = ("gender", "sex")

# Folded gender value -> canonical label. Stores write this a dozen ways, and
# the tag form arrives as "Gender: Women", so the prefix is stripped first.
GENDER_LABELS = {
    "men": "Men", "man": "Men", "mens": "Men", "male": "Men", "menswear": "Men",
    "women": "Women", "woman": "Women", "womens": "Women", "female": "Women",
    "womenswear": "Women", "ladies": "Women",
    "kids": "Kids", "kid": "Kids", "children": "Kids", "child": "Kids",
    "boys": "Kids", "girls": "Kids", "junior": "Kids", "baby": "Kids",
    "unisex": "Unisex",
}

# Only the crawl fields the tables need. Declared rather than inferred: the
# `vendors` object is keyed by vendor name, so inference produces one column
# per vendor and then fails outright where two spellings differ only by case
# (032c and 032C are both real).
CRAWL_SCHEMA = "site STRING, base_url STRING, country STRING, scraped_at STRING"

# A discount steeper than this is a data-entry artifact, not a sale. Stadium
# Goods lists a 190.00 sneaker against a compare_at_price of 25,542,668.00.
# 13 rows of 336,515 exceed it; they lose original_price and discount, and
# keep the price the store is actually charging.
MAX_PLAUSIBLE_DISCOUNT = 95


SEASONS = {12: "Winter", 1: "Winter", 2: "Winter",
           3: "Spring", 4: "Spring", 5: "Spring",
           6: "Summer", 7: "Summer", 8: "Summer",
           9: "Autumn", 10: "Autumn", 11: "Autumn"}


# ── cleaning ───────────────────────────────────────────────────────────────────

def clean_text(column: Column) -> Column:
    """Trim, collapse runs of whitespace, and turn blanks into null.

    Applied to every string that reaches a table. An empty string and a null
    mean the same thing here and would otherwise become two distinct rows in
    whatever lookup Snowflake builds from the column.
    """
    collapsed = trim(regexp_replace(column, r"\s+", " "))
    return when(collapsed == "", None).otherwise(collapsed)


def normalize_case(column: Column) -> Column:
    """Title-case a value only when the store shouted it.

    39% of vendors arrive as `RICK OWENS`, which would sit beside `Rick Owens`
    from another store as two different brands. Values that are already mixed
    case are left exactly as they are: `A.P.C.`, `nanushka` and `HOKA ONE ONE`
    each lose something under a blanket initcap.
    """
    cleaned = clean_text(column)
    return when(cleaned == upper(cleaned), initcap(lower(cleaned))).otherwise(cleaned)


def fold_gender(column: Column) -> Column:
    """Map a raw gender value onto Men / Women / Kids / Unisex, or null.

    Handles both forms stores use: an option value (`Mens`) and a tag
    (`Gender: Women`). Anything unrecognised - `womenswomenstops` is real -
    becomes null rather than a category of its own.
    """
    stripped = lower(trim(regexp_replace(column, r"(?i)^\s*gender\s*:", "")))
    folded = regexp_replace(stripped, r"[^a-z]", "")

    mapped = lit(None).cast("string")
    for raw, label in GENDER_LABELS.items():
        mapped = when(folded == raw, lit(label)).otherwise(mapped)
    return mapped


def product_gender(options: Column, tags: Column) -> Column:
    """Every gender a product declares, resolved to one label.

    Stores state it three ways and often more than once: a `Gender` option
    (307 products), a `Gender: Women` tag (81K occurrences), and a bare `mens`
    tag (238K). All three are folded, and anything unrecognised drops out.

    A product carrying both `Gender: Men` and `Gender: Women` is unisex, not
    whichever tag happened to come first - so the labels are deduplicated and
    a product left with more than one becomes Unisex.
    """
    candidates = concat(
        coalesce(option_values(options, GENDER_OPTIONS), array()),
        coalesce(tags, array()),
    )
    labels = array_distinct(
        array_filter(array_transform(candidates, fold_gender), lambda g: g.isNotNull())
    )

    return (
        when(array_size(labels) == 1, try_element_at(labels, lit(1)))
        .when(array_size(labels) > 1, lit("Unisex"))
    )


def option_values(options: Column, names: tuple[str, ...]) -> Column:
    """The values of the first option whose name is one of `names`.

    Shopify options are an array of {name, position, values}, and the name is
    the only reliable way in - position varies by store.
    """
    matching = array_filter(options, lambda o: lower(trim(o["name"])).isin(list(names)))
    return try_element_at(matching, lit(1))["values"]


def option_position(options: Column, names: tuple[str, ...]) -> Column:
    """The 1-based slot a named option occupies, for reading variant.optionN.

    Cast to int because JSON inference makes it a bigint, and `element_at`
    rejects a bigint index.
    """
    matching = array_filter(options, lambda o: lower(trim(o["name"])).isin(list(names)))
    return try_element_at(matching, lit(1))["position"].cast("int")


def positive(column: Column) -> Column:
    """Null out non-positive money.

    3,422 variants price at 0.00 - gift cards, placeholders, unreleased stock.
    Kept as zero they read as free, and against a `compare_at_price` they
    produce a 100% discount that is not a sale.
    """
    return when(column > 0, column)


def season_of(month_column: Column) -> Column:
    season = lit(None).cast("string")
    for number, name in SEASONS.items():
        season = when(month_column == number, lit(name)).otherwise(season)
    return season


# ── reading ────────────────────────────────────────────────────────────────────

def read_staging(spark: SparkSession, staging_root: Path) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Every staged crawl, across every site.

    `recursiveFileLookup` walks the whole tree, so a site crawled twice
    contributes both timestamps without the pattern having to know the depth.

    The two `.jsonl` files are one record per line, which Spark splits and
    reads in parallel. `crawl.json` is a single indented object per crawl and
    needs `multiLine`, or every line of it comes back as `_corrupt_record`.
    """
    def read(name: str, *, multiline: bool = False, schema: str | None = None) -> DataFrame:
        reader = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", name)
            .option("multiLine", multiline)
        )
        if schema:
            reader = reader.schema(schema)
        return reader.json(str(staging_root))

    return (
        read("crawl.json", multiline=True, schema=CRAWL_SCHEMA),
        read("products.jsonl"),
        read("variants.jsonl"),
    )


# ── tables ─────────────────────────────────────────────────────────────────────

def build_retailers(crawls: DataFrame) -> DataFrame:
    """dim_retailer - name, url, country."""
    return (
        crawls
        .select(
            normalize_case(col("site")).alias("name"),
            clean_text(col("base_url")).alias("url"),
            clean_text(col("country")).alias("country"),
        )
        .dropDuplicates(["name"])
    )


def build_dates(crawls: DataFrame) -> DataFrame:
    """dim_date - one row per date any crawl observed."""
    observed = to_timestamp(col("scraped_at"))

    return (
        crawls
        .select(to_date(observed).alias("date"))
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


def build_products(products: DataFrame) -> DataFrame:
    """dim_product - name, brand, category, gender, color, material.

    `product` and `retailer` identify the row: Snowflake replaces the pair
    with a surrogate key, but the natural pair is what the crawl actually has.
    Gender comes from an option where a store offers one and from tags
    otherwise, which is the difference between 1.6% and 47% coverage.
    """
    return (
        products
        .select(
            col("id").cast("string").alias("product"),
            normalize_case(col("site")).alias("retailer"),
            clean_text(col("title")).alias("name"),
            normalize_case(col("vendor")).alias("brand"),
            normalize_case(col("product_type")).alias("category"),
            product_gender(col("options"), col("tags")).alias("gender"),
            concat_ws(
                " / ",
                array_transform(option_values(col("options"), COLOR_OPTIONS), clean_text),
            ).alias("color"),
            concat_ws(
                " / ",
                array_transform(option_values(col("options"), MATERIAL_OPTIONS), clean_text),
            ).alias("material"),
            to_date(to_timestamp(col("scraped_at"))).alias("date"),
        )
        .withColumn("color", when(col("color") == "", None).otherwise(col("color")))
        .withColumn("material", when(col("material") == "", None).otherwise(col("material")))
        .dropDuplicates(["product", "retailer", "date"])
    )


def build_observations(products: DataFrame, variants: DataFrame) -> DataFrame:
    """fact_product_observation - price and availability at product grain.

    The model puts sizes in their own bridge, so the fact is one row per
    product per date rather than per variant. A product's variants are the
    same garment in different sizes and almost always share a price, so the
    row carries the lowest of them - the price a shopper is quoted.

    `original_price` is only meaningful when the store is actually discounting.
    Shopify stores routinely set `compare_at_price` equal to `price`, or to
    zero, when nothing is on sale; those become null rather than a 0% discount.
    """
    priced = (
        variants
        .select(
            col("product_id").cast("string").alias("product"),
            normalize_case(col("site")).alias("retailer"),
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

    # A sale only exists where the store is charging less than it says it
    # normally would. Shopify stores routinely set compare_at_price equal to
    # price, or to zero, when nothing is on sale.
    discounting = (col("compare_at") > col("price")) & (
        col("compare_at") <= col("price") / (1 - MAX_PLAUSIBLE_DISCOUNT / 100)
    )
    original = when(discounting, col("compare_at"))

    return (
        products.alias("p")
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
                when(original.isNotNull(), (original - col("v.price")) / original * 100),
                2,
            ).alias("discount"),
            col("v.available").alias("available"),
        )
    )


def build_sizes(products: DataFrame, variants: DataFrame) -> DataFrame:
    """size_to_product - one row per product per size it is sold in.

    Which of option1/2/3 holds the size differs per store, so the position is
    read from the product's own option list. Products with no size option -
    fragrance, homeware - contribute nothing rather than a null size.
    """
    positions = products.select(
        col("id").cast("string").alias("product"),
        normalize_case(col("site")).alias("retailer"),
        to_date(to_timestamp(col("scraped_at"))).alias("date"),
        option_position(col("options"), SIZE_OPTIONS).alias("size_position"),
    ).where(col("size_position").isNotNull())

    slots = variants.select(
        col("product_id").cast("string").alias("product"),
        normalize_case(col("site")).alias("retailer"),
        to_date(to_timestamp(col("scraped_at"))).alias("date"),
        array(col("option1"), col("option2"), col("option3")).alias("slots"),
    )

    return (
        positions
        .join(slots, ["product", "retailer", "date"], "inner")
        .select(
            "product",
            "retailer",
            "date",
            clean_text(try_element_at(col("slots"), col("size_position"))).alias("size"),
        )
        .where(col("size").isNotNull())
        .dropDuplicates(["product", "retailer", "date", "size"])
    )


# ── writing ────────────────────────────────────────────────────────────────────

def write_parquet(frame: DataFrame, name: str) -> int:
    """Write one table and report what landed in it."""
    path = OUTPUT_ROOT / name
    frame.write.mode("overwrite").parquet(str(path))

    rows = frame.count()
    print(f"  {name:26} {rows:>9,} rows  -> {path.relative_to(PROJECT_ROOT)}")
    return rows


def process_all(spark: SparkSession) -> None:
    print(f"Reading staged crawls from {STAGING_ROOT.relative_to(PROJECT_ROOT)}\n")
    crawls, products, variants = read_staging(spark, STAGING_ROOT)

    print("Building tables...")
    retailers = build_retailers(crawls)
    dates = build_dates(crawls)
    dim_products = build_products(products).cache()
    observations = build_observations(dim_products, variants)
    sizes = build_sizes(products, variants)

    print("\nWriting parquet...")
    write_parquet(retailers, "dim_retailer")
    write_parquet(dates, "dim_date")
    write_parquet(dim_products.drop("date"), "dim_product")
    write_parquet(observations, "fact_product_observation")
    write_parquet(sizes, "size_to_product")

    print("\nProcessing complete.")


def main() -> None:
    spark = SparkSession.builder.appName("AMARA").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        process_all(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
