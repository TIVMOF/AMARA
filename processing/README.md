# processing

Turns the staged crawls in `../dismantling/data/staging/` into clean parquet
tables under `data/processed/`.

```bash
.venv/bin/pip install -r requirements.txt
JAVA_HOME=$(/usr/libexec/java_home -v 21) .venv/bin/python scripts/processing.py
```

Spark needs a JVM; 21 is what Spark 4 wants.

## What it writes

The data-bearing tables of the model in `../img/amara-analystical-data-diagram.png`:

| table | grain | rows |
|---|---|---:|
| `dim_retailer` | one per retailer | 50 |
| `dim_date` | one per observation date | 2 |
| `dim_product` | one per product per crawl | 336,515 |
| `fact_product_observation` | price and availability, per product per date | 336,515 |
| `size_to_product` | one per product per size | 2,964,643 |

**The lookup tables are not written here** — `dim_brand`, `dim_category`,
`size`, `style`, `tier`, `gender`, `color`, `material`, `country`. Each is the
distinct values of a column this job already produces, and Snowflake derives
them along with every primary and foreign key. So these columns hold natural
values, not surrogate ids: `dim_product.brand` is `"Rick Owens"`, not `4471`.

`brands.yaml` is reference data for the same reason. It carries 263 brands on
three axes — segment, tier, styles — and populates `dim_brand`, `tier`,
`style` and `style_to_brand` once it is loaded into Snowflake. Nothing in this
job reads it.

## Cleaning

This is the only stage that changes a value. Ingestion stores what the store
sent; dismantling only reshapes it. By the time data arrives here it is as
inconsistent as 50 storefronts can make it.

- **Case.** 39% of vendor strings arrive as `RICK OWENS`, which would sit
  beside `Rick Owens` from another store as a second brand. A value is
  title-cased only when it is entirely upper case, so `A.P.C.`, `nanushka` and
  `HOKA ONE ONE` are left alone rather than mangled by a blanket `initcap`.
- **Whitespace.** Trimmed, runs collapsed, and empty strings become null —
  otherwise `""` and `NULL` become two rows in whatever lookup Snowflake builds.
- **Gender.** Stated three ways: a `Gender` option (307 products), a
  `Gender: Women` tag (81K occurrences) and a bare `mens` tag (238K). All three
  are folded together, which is the difference between 16% and 72% coverage. A
  product carrying both `Gender: Men` and `Gender: Women` resolves to `Unisex`
  rather than whichever tag came first.
- **Money.** 3,422 variants price at `0.00` — gift cards, placeholders,
  unreleased stock. Kept as zero they read as free, and against a
  `compare_at_price` they produce a 100% discount that is not a sale. Non-
  positive money becomes null.
- **Discounts.** `original_price` is set only where the store is genuinely
  charging less than its stated normal price. Shopify stores routinely set
  `compare_at_price` equal to `price`. Beyond `MAX_PLAUSIBLE_DISCOUNT` (95%) the
  comparison is discarded as an artifact: Stadium Goods lists a `190.00`
  sneaker against a `compare_at_price` of `25,542,668.00`.

## Coverage

What the sources actually support, measured over all 336,515 products:

| column | filled | distinct |
|---|---:|---:|
| `name` | 100.0% | 301,736 |
| `brand` | 100.0% | 2,792 |
| `category` | 97.4% | 959 |
| `gender` | 71.9% | 4 |
| `color` | 30.8% | 9,954 |
| `material` | 0.5% | 148 |

`color` and `material` are only populated where a store declares them as a
variant option. Both appear unlabelled in `body_html` on many more products,
and extracting them from prose is a separate job. `material` is kept as a
column because the model has one, not because it is usable yet.

## Reading the staged files

Two traps, both already handled in `read_staging`:

- `crawl.json` is one indented object, so it needs `multiLine`. Without it
  every line comes back as `_corrupt_record`.
- Its `vendors` field used to be keyed by vendor name, which made Spark infer
  one column per vendor and then fail outright where two spellings differed
  only by case — `032c` and `032C` are both real. Dismantling now emits a list
  of records, and this job declares `CRAWL_SCHEMA` explicitly so inference
  never runs on that file at all.
