# processing

Turns the staged crawls in `../dismantling/data/staging/` into clean parquet
tables under `data/processed/`, ready to load into Snowflake.

```bash
.venv/bin/pip install -r requirements.txt
JAVA_HOME=$(/usr/libexec/java_home -v 21) .venv/bin/python -m scripts.process
JAVA_HOME=$(/usr/libexec/java_home -v 21) .venv/bin/python -m scripts.process --dry-run
```

Spark needs a JVM; 21 is what Spark 4 wants. `--dry-run` builds and reports
every table but writes no data tables.

## Where this stage stops

**This stage produces clean data, not a model.** No facts, no dimensions, no
keys. Snowflake loads these parquets into a staging schema, assigns the primary
and foreign keys, and derives the analytical star schema of
`../img/amara-analystical-data-diagram.png` from there.

That is why nothing is named `dim_` or `fact_`, and why every column holds a
natural value in upper case rather than a surrogate id: `products.brand` is
`RICK OWENS`, not `4471`. Snowflake does the keying, so it needs something
human to key on.

## Data tables

| table | grain | rows |
|---|---|---:|
| `crawls` | one per crawl run — provenance, and the currency | 50 |
| `retailers` | one per retailer | 50 |
| `dates` | one per observation date | 2 |
| `products` | one per product per retailer — what the garment *is* | 216,926 |
| `variants` | one per variant per crawl — what it *costs* and whether it's in stock | 2,321,812 |

The split is deliberate. A product is a description and does not change between
crawls, so it has no date; a variant is an observation and carries one. Build a
fact from `variants`, at whatever grain the analysis wants.

```
variants: variant, product, retailer, date, sku, size, color,
          price, currency, original_price, discount, available
```

`product` + `retailer` joins back to `products`. `currency` comes down from
the crawl — Shopify states it once per store, and the 50 retailers quote in
USD, EUR, GBP and SEK, so no sum over `price` is meaningful without it.
 Variants of a product with no
size option — fragrance, homeware — keep a null size rather than disappearing;
they still carry a price.

Sizes stay on the variant rather than in a bridge table, because that is where
they mean something: 27.3% of products have some sizes in stock and others
not, and a bridge of bare size strings cannot express that.

## Reference data

`reference/*.yaml` holds the controlled vocabularies — the values AMARA is
willing to report on. Each run reads them, compares against what the last run
wrote, and appends whatever is new:

```
Reference data
  brands        263 values  (+263 new)
  categories     24 values  (unchanged)
  countries      12 values  (+2 new)
```

Editing a YAML is how a vocabulary changes. A value is never removed — rows
written by an earlier run still point at it. The first run has no parquet to
compare against, so everything is new.

| file | writes | what it does |
|---|---|---|
| `brands.yaml` | `brands`, `segments`, `tiers` | the allowlist **and** the classification |
| `categories.yaml` | `categories` | folds 886 raw spellings into 24 categories |
| `genders.yaml` | `genders` | folds `mens`, `Male`, `Gender: Men` into `MEN` |
| `countries.yaml` | `countries` | the ISO codes retailers report |
| `currencies.yaml` | `currencies` | the ISO codes retailers price in |

`countries` is the one vocabulary keyed on something other than `name`: its
canonical value is the ISO code, so `retailers.country` holds `US` and joins
to `countries.code`. `countries.name` is the readable label beside it.

**`brands.yaml` decides the size of every table.** A product whose vendor is
not listed is dropped: 336,515 staged products become 216,926. Adding a brand
is three lines, and order does not matter.

`color`, `size` and `material` deliberately have no reference file. They are
open vocabularies — 6,519 colours, 1,806 sizes across four incompatible scales,
and materials that are fabric compositions like `74%WOOL,26%SILK` rather than
names. They stay plain string columns, cleaned and upper-cased.

Every run reports what the vocabularies did not recognise, most frequent
first. That report is how the YAML grows:

```
  unmatched vendor: Billionaire Boys Club (3,095), MITCHELL & NESS (2,752), ...
  unmatched product_type: Lifestyle (1,437), Product Look (1,169), ...
```

## Cleaning

This is the only stage that changes a value. Ingestion stores what a store
sent; dismantling only reshapes it.

- **Case.** Every name is upper-cased, so `Rick Owens`, `RICK OWENS` and
  `rick owens` are one brand rather than three. Product titles and SKUs keep
  their case — they are prose and identifiers, not categories.
- **Matching** folds case, accents and punctuation, so `ACNE STUDIOS`,
  `Acne Studios` and `Alaïa`/`ALAIA` resolve without an alias between them.
  An alias is only needed where the words differ — `YSL` → `SAINT LAURENT`.
- **Whitespace.** Trimmed, runs collapsed, empty strings become null.
- **Gender.** Stated three ways — a `Gender` option (307 products), a
  `Gender: Women` tag (81K) and a bare `mens` tag (238K). Folding all three
  took coverage from 16% to 72%. A product declaring both Men and Women is
  `UNISEX`, not whichever came first.
- **Money.** Non-positive prices become null: 1.8% of variants price at `0.00`
  and read as free, or as a 100% discount against `compare_at_price`.
- **Discounts.** `original_price` is set only where a store charges less than
  its stated normal price. Beyond `MAX_PLAUSIBLE_DISCOUNT` (95%) the
  comparison is an artifact — Stadium Goods lists a `190.00` sneaker against
  a `compare_at_price` of `25,542,668.00`.
- **Size and colour slots.** Which of `option1/2/3` holds the size differs per
  store, so the slot is read from each product's own option list rather than
  assumed.

## Coverage

Over the 216,926 products that survive the allowlist:

| column | filled | distinct |
|---|---:|---:|
| `name` | 100.0% | 192,115 |
| `brand` | 100.0% | 219 |
| `category` | 89.7% | 24 |
| `gender` | 72.0% | 4 |
| `color` | 25.5% | 6,528 |
| `material` | 0.8% | 136 |

And over their 2,321,812 variants:

| column | filled | distinct |
|---|---:|---:|
| `available` | 100.0% | 2 |
| `sku` | 99.9% | — |
| `size` | 99.6% | 1,806 |
| `price` | 98.2% | — |
| `original_price` | 18.3% | — |
| `color` | 17.8% | 6,519 |

`brand` is 100% by construction. `category` grows by adding aliases. Colour
sits on both tables and means different things: on `products` it is every
colourway the product lists, on `variants` the one that variant is.

## Layout

```
scripts/process.py     the CLI, and the run end to end
scripts/reference.py   YAML vocabularies, and keeping their parquets in step
scripts/staging.py     reading what dismantling wrote
scripts/tables.py      the tables this stage writes
scripts/clean.py       column-level cleaning, and the folding behind matching
scripts/paths.py       where things live
```

## Reading the staged files

Two traps, both handled in `staging.py`: `crawl.json` is one indented object
and needs `multiLine`, and its schema is declared rather than inferred —
`vendors` used to be keyed by vendor name, which made Spark infer a column per
vendor and then fail where two spellings differed only by case.
