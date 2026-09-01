# processing

Turns the staged crawls in `../dismantling/data/staging/` into clean parquet
tables under `data/processed/`.

```bash
.venv/bin/pip install -r requirements.txt
JAVA_HOME=$(/usr/libexec/java_home -v 21) .venv/bin/python -m scripts.process
JAVA_HOME=$(/usr/libexec/java_home -v 21) .venv/bin/python -m scripts.process --dry-run
```

Spark needs a JVM; 21 is what Spark 4 wants. `--dry-run` builds and reports
every table but writes no model tables.

## Reference data

`reference/*.yaml` holds the controlled vocabularies — the values AMARA is
willing to report on. Each run reads them, compares against what the last run
wrote, and appends whatever is new:

```
Reference data
  dim_brand     263 values  (+263 new)
  category       24 values  (unchanged)
  country        12 values  (+2 new)
```

Editing a YAML is how a vocabulary changes. A value is never removed — rows
written by an earlier run still point at it. The first run has no parquet to
compare against, so everything is new.

| file | writes | what it does |
|---|---|---|
| `brands.yaml` | `dim_brand`, `segment`, `tier` | the allowlist **and** the classification |
| `categories.yaml` | `category` | folds 886 raw spellings into 24 categories |
| `genders.yaml` | `gender` | folds `mens`, `Male`, `Gender: Men` into `MEN` |
| `countries.yaml` | `country` | the ISO codes retailers report |

**`brands.yaml` decides the size of every table.** A product whose vendor is
not listed is dropped: 336,515 staged products become 216,926. Adding a brand
is three lines, and order does not matter.

`color`, `size` and `material` deliberately have no reference file. They are
open vocabularies — 9,315 colours, 2,533 sizes across four incompatible
scales, and materials that are fabric compositions like `74%WOOL,26%SILK`
rather than names. They stay plain string columns, cleaned and upper-cased.

Every run reports what the vocabularies did not recognise, most frequent
first. That report is how the YAML grows:

```
  unmatched vendor: Billionaire Boys Club (3,095), MITCHELL & NESS (2,752), ...
  unmatched product_type: Lifestyle (1,437), Product Look (1,169), ...
```

## Model tables

The data-bearing tables of `../img/amara-analystical-data-diagram.png`:

| table | grain | rows |
|---|---|---:|
| `dim_retailer` | one per retailer | 50 |
| `dim_date` | one per observation date | 2 |
| `dim_product` | one per product per crawl | 216,926 |
| `fact_product_observation` | price and availability, per product per date | 216,926 |
| `size_to_product` | one per product per size | 2,284,648 |

The remaining lookups and every primary and foreign key are Snowflake's, so
these columns hold **natural values in upper case**, not surrogate ids:
`dim_product.brand` is `RICK OWENS`, not `4471`.

## Cleaning

This is the only stage that changes a value. Ingestion stores what a store
sent; dismantling only reshapes it.

- **Case.** Every name is upper-cased, so `Rick Owens`, `RICK OWENS` and
  `rick owens` are one brand rather than three.
- **Matching** folds case, accents and punctuation, so `ACNE STUDIOS`,
  `Acne Studios` and `Alaïa`/`ALAIA` resolve without an alias between them.
  An alias is only needed where the words differ — `YSL` → `SAINT LAURENT`.
- **Whitespace.** Trimmed, runs collapsed, empty strings become null.
- **Gender.** Stated three ways — a `Gender` option (307 products), a
  `Gender: Women` tag (81K) and a bare `mens` tag (238K). Folding all three
  took coverage from 16% to 72%. A product declaring both Men and Women is
  `UNISEX`, not whichever came first.
- **Money.** Non-positive prices become null: 3,422 variants price at `0.00`
  and read as free, or as a 100% discount against `compare_at_price`.
- **Discounts.** `original_price` is set only where a store charges less than
  its stated normal price. Beyond `MAX_PLAUSIBLE_DISCOUNT` (95%) the
  comparison is an artifact — Stadium Goods lists a `190.00` sneaker against
  a `compare_at_price` of `25,542,668.00`.

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

`brand` is 100% by construction. `category` grows by adding aliases;
`color` and `material` only appear where a store declares them as a variant
option, and both sit unlabelled in `body_html` on many more products.

## Layout

```
scripts/process.py     the CLI, and the run end to end
scripts/reference.py   YAML vocabularies, and keeping their parquets in step
scripts/staging.py     reading what dismantling wrote
scripts/tables.py      the model tables
scripts/clean.py       column-level cleaning, and the folding behind matching
scripts/paths.py       where things live
```

## Reading the staged files

Two traps, both handled in `staging.py`: `crawl.json` is one indented object
and needs `multiLine`, and its schema is declared rather than inferred —
`vendors` used to be keyed by vendor name, which made Spark infer a column per
vendor and then fail where two spellings differed only by case.
