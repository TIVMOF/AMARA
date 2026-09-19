# cut

Turns the raw crawls in `../get/data/` into clean parquet tables under
`data/trimmed/`, ready to load into Snowflake.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
python cut.py                   # dismantle the crawl, then build the tables
python cut.py --dry-run         # build and report, write no tables
python cut.py --skip-dismantle  # reuse what is already dismantled
python cut.py --collected PATH  # an explicit raw crawl directory or file

python cut.py validate          # is the output sound?
python cut.py upload            # the parquets -> the PROCESSED stage
python cut.py validate-upload   # is every parquet in the stage?
python cut.py cleanup           # empty both data directories, once uploaded
```

## Two halves, one command

This stage used to be two, and the split was a lie about the dependency: the
second half reads what the first half writes, so it could never run first.

1. **Dismantling.** Each raw crawl is one JSON object with product bodies keyed
   by product id, so a product carried by twelve collections is stored once.
   Spark cannot read that shape. It becomes `crawl.json`, `products.jsonl` and
   `variants.jsonl` under `data/dismantled/<site>/<stamp>/`. Standard library
   only, and deliberately so — this is a change of *shape*, not of meaning.
2. **Building the tables.** Spark reads all 50 retailers' dismantled files in
   one go and writes the 12 parquet tables. This is the only place in AMARA
   where a value changes.

The one reshape worth naming: `vendors` goes from a name-keyed map to a list of
`{vendor, products}` records, because Spark's column names are case-insensitive
and would collide `032c` with `032C`.

The dismantled files are kept after a run. Nothing downstream reads them — sew
works from Snowflake — but a table build that fails can then be re-run with
`--skip-dismantle` instead of starting from the raw JSON again. `cleanup`
empties them alongside the parquet.

**It checks that `get` has run.** No raw crawl, or an empty directory, and the
stage stops with what to do about it — before the JVM starts, so a missing
crawl costs a second rather than surfacing as a Spark read failure two minutes
in.

## Running it

Spark needs a JVM; 21 is what Spark 4 wants. Spark is found by asking the
`python` on PATH where pyspark lives — so the virtualenv has to be activated,
or Spark reports `Could not find valid SPARK_HOME`.

`cut.py` sets no master, so `--master` is yours to pass:

```bash
spark-submit --master "local[4]" --driver-memory 8g cut.py
```

## In a container

`amara-cut`, built from the `Dockerfile` here — the only image carrying a JVM.
`reference/*.yaml` ships inside it, so changing a brand means rebuilding.

`AMARA_STORAGE_DIR` points at the one shared volume, and this stage uses three
directories inside it: it reads `collected/`, and writes `dismantled/` and
`trimmed/`. `--collected`, `--dismantled` and `--trimmed` override them
individually per run.

The image sets `SPARK_DRIVER_MEMORY=8g`, which PySpark reads when it starts the
JVM: unset it and the default 1 GB driver will not hold 2.7M variants. The
master stays `local[*]`; a cluster submission means overriding the entrypoint
with `spark-submit`.

```bash
docker run --rm --init -v amara_storage:/data amara-cut
```

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
| `products` | one product observation per retailer and crawl date | 216,926 |
| `variants` | one per variant per crawl — what it *costs* and whether it's in stock | 2,321,812 |

Products and variants are both observations for a crawl date. Build a fact from
`variants`, at whatever grain the analysis wants.

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
cut.py                    the entry point, and the run end to end

  the dismantling half - standard library only
scripts/raw.py            reading one raw crawl, and the records it holds
scripts/dismantle.py      finding the crawl, and writing the three flat files

  the Spark half
scripts/staging.py        reading what dismantling wrote
scripts/reference.py      YAML vocabularies, and keeping their parquets in step
scripts/tables.py         the tables this stage writes
scripts/clean.py          column-level cleaning, and the folding behind matching

  both
scripts/paths.py          where things live
scripts/validate.py       checking what it wrote
scripts/upload_processed.py   the parquets -> the PROCESSED stage
scripts/validate_upload.py    is every parquet in the stage?
scripts/cleanup.py        emptying both data directories
```

## Reading the staged files

Two traps, both handled in `staging.py`: `crawl.json` is one indented object
and needs `multiLine`, and its schema is declared rather than inferred —
`vendors` used to be keyed by vendor name, which made Spark infer a column per
vendor and then fail where two spellings differed only by case.
