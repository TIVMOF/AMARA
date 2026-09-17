# hang

Puts what `../stitch/` wrote into Snowflake, and drives the load from the
processed schema into the analytical one.

```bash
python hang.py upload-raw            # every retailer's crawl JSON -> RAW stage
python hang.py upload-processed      # every parquet table -> PROCESSED stage
python hang.py load-processed        # that stage -> the PROCESSED tables
python hang.py load-analytical       # PROCESSED -> dimensions and facts
python hang.py cleanup               # drop the local data once it is all up
```

`cleanup` deletes `gather/data`, `shear/data` and `stitch/data`. Run it only
once the uploads have succeeded — it is the step that reclaims several GB, and
nothing brings the data back but another crawl.

## Checking the load

Three commands, one per boundary the data crosses. Each exits 1 on an error and
0 on a warning, like the validators in the other stages.

```bash
python hang.py validate-raw          # is every crawl file in the RAW stage?
python hang.py validate-processed    # is every parquet in the PROCESSED stage?
python hang.py validate-loaded       # do the loaded rows match what was sent?
```

`validate-raw` and `validate-processed` compare the stage against what is on
disk, file by file. They compare sizes, and the size a stage reports is **not**
the file's: internal stages encrypt, and the cipher pads to a 16-byte block,
always adding at least one byte. `scripts/stage.py` accounts for it. Comparing
the raw numbers instead reports every file as a mismatch.

`validate-loaded` covers the last two hops together, because the interesting
failure spans them. It counts the rows in the staged parquet — Snowflake can
read a stage directly — and compares that to the table loaded from it, then
compares the analytical layer to the processed one it was built from.

The fact table is why those two belong in one command. `load-analytical` builds
`FACT_PRODUCT_OBSERVATION` with an **inner** join to `DIM_PRODUCT`, so a
variant whose product never reached the dimension is dropped without a word.
Nothing inside either layer reveals that; only the two counts side by side do.

Reference tables are rebuilt whole on every load, so they must match the
parquet exactly. `products`, `variants`, `crawls` and `dates` are cumulative —
every crawl adds its date and earlier ones stay — so only the crawl under test
is comparable, and the checks are scoped to its date.

## Why the star schema is built here

`../stitch/` stops at clean. It emits `products`, `variants`, `crawls`,
`retailers`, `dates` and the reference vocabularies, all holding **natural
values in upper case rather than surrogate ids** — `products.brand` is
`RICK OWENS`, not `4471`.

That is deliberate. Snowflake assigns the keys and derives the dimensions and
facts of `../img/amara-analystical-data-diagram.png`, because that is where the
analytical model lives and where it can change without re-running Spark over
several GB of crawls. Parquet columns of readable text are what make that
possible: the warehouse has something human to key on.

## What goes up

| command | reads | one stage prefix per |
|---|---|---|
| `upload-processed` | `../stitch/data/` | table — `@AMARA_STAGE/products/` |
| `upload-raw` | `../gather/data/` | retailer — `@AMARA_STAGE/kith/` |

Each parquet table is a *directory* of part files. `COPY INTO` reads a whole
prefix, so nothing needs flattening or coalescing first — the part count is
Spark's business. `upload-processed` clears its prefix before writing, because
`COPY INTO` would otherwise load a previous run's part files as data too.

The raw JSON goes up as well, all of it across 50 retailers, so the warehouse
holds the source and not only what was derived from it. A question the
processed tables cannot answer can still be asked of these. Note that
`upload-raw` does *not* clear its prefix: earlier crawls accumulate there, and
`validate-raw` says so.

`AUTO_COMPRESS` is off on both: the parquets are already compressed, and
gzipping the raw JSON here would only have to be undone by `COPY INTO`.

## Credentials

`AMARA_SNOWFLAKE_*` in this component's own `.env`:

```bash
cp .env.example .env
```

Every key is required and there are no fallbacks in code, so a missing one
fails before the first request rather than connecting as somebody else.
Authentication is a **programmatic access token**, not a password: generate one
under the user's settings in Snowsight.

The three schemas are separate keys — `RAW_SCHEMA`, `PROCESSED_SCHEMA` and
`ANALYTICAL_SCHEMA` — and `connect()` takes the one it binds to rather than
reading it itself, so a script that connected to the wrong schema would still
run and just write somewhere unexpected.

## Layout

```
hang.py                        the only entry point

scripts/connection.py          env() and connect(schema)
scripts/paths.py               where the other stages leave their output
scripts/stage.py               listing a stage, and its encrypted sizes
scripts/findings.py            the ERROR/WARN convention the validators share

scripts/upload_raw.py          the crawl JSON -> the RAW schema's stage
scripts/upload_processed.py    the parquets   -> the PROCESSED schema's stage
scripts/load_processed.py      that stage     -> the PROCESSED tables
scripts/load_analytical.py     PROCESSED      -> dimensions and facts
scripts/cleanup.py             drop the local data once it is up

scripts/validate_raw.py        the RAW stage against gather/data
scripts/validate_processed.py  the PROCESSED stage against stitch/data
scripts/validate_loaded.py     both loaded layers, against the stage and each other
```

`scripts/` is a library with no entry point of its own — there is exactly one
way to run this stage, which is the same arrangement as `gather`, `shear` and
`stitch`.

## A note on the folder name

This directory was called `snowflake/`, which collided with the installed
`snowflake-connector-python` package: both are PEP 420 namespace packages, so
with the repo root on `sys.path` they merged into one `snowflake` namespace.
`import snowflake.connector` still resolved, but the day anyone added a
`snowflake/connector.py` it would not have. `hang/` collides with nothing.
