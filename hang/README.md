# hang

Puts what `../stitch/` wrote into Snowflake, and drives the load from the
staging schema into the analytical one.

```bash
python upload_raw.py            # every retailer's crawl JSON -> RAW stage
python upload_processed.py      # every parquet table -> PROCESSED stage
python load_processed.py        # that stage -> the PROCESSED tables
python cleanup.py               # drop the local data once it is all up
```

`cleanup.py` deletes `gather/data`, `shear/data` and
`stitch/data`. Run it only once the uploads have succeeded -
it is the step that reclaims several GB, and nothing brings the data back
but another crawl.

Both commands PUT files into an internal Snowflake stage. Loading them into
tables is `COPY INTO`, which is Snowflake's side of the line.

## Why the star schema is built here

`../stitch/` stops at clean. It emits `products`, `variants`, `crawls`,
`retailers`, `dates` and the reference vocabularies, all holding **natural
values in upper case rather than surrogate ids** — `products.brand` is
`RICK OWENS`, not `4471`.

That is deliberate. Snowflake assigns the keys and derives the dimensions and
facts of `../img/amara-analystical-data-diagram.png`, because that is where
the analytical model lives and where it can change without re-running Spark
over 2.8 GB of crawls. Parquet columns of readable text are what make that
possible: the warehouse has something human to key on.

## What goes up

| script | reads | one stage prefix per |
|---|---|---|
| `upload_processed.py` | `../stitch/data/` | table — `@AMARA_STAGE/products/` |
| `upload_raw.py` | `../gather/data/` | retailer — `@AMARA_STAGE/kith/` |

```
products    11M    200 part files
variants    68M     12 part files
brands      16K      1 part file
```

Each parquet table is a *directory* of part files. `COPY INTO` reads a whole
prefix, so nothing needs flattening or coalescing first — the part count is
Spark's business.

The raw JSON goes up as well, all 2.8 GB of it across 50 retailers, so the
warehouse holds the source and not only what was derived from it. A question
the processed tables cannot answer can still be asked of these.

`AUTO_COMPRESS` is off on both: the parquets are already compressed, and
gzipping the raw JSON here would only have to be undone by `COPY INTO`.

## Credentials

`AMARA_SNOWFLAKE_*` in the project's root `.env` — see `../.env.example`:

```bash
cp ../.env.example ../.env
```

Every key is required and there are no fallbacks in code, so a missing one
fails before the first request rather than connecting as somebody else.
Authentication is a **programmatic access token**, not a password: generate
one under the user's settings in Snowsight.

`AMARA_SNOWFLAKE_STAGE` is the one optional key, defaulting to `AMARA_STAGE`.

## Layout

```
upload_raw.py         the crawl JSON -> the RAW schema's stage
upload_processed.py   the parquets   -> the PROCESSED schema's stage
load_processed.py     that stage     -> the PROCESSED tables
cleanup.py            drop the local raw/staging/processed data once it is up
load_fact.py          the fact table          (not written yet)
load_analytical.py    the analytical schema   (not written yet)
```

Each script carries its own `connect()`. They are four short programs against
one database rather than a library with drivers, and the duplication is three
lines each.

## A note on the folder name

This directory was called `snowflake/`, which collided with the installed
`snowflake-connector-python` package: both are PEP 420 namespace packages, so
with the repo root on `sys.path` they merged into one `snowflake` namespace.
`import snowflake.connector` still resolved, but the day anyone added a
`snowflake/connector.py` it would not have. `hang/` collides with
nothing.
