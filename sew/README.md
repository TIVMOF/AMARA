# sew

Takes what is already in Snowflake's stages and makes tables of it, then derives
the analytical model from those tables.

```bash
python sew.py load-processed        # the PROCESSED stage -> the PROCESSED tables
python sew.py load-analytical       # PROCESSED -> dimensions and facts
python sew.py validate-loaded       # do the loaded rows match what was sent?
```

Filling the stages belongs to the stages that produce the files, so that a
component and the check on its own output stay together:

```bash
(cd ../get && python get.py upload && python get.py validate-upload)
(cd ../cut && python cut.py upload && python cut.py validate-upload)
```

There is no `cleanup` here, because there is nothing to clean: this stage
writes no local file, and the only temporary tables it makes are Snowflake
`TEMPORARY` ones, which go when the session does. Each earlier stage deletes
its own `data/` — `get.py cleanup`, `cut.py cleanup`,
`python cut.py cleanup` — once its output is safely up.

## In a container

`amara-sew`, built from the `Dockerfile` here. It takes **no volume**:
everything it works on is already in Snowflake, and this stage builds exactly
one path in its whole source. All seven `AMARA_SNOWFLAKE_*` values are passed
at run time.

It checks that `cut` has uploaded before it loads anything: an empty
PROCESSED stage stops the run rather than truncating every table and filling
none of them.

```bash
docker run --rm --init -e AMARA_SNOWFLAKE_TOKEN="$TOKEN" amara-sew load-processed
```

## Checking the load

`validate-loaded` covers both loaded layers in one command, because the
interesting failure spans them. It counts the rows in the staged parquet —
Snowflake can read a stage directly — and compares that to the table loaded
from it, then compares the analytical layer to the processed one it was built
from.

The fact table is why those two belong together. `load-analytical` builds
`FACT_PRODUCT_OBSERVATION` with an **inner** join to `DIM_PRODUCT`, so a
variant whose product never reached the dimension is dropped without a word.
Nothing inside either layer reveals that; only the two counts side by side do.

Reference tables are rebuilt whole on every load, so they must match the
parquet exactly. `products`, `variants`, `crawls` and `dates` are cumulative —
every crawl adds its date and earlier ones stay — so only the crawl under test
is comparable, and the checks are scoped to its date.

## Why the star schema is built here

`../cut/` stops at clean. It emits `products`, `variants`, `crawls`,
`retailers`, `dates` and the reference vocabularies, all holding **natural
values in upper case rather than surrogate ids** — `products.brand` is
`RICK OWENS`, not `4471`.

That is deliberate. Snowflake assigns the keys and derives the dimensions and
facts of `../img/amara-analystical-data-diagram.png`, because that is where the
analytical model lives and where it can change without re-running Spark over
several GB of crawls. Parquet columns of readable text are what make that
possible: the warehouse has something human to key on.

## Credentials

`AMARA_SNOWFLAKE_*` in this component's own `.env`:

```bash
cp .env.example .env
```

Every key is required and there are no fallbacks in code, so a missing one
fails before the first request rather than connecting as somebody else.
Authentication is a **programmatic access token**, not a password: generate one
under the user's settings in Snowsight.

`get` and `cut` carry their own copies of these keys, because each
component is meant to run on its own. The cost is that the token exists in more
than one file, and rotating it means rotating all of them.

The three schemas are separate keys — `RAW_SCHEMA`, `PROCESSED_SCHEMA` and
`ANALYTICAL_SCHEMA` — and `connect()` takes the one it binds to rather than
reading it itself, so a script that connected to the wrong schema would still
run and just write somewhere unexpected.

## Layout

```
sew.py                        the only entry point

scripts/connection.py          env() and connect(schema)
scripts/paths.py               where the other stages leave their output
scripts/findings.py            the ERROR/WARN convention the validators share

scripts/load_processed.py      the PROCESSED stage -> the PROCESSED tables
scripts/load_analytical.py     PROCESSED           -> dimensions and facts
scripts/validate_loaded.py     both loaded layers, against the stage and each other
```

`scripts/` is a library with no entry point of its own — there is exactly one
way to run this stage, which is the same arrangement as `get` and `cut`.

## A note on the folder name

This directory was called `snowflake/`, which collided with the installed
`snowflake-connector-python` package: both are PEP 420 namespace packages, so
with the repo root on `sys.path` they merged into one `snowflake` namespace.
`import snowflake.connector` still resolved, but the day anyone added a
`snowflake/connector.py` it would not have. `sew/` collides with nothing.
