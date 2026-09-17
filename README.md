# AMARA

**A**ttire **M**etrics & **A**nalytic **R**etail **A**rchitecture — a clothing
catalogue data collection and analysis platform.

Fifty Shopify retailers become clean parquet tables, ready for Snowflake to
build a star schema from.

## The pipeline

Four components, named for what making a garment takes. AMARA **gathers** the
materials, **shears** the fabric, **stitches** the clothes and **hangs** them.

| stage | takes | produces | why it is separate |
|---|---|---|---|
| `gather/` | 50 storefronts | one JSON per retailer | **collection only** — nothing is filtered, mapped, cleaned or interpreted. Deduplication is the sole exception. |
| `shear/` | those JSON files | three files per retailer | a change of *shape* only, so Spark can read what gather wrote. |
| `stitch/` | the sheared files | 12 parquet tables | the only stage that changes a value. Cleaning, folding, the brand allowlist. |
| `hang/` | the parquets | rows in Snowflake | upload and load. The star schema is built here, not upstream. |

The boundary that matters is the first one: **gather stores what a store sent,
stitch decides what it means.** A rule about brands or categories belongs in
`stitch/reference/*.yaml`, never in a crawler.

## One run is one crawl on one date

The whole pipeline handles **a single crawl, stamped once**. `gather.py` takes
one timestamp at the start and gives it to every retailer, so a run lasting
eighteen hours — which a full crawl does — cannot straddle midnight and split
itself across two dates.

That matters downstream: `products` and `variants` are cumulative in Snowflake,
and a row is told apart from the same product in an earlier crawl **by date
alone**. Two dates inside one run would make one product look like two.

So `hang.py cleanup` runs at the end of every pass, and the next crawl starts
from an empty `data/`. `gather.py validate` fails if it finds more than one
crawl.

```bash
(cd gather && python gather.py crawl        && python gather.py validate)
(cd shear  && python3 shear.py              && python3 shear.py validate)
(cd stitch && spark-submit stitch.py        && spark-submit stitch.py validate)
(cd hang   && python hang.py upload-raw       && python hang.py validate-raw)
(cd hang   && python hang.py upload-processed && python hang.py validate-processed)
(cd hang   && python hang.py load-processed)
(cd hang   && python hang.py load-analytical  && python hang.py validate-loaded)
(cd hang   && python hang.py cleanup)       # drop the local data, once it is up
```

A validator exits 1 on an error — output that is internally inconsistent and
should not be built on — and 0 on a warning, which is data that is thin rather
than wrong.

**`cleanup.py` is the last step for a reason.** It deletes every `data/`
directory, so Snowflake becomes the only copy. Run it once the uploads have
actually succeeded; nothing brings the data back but another crawl.

## Setup

Every component is self-contained: its own `requirements.txt`, and its own
`.env` where it needs one.

```bash
python3 -m venv .venv
.venv/bin/pip install -r gather/requirements.txt \
                     -r shear/requirements.txt \
                     -r stitch/requirements.txt \
                     -r hang/requirements.txt

cp gather/.env.example gather/.env    # crawler settings
cp hang/.env.example   hang/.env      # Snowflake credentials
```

`shear/` is stdlib-only and runs on a bare `python3`. `stitch/` needs a JVM,
and `spark-submit` finds Spark through the `python` on PATH:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$PWD/.venv/bin:$PATH"
```

## Layout

```
gather/   gather.py   scripts/  sites/*.yaml
shear/    shear.py    scripts/
stitch/   stitch.py   scripts/  reference/*.yaml
hang/     hang.py     scripts/
img/      the analytical model this all feeds
```

Each `scripts/` is a library with no entry point of its own — there is exactly
one way to run a stage. Every stage writes into its own gitignored `data/`:

```
gather/data/agjeans-20260916T122802Z.json   one flat file per retailer
shear/data/agjeans/20260916T122802Z/        three files, so a folder each
stitch/data/products/                       one folder per parquet table
```

## The model

`img/amara-analystical-data-diagram.png` is the target. `stitch/` stops short
of it deliberately: it emits `products`, `variants`, `crawls`, `retailers`,
`dates` and the reference vocabularies, all holding natural values in upper
case rather than surrogate ids. Snowflake assigns the keys and builds the
dimensions and facts. See `stitch/README.md` for the table shapes.
