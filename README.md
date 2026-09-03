# AMARA

**A**ttire **M**etrics & **A**nalytic **R**etail **A**rchitecture — a clothing
catalogue data collection and analysis platform.

Fifty Shopify retailers become clean parquet tables, ready for Snowflake to
build a star schema from.

## The pipeline

Four stages, each a directory with one entry point, run in order:

```bash
source .venv/bin/activate

(cd ingestion   && python ingestion.py crawl)   # retailers -> ingestion/data/raw/
(cd dismantling && python dismantling.py)       # raw       -> dismantling/data/staging/
(cd processing  && spark-submit process.py)     # staging   -> processing/data/processed/
(cd snowflake   && python upload_processed.py)  # processed -> a Snowflake stage
```

| stage | takes | produces | why it is separate |
|---|---|---|---|
| `ingestion/` | 50 storefronts | one JSON per crawl | **collection only** — nothing is filtered, mapped, cleaned or interpreted. Deduplication is the sole exception. |
| `dismantling/` | those JSON files | three files per crawl | a change of *shape* only, so Spark can read what ingestion wrote. |
| `processing/` | the staged files | 12 parquet tables | the only stage that changes a value. Cleaning, folding, the brand allowlist. |
| `snowflake/` | the parquets | rows in a warehouse | upload and load. The star schema is built here, not upstream. |

The boundary that matters is the first one: **ingestion stores what a store
sent, processing decides what it means.** A rule about brands or categories
belongs in `processing/reference/*.yaml`, never in a crawler.

## Setup

One virtualenv at the root, shared by every stage:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp ingestion/.env.example ingestion/.env    # crawler settings
cp .env.example .env                        # Snowflake credentials
```

Each stage keeps its own `requirements.txt` recording what that stage alone
needs; the root file just gathers them.

Processing additionally needs a JVM — Spark 4 wants Java 17 or 21:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

## Layout

```
ingestion/     ingestion.py    + scripts/, sites/*.yaml   one YAML per retailer
dismantling/   dismantling.py  + scripts/
processing/    process.py      + scripts/, reference/*.yaml   the vocabularies
snowflake/     upload_*.py     + warehouse.py
img/           the analytical model this all feeds
```

Every stage writes into its own `data/`, which is gitignored. Each `scripts/`
is a library with no entry point of its own — there is exactly one way to run
a stage.

## The model

`img/amara-analystical-data-diagram.png` is the target. Processing stops short
of it deliberately: it emits `products`, `variants`, `crawls`, `retailers`,
`dates` and the reference vocabularies, all holding natural values in upper
case rather than surrogate ids. Snowflake assigns the keys and builds the
dimensions and facts. See `processing/README.md` for the table shapes.
