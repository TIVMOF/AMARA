# AMARA

**A**ttire **M**etrics & **A**nalytic **R**etail **A**rchitecture — a clothing
catalogue data collection and analysis platform.

Fifty Shopify retailers become clean parquet tables, ready for Snowflake to
build a star schema from.

## The pipeline

Three components, named for what making a garment takes. AMARA **gets** the
materials, **cuts** the fabric and **sews** the garment.

| stage | takes | produces | why it is separate |
|---|---|---|---|
| `get/` | 50 storefronts | one JSON per retailer | **collection only** — nothing is filtered, mapped, cleaned or interpreted. Deduplication is the sole exception. |
| `cut/` | those JSON files | 12 parquet tables | the only stage that changes a value. Cleaning, folding, the brand allowlist. |
| `sew/` | the parquets | rows in Snowflake | upload and load. The star schema is built here, not upstream. |

The boundary that matters is the first one: **get stores what a store sent,
cut decides what it means.** A rule about brands or categories belongs in
`cut/reference/*.yaml`, never in a crawler.

`cut` does its work in two halves inside one command. The raw JSON is first
**dismantled** into the flat files Spark can read — a change of *shape* only,
standard library, no interpretation — and only then does Spark build the
tables. The order is not a convention: the second half reads what the first
half writes. The dismantled files stay on disk afterwards, so a failed table
build can be re-run with `--skip-dismantle` rather than starting over.

## One run is one crawl on one date

The whole pipeline handles **a single crawl, stamped once**. `get.py` takes
one timestamp at the start and gives it to every retailer, so a run lasting
hours — which a full crawl does — cannot straddle midnight and split itself
across two dates. `--scraped-at` passes that stamp back in, so a retry can
crawl the retailers that are missing and have them join the same crawl.

That matters downstream: `products` and `variants` are cumulative in Snowflake,
and a row is told apart from the same product in an earlier crawl **by date
alone**. Two dates inside one run would make one product look like two.

So each stage empties its own `data/` at the end of a pass, and the next crawl
starts from an empty one. `get.py validate` fails if it finds more than one
crawl.

```bash
(cd get && python get.py crawl  && python get.py validate)
(cd get && python get.py upload && python get.py validate-upload)
(cd cut && python cut.py        && python cut.py validate)
(cd cut && python cut.py upload && python cut.py validate-upload)
(cd sew && python sew.py load-processed)
(cd sew && python sew.py load-analytical && python sew.py validate-loaded)

# once it is all up, each stage drops its own copy
(cd get && python get.py cleanup)
(cd cut && python cut.py cleanup)
```

Every stage checks that the one before it actually ran. `cut` stops with
something actionable when there is no raw crawl to dismantle, and does it
before the JVM starts; `sew` stops when the PROCESSED stage holds no parquet,
before it truncates a table. Both were silent before — a `COPY INTO` from an
empty stage loads nothing and reports success, which under Airflow is a green
task and an empty warehouse.

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
.venv/bin/pip install -r get/requirements.txt \
                     -r cut/requirements.txt \
                     -r sew/requirements.txt

cp get/.env.example get/.env    # crawler settings, and RAW credentials
cp cut/.env.example cut/.env    # PROCESSED credentials, for `upload`
cp sew/.env.example sew/.env    # PROCESSED and ANALYTICAL credentials
```

`cut/` needs a JVM, and Spark is found through the `python` on PATH:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$PWD/.venv/bin:$PATH"
```

## Containers

Each stage builds into an image of its own — `amara-get`, `amara-cut`,
`amara-sew` — from the `Dockerfile` in its folder. Nothing is read from the
host: a stage's config travels in its image, its data travels on a volume, and
its credentials arrive at run time.

```bash
for s in get cut sew; do docker build -t "amara-$s" "./$s"; done
```

**One volume, `amara_storage`, shared by `get` and `cut`**, with a directory
per stage inside it. `sew` mounts nothing at all: everything it works on is
already in Snowflake.

| directory | written by | read by |
|---|---|---|
| `collected/` | `get` | `cut` |
| `dismantled/` | `cut` | `cut` — its own intermediate, and nothing downstream |
| `trimmed/` | `cut` | `cut upload` |

One environment variable points at the whole thing:

```
AMARA_STORAGE_DIR=/data
```

It names a directory **inside the container** and says nothing about what backs
it — a named volume, a bind mount, or the container's own writable layer are
all the same to the code. Unset, which is how a checkout runs, each stage falls
back to the path beside its code (`get/data`, `cut/data/dismantled`,
`cut/data/trimmed`), so a local `python get.py crawl` behaves exactly as it
always has.

```bash
docker run --rm --init -v amara_storage:/data \
  -e AMARA_SNOWFLAKE_TOKEN="$TOKEN" amara-get crawl
docker run --rm --init -v amara_storage:/data amara-cut
docker run --rm --init -v amara_storage:/data \
  -e AMARA_SNOWFLAKE_TOKEN="$TOKEN" amara-cut upload
docker run --rm --init -e AMARA_SNOWFLAKE_TOKEN="$TOKEN" amara-sew load-processed
```

**Use `--init`.** An exec-form entrypoint makes Python PID 1, and the kernel
disables PID 1's default signal actions — so a `docker stop` arriving before
the crawler has installed its own SIGTERM handler is dropped silently, and the
stop costs the full grace period and a SIGKILL. Measured: 10.1s and exit 137
without `--init`, 0.1s and exit 143 with it. Once the crawl is running its own
handler takes over and a stop returns in 0.2s with exit 130, keeping every
retailer already written.

**What is baked in, and what is not.** `get/sites/*.yaml` and
`cut/reference/*.yaml` ship inside their images — they are versioned config, so
changing a brand or a retailer means rebuilding that image. No `.env` is ever
copied in: `.dockerignore` excludes it, and every `AMARA_SNOWFLAKE_*` value is
passed at run time. `python-dotenv` is loaded without `override`, so a real
environment variable always wins over a file.

Two sizing notes: `cut`'s dismantling half reads a whole crawl file into memory
and the largest is ~308 MB, so give the container 4 GB. Its Spark half runs in
local mode with `SPARK_DRIVER_MEMORY=8g` baked in — unset it and PySpark's 1 GB
default will not hold 2.7M variants.

## Layout

```
get/   get.py   scripts/  sites/*.yaml       crawl, and put the JSON up
cut/   cut.py   scripts/  reference/*.yaml   dismantle it, build the tables, put them up
sew/   sew.py   scripts/                     load them, and derive the model
img/   the analytical model this all feeds

Each stage owns what it produced: it uploads its own output, checks its own
upload, and deletes its own copy when that is safe. `sew` owns only what
happens once the data is in Snowflake, and has nothing local to clean up.
```

Each `scripts/` is a library with no entry point of its own — there is exactly
one way to run a stage. Every stage writes into its own gitignored `data/`:

```
get/data/agjeans-20260916T122802Z.json              one flat file per retailer
cut/data/dismantled/agjeans/20260916T122802Z/       three files, so a folder each
cut/data/trimmed/products/                          one folder per parquet table
```

## The model

`img/amara-analystical-data-diagram.png` is the target. `cut/` stops short of
it deliberately: it emits `products`, `variants`, `crawls`, `retailers`,
`dates` and the reference vocabularies, all holding natural values in upper
case rather than surrogate ids. Snowflake assigns the keys and builds the
dimensions and facts. See `cut/README.md` for the table shapes.
