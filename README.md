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

So each stage empties its own `data/` at the end of a pass, and the next crawl
starts from an empty one. `gather.py validate` fails if it finds more than one
crawl.

```bash
(cd gather && python gather.py crawl  && python gather.py validate)
(cd gather && python gather.py upload && python gather.py validate-upload)
(cd shear  && python3 shear.py        && python3 shear.py validate)
(cd stitch && spark-submit stitch.py  && spark-submit stitch.py validate)
(cd stitch && spark-submit stitch.py upload && spark-submit stitch.py validate-upload)
(cd hang   && python hang.py load-processed)
(cd hang   && python hang.py load-analytical && python hang.py validate-loaded)

# once it is all up, each stage drops its own copy
(cd gather && python gather.py cleanup)
(cd shear  && python3 shear.py cleanup)
(cd stitch && spark-submit stitch.py cleanup)
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

cp gather/.env.example gather/.env    # crawler settings, and RAW credentials
cp stitch/.env.example stitch/.env    # PROCESSED credentials, for `upload`
cp hang/.env.example   hang/.env      # PROCESSED and ANALYTICAL credentials
```

`shear/` is stdlib-only and runs on a bare `python3`. `stitch/` needs a JVM,
and `spark-submit` finds Spark through the `python` on PATH:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$PWD/.venv/bin:$PATH"
```

## Containers

Each stage builds into an image of its own — `amara-gather`, `amara-shear`,
`amara-stitch`, `amara-hang` — from the `Dockerfile` in its folder. Nothing is
read from the host: a stage's config travels in its image, its data travels on
a volume, and its credentials arrive at run time.

```bash
for s in gather shear stitch hang; do docker build -t "amara-$s" "./$s"; done
```

Three volumes carry the data between them, and each stage is told where its own
storage is mounted:

| variable | stage reads | stage writes | default with nothing set |
|---|---|---|---|
| `AMARA_GATHERED_DIR` | shear | gather | `gather/data` |
| `AMARA_SHEARED_DIR` | stitch | shear | `shear/data` |
| `AMARA_STITCHED_DIR` | — | stitch | `stitch/data` |

They name a directory **inside the container**, and say nothing about what
backs it. The images set them to `/data/gathered`, `/data/sheared` and
`/data/stitched`; mount a volume there and the data persists, mount nothing and
it lands in the container's own layer and is thrown away. Unset — which is how
a checkout runs — each falls back to the path above, so a local
`python gather.py crawl` behaves exactly as it always has.

```bash
docker run --rm --init -v amara_gathered:/data/gathered \
  -e AMARA_SNOWFLAKE_TOKEN="$TOKEN" amara-gather crawl
docker run --rm --init -v amara_gathered:/data/gathered:ro \
                       -v amara_sheared:/data/sheared amara-shear
docker run --rm --init -v amara_sheared:/data/sheared:ro \
                       -v amara_stitched:/data/stitched amara-stitch
docker run --rm --init -e AMARA_SNOWFLAKE_TOKEN="$TOKEN" amara-hang load-processed
```

`hang` takes no volume: everything it works on is already in Snowflake.

**Use `--init`.** An exec-form entrypoint makes Python PID 1, and the kernel
disables PID 1's default signal actions — so a `docker stop` arriving before
the crawler has installed its own SIGTERM handler is dropped silently, and the
stop costs the full grace period and a SIGKILL. Measured: 10.1s and exit 137
without `--init`, 0.1s and exit 143 with it. Once the crawl is running its own
handler takes over and a stop returns in 0.2s with exit 130, keeping every
retailer already written.

**What is baked in, and what is not.** `gather/sites/*.yaml` and
`stitch/reference/*.yaml` ship inside their images — they are versioned config,
so changing a brand or a retailer means rebuilding that image. No `.env` is
ever copied in: `.dockerignore` excludes it, and every `AMARA_SNOWFLAKE_*`
value is passed at run time. `python-dotenv` is loaded without `override`, so a
real environment variable always wins over a file.

Two sizing notes: `shear` reads a whole crawl file into memory and the largest
is ~308 MB, so give it 4 GB. `stitch` runs Spark in local mode with
`SPARK_DRIVER_MEMORY=8g` baked in — unset it and PySpark's 1 GB default will
not hold 2.7M variants.

## Layout

```
gather/   gather.py   scripts/  sites/*.yaml       crawl, and put the JSON up
shear/    shear.py    scripts/                     cut it into Spark-shaped files
stitch/   stitch.py   scripts/  reference/*.yaml   build the tables, and put them up
hang/     hang.py     scripts/                     load them, and derive the model
img/      the analytical model this all feeds

Each stage owns what it produced: it uploads its own output, checks its own
upload, and deletes its own copy when that is safe. `hang` owns only what
happens once the data is in Snowflake, and has nothing local to clean up.
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
