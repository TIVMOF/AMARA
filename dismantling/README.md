# dismantling

Takes raw crawls apart into files Spark can read.

```bash
python3 scripts/dismantle_raw.py                      # every crawl under ingestion/data/raw
python3 scripts/dismantle_raw.py path/to/crawl.json   # one crawl
```

No dependencies beyond the standard library.

## Why this stage exists

Ingestion writes one JSON object per crawl, with product bodies keyed by
product id so a product carried by twelve collections is stored once. That
shape is right for storage — it is the difference between 2.8 GB and 16 GB —
and wrong for Spark, which infers one column per key:

```
spark.read.json(raw)                      532,091 rows, 1 column   (reads each line as a record)
spark.read.option("multiLine",True)...    1 row, products -> StructType with 44,096 fields
```

So each crawl becomes three files under `data/staging/<site>/<timestamp>/`:

```
crawl.json       the crawl's own metadata, one indented object
products.jsonl   one product per line, without its variants
variants.jsonl   one variant per line, carrying its product id
```

`.jsonl` is one record per line, which Spark splits and reads in parallel.

Nothing is cleaned or renamed here — that is `../processing/`. This stage only
changes the shape: maps become lines, and variants are lifted out of their
product so the two can be read as two tables. Every row carries `site` and
`scraped_at` so they can be joined back together, and so a table built from
several crawls knows which crawl each row came from.

The one field that is reshaped rather than copied is `vendors`. It arrives
keyed by vendor name — the same map shape that makes the product bodies
unreadable — and two spellings differing only in case collide outright under
Spark's case-insensitive column names, which `032c` and `032C` do. It becomes
a list of `{vendor, products}` records.

## Cost

50 crawls, 2.8 GB of raw in and 2.0 GB of staging out, in a few minutes. One
malformed crawl is reported and skipped rather than stopping the other 49.
`data/` is gitignored.
