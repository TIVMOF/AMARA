# ingestion

Collects raw product data from fashion retailers into JSON files under
`data/raw/`.

Collection only. Every product a store serves is stored exactly as it arrived —
no filtering, no field mapping, no cleaning, no interpretation. The one thing
imposed is deduplication: a product carried by twelve collections is stored
once, because storing it twelve times cost 16 GB where 2.8 GB holds the same
catalogue.

Everything that decides what a value *means* — brand classification, the
product record shape, the dimensional model — lives in `../processing/`.

## Setup

The virtualenv is the one at the repo root, shared by every stage:

```bash
cd .. && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cd ingestion && cp .env.example .env
```

`requirements.txt` here records what this stage alone needs; the root file
gathers them all.

## Usage

```bash
python ingestion.py crawl                       # every enabled site
python ingestion.py crawl brownsfashion kith    # named sites
python ingestion.py crawl kith --max-pages 2    # short run, to eyeball
python ingestion.py probe example.com           # can this site be scraped?
python ingestion.py sites                       # what is configured
python ingestion.py collections kith            # what a store publishes
```

## How it fits together

```
sites/*.yaml ──► registry.py ──► adapters/<adapter>.py ──► store.py ──► data/raw/
                                          │
                                       fetch.py
```

- **`sites/*.yaml`** — one file per retailer. Adding a site is adding a file.
- **`registry.py`** — loads them, and maps a site's `adapter:` key to its module.
- **`adapters/`** — one module per *access method*, not per website. Every
  Shopify store shares `shopify.py` because they share an identical API;
  a store on another platform gets its own module, never a branch inside
  an existing one.
- **`fetch.py`** — the only module that touches the network. Rate limiting,
  retries and the User-Agent live here.
- **`store.py`** — JSON writing. Nothing else; it does not interpret a product.
- **`site_config.py`** — the shape of a `sites/*.yaml`. It exists to fail loudly:
  an unknown key is an error, so `discover_colections: true` is caught at load
  rather than silently leaving sharding off.

## Adapters

| adapter | endpoint | status |
|---|---|---|
| `shopify` | `/products.json`, `/collections/<h>/products.json`, `/meta.json` | written |
| `jsonld` | `<script type="application/ld+json">` on product pages | not written |
| `woo` | `/wp-json/wc/store/products` | not written |
| `dom` | CSS selectors from the site's YAML | not written |

## Output

One file per crawl:

```
data/raw/<site>/<timestamp>.json
```

It holds every product body the store served, exactly as it served them. No
product is dropped, no field is interpreted, nothing is cleaned or renamed.
Normalization is a separate job for whatever reads these files.

```json
{
  "site": "brownsfashion",
  "adapter": "shopify",
  "base_url": "https://www.brownsfashion.com",
  "currency": "GBP",
  "country": "GB",
  "brand_override": null,
  "scraped_at": "2026-08-30T09:14:02Z",

  "products_received": 189619,
  "products_stored": 44096,
  "vendors": { "Gucci": 812, "Saint Laurent": 604, ... },

  "complete": false,
  "pages": 806,
  "short_pages": 12,
  "collections_crawled": 45,
  "listings": [
    { "label": "*unfiltered*", "pages_fetched": 100,
      "stopped_reason": "page_ceiling", "short_pages": [], "new_products": 25000 }
  ],
  "errors": [],
  "throttled": 0,
  "rate_limit_start": 1.0,
  "rate_limit_final": 1.0,

  "responses": [
    { "listing": "*unfiltered*", "page": 1,
      "url": "https://www.brownsfashion.com/products.json?limit=250&page=1",
      "count": 250, "product_ids": ["7639913463879", ...] }
  ],
  "products": {
    "7639913463879": { ...the body Shopify sent, untouched... }
  }
}
```

`brand_override` is what the site config claims about the store, recorded and
not applied — therow.com is configured `The Row` and serves seven vendors,
including archive Yohji Yamamoto and Madame Grès. Both the claim and what the
store actually said are kept, so processing can decide.

`products_received` counts deliveries, `products_stored` counts distinct
bodies. Bodies are keyed by id and stored once: collections overlap by design,
so storing whole response bodies meant writing the same product up to fourteen
times — 1.7M bodies for 284K products, 16 GB of raw. `responses` keeps the full
page-by-page trace as id lists, which is what the attribution is needed for.
See issue #15.

### How raw is raw

Raw at the record level, not the HTTP level. `fetch.py` returns
`response.json()`, so what is stored is the parsed product object — every
field, every nesting level, original key order, no selection or coercion —
re-serialized at `indent=2`. Same data, different bytes.

Not retained: the HTTP status and headers, the `{"products": …}` envelope,
`/meta.json` and `/collections.json` responses, the losing attempts of a
short-page retry, and later copies of a duplicate id (first delivery wins, so
a mid-crawl price change is invisible).

## Sharding by collection

An unfiltered `/products.json` stops at 100 pages, so it cannot reach past
25,000 products. Collection-scoped endpoints each get their own page budget,
which is the only way past that — tag-filtered collection URLs return 404 and
`sort_by` is accepted on these endpoints and then silently ignored.

**The decision is made at run time, not in config.** `/products.json` is
crawled first. If it ends on an empty page it has shown the whole catalogue and
nothing is discovered at all; if it hits the ceiling, collections are discovered
then. In the last full run **5 of 50 sites sharded** — the five whose listing
hits the ceiling. `discover_collections: true` is permission, not instruction.

Where sharding does run, the crawl takes the largest `max_collections` and then
keeps walking the list only while it is still finding products it has not
already seen, stopping after five collections that add fewer than 25 each:

```
brownsfashion  tail contributions [0, 0, 0, 0, 0]                stopped at 45
stadiumgoods   tail contributions [1196, 73, 0, 858, 367, ...]   stopped at 65
```

Four of the five had nothing past collection 40. stadiumgoods had 1,196 and 858
and 927 sitting at ranks 41, 44 and 59, and a fixed cap would have lost them.

`python ingestion.py collections <site>` lists what a store publishes, largest
first. Browns has 2,700 non-empty collections; they overlap heavily, so the
largest few dozen cover the catalogue for a fraction of the ~3,500 requests
crawling all of them would cost.

## Adding a site

```bash
python ingestion.py probe someretailer.com
```

It reports whether the JSON is open, whether the store is multi-brand or
single-brand, its currency, and prints a `sites/*.yaml` to paste in.

For single-brand stores, check the vendor strings first. Several put something
other than the brand in Shopify's `vendor` field — SKIMS uses fabric names
(`COTTON JERSEY`), Dries Van Noten uses seasons (`AW26 MEN`), Ted Baker uses
licensees (`Jack Victor`). Set `brand_override` for those — the crawl records
it alongside the vendors it actually saw, and does not act on it.

## Notes

- `.env` holds the User-Agent and HTTP tunables. There are no fallbacks in
  code: a missing key raises rather than silently using a stale value.
- Rate limit defaults to 1 req/s per host, overridable per site. A 429
  widens the interval for the rest of that crawl rather than failing.
- `data/` is gitignored.
