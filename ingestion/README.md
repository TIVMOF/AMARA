# ingestion

Collects raw product data from fashion retailers into JSON files under `data/`.

No database, no cleaning. Values arrive as the site sent them — prices stay
strings, brands stay however the retailer spelled them, categories stay raw.
The only thing imposed is a consistent *shape* so every site's output is
readable side by side.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

## Usage

```bash
.venv/bin/python -m scripts crawl                       # every enabled site
.venv/bin/python -m scripts crawl brownsfashion kith    # named sites
.venv/bin/python -m scripts crawl kith --max-pages 2    # short run, to eyeball
.venv/bin/python -m scripts probe example.com           # can this site be scraped?
.venv/bin/python -m scripts sites                       # what is configured
.venv/bin/python -m scripts collections kith            # what a store publishes
.venv/bin/python -m scripts brands                      # what the allowlist holds
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
- **`brands.yaml`** — a brand list with a segment/tier/style taxonomy. Nothing
  in the crawl path reads it; it is here for whatever consumes `data/raw/`.

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
  "currency": "GBP",
  "country": "GB",
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

## The brand list

**Nothing in the crawl path reads this.** The crawl collects every product a
store serves, whatever the vendor. `brands.yaml` is kept here, with its loader
and the `brands` command, for whatever consumes `data/raw/` later.

A brand sits on three independent axes rather than one label:

```
Yohji Yamamoto   segment=Designer               tier=Luxury      styles=[Avant-Garde]
Nike             segment=Sportswear             tier=Premium     styles=[Performance]
Zara             segment=High Street            tier=Mainstream  styles=[Casual]
Uma Wang         segment=Contemporary Designer  tier=Luxury      styles=[Avant-Garde, Minimalist]
```

`styles` is a list — a brand routinely sits in more than one, and forcing a
single label throws away half the classification.

| axis | vocabulary |
|---|---|
| `segment` | Luxury House · Designer · Contemporary Designer · Streetwear · Sportswear · Outdoor · Casual · High Street · Denim · Basics |
| `tier` | High Luxury · Luxury · Premium · Mainstream |
| `styles` | Avant-Garde · Contemporary · Lifestyle · Occasion · Minimalist · Classic · Glamour · Feminine · Casual · Street · Performance · Technical · Heritage |

What a brand *makes* is deliberately not an axis. That Church's sells shoes,
Serapian sells bags and Destin makes knitwear is a fact about the product, not
about the brand. Likewise which genders a brand dresses.

Brands are grouped in `brands.yaml` by shared (segment, tier, styles) to keep
~220 entries readable. Every value is checked against the vocabularies at load
time, so a typo fails loudly rather than inventing an eleventh style:

```
error: brands.yaml: group 8: style='Avante-Garde' is not in the declared styles
       (Avant-Garde, Casual, Classic, Feminine, Glamour, Heritage, ...)
```

Aliases are checked too — one defined for a brand that is not listed is an error.

```bash
.venv/bin/python -m scripts brands --by tier
.venv/bin/python -m scripts brands --by style --detail
```

### Matching

Matching folds case, accents and punctuation, so `ACNE STUDIOS`, `Acne Studios`,
`ALAIA` and `Alaïa` all match. Sub-labels and different spellings need an entry
under `aliases:` — `Air Jordan → Jordan`, `Valentino Garavani → Valentino`.

Every crawl prints the vendors it saw, most frequent first, and the full tally
is in the file under `vendors`:

```
263 vendors: Gucci (812), Saint Laurent (604), Prada (455), ...
```

That report is how the list grows. Anything in it that should be classified is
either a missing brand or a missing alias.

## Sharding by collection

An unfiltered `/products.json` stops at 100 pages, so it cannot reach past
25,000 products. It is also incomplete well below that on some stores - renarts
serves 249 products unfiltered and 1,975 across its collections.

Both are fixed by crawling collection-scoped endpoints, each of which gets its
own page budget:

```yaml
discover_collections: true
max_collections: 40
```

`python -m scripts collections <site>` lists what a store publishes, largest
first. Browns has 2,700 non-empty collections; they overlap heavily, so the
largest few dozen cover the catalogue for a fraction of the ~3,500 requests
crawling all of them would cost.

## Adding a site

```bash
.venv/bin/python -m scripts probe someretailer.com
```

It reports whether the JSON is open, whether the store is multi-brand or
single-brand, its currency, and prints a `sites/*.yaml` to paste in.

For single-brand stores, check the vendor strings first. Several put something
other than the brand in Shopify's `vendor` field — SKIMS uses fabric names
(`COTTON JERSEY`), Dries Van Noten uses seasons (`AW26 MEN`), Ted Baker uses
licensees (`Jack Victor`). Set `brand_override` for those.

## Notes

- `.env` holds the User-Agent and HTTP tunables. There are no fallbacks in
  code: a missing key raises rather than silently using a stale value.
- Rate limit defaults to 0.5 req/s per host, overridable per site.
- `data/` is gitignored.
