# data-scraper

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
.venv/bin/python -m scripts brands                      # what the allowlist holds
```

## How it fits together

```
sites/*.yaml  ──┐
                ├──► registry.py ──► adapters/<adapter>.py ──► normalize.py ──► data/
brands.yaml   ──┘                          │
                                        fetch.py
```

- **`sites/*.yaml`** — one file per retailer. Adding a site is adding a file.
- **`brands.yaml`** — the allowlist. Products from unlisted brands are dropped.
- **`registry.py`** — loads both, and maps a site's `adapter:` key to its module.
- **`adapters/`** — one module per *access method*, not per website. Every
  Shopify store shares `shopify.py` because they share an identical API;
  a store on another platform gets its own module, never a branch inside
  an existing one.
- **`fetch.py`** — the only module that touches the network. Rate limiting,
  retries and the User-Agent live here.
- **`normalize.py`** — envelope and JSON writing. Shape only, no cleaning.

## Adapters

| adapter | endpoint | status |
|---|---|---|
| `shopify` | `/products.json`, `/collections/<h>/products.json`, `/meta.json` | written |
| `jsonld` | `<script type="application/ld+json">` on product pages | not written |
| `woo` | `/wp-json/wc/store/products` | not written |
| `dom` | CSS selectors from the site's YAML | not written |

## Output

One file per site per run: `data/<site>/<timestamp>.json`.

```json
{
  "site": "brownsfashion",
  "currency": "GBP",
  "country": "GB",
  "scraped_at": "2026-08-28T16:06:32Z",
  "products_seen": 500,
  "products_kept": 241,
  "unmatched_vendors": { "Valentino Garavani": 24, "Self-Portrait": 15 },
  "products": [ ... ]
}
```

Each product:

```json
{
  "name": "draped maxi dress",
  "brand": "Alaïa",
  "brand_segment": "Luxury House",
  "brand_tier": "Luxury",
  "brand_styles": ["Glamour"],
  "category": "Clothing",
  "gender": "Women",
  "product_url": "https://www.brownsfashion.com/products/ala-a-draped-maxi-dress-...",
  "price": "3500.00",
  "original_price": null,
  "currency": "GBP",
  "color": null,
  "material": null,
  "sizes": [{ "size": "36", "available": true, "price": "3500.00", "sku": "..." }],
  "availability": true,

  "retailer": "brownsfashion",
  "product_id": "10748979511560",
  "sku": "OLNSS2600161163",
  "images": ["https://cdn.shopify.com/..."],
  "scraped_at": "2026-08-28T16:06:32Z",

  "source": { "vendor": "...", "tags": [...], "description_bullets": [...], "body_html": "...", "options": [...] }
}
```

`source` holds the raw fields the rest were derived from. It is where `color`
and `material` are hiding until a parser is written — most retailers bury them
in an unlabelled bullet list:

```
Browns  → ["black", "velvet", "rhinestone embellishments", "draped detail", ...]
CNCPTS  → ["Leather upper with soft suede overlays; frayed edges", ...]
```

Bullet 0 is the colour on Browns. It is not on CNCPTS. That is why the parser
does not exist yet: the pattern is per-retailer, and guessing it now would bake
in errors that are invisible once the raw text is discarded.

### Fields that are not always populated

| field | why |
|---|---|
| `color` | only set when the store declares a Color variant option (~40% of records) |
| `material` | always null for now; the text is in `source.description_bullets` |
| `gender` | read from tags; null when the store does not tag gender |
| `category` | the store's own `product_type` string, not a shared taxonomy |
| `original_price` | some stores set it equal to `price` when not on sale |

## The brand allowlist

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
Serapian sells bags and Destin makes knitwear is a fact about the product, and
lives in the product's `category` field. Likewise which genders a brand dresses
— that is the product's `gender`.

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

Every crawl prints the vendors it dropped, most frequent first:

```
dropped brands: Second/Layer (12), Soft Goat (10), Tory Burch (4), ...
```

That report is how the allowlist grows. Anything in it that should have been
kept is either a missing brand or a missing alias.

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
