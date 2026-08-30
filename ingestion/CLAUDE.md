# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`ingestion/` collects clothing catalogues from fashion retailers into JSON
files. It reaches every page a store will serve and stores what arrived. **No
filtering, no field mapping, no cleaning, no interpretation.**

That boundary is the central design fact and it is load-bearing: interpretation
— brand classification, the product record shape, the transformation into the
star schema — belongs to `../processing/`. If a change would make this folder
decide what a value *means*, it goes there instead.

Deduplication is the one exception, and it is structural rather than a policy —
see **The output file** below.

## Commands

```bash
.venv/bin/python -m scripts crawl                    # every enabled site (~2h, 50 sites)
.venv/bin/python -m scripts crawl kith brownsfashion # named sites
.venv/bin/python -m scripts crawl kith --max-pages 2 # short run, to eyeball output
.venv/bin/python -m scripts probe someretailer.com   # is the JSON open? prints a sites/*.yaml
.venv/bin/python -m scripts sites                    # what is configured
.venv/bin/python -m scripts collections kith         # what a store publishes, largest first
```

**There is no test suite** — no pytest, no test files, no CI. Changes are
verified by crawling a real store and checking the output. Small stores good
for this: `modes` (389 products, ~30s), `unionlosangeles` (834), `amiparis`
(656). `brownsfashion` (~25 min) is the cheapest store that actually shards.

A full crawl outlives a laptop's idle timer, and a sleeping Mac silently stalls
it — wrap long runs in `caffeinate -i -m`.

## Architecture

**Adapters are per access method, not per website.** One `adapters/shopify.py`
serves all 50 stores because they share an identical API; they differ only in a
YAML file. A store on another platform gets its own module, never a branch
inside an existing one. `registry.ADAPTERS` maps a site's `adapter:` key to its
module.

```
sites/*.yaml ──► registry.py ──► adapters/shopify.py ──► store.py ──► data/raw/
                                          │
                                       fetch.py     (the only module on the network)
```

`fetch.py` reads its config from `AMARA_INGESTION_*` in `.env` with **no code
fallbacks** — a missing key raises rather than silently using a stale default.

`site_config.py` exists to fail loudly: an unknown key in a site YAML is an
error, so `discover_colections: true` is caught at load rather than silently
leaving sharding off.

### What the Shopify API forces

Everything awkward in `shopify.py` traces to one constraint: a listing endpoint
serves at most `MAX_PAGE` (100) × `PAGE_SIZE` (250) = **25,000 products**, and
page 101 answers HTTP 400. Collection-scoped endpoints each get their own
budget, which is the only way past it. Both alternatives were probed and do not
exist: `/collections/<h>/<tag>/products.json` returns 404, and `sort_by` is
accepted on these endpoints and then silently ignored.

**Sharding is decided at run time, not in config.** `/products.json` is crawled
first; if it ends on an empty page it has shown the whole catalogue and nothing
is discovered at all. Only if it hits the ceiling are collections discovered.
5 of 50 sites shard. `discover_collections: true` is permission, not instruction.

Where sharding runs, the crawl takes the largest `max_collections` and then
keeps walking the list only while collections are still contributing products
it has not seen — `TAIL_PATIENCE` (5) consecutive collections adding fewer than
`TAIL_MIN_NEW` (25) each ends it. Every listing records `new_products`, which is
the number to reason about when tuning depth.

The unfiltered listing is labelled `*unfiltered*` because asterisks cannot
appear in a Shopify handle, and many stores publish a real collection called
`all` that would otherwise collide with it.

A short page is **not** the end of a catalogue — these stores soft-throttle by
serving fewer items instead of returning 429, so `_fetch_page` re-requests a
short page `PAGE_ATTEMPTS` (3) times and only an empty page ends a listing.

### The output file

One per crawl at `data/raw/<site>/<timestamp>.json`. Product bodies keyed by id
under `products`, the page-by-page trace as id lists under `responses`, and the
crawl report alongside — `complete`, `pages`, `short_pages`, `listings`,
`errors`, `vendors`, `throttled`, `rate_limit_*`.

**Deduplication is the only processing done here, and it is structural**:
`products` is a dict keyed by product id, so duplicates are impossible. This
matters — collections overlap heavily by design, and keeping every copy cost
16 GB where 2.8 GB holds the same 336,822 products.

Raw is raw at the *record* level, not the HTTP level. `fetch.py` returns
`response.json()`, so status, headers, the `{"products": …}` envelope, and
byte-level formatting are not retained. Every product body is verbatim.

`brand_override` is recorded and **not applied** — therow.com is configured
`The Row` and serves seven vendors including archive Yohji Yamamoto. The file
keeps both the claim and what the store said.

## Working on this folder

**The issue tracker is the reasoning record.** Code comments cite `#N` freely
and those numbers matter; read the issue before changing the code it guards.
Every issue documents a real failure with the measurement that proved it.

**Claims here are measured, not reasoned.** Several confident-sounding
conclusions turned out to be wrong and were only caught by checking:

- "renarts serves 249 unfiltered against 1,975 sharded" — **false**, and it
  survived in comments long after the fix. It serves all 1,975 unfiltered; the
  249 was a short-page bug reading a 241-item page 2 as the end of the store.
- "a truncated shard means products are unreachable" — **false**. The shards
  that truncate are catch-alls, re-orderings of the whole catalogue. Acting on
  it cost 468 pages for 2 products.
- `products_count` from `/collections.json` is not a coverage measure. modes
  claims 20,822 in its largest collection and serves 389.

Before writing a fact into a comment, a config file or a README, verify it —
and when a fact is corrected, grep for every place it was repeated.

**Gotchas that have already cost time:**

- YAML 1.1 parses bare `On` as boolean `true`. Any brand or handle named "On"
  must be quoted.
- Farfetch (Akamai) and SSENSE (Cloudflare) return 403. The whole project
  pivoted to open Shopify JSON endpoints because of it.
- zsh does not word-split unquoted variables, so `crawl $SITES` passes one
  argument. Pass site names explicitly.
- `.env` values are single-quoted: the User-Agent contains `(`, `)` and `;`,
  which break `source .env`. python-dotenv strips the quotes; Docker
  `--env-file` does not.
- Sending `Accept-Language` shrank catalogues on some stores — notre-shop went
  from 249 items per page to 142. `fetch.py` does not send it.

`data/` is gitignored. A full crawl is ~2.8 GB.
