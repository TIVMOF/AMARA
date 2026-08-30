# processing

Turns the raw crawls in `../ingestion/data/raw/` into the analytical model.

Nothing here runs yet. What is present is the classification work that used to
sit inside the crawler, moved out when ingestion was reduced to collection only:

```
brands.yaml        263 brands on three axes — segment, tier, styles
brands.py          load_brands(), which reads that file into a folded index
models/brand.py    Brand, BrandIndex, and the Unicode folding behind matching
models/product.py  the product record shape
models/size.py     one size of one product
```

`brands.py` works today:

```python
from processing.brands import load_brands
load_brands().match("Yohji Yamamoto")   # Designer / Luxury / [Avant-Garde]
```

## The taxonomy

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

What a brand *makes* is deliberately not an axis. That Church's sells shoes and
Serapian sells bags is a fact about the product, not the brand — it belongs to
`dim_category`. Likewise which genders a brand dresses.

Values are checked against the vocabularies at load time, so a typo fails
loudly rather than inventing a fourteenth style:

```
error: brands.yaml: group 8: style='Avante-Garde' is not in the declared styles
       (Avant-Garde, Casual, Classic, Feminine, Glamour, Heritage, ...)
```

Matching folds case, accents and punctuation, so `ACNE STUDIOS`, `Acne Studios`,
`ALAIA` and `Alaïa` all match. Sub-labels and alternative spellings need an
entry under `aliases:` — `Air Jordan → Jordan`, `Valentino Garavani → Valentino`.

Every crawl reports the vendors it saw under `vendors`, unfiltered and counted.
That report is how the list grows: anything in it that should be classified is
either a missing brand or a missing alias.

## Target model

`../img/amara-analystical-data-diagram.png` — a star schema around
`fact_product_observation`, with `dim_product`, `dim_brand`, `dim_category`,
`dim_retailer` and `dim_date`, bridged to `size`, `style`, `gender`, `color`
and `material`.
