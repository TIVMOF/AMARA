"""The record written to data/.

Field order here IS the JSON key order (dataclasses preserve declaration order
through asdict), so output files read the way you'd want them to.

Nothing here cleans or standardises values. Prices stay as the strings the site
sent, brand stays as the site spelled it, category stays raw. The only job is to
give every site the same *shape*.
"""

from dataclasses import dataclass, asdict
from typing import Any

from .size import Size  # noqa: F401  (used in the sizes annotation below)


@dataclass
class Product:
    """One product as written to data/<site>/<timestamp>.json.

    The first block is the fields you asked for. The second is the identity and
    context needed to actually join and compare records. `source` at the bottom
    holds the raw fields the derived ones came from - that is where colour and
    material are hiding until we write a parser for them.
    """

    # ── the fields you asked for ───────────────────────────────────────────
    name: str | None
    brand: str | None
    brand_segment: str | None   # Luxury House, Designer, Streetwear, ...
    brand_tier: str | None      # High Luxury, Luxury, Premium, Mainstream
    brand_styles: list[str]     # Avant-Garde, Minimalist, Performance, ...
    category: str | None
    gender: str | None
    product_url: str | None
    price: str | None
    original_price: str | None
    currency: str | None
    color: str | None
    material: str | None
    sizes: list[Size]
    availability: bool | None

    # ── identity + context ─────────────────────────────────────────────────
    retailer: str
    product_id: str | None
    sku: str | None
    images: list[str]
    scraped_at: str

    # ── raw evidence, untouched ────────────────────────────────────────────
    source: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
