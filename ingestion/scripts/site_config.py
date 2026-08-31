"""Validated view of one file in sites/."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SiteConfig:
    """One file in sites/. `adapter` picks which module in adapters/ handles it."""

    name: str
    adapter: str
    base_url: str

    enabled: bool = True

    # Some single-brand stores put junk in Shopify's `vendor` field (season
    # names, fabric names, licensee names). When set, this wins over `vendor`.
    brand_override: str | None = None

    # Empty = whole catalogue. Otherwise only these Shopify collection handles.
    collections: list[str] = field(default_factory=list)

    # Permission to shard, not an instruction to. Shopify serves at most 100
    # pages per listing endpoint, so an unfiltered /products.json cannot reach
    # past 25,000 products; collection endpoints each get their own budget and
    # are the only way past that. Whether a store actually needs them is
    # decided at run time from what its unfiltered listing does - 16 of 21
    # retailers gained nothing from sharding and paid 10x the requests for it
    # (issue #16). Set false to forbid sharding a store outright.
    discover_collections: bool = True

    # How many discovered collections to crawl, largest first. Browns publishes
    # 2,700 non-empty collections; crawling them all is ~3,500 requests for a
    # catalogue the largest few dozen already cover, since they overlap heavily.
    max_collections: int = 50

    # Safety rails.
    max_pages: int | None = None

    # Requests per second against this host. None takes the default from .env,
    # which is the usual case - set it here only for a store that needs to be
    # treated differently from the rest.
    rate_limit_rps: float | None = None

    # Filled from the site's /meta.json at runtime if left null.
    currency: str | None = None
    country: str | None = None

    notes: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, source: str) -> "SiteConfig":
        known = set(cls.__dataclass_fields__)
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"{source}: unknown key(s) {sorted(unknown)}")
        for required in ("name", "adapter", "base_url"):
            if not data.get(required):
                raise ValueError(f"{source}: missing required key {required!r}")
        return cls(**data)
