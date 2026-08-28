"""Validated view of one file in sites/."""

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

    # Safety rails.
    max_pages: int | None = None
    rate_limit_rps: float = 0.5

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
