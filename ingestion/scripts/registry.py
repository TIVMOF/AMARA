from __future__ import annotations

from pathlib import Path
from types import ModuleType

import yaml

from .adapters import shopify
from .site_config import SiteConfig

ROOT = Path(__file__).resolve().parent.parent
SITES_DIR = ROOT / "sites"

# Adapters are per access method, not per website. Every Shopify store shares
# shopify.py because they share an identical API - kith, brownsfashion and
# antonioli differ only in their config file, never in code. A store on another
# platform gets its own module, never a branch inside an existing one.
#   jsonld / woo / dom are the planned next ones.
ADAPTERS: dict[str, ModuleType] = {
    "shopify": shopify,
}


def get_adapter(name: str) -> ModuleType:
    # Return the adapter module for `name`, or raise listing the valid ones.
    try:
        return ADAPTERS[name]
    except KeyError:
        raise ValueError(
            f"unknown adapter {name!r}; available: {', '.join(sorted(ADAPTERS))}"
        ) from None


def load_sites(names: list[str] | None = None,
               *, include_disabled: bool = False) -> list[SiteConfig]:
    # Read sites/*.yaml.
    #
    # `names` filters to specific sites by file stem or `name` key. Disabled
    # sites are skipped unless asked for by name.
    if not SITES_DIR.is_dir():
        raise FileNotFoundError(f"no sites directory at {SITES_DIR}")

    sites: list[SiteConfig] = []
    for path in sorted(SITES_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        site = SiteConfig.from_dict(data, source=path.name)
        if names is not None and site.name not in names and path.stem not in names:
            continue
        if not site.enabled and not include_disabled and names is None:
            continue
        sites.append(site)

    if names:
        found = {s.name for s in sites}
        missing = [n for n in names if n not in found]
        if missing:
            available = ", ".join(sorted(p.stem for p in SITES_DIR.glob("*.yaml")))
            raise ValueError(f"unknown site(s): {missing}\n  available: {available}")

    return sites
