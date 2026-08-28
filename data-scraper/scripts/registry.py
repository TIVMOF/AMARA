"""Everything the crawler needs to look up before it starts.

Three lookups, one place:
  load_sites()   - the site configs in sites/*.yaml
  load_brands()  - the allowlist in brands.yaml
  get_adapter()  - the module that handles a site's `adapter:` key

Adding a site is adding a YAML file, not writing a module. Adding a brand is
adding a line to brands.yaml.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import yaml

from .adapters import shopify
from .models.brand import Brand, BrandIndex
from .models.site_config import SiteConfig

ROOT = Path(__file__).resolve().parent.parent
SITES_DIR = ROOT / "sites"
BRANDS_FILE = ROOT / "brands.yaml"

# Adapters are per access method, not per website. Every Shopify store shares
# shopify.py because they share an identical API - kith, brownsfashion and
# antonioli differ only in their config file, never in code. A store on another
# platform gets its own module, never a branch inside an existing one.
#   jsonld / woo / dom are the planned next ones.
ADAPTERS: dict[str, ModuleType] = {
    "shopify": shopify,
}


def get_adapter(name: str) -> ModuleType:
    """Return the adapter module for `name`, or raise listing the valid ones."""
    try:
        return ADAPTERS[name]
    except KeyError:
        raise ValueError(
            f"unknown adapter {name!r}; available: {', '.join(sorted(ADAPTERS))}"
        ) from None


def load_sites(names: list[str] | None = None,
               *, include_disabled: bool = False) -> list[SiteConfig]:
    """Read sites/*.yaml.

    `names` filters to specific sites by file stem or `name` key. Disabled
    sites are skipped unless asked for by name.
    """
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


def load_brands() -> BrandIndex:
    """Read brands.yaml into a folded lookup index.

    Brands are grouped by the three axes they share - segment, tier and style -
    so the file stays readable at ~200 brands. Every value is checked against
    the vocabularies declared at the top of the file, which is what catches a
    typo like style: Avante-Garde before it silently becomes a 14th style.

    A brand listed in more than one group keeps the FIRST group it appears in,
    so ordering in brands.yaml is meaningful.
    """
    if not BRANDS_FILE.is_file():
        raise FileNotFoundError(f"no brand allowlist at {BRANDS_FILE}")

    data = yaml.safe_load(BRANDS_FILE.read_text(encoding="utf-8")) or {}
    aliases = {k: list(v) for k, v in (data.get("aliases") or {}).items()}

    vocab = {axis: set(data.get(f"{axis}s") or []) for axis in ("segment", "tier", "style")}
    for axis, values in vocab.items():
        if not values:
            raise ValueError(f"{BRANDS_FILE.name}: no {axis}s vocabulary declared")

    brands: list[Brand] = []
    seen: set[str] = set()

    for index, group in enumerate(data.get("groups") or []):
        where = f"{BRANDS_FILE.name}: group {index + 1}"
        axes = {}
        for axis in ("segment", "tier"):
            value = group.get(axis)
            if value not in vocab[axis]:
                raise ValueError(
                    f"{where}: {axis}={value!r} is not in the declared {axis}s "
                    f"({', '.join(sorted(vocab[axis]))})"
                )
            axes[axis] = value

        styles = group.get("styles")
        if not isinstance(styles, list) or not styles:
            raise ValueError(f"{where}: styles must be a non-empty list, got {styles!r}")
        for style in styles:
            if style not in vocab["style"]:
                raise ValueError(
                    f"{where}: style={style!r} is not in the declared styles "
                    f"({', '.join(sorted(vocab['style']))})"
                )
        axes["styles"] = styles

        for name in group.get("brands") or []:
            if not isinstance(name, str):
                # YAML 1.1 reads bare On/Off/Yes/No/True/False as booleans, which
                # silently turns the brand "On" into True. Quote it in brands.yaml.
                raise ValueError(
                    f"{where}: brand {name!r} is {type(name).__name__}, not a "
                    f"string - quote it in the YAML"
                )
            if name in seen:
                continue
            seen.add(name)
            brands.append(Brand(name=name, aliases=aliases.get(name, []), **axes))

    unknown = sorted(set(aliases) - seen)
    if unknown:
        raise ValueError(
            f"{BRANDS_FILE.name}: aliases defined for brands that are not listed: {unknown}"
        )

    if not brands:
        raise ValueError(f"{BRANDS_FILE} contains no brands")

    return BrandIndex(brands)
