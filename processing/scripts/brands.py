"""Reads brands.yaml into a folded lookup index.

Moved out of ingestion, which no longer classifies anything: the crawl stores
every product a store serves, whatever its vendor. Deciding which brands matter
belongs here, alongside the file that lists them.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from .brand import Brand, BrandIndex

ROOT = Path(__file__).resolve().parent.parent
BRANDS_FILE = ROOT / "brands.yaml"


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
