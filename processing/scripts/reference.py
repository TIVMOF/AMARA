"""Reference data: the YAML files, and the parquets kept in step with them.

A reference file is a controlled vocabulary - the values AMARA is willing to
report on. Editing the YAML is how the vocabulary changes; this module is what
carries that edit into `data/processed/`.

Each run reads the YAML, reads the parquet written last time, and appends
whatever is new. A value is never removed: a category dropped from the YAML
stays in the parquet, because rows written by earlier runs still point at it.
The first run has no parquet to compare against, so every value is new.

Only closed vocabularies live here. `color`, `size` and `material` are open -
9,315 colours, 1,806 sizes across four incompatible scales, and materials that
are fabric compositions rather than names - so they stay plain string columns
on the product and the variant.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml
from pyspark.sql import SparkSession

from . import paths
from .clean import fold


@dataclass
class Reference:
    """One vocabulary: how to recognise a value, and what to write for it."""

    name: str
    # Folded spelling -> canonical value, upper case. Holds the canonical names
    # and every alias, so one lookup resolves both.
    lookup: dict[str, str] = field(default_factory=dict)
    # What the parquet holds: the canonical value plus any attributes.
    rows: list[dict[str, Any]] = field(default_factory=list)
    # The column holding the canonical value - what `lookup` resolves to and
    # what rows are matched on. Countries key on the ISO code, not the name.
    key: str = "name"

    @property
    def values(self) -> list[str]:
        return [row[self.key] for row in self.rows]


def _read(filename: str) -> dict[str, Any]:
    path = paths.REFERENCE_ROOT / filename
    if not path.exists():
        raise FileNotFoundError(f"no reference file at {paths.relative(path)}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _entries(name: str, filename: str, block: str, *, column: str = "name",
             attributes: tuple[str, ...] = ()) -> Reference:
    """Build a Reference from a `{canonical: {aliases: [...], ...}}` block.

    The canonical spelling is always its own alias, so a value needs an entry
    under `aliases` only where the words differ - not merely the case or the
    punctuation.

    `column` is where the canonical value lands, and is `name` everywhere but
    countries, whose canonical value is the ISO code and whose `name` is the
    readable label beside it. An attribute may not be called `column`, or it
    would overwrite the value every other table joins on.
    """
    if column in attributes:
        raise ValueError(f"{filename}: attribute {column!r} would overwrite the "
                         f"canonical value")

    entries = _read(filename).get(block) or {}
    reference = Reference(name=name, key=column)

    for canonical, body in entries.items():
        body = body or {}
        value = str(canonical).upper()
        row = {column: value}
        for attribute in attributes:
            row[attribute] = (str(body[attribute]).upper()
                              if body.get(attribute) is not None else None)
        reference.rows.append(row)

        for spelling in [canonical, *(body.get("aliases") or [])]:
            reference.lookup[fold(str(spelling))] = value

    return reference


def _vocabulary(name: str, filename: str, block: str) -> Reference:
    """A Reference over a bare list, for vocabularies that are only values."""
    reference = Reference(name=name)
    for value in _read(filename).get(block) or []:
        canonical = str(value).upper()
        reference.rows.append({"name": canonical})
        reference.lookup[fold(str(value))] = canonical
    return reference


# ── the vocabularies ───────────────────────────────────────────────────────────

def load_brands() -> Reference:
    """The allowlist and the classification in one file.

    A product whose vendor is not here is dropped, so this is what decides the
    size of every table downstream. `segment` and `tier` are validated against
    the vocabularies in the same file - a typo fails at load rather than
    inventing a value that then appears in a parquet.
    """
    document = _read("brands.yaml")
    segments = {str(s).upper() for s in document.get("segments") or []}
    tiers = {str(t).upper() for t in document.get("tiers") or []}
    reference = Reference(name="brands")

    for brand, body in (document.get("brands") or {}).items():
        body = body or {}
        if not isinstance(brand, str):
            # YAML 1.1 reads a bare `On` as boolean true. Quote it.
            raise ValueError(
                f"brands.yaml: brand {brand!r} is {type(brand).__name__}, not a "
                f"string - quote it in the YAML"
            )
        segment = str(body.get("segment", "")).upper()
        tier = str(body.get("tier", "")).upper()
        if segment not in segments:
            raise ValueError(f"brands.yaml: {brand}: segment {body.get('segment')!r} "
                             f"is not one of {sorted(segments)}")
        if tier not in tiers:
            raise ValueError(f"brands.yaml: {brand}: tier {body.get('tier')!r} "
                             f"is not one of {sorted(tiers)}")

        reference.rows.append({"name": brand.upper(), "segment": segment, "tier": tier})
        for spelling in [brand, *(body.get("aliases") or [])]:
            reference.lookup[fold(str(spelling))] = brand.upper()

    return reference


def load_segments() -> Reference:
    return _vocabulary("segments", "brands.yaml", "segments")


def load_tiers() -> Reference:
    return _vocabulary("tiers", "brands.yaml", "tiers")


def load_categories() -> Reference:
    return _entries("categories", "categories.yaml", "categories")


def load_genders() -> Reference:
    return _entries("genders", "genders.yaml", "genders")


def load_countries() -> Reference:
    """Keyed on the ISO code, so tables join on US rather than UNITED STATES."""
    return _entries("countries", "countries.yaml", "countries",
                    column="code", attributes=("name",))


def load_currencies() -> Reference:
    return _vocabulary("currencies", "currencies.yaml", "currencies")


def load_all() -> list[Reference]:
    """Every vocabulary, in the order the run reports them."""
    return [load_brands(), load_segments(), load_tiers(), load_categories(),
            load_genders(), load_countries(), load_currencies()]


# ── keeping the parquets in step ───────────────────────────────────────────────

def sync(spark: SparkSession, reference: Reference) -> tuple[int, int]:
    """Append anything the YAML has that the parquet does not.

    Returns (added, total). Values are only ever added: rows written by an
    earlier run still point at a value that has since left the YAML, and
    removing it would strand them.

    The merge happens in Python rather than Spark. These are vocabularies -
    263 brands is the largest - and reading a parquet in order to overwrite
    the same path is not something Spark will do.
    """
    path = paths.OUTPUT_ROOT / reference.name
    held: list[dict[str, Any]] = []
    if (path / "_SUCCESS").exists():
        held = [row.asDict() for row in spark.read.parquet(str(path)).collect()]

    known = {row[reference.key] for row in held}
    new = [row for row in reference.rows if row[reference.key] not in known]
    combined = held + new
    if not combined:
        return 0, 0

    # A YAML that gained an attribute leaves older rows without it.
    columns = list(dict.fromkeys(key for row in combined for key in row))
    rows = [{column: row.get(column) for column in columns} for row in combined]

    spark.createDataFrame(rows).coalesce(1).write.mode("overwrite").parquet(str(path))
    return len(new), len(combined)
