"""Column-level cleaning.

Nothing here knows about the model. Each function takes a column of whatever
50 storefronts happened to type and returns a column fit to put in a table.
"""

from __future__ import annotations

import re
import unicodedata

from pyspark.sql import Column
from pyspark.sql.functions import (
    array,
    array_distinct,
    filter as array_filter,
    coalesce,
    concat,
    create_map,
    lit,
    lower,
    regexp_replace,
    size as array_size,
    translate,
    trim,
    try_element_at,
    upper,
    when,
)


# Characters that fold to more than one letter, so `translate` cannot do them.
LIGATURES = {"ø": "o", "æ": "ae", "œ": "oe", "ß": "ss", "đ": "d", "ł": "l", "&": " and "}

# One-to-one accent folding. Both strings must stay the same length.
ACCENTED = "áàâäãåéèêëíìîïóòôöõúùûüýñçšž"
PLAIN = "aaaaaaeeeeiiiiooooouuuuyncsz"

SEASONS = {12: "Winter", 1: "Winter", 2: "Winter",
           3: "Spring", 4: "Spring", 5: "Spring",
           6: "Summer", 7: "Summer", 8: "Summer",
           9: "Autumn", 10: "Autumn", 11: "Autumn"}


# ── folding ─────────────────────────────────────────────────────────────────

def fold(value: str) -> str:
    """Reduce a name to the form two spellings of it share.

    `ACNE STUDIOS`, `Acne Studios` and `acne-studios` all fold to
    `acnestudios`, so a reference file needs an alias only where the words
    themselves differ. The Spark equivalent is `fold_column`; the two must
    agree or lookups silently miss.
    """
    for source, target in LIGATURES.items():
        value = value.replace(source, target).replace(source.upper(), target)
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def fold_column(column: Column) -> Column:
    """`fold`, in Spark. Kept beside it so the two stay in step."""
    folded = lower(column)
    for source, target in LIGATURES.items():
        folded = regexp_replace(folded, re.escape(source), target)
    folded = translate(folded, ACCENTED, PLAIN)
    return regexp_replace(folded, "[^a-z0-9]", "")


# ── text ────────────────────────────────────────────────────────────────────

def clean_text(column: Column) -> Column:
    """Trim, collapse runs of whitespace, and turn blanks into null.

    An empty string and a null mean the same thing, and would otherwise become
    two distinct values in whatever lookup is built from the column.
    """
    collapsed = trim(regexp_replace(column, r"\s+", " "))
    return when(collapsed == "", None).otherwise(collapsed)


def name(column: Column) -> Column:
    """A name as tables hold it: cleaned and upper case."""
    return upper(clean_text(column))


# ── money ───────────────────────────────────────────────────────────────────

def positive(column: Column) -> Column:
    """Null out non-positive money.

    Thousands of variants price at 0.00 - gift cards, placeholders, unreleased
    stock. Kept as zero they read as free, and against a `compare_at_price`
    they produce a 100% discount that is not a sale.
    """
    return when(column > 0, column)


# ── vocabulary lookups ──────────────────────────────────────────────────────

def lookup(column: Column, mapping: dict[str, str]) -> Column:
    """Map a raw value onto its canonical one, or null.

    `mapping` is keyed by folded spelling, so it carries both the canonical
    names and every alias. Anything absent becomes null rather than a category
    of its own, and the caller reports what missed.
    """
    if not mapping:
        return lit(None).cast("string")
    pairs = [item for key, value in mapping.items() for item in (lit(key), lit(value))]
    return try_element_at(create_map(*pairs), fold_column(column))


def first_lookup(columns: list[Column], mapping: dict[str, str]) -> Column:
    """The first of several raw columns that maps to something."""
    return coalesce(*[lookup(c, mapping) for c in columns])


def one_of(values: Column, mapping: dict[str, str], several: str) -> Column:
    """Resolve an array of raw values to a single canonical one.

    A product that declares both `Men` and `Women` is not whichever came
    first - it is `several`, which for gender means Unisex.
    """
    if not mapping:
        return lit(None).cast("string")
    pairs = [item for key, value in mapping.items() for item in (lit(key), lit(value))]
    table = create_map(*pairs)
    mapped = array_distinct(
        array_filter(
            transform_column(values, lambda v: try_element_at(table, fold_column(v))),
            lambda v: v.isNotNull(),
        )
    )
    return (
        when(array_size(mapped) == 1, try_element_at(mapped, lit(1)))
        .when(array_size(mapped) > 1, lit(several))
    )


def transform_column(values: Column, fn) -> Column:
    """`transform`, imported here so `one_of` reads in one place."""
    from pyspark.sql.functions import transform

    return transform(coalesce(values, array()), fn)


# ── arrays and dates ────────────────────────────────────────────────────────

def merge(*columns: Column) -> Column:
    """Concatenate several arrays, treating null as empty."""
    return concat(*[coalesce(c, array()) for c in columns])


def season_of(month_column: Column) -> Column:
    season = lit(None).cast("string")
    for number, label in SEASONS.items():
        season = when(month_column == number, lit(label)).otherwise(season)
    return season
