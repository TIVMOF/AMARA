from __future__ import annotations

import argparse
from decimal import Decimal
from pathlib import Path
from typing import Iterator, NamedTuple

import pyarrow.parquet as pq

from scripts import paths
from scripts.tables import MAX_PLAUSIBLE_DISCOUNT

# Read with pyarrow, not Spark: 79 MB, and checking the output with a different
# engine than the one that wrote it is the point.
USAGE = "python validate_processing.py    every table under data/processed"

ERROR, WARN = "ERROR", "WARN"

class Finding(NamedTuple):
    level: str
    table: str
    message: str

class Spec(NamedTuple):
    grain: tuple[str, ...]        # columns that together identify a row
    filled: tuple[str, ...]       # columns with no null by construction
    upper: tuple[str, ...] = ()   # upper case; free text is left out, so
                                  # products.name and variants.sku keep theirs
TABLES = {
    "crawls": Spec(("site", "date"),
                   ("site", "base_url", "currency", "date", "products_received",
                    "products_stored", "pages", "short_pages", "collections_crawled"),
                   ("site", "currency", "name")),
    "retailers": Spec(("name",), ("name", "url", "country"), ("name", "country")),
    "dates": Spec(("date",), ("date", "day", "week", "month", "quarter", "season",
                              "year"), ("season",)),
    "products": Spec(("product", "retailer"), ("product", "retailer", "name", "brand"),
                     ("retailer", "brand", "category", "gender", "color", "material")),
    "variants": Spec(("variant", "product", "retailer", "date"),
                     ("variant", "product", "retailer", "date", "currency", "available"),
                     ("retailer", "size", "color", "currency")),
    "brands": Spec(("name",), ("name", "segment", "tier"), ("name", "segment", "tier")),
    "countries": Spec(("code",), ("code", "name"), ("code", "name")),
    **{v: Spec(("name",), ("name",), ("name",))
       for v in ("categories", "currencies", "genders", "segments", "tiers")},
}

# Columns filled from a reference/*.yaml, and the vocabulary each came from. Not
# foreign keys - there are none until Snowflake makes them. Narrower: lookup()
# only ever emitted a canonical value, so the two cannot drift apart.
VOCABULARIES = (("products", "brand", "brands", "name"),
                ("products", "category", "categories", "name"),
                ("products", "gender", "genders", "name"),
                ("variants", "currency", "currencies", "name"),
                ("retailers", "country", "countries", "code"))

def load(root: Path) -> tuple[dict, list[Finding]]:
    # A missing _SUCCESS means Spark did not finish: the part files present are
    # a partial table, not a small one.
    tables, findings = {}, []
    for name in TABLES:
        directory = root / name
        if not directory.is_dir():
            findings.append(Finding(ERROR, name, "table directory is missing"))
        elif not (directory / "_SUCCESS").exists():
            findings.append(Finding(ERROR, name, "no _SUCCESS; the write did not finish"))
        else:
            tables[name] = pq.read_table(directory)
    if extra := sorted({p.name for p in root.iterdir() if p.is_dir()} - set(TABLES)):
        findings.append(Finding(WARN, "output", f"unexpected dir(s): {', '.join(extra)}"))
    return tables, findings

def values(table, column: str) -> list:
    return table.column(column).to_pylist()
def check_table(name: str, table, spec: Spec) -> Iterator[Finding]:
    if gone := [c for c in (*spec.grain, *spec.filled) if c not in table.schema.names]:
        yield Finding(ERROR, name, f"missing column(s): {', '.join(gone)}")
        return
    if not table.num_rows:
        yield Finding(ERROR, name, "no rows")
        return
    for column in spec.filled:
        if n := table.column(column).null_count:
            yield Finding(ERROR, name, f"{column} null in {n:,}/{table.num_rows:,} rows")
    # A duplicate on the grain becomes a fan-out the moment Snowflake joins on it.
    rows = list(zip(*[values(table, c) for c in spec.grain]))
    if len(set(rows)) != len(rows):
        yield Finding(ERROR, name, f"{len(rows) - len(set(rows)):,} duplicate row(s) on "
                                   f"({', '.join(spec.grain)})")
    # Controlled values are upper case: RICK OWENS and Rick Owens cannot both exist.
    for column in spec.upper:
        if off := [v for v in values(table, column)
                   if isinstance(v, str) and v != v.upper()]:
            yield Finding(ERROR, name, f"{column}: {len(off):,} value(s) not upper case, "
                                       f"e.g. {off[0]!r}")
    # A column null everywhere is a broken mapping, not thin data.
    for column in table.schema.names:
        if column not in spec.filled and table.column(column).null_count == table.num_rows:
            yield Finding(WARN, name, f"{column} is null in every row")

def check_values(tables: dict) -> Iterator[Finding]:
    # Null is allowed - a product may have no category - so only values that are
    # there have to resolve.
    for table, column, vocabulary, key in VOCABULARIES:
        if table not in tables or vocabulary not in tables:
            continue
        known = set(values(tables[vocabulary], key))
        if unknown := {v for v in values(tables[table], column)
                       if v is not None and v not in known}:
            yield Finding(ERROR, table, f"{len(unknown):,} {column} value(s) not in "
                                        f"{vocabulary}, e.g. {sorted(unknown)[0]!r}")
    # The one join this stage makes: variants filtered to allowlisted products.
    if "variants" in tables and "products" in tables:
        catalogue = set(zip(values(tables["products"], "product"),
                            values(tables["products"], "retailer")))
        orphans = {r for r in zip(values(tables["variants"], "product"),
                                  values(tables["variants"], "retailer"))
                   if r not in catalogue}
        if orphans:
            yield Finding(ERROR, "variants", f"{len(orphans):,} variant(s) whose product "
                                             f"is not in products")

def check_money(tables: dict) -> Iterator[Finding]:
    # The rules tables.variants claims to enforce, against what it wrote.
    if "variants" not in tables:
        return
    price, original, discount = (values(tables["variants"], c)
                                 for c in ("price", "original_price", "discount"))
    ceiling = Decimal(MAX_PLAUSIBLE_DISCOUNT)
    yield from (Finding(ERROR, "variants", f"{n:,} {msg}") for n, msg in (
        (sum(1 for p in price if p is not None and p <= 0),
         "row(s) at or below zero; positive() should have nulled them"),
        (sum(1 for o, p in zip(original, price)
             if o is not None and p is not None and o <= p),
         "row(s) whose original_price is not above price - not a discount"),
        (sum(1 for o, p in zip(original, price) if o is not None and p is None),
         "row(s) with an original_price but no price"),
        (sum(1 for d, o in zip(discount, original) if (d is None) != (o is None)),
         "row(s) where discount and original_price disagree about being a sale"),
        (sum(1 for d in discount if d is not None and not 0 <= d <= ceiling),
         f"discount(s) outside 0-{MAX_PLAUSIBLE_DISCOUNT}%"),
    ) if n)

def check_coverage(tables: dict, staging: Path) -> Iterator[Finding]:
    # A site staged but absent from crawls was skipped, which no row count shows.
    if "crawls" not in tables:
        return
    if not staging.is_dir():
        yield Finding(WARN, "coverage", f"no staging directory at {staging}")
        return
    staged = {p.parent.name.upper() for p in staging.glob("*/*") if p.is_dir()}
    processed = set(values(tables["crawls"], "site"))
    if skipped := sorted(staged - processed):
        yield Finding(ERROR, "coverage", f"{len(skipped)} staged site(s) missing from "
                                         f"crawls: {', '.join(skipped[:6])}")
    if extra := sorted(processed - staged):
        yield Finding(WARN, "coverage", f"{len(extra)} site(s) in crawls, nothing "
                                        f"staged: {', '.join(extra[:6])}")

def validate(root: Path, staging: Path) -> list[Finding]:
    if not root.is_dir():
        raise SystemExit(f"error: no processed directory at {root}")
    print(f"Validating {paths.relative(root)}\n")
    if not (loaded := load(root))[0]:
        raise SystemExit(f"error: no readable tables in {root}")
    tables, findings = loaded
    for name, spec in TABLES.items():
        if name in tables:
            findings += check_table(name, tables[name], spec)
    findings += [*check_values(tables), *check_money(tables),
                 *check_coverage(tables, staging)]

    # Not a check: what you look at to decide if a reference file needs an alias.
    print(f"{'table':13} {'rows':>11}   optional columns")
    for name, spec in TABLES.items():
        if (table := tables.get(name)) is None:
            continue
        fills = "  ".join(
            f"{c} {(table.num_rows - table.column(c).null_count) / table.num_rows:.0%}"
            for c in table.schema.names if c not in spec.filled)
        print(f"  {name:11} {table.num_rows:>11,}   {fills or '-'}")
    return findings

def main() -> None:
    parser = argparse.ArgumentParser(prog="python validate_processing.py",
        description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--processed", type=Path, default=paths.OUTPUT_ROOT)
    parser.add_argument("--staging", type=Path, default=paths.STAGING_ROOT)
    findings = validate(*vars(parser.parse_args()).values())
    for level in (ERROR, WARN):
        if hits := [f for f in findings if f.level == level]:
            print(f"\n{level}S ({len(hits)})")
            print("\n".join(f"  {f.table}: {f.message}" for f in hits))
    errors = sum(1 for f in findings if f.level == ERROR)
    if errors:
        raise SystemExit(f"\nFAILED: {errors} error(s), {len(findings) - errors} warning(s)")
    print("\nOK: no errors" + (f", {len(findings)} warning(s)" if findings else ""))

if __name__ == "__main__":
    main()
