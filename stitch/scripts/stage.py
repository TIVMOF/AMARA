from __future__ import annotations

from pathlib import Path


# Files in an internal stage are encrypted, and LIST reports the encrypted
# size, not the file's. The cipher pads to a 16-byte block and always adds at
# least one byte, so a file that is already a multiple of 16 gains a whole
# block. Verified against all 137 files of a full crawl, both stages, with no
# exceptions. Comparing the raw sizes instead would report every single file as
# a mismatch - which is exactly what it did before this was understood.
BLOCK = 16


def encrypted_size(size: int) -> int:
    # What LIST will report for a local file of this size.
    return (size // BLOCK + 1) * BLOCK


def listing(cursor, stage: str) -> list[tuple[str, int]]:
    # Every file in a stage, as (full stage path, encrypted size).
    cursor.execute(f"LIST {stage}")
    return [(path, size) for path, size, *_ in cursor.fetchall()]


def by_dataset(cursor, stage: str) -> dict[str, dict[str, int]]:
    # Keyed by the directory a file sits in, then by base name. The processed
    # stage is laid out one directory per table.
    held: dict[str, dict[str, int]] = {}
    for path, size in listing(cursor, stage):
        parts = Path(path).parts
        if len(parts) >= 2:
            held.setdefault(parts[-2], {})[parts[-1]] = size
    return held
