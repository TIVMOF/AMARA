from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"


class RawValidationError(Exception):
    # Raised when raw ingestion data fails validation.
    pass


def validate_file(file: Path) -> None:
    if file.stat().st_size == 0:
        raise RawValidationError(f"File is empty: {file}")

    try:
        with file.open("r", encoding="utf-8") as stream:
            data = json.load(stream)
    except json.JSONDecodeError as error:
        raise RawValidationError(
            f"Invalid JSON in '{file}': {error}"
        ) from error

    if not isinstance(data, dict):
        raise RawValidationError(
            f"Expected JSON object at root in '{file}', "
            f"got {type(data).__name__}."
        )

    if not data:
        raise RawValidationError(f"JSON object is empty: {file}")


def validate_retailer(retailer_directory: Path) -> int:
    files = sorted(retailer_directory.glob("*.json"))

    if not files:
        raise RawValidationError(
            f"No JSON files found for retailer '{retailer_directory.name}'."
        )

    for file in files:
        validate_file(file)

    return len(files)


def validate_raw() -> None:
    if not RAW_ROOT.exists():
        raise RawValidationError(
            f"Raw directory does not exist: {RAW_ROOT}"
        )

    retailers = sorted(
        path
        for path in RAW_ROOT.iterdir()
        if path.is_dir()
    )

    if not retailers:
        raise RawValidationError(
            f"No retailer directories found in '{RAW_ROOT}'."
        )

    total_files = 0

    print("Validating raw ingestion data...\n")

    for retailer in retailers:
        file_count = validate_retailer(retailer)

        total_files += file_count

        print(
            f"✓ {retailer.name}: "
            f"{file_count} file(s) validated"
        )

    print(
        f"\nRaw validation successful. "
        f"{len(retailers)} retailer(s), "
        f"{total_files} file(s)."
    )


if __name__ == "__main__":
    validate_raw()