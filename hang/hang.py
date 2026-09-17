from __future__ import annotations

import argparse
import sys

from scripts import (cleanup, load_analytical, load_processed, upload_processed,
                     upload_raw, validate_loaded, validate_processed, validate_raw)


# What --help prints above the options.
USAGE = """\
python hang.py upload-raw            crawl JSON  -> the raw stage
python hang.py upload-processed      parquets    -> the processed stage
python hang.py load-processed        the processed stage -> PROCESSED tables
python hang.py load-analytical       PROCESSED   -> dimensions and facts
python hang.py cleanup               empty the local data directories

python hang.py validate-raw          is every crawl file staged?
python hang.py validate-processed    is every parquet staged?
python hang.py validate-loaded       do the loaded rows match what was sent?

Run a command with --help for its own options.
"""

# Each command takes the rest of the command line, so a script keeps whatever
# arguments it declares rather than having them restated here.
COMMANDS = {
    "upload-raw": lambda argv: upload_raw.upload(),
    "upload-processed": upload_processed.main,
    "load-processed": lambda argv: load_processed.load(),
    "load-analytical": lambda argv: load_analytical.load(),
    "cleanup": lambda argv: cleanup.clear_data(),
    "validate-raw": validate_raw.main,
    "validate-processed": validate_processed.main,
    "validate-loaded": validate_loaded.main,
}


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv

    if argv and argv[0] in COMMANDS:
        COMMANDS[argv[0]](argv[1:])
        return 0

    parser = argparse.ArgumentParser(
        prog="python hang.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=sorted(COMMANDS),
                        help="see the list above")
    parser.parse_args(argv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
