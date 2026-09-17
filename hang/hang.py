from __future__ import annotations

import argparse
import sys

from scripts import load_analytical, load_processed, validate_loaded


# What --help prints above the options.
USAGE = """\
python hang.py load-processed        the processed stage -> PROCESSED tables
python hang.py load-analytical       PROCESSED   -> dimensions and facts

python hang.py validate-loaded       do the loaded rows match what was sent?

The two stages are checked where they are filled: `gather.py validate-upload`
and `spark-submit stitch.py validate-upload`.

Run a command with --help for its own options.
"""

# Each command takes the rest of the command line, so a script keeps whatever
# arguments it declares rather than having them restated here.
COMMANDS = {
    "load-processed": lambda argv: load_processed.load(),
    "load-analytical": lambda argv: load_analytical.load(),
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
