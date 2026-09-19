from __future__ import annotations

import argparse
import sys
from typing import Callable

from scripts import load_analytical, load_processed, validate_loaded


# What --help prints above the options.
USAGE = """\
python sew.py load-processed        the processed stage -> PROCESSED tables
python sew.py load-analytical       PROCESSED   -> dimensions and facts

python sew.py validate-loaded       do the loaded rows match what was sent?

The two stages are checked where they are filled: `get.py validate-upload`
and `spark-submit cut.py validate-upload`.

Run a command with --help for its own options.
"""

def _takes_no_arguments(name: str, run: Callable[[], object]) -> Callable[[list[str]], int]:
    # The two loads declare no options. A lambda that ignored what followed
    # meant `sew.py load-processed --help` silently ran a full Snowflake load
    # instead of printing help, so anything unexpected stops the command.
    def command(argv: list[str]) -> int:
        if argv in (["-h"], ["--help"]):
            print(f"python sew.py {name}\n\nTakes no options.")
            return 0
        if argv:
            raise SystemExit(f"error: {name} takes no arguments, got: {' '.join(argv)}")
        run()
        return 0
    return command


# Each command takes the rest of the command line, so a script keeps whatever
# arguments it declares rather than having them restated here.
COMMANDS: dict[str, Callable[[list[str]], int]] = {
    "load-processed": _takes_no_arguments("load-processed", load_processed.load),
    "load-analytical": _takes_no_arguments("load-analytical", load_analytical.load),
    "validate-loaded": validate_loaded.main,
}


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv

    if argv and argv[0] in COMMANDS:
        # `or 0` because these signal failure by raising SystemExit rather than
        # returning. Dropping the return value would turn a validator that ever
        # starts returning a code into a silent success, which under Airflow is
        # a green task on bad data.
        return COMMANDS[argv[0]](argv[1:]) or 0

    parser = argparse.ArgumentParser(
        prog="python sew.py",
        description=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=sorted(COMMANDS),
                        help="see the list above")
    parser.parse_args(argv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
