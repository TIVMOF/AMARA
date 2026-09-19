from __future__ import annotations

from typing import NamedTuple


ERROR, WARN = "ERROR", "WARN"


class Finding(NamedTuple):
    level: str
    where: str
    message: str


def error(where: str, message: str) -> Finding:
    # Internally inconsistent: what is in Snowflake does not match what was sent.
    return Finding(ERROR, where, message)


def warn(where: str, message: str) -> Finding:
    # Not wrong, but worth knowing - thin data, or something left behind.
    return Finding(WARN, where, message)


def report(findings: list[Finding], subject: str) -> None:
    # Print the findings and exit non-zero if any of them is an error.
    for level in (ERROR, WARN):
        if hits := [f for f in findings if f.level == level]:
            print(f"\n{level}S ({len(hits)})")
            print("\n".join(f"  {f.where}: {f.message}" for f in hits))
    errors = sum(1 for f in findings if f.level == ERROR)
    if errors:
        raise SystemExit(f"\nFAILED: {subject} - {errors} error(s), "
                         f"{len(findings) - errors} warning(s)")
    print(f"\nOK: {subject}" + (f", {len(findings)} warning(s)" if findings else ""))
