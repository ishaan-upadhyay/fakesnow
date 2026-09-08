from __future__ import annotations

import re
from dataclasses import dataclass

from snowflake.connector.errors import ProgrammingError

_MARKER = re.compile(r"\[FAKESNOW:(\d+):([0-9A-Z]+)\]\s*(.*)", re.DOTALL)


@dataclass
class VariantRuntimeError(ValueError):
    message: str
    errno: int
    sqlstate: str = "22000"

    def __str__(self) -> str:
        return f"[FAKESNOW:{self.errno}:{self.sqlstate}] {self.message}"


def programming_error(error: BaseException) -> ProgrammingError | None:
    """Recover a Snowflake error from an exception raised through a DuckDB Python UDF."""
    match = _MARKER.search(str(error))
    if match is None:
        return None
    errno, sqlstate, message = match.groups()
    # DuckDB appends its own traceback after a Python UDF failure.
    message = message.split("\nAt:", 1)[0].rstrip()
    return ProgrammingError(msg=message, errno=int(errno), sqlstate=sqlstate)


def cast_error(value: object, target: str) -> VariantRuntimeError:
    from fakesnow.variant.render import sf_json_compact

    rendered = sf_json_compact(value)
    return VariantRuntimeError(f"Failed to cast variant value {rendered} to {target}", 100071)
