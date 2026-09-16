from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal

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


def _error_number(value: Decimal | float | int) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        return format(value, ".16g")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def snowflake_error_json(value: object, *, kind: str | None = None) -> str:
    if value is None or kind == "NULL_VALUE":
        return "null"
    if kind == "BOOLEAN" or isinstance(value, bool):
        return "true" if value else "false"
    if kind == "OBJECT" or isinstance(value, dict):
        payload: object = value
        if isinstance(value, str):
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                payload = value
        if isinstance(payload, dict):
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    if kind == "ARRAY" or isinstance(value, list):
        payload = value
        if isinstance(value, str):
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                payload = value
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    if kind == "VARCHAR" or (isinstance(value, str) and kind not in {"INTEGER", "DECIMAL", "DOUBLE"}):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return _error_number(value)
    if isinstance(value, bytes):
        return json.dumps(value.hex().upper())
    return str(value)


def cast_error(value: object, target: str, *, kind: str | None = None) -> VariantRuntimeError:
    rendered = snowflake_error_json(value, kind=kind)
    return VariantRuntimeError(f"Failed to cast variant value {rendered} to {target}", 100071)
