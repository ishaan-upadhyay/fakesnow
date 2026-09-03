# ruff: noqa: ANN401
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from fakesnow.variant.render import _map_items
from fakesnow.variant.sentinels import is_bigint, is_decimal, is_json_null, is_nan, is_undefined


def typeof(value: Any) -> str | None:
    if value is None:
        return None
    if is_json_null(value):
        return "NULL_VALUE"
    if is_undefined(value):
        return None
    if is_bigint(value):
        return "INTEGER"
    if is_decimal(value):
        return "DECIMAL"
    if is_nan(value) or isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, Decimal):
        return "DECIMAL"
    if isinstance(value, str):
        return "VARCHAR"
    if isinstance(value, bytes):
        return "BINARY"
    if isinstance(value, datetime):
        return "TIMESTAMP_TZ" if value.tzinfo is not None else "TIMESTAMP_NTZ"
    if isinstance(value, date):
        return "DATE"
    if isinstance(value, time):
        return "TIME"
    if _map_items(value) is not None:
        return "OBJECT"
    if isinstance(value, (list, tuple)):
        return "ARRAY"
    raise TypeError(f"Unsupported VARIANT value: {type(value).__name__}")


_fs_typeof = typeof
