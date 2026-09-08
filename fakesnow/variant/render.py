# ruff: noqa: ANN401
from __future__ import annotations

import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from fakesnow.variant.sentinels import (
    BIGINT_PREFIX,
    DECIMAL_PREFIX,
    is_bigint,
    is_decimal,
    is_json_null,
    is_nan,
    is_undefined,
    timestamp_kind,
    timestamp_value,
)


def _decimal(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in ("", "-0") else rendered


def _double(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return f"{value:.15e}"


def _timestamp(value: datetime) -> str:
    rendered = value.isoformat(sep=" ", timespec="milliseconds")
    return json.dumps(rendered, ensure_ascii=False)


def _time(value: time) -> str:
    timespec = "seconds" if value.microsecond == 0 else "milliseconds"
    return json.dumps(value.isoformat(timespec=timespec), ensure_ascii=False)


def _map_items(value: object) -> list[tuple[str, Any]] | None:
    if isinstance(value, dict):
        return [(str(key), item) for key, item in value.items()]
    if (
        isinstance(value, list)
        and value
        and all(isinstance(entry, dict) and set(entry) == {"key", "value"} for entry in value)
    ):
        return [(str(entry["key"]), entry["value"]) for entry in value]
    return None


def _scalar(value: Any) -> str | None:
    if is_bigint(value):
        return value.removeprefix(BIGINT_PREFIX)
    if is_decimal(value):
        return _decimal(Decimal(value.removeprefix(DECIMAL_PREFIX)))
    if timestamp_kind(value):
        return json.dumps(timestamp_value(value), ensure_ascii=False)
    if is_json_null(value):
        return "null"
    if is_undefined(value) or value is None:
        return "undefined"
    if is_nan(value):
        return "NaN"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _decimal(value)
    if isinstance(value, float):
        return _double(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bytes):
        return json.dumps(value.hex().upper())
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return json.dumps(value.isoformat())
    if isinstance(value, time):
        return _time(value)
    return None


def _render(value: Any, *, pretty: bool, level: int = 0) -> str:
    scalar = _scalar(value)
    if scalar is not None:
        return scalar

    items = _map_items(value)
    if items is not None:
        items.sort(key=lambda pair: pair[0].encode())
        if not items:
            return "{}"
        if not pretty:
            return (
                "{"
                + ",".join(
                    f"{json.dumps(key, ensure_ascii=False)}:{_render(item, pretty=False)}" for key, item in items
                )
                + "}"
            )
        indent = "  " * (level + 1)
        closing = "  " * level
        body = ",\n".join(
            f"{indent}{json.dumps(key, ensure_ascii=False)}: {_render(item, pretty=True, level=level + 1)}"
            for key, item in items
        )
        return f"{{\n{body}\n{closing}}}"

    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        if not pretty:
            return "[" + ",".join(_render(item, pretty=False) for item in value) + "]"
        indent = "  " * (level + 1)
        closing = "  " * level
        body = ",\n".join(f"{indent}{_render(item, pretty=True, level=level + 1)}" for item in value)
        return f"[\n{body}\n{closing}]"

    raise TypeError(f"Unsupported VARIANT value: {type(value).__name__}")


def sf_json(value: Any) -> str | None:
    return None if value is None else _render(value, pretty=True)


def sf_json_compact(value: Any) -> str | None:
    return None if value is None else _render(value, pretty=False)


_fs_sf_json = sf_json
_fs_sf_json_compact = sf_json_compact
