# ruff: noqa: ANN401
from __future__ import annotations

import json
import math
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any

from fakesnow.variant.render import _map_items, sf_json_compact
from fakesnow.variant.sentinels import (
    BIGINT_PREFIX,
    DECIMAL_PREFIX,
    is_bigint,
    is_decimal,
    is_json_null,
    is_nan,
    is_undefined,
)

_KIND_ORDER = {
    "BOOLEAN": 0,
    "NUMBER": 1,
    "VARCHAR": 2,
    "BINARY": 2,
    "DATE": 2,
    "TIME": 2,
    "TIMESTAMP": 2,
    "OBJECT": 3,
    "ARRAY": 4,
    "JSON_NULL": 5,
    "SQL_NULL": 6,
}


def _kind(value: Any) -> str:
    if value is None:
        return "SQL_NULL"
    if is_json_null(value):
        return "JSON_NULL"
    if is_undefined(value):
        return "SQL_NULL"
    if is_bigint(value) or is_decimal(value):
        return "NUMBER"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, (int, Decimal, float)) or is_nan(value):
        return "NUMBER"
    if isinstance(value, str):
        return "VARCHAR"
    if _map_items(value) is not None:
        return "OBJECT"
    if isinstance(value, (list, tuple)):
        return "ARRAY"
    if isinstance(value, bytes):
        return "BINARY"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, date):
        return "DATE"
    if isinstance(value, time):
        return "TIME"
    raise TypeError(f"Unsupported VARIANT value: {type(value).__name__}")


def _numeric_key(value: Any) -> Decimal:
    if is_bigint(value):
        return Decimal(value.removeprefix(BIGINT_PREFIX))
    if is_decimal(value):
        return Decimal(value.removeprefix(DECIMAL_PREFIX))
    if is_nan(value):
        return Decimal("NaN")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if math.isinf(value):
            return Decimal("Infinity") if value > 0 else Decimal("-Infinity")
        return Decimal(str(value))
    raise TypeError(value)


def variant_eq(left: Any, right: Any) -> bool | None:
    if left is None or right is None:
        return None if left is not right else True
    if is_undefined(left) or is_undefined(right):
        return is_undefined(left) and is_undefined(right)
    if is_json_null(left) or is_json_null(right):
        return left == right if is_json_null(left) and is_json_null(right) else False
    lk, rk = _kind(left), _kind(right)
    if lk == "NUMBER" and rk == "NUMBER":
        return _numeric_key(left) == _numeric_key(right)
    if lk != rk:
        return False
    if lk == "BOOLEAN":
        return bool(left) == bool(right)
    if lk == "VARCHAR":
        return str(left) == str(right)
    if lk == "OBJECT":
        lm, rm = _map_items(left), _map_items(right)
        assert lm is not None and rm is not None
        if len(lm) != len(rm):
            return False
        for (lk2, lv), (rk2, rv) in zip(sorted(lm), sorted(rm), strict=True):
            if lk2 != rk2:
                return False
            eq = variant_eq(lv, rv)
            if not eq:
                return False
        return True
    if lk == "ARRAY":
        la, ra = list(left), list(right)
        if len(la) != len(ra):
            return False
        return all(variant_eq(a, b) for a, b in zip(la, ra, strict=True))
    return False


def variant_eq_sql(left: Any, right: Any) -> bool | None:
    result = variant_eq(left, right)
    if result is not False:
        return result
    if is_json_null(left) and isinstance(right, str):
        return None
    if is_json_null(right) and isinstance(left, str):
        return None
    if isinstance(left, str) and isinstance(right, (int, Decimal, float)) and not isinstance(right, bool):
        try:
            return Decimal(left) == _numeric_key(right)
        except (InvalidOperation, ValueError):
            return False
    if isinstance(right, str) and isinstance(left, (int, Decimal, float)) and not isinstance(left, bool):
        try:
            return _numeric_key(left) == Decimal(right)
        except (InvalidOperation, ValueError):
            return False
    if isinstance(left, str) and isinstance(right, bool):
        return left.lower() == str(right).lower()
    if isinstance(right, str) and isinstance(left, bool):
        return str(left).lower() == right.lower()
    if isinstance(left, str) and isinstance(right, (date, datetime, time)):
        return left == str(right)
    if isinstance(right, str) and isinstance(left, (date, datetime, time)):
        return str(left) == right
    return False


def variant_key(value: Any) -> str:
    if value is None:
        return f"{_KIND_ORDER['SQL_NULL']:02d}"
    if is_json_null(value):
        return f"{_KIND_ORDER['JSON_NULL']:02d}"
    kind = _kind(value)
    prefix = f"{_KIND_ORDER[kind]:02d}"
    if kind == "BOOLEAN":
        return prefix + ("1" if value else "0")
    if kind == "NUMBER":
        num = _numeric_key(value)
        if num.is_nan():
            return prefix + "5"
        if num.is_infinite():
            return prefix + ("4" if num > 0 else "0")
        normalized = num.normalize()
        if not normalized:
            return prefix + "2"
        digits = "".join(str(digit) for digit in normalized.as_tuple().digits).ljust(64, "0")
        magnitude = f"{normalized.adjusted() + 1_000_000:07d}:{digits}"
        if normalized < 0:
            magnitude = "".join(str(9 - int(char)) if char.isdigit() else char for char in magnitude)
            return prefix + "1" + magnitude
        return prefix + "3" + magnitude
    if kind in {"VARCHAR", "BINARY", "DATE", "TIME", "TIMESTAMP"}:
        subtype = {
            "VARCHAR": "0",
            "BINARY": "1",
            "DATE": "2",
            "TIME": "3",
            "TIMESTAMP": "4",
        }[kind]
        if kind != "VARCHAR":
            value = sf_json_compact(value)
        return prefix + subtype + json.dumps(value, ensure_ascii=False)
    if kind == "OBJECT":
        items = _map_items(value)
        assert items is not None
        body = ",".join(
            f"{''.join(chr(0x10FFFF - ord(char)) for char in k)}:{variant_key(v)}" for k, v in sorted(items)
        )
        return prefix + "{" + body + "}"
    if kind == "ARRAY":
        body = ",".join(variant_key(item) for item in value)
        return prefix + f"[{len(value):010d}:{body}]"
    raise TypeError(value)
