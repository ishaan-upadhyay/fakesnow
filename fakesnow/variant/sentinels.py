# ruff: noqa: ANN401
from __future__ import annotations

from datetime import timedelta
from typing import Any, TypeGuard
from uuid import UUID

JSON_NULL = UUID("00000000-0000-0000-0000-000000000000")
UNDEFINED = timedelta(0)
NAN = UUID("00000000-0000-0000-0000-000000000001")
BIGINT_PREFIX = "__FAKESNOW_BIGINT__"
DECIMAL_PREFIX = "__FAKESNOW_DECIMAL__"


def is_json_null(value: object) -> TypeGuard[UUID]:
    return isinstance(value, UUID) and value == JSON_NULL


def is_undefined(value: object) -> TypeGuard[timedelta]:
    return isinstance(value, timedelta) and value == UNDEFINED


def is_nan(value: object) -> bool:
    return (isinstance(value, UUID) and value == NAN) or (isinstance(value, float) and value != value)


def is_bigint(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and value.startswith(BIGINT_PREFIX)


def is_decimal(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and value.startswith(DECIMAL_PREFIX)


def is_sentinel(value: Any) -> bool:
    return is_json_null(value) or is_undefined(value) or is_nan(value)
