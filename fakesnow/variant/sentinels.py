# ruff: noqa: ANN401
from __future__ import annotations

from datetime import timedelta
from typing import Any, TypeGuard
from uuid import UUID

JSON_NULL = UUID("00000000-0000-0000-0000-000000000000")
UNDEFINED = timedelta(0)
NAN = UUID("00000000-0000-0000-0000-000000000001")
BIGINT_PREFIX = "__FAKESNOW_BIGINT__"
TIMESTAMP_LTZ_PREFIX = "__FAKESNOW_TIMESTAMP_LTZ__"
TIMESTAMP_NTZ_PREFIX = "__FAKESNOW_TIMESTAMP_NTZ__"
TIMESTAMP_TZ_PREFIX = "__FAKESNOW_TIMESTAMP_TZ__"


def is_json_null(value: object) -> TypeGuard[UUID]:
    return isinstance(value, UUID) and value == JSON_NULL


def is_undefined(value: object) -> TypeGuard[timedelta]:
    return isinstance(value, timedelta) and value == UNDEFINED


def is_nan(value: object) -> bool:
    return (isinstance(value, UUID) and value == NAN) or (isinstance(value, float) and value != value)


def is_bigint(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and value.startswith(BIGINT_PREFIX)


def timestamp_kind(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    prefixes = {
        TIMESTAMP_LTZ_PREFIX: "TIMESTAMP_LTZ",
        TIMESTAMP_NTZ_PREFIX: "TIMESTAMP_NTZ",
        TIMESTAMP_TZ_PREFIX: "TIMESTAMP_TZ",
    }
    return next((kind for prefix, kind in prefixes.items() if value.startswith(prefix)), None)


def timestamp_value(value: str) -> str:
    prefixes = (TIMESTAMP_LTZ_PREFIX, TIMESTAMP_NTZ_PREFIX, TIMESTAMP_TZ_PREFIX)
    return next(value.removeprefix(prefix) for prefix in prefixes if value.startswith(prefix))


def is_sentinel(value: Any) -> bool:
    return is_json_null(value) or is_undefined(value) or is_nan(value)
