# ruff: noqa: ANN401
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, TypeVar

from fakesnow.variant.errors import cast_error
from fakesnow.variant.render import sf_json_compact
from fakesnow.variant.sentinels import is_json_null, is_nan, is_undefined, timestamp_kind, timestamp_value

T = TypeVar("T")


def _null(value: object) -> bool:
    return value is None or is_json_null(value) or is_undefined(value)


def _convert(value: Any, target: str, converter: Callable[[], T]) -> T | None:
    if _null(value):
        return None
    try:
        return converter()
    except (ValueError, TypeError, OverflowError, InvalidOperation) as error:
        raise cast_error(value, target) from error


def to_varchar(value: Any) -> str | None:
    if _null(value):
        return None
    if timestamp_kind(value):
        return timestamp_value(value)
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="milliseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not is_nan(value):
        return str(value)
    return sf_json_compact(value)


def to_boolean(value: Any) -> bool | None:
    def convert() -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in ("true", "false", "yes"):
            return value.lower() in ("true", "yes")
        raise ValueError

    return _convert(value, "BOOLEAN", convert)


def to_decimal(value: Any, precision: int = 38, scale: int = 0) -> Decimal | None:
    def convert() -> Decimal:
        if is_nan(value):
            raise ValueError
        if isinstance(value, bool):
            number = Decimal(int(value))
        elif isinstance(value, Decimal):
            number = value
        elif isinstance(value, (int, float, str)):
            number = Decimal(str(value))
        else:
            raise ValueError
        quantized = number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
        if len(quantized.as_tuple().digits) > precision:
            raise OverflowError
        return quantized

    return _convert(value, "FIXED", convert)


def to_bigint(value: Any) -> int | None:
    converted = to_decimal(value)
    return None if converted is None else int(converted)


def to_double(value: Any) -> float | None:
    def convert() -> float:
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float, Decimal, str)) and not is_nan(value):
            return float(value)
        raise ValueError

    return _convert(value, "REAL", convert)


def to_date(value: Any) -> date | None:
    def convert() -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError

    return _convert(value, "DATE", convert)


def to_time(value: Any) -> time | None:
    def convert() -> time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            return time.fromisoformat(value.removesuffix("Z"))
        raise ValueError

    return _convert(value, "TIME", convert)


def to_timestamp(value: Any) -> datetime | None:
    def convert() -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time())
        if isinstance(value, str):
            source = timestamp_value(value) if timestamp_kind(value) else value
            return datetime.fromisoformat(source.removesuffix(" Z").replace(" Z", "+00:00"))
        if isinstance(value, (int, Decimal)):
            return datetime.fromtimestamp(int(value), tz=UTC).replace(tzinfo=None)
        raise ValueError

    return _convert(value, "TIMESTAMP_NTZ", convert)


def to_binary(value: Any) -> bytes | None:
    def convert() -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return bytes.fromhex(value)
        raise ValueError

    return _convert(value, "BINARY", convert)
