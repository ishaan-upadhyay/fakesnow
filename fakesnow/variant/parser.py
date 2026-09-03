# ruff: noqa: ANN401
from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from fakesnow.variant.errors import VariantRuntimeError
from fakesnow.variant.sentinels import JSON_NULL, NAN, UNDEFINED

_UNDEFINED_TOKEN = "\x00fakesnow-undefined\x00"
_IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
_MAX_INT128 = 2**127 - 1


def _fixed_number(text: str) -> int | Decimal | float:
    if "e" in text.lower():
        return float(text)
    try:
        value = Decimal(text)
    except InvalidOperation:
        return float(text)
    if value == value.to_integral_value():
        integer = int(value)
        return integer if abs(integer) <= _MAX_INT128 else float(text)

    digits = value.as_tuple().digits
    coefficient = int("".join(map(str, digits)))
    exponent = value.as_tuple().exponent
    scale = max(-exponent, 0) if isinstance(exponent, int) else 0
    return value if coefficient <= _MAX_INT128 and scale <= 37 else float(text)


def _constant(text: str) -> float | object:
    if text == "NaN":
        return NAN
    if text == "Infinity":
        return float("inf")
    if text == "-Infinity":
        return float("-inf")
    raise ValueError(text)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VariantRuntimeError(f'Error parsing JSON: duplicate object attribute "{key}"', 100069, "22P02")
        result[key] = value
    return result


def _sanitize(source: str) -> str:
    """Quote bare object keys and remove trailing commas without touching strings."""
    output: list[str] = []
    index = 0
    in_string = False
    escaped = False
    while index < len(source):
        char = source[index]
        if in_string:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            output.append(char)
            index += 1
            continue
        if char == ",":
            lookahead = index + 1
            while lookahead < len(source) and source[lookahead].isspace():
                lookahead += 1
            if lookahead < len(source) and source[lookahead] in "]}":
                index += 1
                continue
        if char.isalpha() or char in "_$":
            match = _IDENTIFIER.match(source, index)
            assert match is not None
            token = match.group()
            lookahead = match.end()
            while lookahead < len(source) and source[lookahead].isspace():
                lookahead += 1
            previous = next((item for item in reversed(output) if not item.isspace()), "")
            if lookahead < len(source) and source[lookahead] == ":" and previous in ("", "{", ","):
                output.extend(json.dumps(token))
            elif token == "undefined":
                output.extend(json.dumps(_UNDEFINED_TOKEN))
            else:
                output.append(token)
            index = match.end()
            continue
        output.append(char)
        index += 1
    return "".join(output)


def _replace_specials(value: Any, *, in_array: bool = False) -> Any:
    if value is None:
        return JSON_NULL
    if value == _UNDEFINED_TOKEN:
        if not in_array:
            raise VariantRuntimeError("Error parsing JSON: undefined value outside array", 100069, "22P02")
        return UNDEFINED
    if isinstance(value, list):
        return [_replace_specials(item, in_array=True) for item in value]
    if isinstance(value, dict):
        return {key: _replace_specials(item) for key, item in value.items()}
    return value


def parse_json(value: str | None) -> Any:
    """Parse Snowflake's permissive JSON syntax into DuckDB VARIANT-compatible values."""
    if value is None or not value.strip():
        return None
    try:
        parsed = json.loads(
            _sanitize(value),
            parse_int=_fixed_number,
            parse_float=_fixed_number,
            parse_constant=_constant,
            object_pairs_hook=_pairs,
        )
        return _replace_specials(parsed)
    except VariantRuntimeError:
        raise
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        if "Invalid control character" in str(error):
            raise VariantRuntimeError(
                "Error parsing JSON: unterminated string, line 2, pos 0",
                100069,
                "22P02",
            ) from error
        position = getattr(error, "pos", None)
        suffix = f", pos {position}" if position is not None else ""
        raise VariantRuntimeError(f"Error parsing JSON: {error}{suffix}", 100069, "22P02") from error


_fs_parse_json = parse_json
