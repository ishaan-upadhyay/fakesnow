# ruff: noqa: ANN401
from __future__ import annotations

import contextlib
import re
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import duckdb
from _duckdb._sqltypes import DuckDBPyType
from duckdb import DuckDBPyConnection, sqltypes

from fakesnow.variant.cast import (
    to_bigint,
    to_binary,
    to_boolean,
    to_date,
    to_decimal,
    to_double,
    to_time,
    to_timestamp,
    to_varchar,
)
from fakesnow.variant.compare import variant_eq, variant_eq_sql, variant_key
from fakesnow.variant.errors import VariantRuntimeError
from fakesnow.variant.parser import parse_json
from fakesnow.variant.render import _map_items, sf_json, sf_json_compact
from fakesnow.variant.sentinels import (
    BIGINT_PREFIX,
    DECIMAL_PREFIX,
    JSON_NULL,
    TIMESTAMP_LTZ_PREFIX,
    TIMESTAMP_NTZ_PREFIX,
    TIMESTAMP_TZ_PREFIX,
    is_json_null,
)
from fakesnow.variant.typeof import typeof


def _variant_output(value: Any) -> Any:
    if isinstance(value, int) and not isinstance(value, bool) and len(str(abs(value))) > 38:
        return f"{BIGINT_PREFIX}{value}"
    if isinstance(value, Decimal) and len(value.as_tuple().digits) > 38:
        return f"{DECIMAL_PREFIX}{value:f}"
    if isinstance(value, dict) and not value:
        return duckdb.Value("{}", duckdb.sqltype("JSON"))
    if (items := _map_items(value)) is not None:
        return [
            {
                "key": key,
                "value": duckdb.Value(_variant_output(item), sqltypes.VARIANT),
            }
            for key, item in items
        ]
    if isinstance(value, list):
        return [duckdb.Value(_variant_output(item), sqltypes.VARIANT) for item in value]
    if isinstance(value, dict):
        return [
            {
                "key": key,
                "value": duckdb.Value(_variant_output(item), sqltypes.VARIANT),
            }
            for key, item in value.items()
        ]
    return value


def _parse_json(value: str | None) -> Any:
    parsed = parse_json(value)
    if parsed is None:
        return None
    return duckdb.Value(_variant_output(parsed), sqltypes.VARIANT)


def _to_variant_timestamp(value: str | None, kind: str) -> Any:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    rendered = parsed.replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")
    prefixes = {
        "LTZ": TIMESTAMP_LTZ_PREFIX,
        "NTZ": TIMESTAMP_NTZ_PREFIX,
        "TZ": TIMESTAMP_TZ_PREFIX,
    }
    if kind == "LTZ":
        rendered += " Z"
    elif kind == "TZ":
        offset = parsed.strftime("%z")
        rendered += f" {'Z' if offset == '+0000' else offset}"
    return duckdb.Value(prefixes[kind] + rendered, sqltypes.VARIANT)


def _object_keep_null(value: Any) -> Any:
    if value is None:
        return None
    items = _map_items(value)
    if items is None:
        return value

    def replace_nulls(item: Any) -> Any:
        if item is None:
            return JSON_NULL
        if (nested_items := _map_items(item)) is not None:
            return {key: replace_nulls(nested) for key, nested in nested_items}
        if isinstance(item, list):
            return [replace_nulls(nested) for nested in item]
        return item

    return {key: duckdb.Value(_variant_output(replace_nulls(item)), sqltypes.VARIANT) for key, item in items}


def _object_drop_null(value: Any) -> Any:
    if value is None:
        return None
    items = _map_items(value)
    if items is None:
        return value
    return {
        key: duckdb.Value(_variant_output(item), sqltypes.VARIANT)
        for key, item in items
        if item is not None
    }


def _object_result(items: list[tuple[str, Any]]) -> dict[str, Any]:
    return {
        key: duckdb.Value(_variant_output(value), sqltypes.VARIANT)
        for key, value in items
    }


def _object_construct(keys: list[Any] | None, values: list[Any] | None, keep_nulls: bool | None) -> Any:
    if keys is None or values is None:
        return {}
    items: list[tuple[str, Any]] = []
    seen: set[str] = set()
    for key, value in zip(keys, values, strict=True):
        if key is None:
            continue
        if not isinstance(key, str):
            raise VariantRuntimeError("SQL compilation error:", 2270)
        if key in seen:
            raise VariantRuntimeError(f"Duplicate field key '{key}'", 100103)
        seen.add(key)
        if value is not None or keep_nulls:
            items.append((key, JSON_NULL if value is None else value))
    return _object_result(items)


def _object_validate_keys(keys: list[Any] | None) -> Any:
    if keys is None:
        return []
    validated: list[str | None] = []
    seen: set[str] = set()
    for key in keys:
        if key is None:
            validated.append(None)
            continue
        if not isinstance(key, str):
            raise VariantRuntimeError("SQL compilation error:", 2270)
        if key in seen:
            raise VariantRuntimeError(f"Duplicate field key '{key}'", 100103)
        seen.add(key)
        validated.append(key)
    return validated


def _object_items(value: Any) -> list[tuple[str, Any]] | None:
    if value is None:
        return None
    items = _map_items(value)
    if items is None:
        raise VariantRuntimeError(
            f"Failed to cast variant value {sf_json_compact(value)} to OBJECT",
            100071,
        )
    return items


def _object_insert(value: Any, key: Any, item: Any, update: bool | None) -> Any:
    items = _object_items(value)
    if items is None:
        return None
    if key is None or item is None:
        return _object_result(items)
    if not isinstance(key, str):
        raise VariantRuntimeError("SQL compilation error:", 2270)
    values = dict(items)
    if key in values and not update:
        raise VariantRuntimeError(f"Duplicate field key '{key}'", 100103)
    values[key] = item
    return _object_result(list(values.items()))


def _object_delete(value: Any, keys: list[Any] | None) -> Any:
    items = _object_items(value)
    if items is None:
        return None
    deleted = {key for key in keys or [] if isinstance(key, str)}
    return _object_result([(key, item) for key, item in items if key not in deleted])


def _object_pick(value: Any, keys: list[Any] | None) -> Any:
    items = _object_items(value)
    if items is None:
        return None
    picked = {key for key in keys or [] if isinstance(key, str)}
    return _object_result([(key, item) for key, item in items if key in picked])


def _object_keys(value: Any) -> Any:
    items = _object_items(value)
    if items is None:
        return None
    return [duckdb.Value(key, sqltypes.VARIANT) for key, _ in sorted(items)]


def _object_cat(left: Any, right: Any) -> Any:
    left_items = _object_items(left)
    right_items = _object_items(right)
    if left_items is None or right_items is None:
        return None
    values = dict(left_items)
    values.update(right_items)
    return _object_result(list(values.items()))


def _to_array(value: Any) -> Any:
    if value is None or is_json_null(value):
        return None
    values = [value] if _map_items(value) is not None else value if isinstance(value, list) else [value]
    return [duckdb.Value(_variant_output(item), sqltypes.VARIANT) for item in values]


def _to_object(value: Any) -> Any:
    if value is None or is_json_null(value):
        return None
    items = _map_items(value)
    if items is None:
        rendered = sf_json_compact(value)
        raise VariantRuntimeError(
            f"Failed to cast variant value {rendered} to OBJECT",
            100071,
            "22000",
        )
    return {key: duckdb.Value(_variant_output(item), sqltypes.VARIANT) for key, item in items}


def _get(value: Any, key: Any) -> Any:
    if value is None or key is None:
        return None
    result: Any
    if (items := _map_items(value)) is not None:
        if not isinstance(key, str):
            return None
        result = dict(items).get(key)
    elif isinstance(value, (list, tuple)):
        if isinstance(key, bool) or not isinstance(key, (int, float)) or int(key) != key or key < 0:
            return None
        result = value[int(key)] if int(key) < len(value) else None
    else:
        return None
    if result is None:
        return None
    return duckdb.Value(_variant_output(result), sqltypes.VARIANT)


def _resolve_path(value: Any, path: str | None) -> Any:
    if value is None or path is None:
        return None
    if not path:
        raise VariantRuntimeError(
            "Bad compound object's field path name '' in GET_PATH",
            100073,
            "22000",
        )
    tokens = re.findall(
        r'(?:^|\.)([^.\[]+)|\[(?:\'([^\']*)\'|"([^"]*)"|(\d+))\]',
        path.lstrip("$").lstrip("."),
    )
    current = value
    for plain_key, single_key, double_key, index in tokens:
        key: Any = int(index) if index else single_key or double_key or plain_key
        if (items := _map_items(current)) is not None:
            current = dict(items).get(str(key))
        elif isinstance(current, (list, tuple)):
            if not isinstance(key, int) or key < 0 or key >= len(current):
                return None
            current = current[key]
        else:
            return None
        if current is None:
            return None
    return current


def _get_path(value: Any, path: str | None) -> Any:
    current = _resolve_path(value, path)
    if current is None:
        return None
    return duckdb.Value(_variant_output(current), sqltypes.VARIANT)


def _get_ignore_case(value: Any, key: str | None) -> Any:
    items = _map_items(value)
    if items is None or key is None:
        return None
    values = dict(items)
    actual_key = (
        key
        if key in values
        else next(
            (candidate for candidate in reversed(values) if candidate.lower() == key.lower()),
            None,
        )
    )
    if actual_key is None:
        return None
    return duckdb.Value(_variant_output(values[actual_key]), sqltypes.VARIANT)


def _object_entries(value: Any) -> Any:
    items = _map_items(value)
    if items is None:
        return []
    return [
        {
            "key": key,
            "value": duckdb.Value(_variant_output(item), sqltypes.VARIANT),
        }
        for key, item in items
    ]


def _flatten_rows(
    value: Any,
    path: str | None,
    outer: bool | None,
    recursive: bool | None,
    mode: str | None,
    sequence: int | None,
) -> Any:
    target = _resolve_path(value, path) if path else value
    flatten_mode = (mode or "BOTH").upper()
    rows: list[dict[str, Any]] = []

    def add_children(container: Any, prefix: str) -> None:
        items = _map_items(container)
        if items is not None:
            if flatten_mode in {"BOTH", "OBJECT"}:
                for key, child in sorted(items, key=lambda pair: pair[0].encode()):
                    child_path = f"{prefix}.{key}" if prefix else key
                    rows.append(
                        {
                            "seq": sequence,
                            "key": key,
                            "path": child_path,
                            "index": None,
                            "value": duckdb.Value(_variant_output(child), sqltypes.VARIANT),
                            "this": duckdb.Value(_variant_output(container), sqltypes.VARIANT),
                        }
                    )
                    if recursive:
                        add_children(child, child_path)
            return
        if isinstance(container, (list, tuple)) and flatten_mode in {"BOTH", "ARRAY"}:
            for index, child in enumerate(container):
                child_path = f"{prefix}[{index}]"
                rows.append(
                    {
                        "seq": sequence,
                        "key": None,
                        "path": child_path,
                        "index": index,
                        "value": duckdb.Value(_variant_output(child), sqltypes.VARIANT),
                        "this": duckdb.Value(_variant_output(container), sqltypes.VARIANT),
                    }
                )
                if recursive:
                    add_children(child, child_path)

    if target is not None:
        add_children(target, path or "")
    if not rows and outer:
        rows.append(
            {
                "seq": sequence,
                "key": None,
                "path": "" if isinstance(target, (list, dict)) else None,
                "index": None,
                "value": None,
                "this": (
                    duckdb.Value(_variant_output(target), sqltypes.VARIANT)
                    if isinstance(target, (list, dict))
                    else None
                ),
            }
        )
    return rows


def _flatten_map_rows(
    value: dict[str, int] | None,
    path: str | None,
    mode: str | None,
    sequence: int | None,
) -> Any:
    if value is None or (mode or "BOTH").upper() not in {"BOTH", "OBJECT"}:
        return []
    prefix = f"{path}." if path else ""
    return [
        {
            "seq": sequence,
            "key": key,
            "path": f"{prefix}{key}",
            "index": None,
            "value": item,
            "this": value,
        }
        for key, item in sorted(value.items())
    ]


def _key(value: Any) -> str:
    return variant_key(value)


def _object_json(value: Any) -> str | None:
    if value is None:
        return None
    return "{}" if value == [] else sf_json(value)


def _register(
    conn: DuckDBPyConnection,
    name: str,
    function: Callable[..., Any],
    parameters: list[DuckDBPyType],
    return_type: DuckDBPyType,
) -> None:
    with contextlib.suppress(duckdb.CatalogException, duckdb.InvalidInputException):
        conn.remove_function(name)
    try:
        conn.create_function(  # pyright: ignore[reportCallIssue]
            name,
            function,
            parameters,  # pyright: ignore[reportArgumentType]
            return_type,
            null_handling="special",  # pyright: ignore[reportArgumentType]
        )
    except duckdb.CatalogException as error:
        if "already exists" not in str(error):
            raise


def register_variant_udfs(conn: DuckDBPyConnection) -> None:
    variant = sqltypes.VARIANT
    integer = sqltypes.INTEGER
    decimal = duckdb.decimal_type(38, 18)
    object_type = duckdb.map_type(sqltypes.VARCHAR, variant)
    variant_array = duckdb.list_type(variant)
    definitions: list[tuple[str, Callable[..., Any], list[DuckDBPyType], DuckDBPyType]] = [
        ("_fs_parse_json", _parse_json, [sqltypes.VARCHAR], variant),
        (
            "_fs_to_variant_timestamp",
            _to_variant_timestamp,
            [sqltypes.VARCHAR, sqltypes.VARCHAR],
            variant,
        ),
        ("_fs_object_drop_null", _object_drop_null, [variant], object_type),
        ("_fs_object_keep_null", _object_keep_null, [variant], object_type),
        ("_fs_object_validate_keys", _object_validate_keys, [variant_array], duckdb.list_type(sqltypes.VARCHAR)),
        ("_fs_object_construct", _object_construct, [variant_array, variant_array, sqltypes.BOOLEAN], object_type),
        ("_fs_object_insert", _object_insert, [variant, variant, variant, sqltypes.BOOLEAN], object_type),
        ("_fs_object_delete", _object_delete, [variant, variant_array], object_type),
        ("_fs_object_pick", _object_pick, [variant, variant_array], object_type),
        ("_fs_object_keys", _object_keys, [variant], variant_array),
        ("_fs_object_cat", _object_cat, [variant, variant], object_type),
        ("_fs_variant_to_array", _to_array, [variant], variant_array),
        ("_fs_variant_get", _get, [variant, variant], variant),
        ("_fs_variant_get_ignore_case", _get_ignore_case, [variant, sqltypes.VARCHAR], variant),
        ("_fs_variant_get_path", _get_path, [variant, sqltypes.VARCHAR], variant),
        (
            "_fs_variant_object_entries",
            _object_entries,
            [variant],
            duckdb.list_type(duckdb.struct_type({"key": sqltypes.VARCHAR, "value": variant})),
        ),
        (
            "_fs_variant_flatten_rows",
            _flatten_rows,
            [
                variant,
                sqltypes.VARCHAR,
                sqltypes.BOOLEAN,
                sqltypes.BOOLEAN,
                sqltypes.VARCHAR,
                sqltypes.UBIGINT,
            ],
            duckdb.list_type(
                duckdb.struct_type(
                    {
                        "seq": sqltypes.UBIGINT,
                        "key": sqltypes.VARCHAR,
                        "path": sqltypes.VARCHAR,
                        "index": sqltypes.BIGINT,
                        "value": variant,
                        "this": variant,
                    }
                )
            ),
        ),
        (
            "_fs_variant_flatten_map_rows",
            _flatten_map_rows,
            [
                duckdb.map_type(sqltypes.VARCHAR, sqltypes.BIGINT),
                sqltypes.VARCHAR,
                sqltypes.VARCHAR,
                sqltypes.UBIGINT,
            ],
            duckdb.list_type(
                duckdb.struct_type(
                    {
                        "seq": sqltypes.UBIGINT,
                        "key": sqltypes.VARCHAR,
                        "path": sqltypes.VARCHAR,
                        "index": sqltypes.BIGINT,
                        "value": sqltypes.BIGINT,
                        "this": duckdb.map_type(sqltypes.VARCHAR, sqltypes.BIGINT),
                    }
                )
            ),
        ),
        (
            "_fs_variant_to_object",
            _to_object,
            [variant],
            object_type,
        ),
        ("_fs_sf_json", sf_json, [variant], sqltypes.VARCHAR),
        ("_fs_sf_object_json", _object_json, [variant], sqltypes.VARCHAR),
        ("_fs_sf_json_compact", sf_json_compact, [variant], sqltypes.VARCHAR),
        ("_fs_variant_key", _key, [variant], sqltypes.VARCHAR),
        ("_fs_variant_eq", variant_eq, [variant, variant], sqltypes.BOOLEAN),
        ("_fs_variant_eq_sql", variant_eq_sql, [variant, variant], sqltypes.BOOLEAN),
        ("_fs_typeof", typeof, [variant], sqltypes.VARCHAR),
        ("_fs_variant_to_varchar", to_varchar, [variant], sqltypes.VARCHAR),
        ("_fs_variant_to_boolean", to_boolean, [variant], sqltypes.BOOLEAN),
        ("_fs_variant_to_decimal", to_decimal, [variant, integer, integer], decimal),
        ("_fs_variant_to_bigint", to_bigint, [variant], sqltypes.BIGINT),
        ("_fs_variant_to_double", to_double, [variant], sqltypes.DOUBLE),
        ("_fs_variant_to_date", to_date, [variant], sqltypes.DATE),
        ("_fs_variant_to_time", to_time, [variant], sqltypes.TIME),
        ("_fs_variant_to_timestamp", to_timestamp, [variant], sqltypes.TIMESTAMP),
        ("_fs_variant_to_binary", to_binary, [variant], sqltypes.BLOB),
    ]
    for definition in definitions:
        _register(conn, *definition)
