# ruff: noqa: ANN401
from __future__ import annotations

import contextlib
import re
from typing import Any

import duckdb
from _duckdb._func import FunctionNullHandling
from duckdb import DuckDBPyConnection, sqltypes


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


def _variant_output(value: Any) -> Any:
    if (items := _map_items(value)) is not None:
        return {key: duckdb.Value(_variant_output(item), sqltypes.VARIANT) for key, item in items}
    if isinstance(value, list):
        return [duckdb.Value(_variant_output(item), sqltypes.VARIANT) for item in value]
    return value


def _resolve_path(value: Any, path: str | None) -> Any:
    if value is None or path is None:
        return None
    current = value
    for plain_key, single_key, double_key, index in re.findall(
        r'(?:^|\.)([^.\[]+)|\[(?:\'([^\']*)\'|"([^"]*)"|(\d+))\]',
        path.lstrip("$").lstrip("."),
    ):
        key: str | int = int(index) if index else single_key or double_key or plain_key
        if (items := _map_items(current)) is not None:
            current = dict(items).get(str(key))
        elif isinstance(current, list):
            if not isinstance(key, int) or key < 0 or key >= len(current):
                return None
            current = current[key]
        else:
            return None
    return current


def _flatten_rows(
    value: Any,
    path: str | None,
    outer: bool | None,
    recursive: bool | None,
    mode: str | None,
    sequence: int | None,
) -> list[dict[str, Any]]:
    target = _resolve_path(value, path) if path else value
    flatten_mode = (mode or "BOTH").upper()
    rows: list[dict[str, Any]] = []

    def variant(item: Any) -> duckdb.Value:
        return duckdb.Value(_variant_output(item), sqltypes.VARIANT)

    def add_children(container: Any, prefix: str) -> None:
        if (items := _map_items(container)) is not None:
            if flatten_mode in {"BOTH", "OBJECT"}:
                for key, child in sorted(items, key=lambda pair: pair[0].encode()):
                    child_path = f"{prefix}.{key}" if prefix else key
                    rows.append(
                        {
                            "seq": sequence,
                            "key": key,
                            "path": child_path,
                            "index": None,
                            "value": variant(child),
                            "this": variant(container),
                            "json_null": child is None,
                        }
                    )
                    if recursive:
                        add_children(child, child_path)
            return

        if isinstance(container, list) and flatten_mode in {"BOTH", "ARRAY"}:
            for index, child in enumerate(container):
                child_path = f"{prefix}[{index}]"
                rows.append(
                    {
                        "seq": sequence,
                        "key": None,
                        "path": child_path,
                        "index": index,
                        "value": variant(child),
                        "this": variant(container),
                        "json_null": child is None,
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
                "this": variant(target) if isinstance(target, (list, dict)) else None,
                "json_null": False,
            }
        )
    return rows


def register_flatten_udf(conn: DuckDBPyConnection) -> None:
    name = "_fs_variant_flatten_rows"
    with contextlib.suppress(duckdb.CatalogException, duckdb.InvalidInputException):
        conn.remove_function(name)

    variant = sqltypes.VARIANT
    row_type = duckdb.struct_type(
        {
            "seq": sqltypes.UBIGINT,
            "key": sqltypes.VARCHAR,
            "path": sqltypes.VARCHAR,
            "index": sqltypes.BIGINT,
            "value": variant,
            "this": variant,
            "json_null": sqltypes.BOOLEAN,
        }
    )
    conn.create_function(
        name,
        _flatten_rows,
        [
            variant,
            sqltypes.VARCHAR,
            sqltypes.BOOLEAN,
            sqltypes.BOOLEAN,
            sqltypes.VARCHAR,
            sqltypes.UBIGINT,
        ],
        duckdb.list_type(row_type),
        null_handling=FunctionNullHandling.SPECIAL,
    )
