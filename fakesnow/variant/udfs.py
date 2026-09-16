# ruff: noqa: ANN401
"""Python UDFs that keep variant macros opaque to DuckDB's planner.

SQL macros are inlined into every CASE arm. These functions exist so a GET path is
copied once as a UDF argument instead of exploding into a CASE DAG. They must stay
usable inside list lambdas, so they cannot be subquery wrappers.
"""

from __future__ import annotations

import contextlib
import json
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import duckdb
from _duckdb._func import FunctionNullHandling
from duckdb import DuckDBPyConnection, sqltypes

from fakesnow.variant.errors import cast_error, snowflake_error_json
from fakesnow.variant.flatten import _map_items, _variant_output

_TS_LTZ = "__FAKESNOW_TIMESTAMP_LTZ__"
_TS_TZ = "__FAKESNOW_TIMESTAMP_TZ__"
_TS_NTZ = "__FAKESNOW_TIMESTAMP_NTZ__"


def _timestamp_or_varchar(value: object) -> str:
    if isinstance(value, str):
        if value.startswith(_TS_LTZ):
            return "TIMESTAMP_LTZ"
        if value.startswith(_TS_TZ):
            return "TIMESTAMP_TZ"
        if value.startswith(_TS_NTZ):
            return "TIMESTAMP_NTZ"
    return "VARCHAR"


def _typeof_tag(value: object, variant_typeof: str | None) -> str | None:
    if variant_typeof is None:
        return None
    t = variant_typeof
    if t == "VARIANT_NULL":
        return "NULL_VALUE"
    if t.startswith(("INT", "UINT", "HUGEINT")):
        return "INTEGER"
    if t.startswith("BOOL"):
        return "BOOLEAN"
    if t.startswith("ARRAY"):
        return "ARRAY"
    if t.startswith("OBJECT"):
        return "OBJECT"
    if t.startswith("VARCHAR"):
        return _timestamp_or_varchar(value)
    if t.startswith(("DOUBLE", "FLOAT")):
        return "DOUBLE"
    if t.startswith("DECIMAL"):
        return "DECIMAL"
    if t.startswith(("BLOB", "BINARY")):
        return "BINARY"
    if t.startswith("DATE"):
        return "DATE"
    if t.startswith("TIMESTAMP"):
        if "TIME ZONE" in t or "TZ" in t:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP_NTZ"
    if t.startswith("TIME"):
        return "TIME"
    return t


def snowflake_typeof(value: object, sql_typeof: str | None, variant_typeof: str | None) -> str | None:
    if value is None and variant_typeof != "VARIANT_NULL":
        return None
    if sql_typeof is None:
        return None
    t = sql_typeof
    if t.startswith("MAP("):
        return "OBJECT"
    if t.endswith("[]"):
        return "ARRAY"
    if t == "VARCHAR":
        return _timestamp_or_varchar(value)
    if t == "BOOLEAN":
        return "BOOLEAN"
    if t.startswith(("INT", "UINT", "HUGEINT")):
        return "INTEGER"
    if t.startswith(("DOUBLE", "FLOAT")):
        return "DOUBLE"
    if t.startswith("DECIMAL"):
        return "DECIMAL"
    if t.startswith("DATE"):
        return "DATE"
    if t.startswith("TIMESTAMP"):
        if "TIME ZONE" in t or "TZ" in t:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP_NTZ"
    if t.startswith("TIME"):
        return "TIME"
    return _typeof_tag(value, variant_typeof)


def _fs_typeof_py(value: Any, sql_typeof: Any, variant_typeof: Any) -> str | None:
    sql_t = str(sql_typeof) if sql_typeof is not None else None
    var_t = str(variant_typeof) if variant_typeof is not None else None
    return snowflake_typeof(value, sql_t, var_t)


def _fs_is_json_null_py(sql_typeof: Any, variant_typeof: Any) -> bool:
    return sql_typeof == "VARIANT" and variant_typeof == "VARIANT_NULL"


def _fs_is_numeric_variant_py(variant_typeof: Any) -> bool:
    if variant_typeof is None:
        return False
    t = str(variant_typeof)
    return t.startswith(("INT", "UINT", "DECIMAL", "DOUBLE", "FLOAT", "HUGEINT"))


def _fs_variant_sort_rank_py(sql_typeof: Any, variant_typeof: Any) -> int:
    if sql_typeof is None:
        return 90
    sql_t = str(sql_typeof)
    vt = str(variant_typeof) if variant_typeof is not None else sql_t
    if vt == "VARIANT_NULL":
        return 80
    if vt.startswith("BOOL"):
        return 10
    if _fs_is_numeric_variant_py(vt):
        return 20
    if vt.startswith("VARCHAR"):
        return 30
    if vt.startswith(("BLOB", "BINARY")):
        return 40
    if vt.startswith("DATE"):
        return 50
    if vt.startswith("TIMESTAMP"):
        return 70
    if vt.startswith("TIME"):
        return 60
    if sql_t.startswith("MAP(") or vt.startswith("OBJECT"):
        return 75
    if sql_t.endswith("[]") or vt.startswith("ARRAY"):
        return 78
    return 85


def _as_variant_value(value: Any) -> Any:
    if value is None:
        return duckdb.Value(None, sqltypes.VARIANT)
    return duckdb.Value(_variant_output(value), sqltypes.VARIANT)


def _map_lookup(container: Any, key: Any) -> tuple[str, Any] | None:
    if container is None or key is None:
        return None
    current: Any = container
    if isinstance(current, str):
        try:
            current = json.loads(current)
        except json.JSONDecodeError:
            return ("missing", None)
    items = _map_items(current)
    if items is None:
        return ("missing", None)
    lookup = dict(items)
    name = str(key)
    if name not in lookup:
        return ("missing", None)
    value = lookup[name]
    if value is None:
        return ("json_null", None)
    return ("value", value)


def _fs_map_get_kind_py(container: Any, key: Any) -> str | None:
    found = _map_lookup(container, key)
    if found is None:
        return None
    return found[0]


def _fs_map_get_py(container: Any, key: Any) -> Any:
    found = _map_lookup(container, key)
    if found is None or found[0] != "value":
        return None
    return _as_variant_value(found[1])


def _numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal, str)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _variant_type_name(sql_typeof: Any, variant_typeof: Any) -> str | None:
    if variant_typeof is not None:
        return str(variant_typeof)
    if sql_typeof is None:
        return None
    return str(sql_typeof)


def _as_bool(value: Any) -> bool | None:
    if type(value) is bool:
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _fs_variant_eq_py(
    left: Any,
    left_typeof: Any,
    left_variant_typeof: Any,
    right: Any,
    right_typeof: Any,
    right_variant_typeof: Any,
) -> bool:
    if left_typeof is None and right_typeof is None:
        return True
    if left_typeof is None or right_typeof is None:
        return False
    left_sql = str(left_typeof)
    right_sql = str(right_typeof)
    left_vt = str(left_variant_typeof) if left_variant_typeof is not None else None
    right_vt = str(right_variant_typeof) if right_variant_typeof is not None else None
    left_kind = snowflake_typeof(left, left_sql, left_vt)
    right_kind = snowflake_typeof(right, right_sql, right_vt)
    if left_kind == "NULL_VALUE" or right_kind == "NULL_VALUE":
        return left_kind == right_kind

    left_bool = type(left) is bool or left_kind == "BOOLEAN"
    right_bool = type(right) is bool or right_kind == "BOOLEAN"
    if left_bool and right_bool:
        return bool(left) == bool(right)

    if not left_bool and not right_bool:
        left_numeric = _fs_is_numeric_variant_py(_variant_type_name(left_typeof, left_variant_typeof))
        right_numeric = _fs_is_numeric_variant_py(_variant_type_name(right_typeof, right_variant_typeof))
        if left_numeric and right_numeric:
            left_number = _numeric(left)
            right_number = _numeric(right)
            if left_number is not None and right_number is not None:
                return left_number == right_number
        if left == right:
            return True

    if left_kind == "VARCHAR" or right_kind == "VARCHAR":
        if left_bool or right_bool:
            left_as_bool = _as_bool(left)
            right_as_bool = _as_bool(right)
            if left_as_bool is not None and right_as_bool is not None:
                return left_as_bool == right_as_bool
        left_as_date = _as_date(left)
        right_as_date = _as_date(right)
        if left_as_date is not None and right_as_date is not None:
            return left_as_date == right_as_date
    return False


def _is_sql_null_type(sql_typeof: Any) -> bool:
    if sql_typeof is None:
        return True
    return str(sql_typeof).strip('"') in {"NULL", "SQLNULL"}


def _is_sql_null_arg(value: Any, sql_typeof: Any, variant_typeof: Any) -> bool:
    if _is_sql_null_type(sql_typeof):
        return True
    return value is None and variant_typeof is None


def _fs_variant_eq_sql_py(
    left: Any,
    left_typeof: Any,
    left_variant_typeof: Any,
    right: Any,
    right_typeof: Any,
    right_variant_typeof: Any,
) -> bool | None:
    if _is_sql_null_arg(left, left_typeof, left_variant_typeof) or _is_sql_null_arg(
        right, right_typeof, right_variant_typeof
    ):
        return None
    if _fs_variant_eq_py(
        left,
        left_typeof,
        left_variant_typeof,
        right,
        right_typeof,
        right_variant_typeof,
    ):
        return True
    left_sql = str(left_typeof)
    right_sql = str(right_typeof)
    left_vt = str(left_variant_typeof) if left_variant_typeof is not None else None
    right_vt = str(right_variant_typeof) if right_variant_typeof is not None else None
    left_kind = snowflake_typeof(left, left_sql, left_vt)
    right_kind = snowflake_typeof(right, right_sql, right_vt)
    if left_kind == "VARCHAR" or right_kind == "VARCHAR":
        left_text = None if left is None or left_kind == "NULL_VALUE" else str(left)
        right_text = None if right is None or right_kind == "NULL_VALUE" else str(right)
        if left_text is None or right_text is None:
            return None
        return left_text == right_text
    return False


def _cmp_key(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return tuple(sorted((str(k), _cmp_key(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_cmp_key(item) for item in value)
    return value


def _fs_variant_lt_py(
    left: Any,
    left_typeof: Any,
    left_variant_typeof: Any,
    right: Any,
    right_typeof: Any,
    right_variant_typeof: Any,
) -> bool | None:
    if left_typeof is None or right_typeof is None:
        return None
    left_rank = _fs_variant_sort_rank_py(left_typeof, left_variant_typeof)
    right_rank = _fs_variant_sort_rank_py(right_typeof, right_variant_typeof)
    if left_rank < right_rank:
        return True
    if left_rank > right_rank:
        return False
    left_numeric = _fs_is_numeric_variant_py(_variant_type_name(left_typeof, left_variant_typeof))
    right_numeric = _fs_is_numeric_variant_py(_variant_type_name(right_typeof, right_variant_typeof))
    if left_numeric and right_numeric:
        left_number = _numeric(left)
        right_number = _numeric(right)
        if left_number is not None and right_number is not None:
            return left_number < right_number
    try:
        return _cmp_key(left) < _cmp_key(right)
    except TypeError:
        return str(left) < str(right)


def _whole_index(key: Any) -> int | None:
    if isinstance(key, (bool, str)):
        return None
    if isinstance(key, int):
        return key
    if isinstance(key, float):
        return int(key) if key.is_integer() else None
    if isinstance(key, Decimal):
        integral = key.to_integral_value()
        return int(integral) if key == integral else None
    try:
        number = Decimal(str(key))
    except (ArithmeticError, ValueError):
        return None
    integral = number.to_integral_value()
    return int(integral) if number == integral else None


def _fs_variant_get_index_py(container: Any, key: Any) -> Any:
    if container is None or key is None:
        return None
    index = _whole_index(key)
    if index is None or index < 0 or not isinstance(container, list) or index >= len(container):
        return None
    return _as_variant_value(container[index])


def _strip_timestamp_sentinel(value: str) -> str:
    kind = "NTZ"
    body = value
    for prefix, name in ((_TS_LTZ, "LTZ"), (_TS_TZ, "TZ"), (_TS_NTZ, "NTZ")):
        if value.startswith(prefix):
            kind = name
            body = value[len(prefix) :]
            break
    suffix = ""
    if kind == "LTZ":
        if body.endswith(" Z"):
            body = body[:-2]
        suffix = " Z"
    elif kind == "TZ":
        for sep in ("+", "-"):
            index = body.rfind(sep)
            if index > 10:
                tz = body[index:].replace(":", "")
                if not tz.startswith(("+", "-")):
                    continue
                tz = " " + tz
                body = body[:index]
                suffix = tz
                break
    body = body[:23] if "." in body else body[:19] + ".000"
    return body + suffix


def _fs_variant_to_varchar_py(value: Any, sql_typeof: Any, variant_typeof: Any) -> str | None:
    sql_t = str(sql_typeof) if sql_typeof is not None else None
    var_t = str(variant_typeof) if variant_typeof is not None else None
    kind = snowflake_typeof(value, sql_t, var_t)
    if kind is None or kind == "NULL_VALUE":
        return None
    if isinstance(value, str) and value.startswith("__FAKESNOW_TIMESTAMP_"):
        return _strip_timestamp_sentinel(value)
    if kind in {"ARRAY", "OBJECT"} or isinstance(value, (dict, list)):
        payload: Any = value
        if isinstance(value, str):
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                return value
        return json.dumps(payload, default=str, ensure_ascii=False, separators=(",", ":"))
    if kind == "BOOLEAN":
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _fs_variant_to_bigint_py(value: Any, sql_typeof: Any, variant_typeof: Any) -> int | None:
    sql_t = str(sql_typeof) if sql_typeof is not None else None
    var_t = str(variant_typeof) if variant_typeof is not None else None
    kind = snowflake_typeof(value, sql_t, var_t)
    if kind is None or kind == "NULL_VALUE":
        return None
    if kind in {"INTEGER", "DECIMAL", "DOUBLE", "VARCHAR", "BOOLEAN"}:
        if value is None:
            return None
        try:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int, float, Decimal, str)):
                number = value if isinstance(value, Decimal) else Decimal(str(value))
                return int(number.to_integral_value(rounding=ROUND_HALF_UP))
        except (TypeError, ValueError, OverflowError, InvalidOperation):
            raise cast_error(value, "FIXED", kind=kind) from None
    raise cast_error(value, "FIXED", kind=kind)


def _fs_variant_to_double_py(value: Any, sql_typeof: Any, variant_typeof: Any) -> float | None:
    sql_t = str(sql_typeof) if sql_typeof is not None else None
    var_t = str(variant_typeof) if variant_typeof is not None else None
    kind = snowflake_typeof(value, sql_t, var_t)
    if kind is None or kind == "NULL_VALUE":
        return None
    if kind in {"INTEGER", "DECIMAL", "DOUBLE", "VARCHAR"}:
        if value is None:
            return None
        try:
            if isinstance(value, bool):
                raise TypeError
            if isinstance(value, (int, float, Decimal, str)):
                return float(value)
        except (TypeError, ValueError):
            raise cast_error(value, "REAL", kind=kind) from None
    raise cast_error(value, "REAL", kind=kind)


def _fs_cast_error_value_py(value: Any, sql_typeof: Any, variant_typeof: Any) -> str:
    sql_t = str(sql_typeof) if sql_typeof is not None else None
    var_t = str(variant_typeof) if variant_typeof is not None else None
    kind = snowflake_typeof(value, sql_t, var_t)
    return snowflake_error_json(value, kind=kind)


def register_variant_udfs(conn: DuckDBPyConnection) -> None:
    already = {
        str(row[0]).lower()
        for row in conn.execute(
            "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '_fs_%_py'"
        ).fetchall()
    }
    specs: list[tuple[str, Any, Any]] = [
        ("_fs_typeof_py", _fs_typeof_py, sqltypes.VARCHAR),
        ("_fs_is_json_null_py", _fs_is_json_null_py, sqltypes.BOOLEAN),
        ("_fs_is_numeric_variant_py", _fs_is_numeric_variant_py, sqltypes.BOOLEAN),
        ("_fs_variant_sort_rank_py", _fs_variant_sort_rank_py, sqltypes.INTEGER),
        ("_fs_variant_to_double_py", _fs_variant_to_double_py, sqltypes.DOUBLE),
        ("_fs_variant_to_varchar_py", _fs_variant_to_varchar_py, sqltypes.VARCHAR),
        ("_fs_cast_error_value_py", _fs_cast_error_value_py, sqltypes.VARCHAR),
        ("_fs_variant_to_bigint_py", _fs_variant_to_bigint_py, sqltypes.BIGINT),
        ("_fs_map_get_py", _fs_map_get_py, sqltypes.VARIANT),
        ("_fs_map_get_kind_py", _fs_map_get_kind_py, sqltypes.VARCHAR),
        ("_fs_variant_get_index_py", _fs_variant_get_index_py, sqltypes.VARIANT),
        ("_fs_variant_eq_py", _fs_variant_eq_py, sqltypes.BOOLEAN),
        ("_fs_variant_eq_sql_py", _fs_variant_eq_sql_py, sqltypes.BOOLEAN),
        ("_fs_variant_lt_py", _fs_variant_lt_py, sqltypes.BOOLEAN),
    ]
    for name, function, return_type in specs:
        if name.lower() in already:
            continue
        with contextlib.suppress(duckdb.CatalogException, duckdb.InvalidInputException):
            conn.remove_function(name)
        conn.create_function(
            name,
            function,
            None,
            return_type,
            null_handling=FunctionNullHandling.SPECIAL,
        )
