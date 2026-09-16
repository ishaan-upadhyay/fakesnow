from __future__ import annotations

import json
import re

import pyarrow as pa
import pyarrow.compute as pc
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from fakesnow.rowtype import ColumnInfo

_PARQUET_VARIANT_EXT = b"arrow.parquet.variant"
_VARIANT_JSON_RELATION = "_fs_arrow_variant_json"


def _is_parquet_variant(field: pa.Field) -> bool:
    meta = field.metadata or {}
    return meta.get(b"ARROW:extension:name") == _PARQUET_VARIANT_EXT


def contains_parquet_variant(field: pa.Field) -> bool:
    if _is_parquet_variant(field):
        return True
    t = field.type
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return contains_parquet_variant(t.value_field)
    if pa.types.is_map(t):
        return contains_parquet_variant(t.item_field)
    if pa.types.is_struct(t):
        return any(contains_parquet_variant(c) for c in t)
    return False


def _needs_snowflake_json(field: pa.Field) -> bool:
    return contains_parquet_variant(field) or pa.types.is_map(field.type)


def _quoted_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _list_has_invalid_slots(raw_column: pa.Array | pa.ChunkedArray) -> list[bool]:
    column = raw_column.combine_chunks() if isinstance(raw_column, pa.ChunkedArray) else raw_column
    flags: list[bool] = []
    for row_index in range(len(column)):
        value = column[row_index]
        if value is None:
            flags.append(False)
            continue
        values = value.values if hasattr(value, "values") else None
        if values is None:
            flags.append(False)
            continue
        validity = values.is_valid()
        flags.append(any(not validity[slot] for slot in range(len(validity))))
    return flags


def _fix_array_undefined(json_text: str | None, raw_list: object) -> str | None:
    if json_text is None or raw_list is None:
        return json_text
    values = getattr(raw_list, "values", None)
    if values is None or not hasattr(values, "is_valid"):
        return json_text
    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        return json_text
    if not isinstance(parsed, list):
        return json_text
    validity = values.is_valid()
    if not any(not validity[index] for index in range(min(len(parsed), len(validity)))):
        return json_text
    rendered = []
    for index, item in enumerate(parsed):
        text = (
            "undefined"
            if index < len(validity) and not validity[index]
            else json.dumps(item, ensure_ascii=False, indent=2)
        )
        rendered.append("\n".join(f"  {line}" for line in text.splitlines()))
    return "[\n" + ",\n".join(rendered) + "\n]"


_UNDEFINED_TOKEN = re.compile(r"(?<=[\[,:])\s*undefined\s*(?=[,\]}])")
_SCIENTIFIC_NUMBER = re.compile(r"(?<![\"\w])(-?\d+\.\d+e[+-]?\d+)(?![\"\w])")


def _normalize_json_number(value: str) -> str:
    if value.count(".") == 1 and value.replace(".", "", 1).replace("-", "", 1).isdigit():
        stripped = value.rstrip("0").rstrip(".")
        return stripped if stripped not in {"", "-"} else "0"
    return value


def _pretty_semistructured(value: str | None, *, compact: bool = False) -> str | None:
    if value is None:
        return None
    if value in {"inf", "+inf", "Inf", "+Inf"}:
        return "Infinity"
    if value in {"-inf", "-Inf"}:
        return "-Infinity"
    if _UNDEFINED_TOKEN.search(value):
        return value
    prepared = value
    scientific: list[str] = []

    def _quote_scientific(match: re.Match[str]) -> str:
        scientific.append(match.group(1))
        return json.dumps(match.group(1))

    prepared = _SCIENTIFIC_NUMBER.sub(_quote_scientific, prepared)
    try:
        parsed = json.loads(prepared)
    except json.JSONDecodeError:
        return _normalize_json_number(value)
    if isinstance(parsed, (dict, list)):
        dumped = (
            json.dumps(parsed, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            if compact
            else json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
        )
        for token in scientific:
            dumped = dumped.replace(json.dumps(token), token)
        return dumped
    if isinstance(parsed, (int, float)) and not isinstance(parsed, bool):
        return _normalize_json_number(value)
    return value


def parquet_variant_to_json(
    conn: DuckDBPyConnection,
    table: pa.Table,
    render_columns: list[bool] | None = None,
) -> pa.Table:
    should_render = [
        _needs_snowflake_json(table.schema.field(i)) or bool(render_columns and render_columns[i])
        for i in range(table.num_columns)
    ]
    if table.num_columns == 0 or not any(should_render):
        return table

    projections = []
    for index in range(table.num_columns):
        ident = _quoted_ident(table.schema.field(index).name)
        if should_render[index]:
            field = table.schema.field(index)
            if contains_parquet_variant(field):
                projections.append(f"_fs_to_json({ident}) AS {ident}")
            else:
                projections.append(f"CAST({ident} AS JSON) AS {ident}")
        else:
            projections.append(ident)

    conn.register(_VARIANT_JSON_RELATION, table)
    try:
        json_table = conn.execute(f"SELECT {', '.join(projections)} FROM {_VARIANT_JSON_RELATION}").to_arrow_table()
    finally:
        conn.unregister(_VARIANT_JSON_RELATION)

    arrays: list[pa.Array] = []
    for index in range(table.num_columns):
        field = table.schema.field(index)
        json_column = json_table.column(field.name)
        if pa.types.is_list(table.schema.field(index).type) and (
            contains_parquet_variant(field) or any(_list_has_invalid_slots(table.column(index)))
        ):
            invalid_rows = _list_has_invalid_slots(table.column(index))
            fixed = []
            json_values = json_column.combine_chunks().to_pylist()
            for row_index, json_text in enumerate(json_values):
                raw_value = table.column(index)[row_index]
                fixed.append(_fix_array_undefined(json_text, raw_value) if invalid_rows[row_index] else json_text)
            arrays.append(pa.array(fixed, type=pa.string()))
        elif should_render[index]:
            raw_values = table.column(index).combine_chunks().to_pylist()
            json_values = json_column.combine_chunks().to_pylist()
            fixed = [None if raw is None else json_text for raw, json_text in zip(raw_values, json_values, strict=True)]
            arrays.append(pa.array(fixed, type=pa.string()))
        else:
            arrays.append(table.column(index).combine_chunks())
    return pa.Table.from_arrays(arrays, names=table.column_names)


def _is_variant_sql_json(duck_type: str) -> bool:
    return duck_type == "VARIANT" or duck_type == "VARIANT[]"


def _is_container_json(duck_type: str) -> bool:
    if duck_type == "VARIANT[]":
        return False
    return duck_type.startswith(("MAP(", "STRUCT(", "JSON")) or duck_type.endswith("[]")


def render_fetch_table(
    conn: DuckDBPyConnection,
    relation: DuckDBPyRelation,
    pretty_json_columns: list[bool] | None = None,
    compact_json_columns: list[bool] | None = None,
) -> pa.Table:
    duck_types = [str(column_type) for column_type in relation.types]
    names = list(relation.columns)
    if not names:
        return relation.to_arrow_table()
    sql_render = [_is_variant_sql_json(duck_type) for duck_type in duck_types]
    arrow_render = [_is_container_json(duck_type) for duck_type in duck_types]
    table = relation.to_arrow_table()
    render_columns = [sql or arrow for sql, arrow in zip(sql_render, arrow_render, strict=True)]
    rendered = parquet_variant_to_json(conn, table, render_columns) if any(render_columns) else table
    pretty_columns = [
        sql_render[index]
        or arrow_render[index]
        or bool(pretty_json_columns and index < len(pretty_json_columns) and pretty_json_columns[index])
        or bool(compact_json_columns and index < len(compact_json_columns) and compact_json_columns[index])
        for index in range(len(names))
    ]
    if not any(pretty_columns):
        return rendered

    arrays: list[pa.Array | pa.ChunkedArray] = []
    for index, column in enumerate(rendered.columns):
        if not pretty_columns[index]:
            arrays.append(column)
            continue
        compact = bool(compact_json_columns and index < len(compact_json_columns) and compact_json_columns[index])
        pretty_values = []
        for value in column.to_pylist():
            if not isinstance(value, str):
                pretty_values.append(value)
                continue
            pretty_values.append(_pretty_semistructured(value, compact=compact))
        if all(item is None or isinstance(item, str) for item in pretty_values):
            arrays.append(pa.array(pretty_values, type=pa.string()))
        else:
            arrays.append(column)
    return pa.Table.from_arrays(arrays, names=rendered.column_names)


def to_sf_schema(schema: pa.Schema, rowtype: list[ColumnInfo]) -> pa.Schema:
    assert len(schema) == len(rowtype), f"schema and rowtype must be same length but f{len(schema)=} f{len(rowtype)=}"

    def sf_field(field: pa.Field, c: ColumnInfo) -> pa.Field:
        if isinstance(field.type, pa.TimestampType):
            fields = [pa.field("epoch", pa.int64(), nullable=False), pa.field("fraction", pa.int32(), nullable=False)]
            if field.type.tz:
                fields.append(pa.field("timezone", nullable=False, type=pa.int32()))
            field = field.with_type(pa.struct(fields))
        elif isinstance(field.type, pa.Time64Type) or pa.types.is_uint64(field.type):
            field = field.with_type(pa.int64())
        return field.with_metadata(
            {
                "logicalType": c["type"].upper(),
                "precision": str(c["precision"] or 38),
                "scale": str(c["scale"] or 0),
                "charLength": str(c["length"] or 0),
            }
        )

    return pa.schema([sf_field(schema.field(i), c) for i, c in enumerate(rowtype)])


def to_ipc(table: pa.Table) -> pa.Buffer:
    batches = table.to_batches()
    if len(batches) != 1:
        raise NotImplementedError(f"{len(batches)} batches")
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_batch(batches[0])
    return sink.getvalue()


def to_sf(table: pa.Table, rowtype: list[ColumnInfo]) -> pa.Table:
    def to_sf_col(col: pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
        if pa.types.is_timestamp(col.type):
            return timestamp_to_sf_struct(col)
        if pa.types.is_time(col.type):
            return pc.multiply(col.cast(pa.int64()), 1000)
        return col

    return pa.Table.from_arrays([to_sf_col(c) for c in table.columns], schema=to_sf_schema(table.schema, rowtype))


def timestamp_to_sf_struct(ts: pa.Array | pa.ChunkedArray) -> pa.Array:
    if isinstance(ts, pa.ChunkedArray):
        ts = ts.combine_chunks()
    if not isinstance(ts.type, pa.TimestampType):
        raise ValueError(f"Expected TimestampArray, got {type(ts)}")
    tsa_without_us = pc.floor_temporal(ts, unit="second")
    epoch = pc.divide(tsa_without_us.cast(pa.int64()), 1_000_000)
    fraction = pc.round(pc.multiply(pc.subsecond(ts), 1_000_000_000)).cast(pa.int32())  # type: ignore
    if ts.type.tz:
        assert ts.type.tz == "UTC", f"Timezone {ts.type.tz} not yet supported"
        timezone = pa.array([1440] * len(ts), type=pa.int32())
        return pa.StructArray.from_arrays(
            arrays=[epoch, fraction, timezone],
            fields=[
                pa.field("epoch", nullable=False, type=pa.int64()),
                pa.field("fraction", nullable=False, type=pa.int32()),
                pa.field("timezone", nullable=False, type=pa.int32()),
            ],
        )
    return pa.StructArray.from_arrays(
        arrays=[epoch, fraction],
        fields=[
            pa.field("epoch", nullable=False, type=pa.int64()),
            pa.field("fraction", nullable=False, type=pa.int32()),
        ],
    )
