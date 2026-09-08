import re
from typing import TypedDict

from snowflake.connector.cursor import ResultMetadata


class ColumnInfo(TypedDict):
    name: str
    database: str
    schema: str
    table: str
    nullable: bool
    type: str
    byteLength: int | None
    length: int | None
    scale: int | None
    precision: int | None
    collation: str | None


duckdb_to_sf_type = {
    "BIGINT": "fixed",
    "BLOB": "binary",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DECIMAL": "fixed",
    "DOUBLE": "real",
    "HUGEINT": "fixed",
    "INTEGER": "fixed",
    "JSON": "variant",
    "VARIANT": "variant",
    "TIME": "time",
    "TIMESTAMP WITH TIME ZONE": "timestamp_tz",
    "TIMESTAMP_NS": "timestamp_ntz",
    "TIMESTAMP": "timestamp_ntz",
    "UBIGINT": "fixed",
    "VARCHAR": "text",
}


def describe_as_rowtype(describe_results: list) -> list[ColumnInfo]:
    """Convert duckdb column type to snowflake rowtype returned by the API."""

    def as_column_info(column_name: str, column_type: str) -> ColumnInfo:
        if column_type.endswith("[]"):
            sf_type = "array"
        elif column_type.startswith(("MAP(", "STRUCT(")):
            sf_type = "object"
        else:
            normalized_type = (
                "DECIMAL"
                if column_type.startswith("DECIMAL")
                else "VARCHAR"
                if column_type.startswith("VARCHAR")
                else "BLOB"
                if column_type.startswith("BLOB")
                else column_type
            )
            sf_type = duckdb_to_sf_type.get(normalized_type)
        if not sf_type:
            raise NotImplementedError(f"for column type {column_type}")

        info: ColumnInfo = {
            "name": column_name,
            # TODO
            "database": "",
            "schema": "",
            "table": "",
            # TODO
            "nullable": True,
            "type": sf_type,
            "byteLength": None,
            "length": None,
            "scale": None,
            "precision": None,
            "collation": None,
        }

        if column_type.startswith("DECIMAL"):
            match = re.search(r"\((\d+),(\d+)\)", column_type)
            info["precision"] = int(match[1]) if match else 38
            info["scale"] = int(match[2]) if match else 0
        elif sf_type == "fixed":
            info["precision"] = 38
            info["scale"] = 0
        elif sf_type == "text":
            match = re.search(r"\((\d+)\)", column_type)
            length = int(match[1]) if match else 16777216
            info["byteLength"] = length
            info["length"] = length
        elif sf_type.startswith("time"):
            info["precision"] = 0
            info["scale"] = 9
        elif sf_type == "binary":
            match = re.search(r"\((\d+)\)", column_type)
            length = int(match[1]) if match else 8388608
            info["byteLength"] = length
            info["length"] = length
        elif sf_type == "array":
            info["byteLength"] = 16777216
            info["length"] = 16777216

        return info

    column_infos = [
        as_column_info(column_name, column_type)
        for (column_name, column_type, _null, _key, _default, _extra) in describe_results
    ]
    return column_infos


def describe_as_result_metadata(describe_results: list) -> list[ResultMetadata]:
    return [ResultMetadata.from_column(c) for c in describe_as_rowtype(describe_results)]  # pyright: ignore[reportArgumentType]
