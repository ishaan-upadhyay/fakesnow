from __future__ import annotations

from duckdb import DuckDBPyConnection

from fakesnow.variant import native_macros


def register_variant_macros(conn: DuckDBPyConnection, catalog: str = "_fs_global") -> None:
    conn.execute(native_macros.global_creation_sql())
    if catalog:
        conn.execute(native_macros.creation_sql(catalog))
