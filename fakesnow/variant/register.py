from __future__ import annotations

import atexit
import os
import tempfile
import threading
from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

from fakesnow.variant import native_macros
from fakesnow.variant.udfs import register_variant_udfs

VARIANT_DATABASE_NAME = "_fs_variant"
_template_lock = threading.Lock()
_template_path: Path | None = None


def _remove_template() -> None:
    if _template_path is not None:
        _template_path.unlink(missing_ok=True)


atexit.register(_remove_template)


def _variant_template() -> Path:
    global _template_path
    with _template_lock:
        if _template_path is not None:
            return _template_path

        fd, path = tempfile.mkstemp(prefix="fakesnow-variant-", suffix=".duckdb")
        os.close(fd)
        os.unlink(path)
        template = Path(path)
        builder = duckdb.connect(database=":memory:")
        try:
            builder.execute("SET GLOBAL lambda_syntax = 'ENABLE_SINGLE_ARROW'")
            builder.execute(f"ATTACH '{template}' AS {VARIANT_DATABASE_NAME}")
            register_variant_udfs(builder)
            builder.execute(native_macros.creation_sql(VARIANT_DATABASE_NAME))
            builder.execute(f"DETACH {VARIANT_DATABASE_NAME}")
        finally:
            builder.close()
        _template_path = template
        return template


def register_variant_macros(conn: DuckDBPyConnection) -> None:
    register_variant_udfs(conn)
    template = str(_variant_template()).replace("'", "''")
    conn.execute(f"ATTACH '{template}' AS {VARIANT_DATABASE_NAME} (READ_ONLY)")
