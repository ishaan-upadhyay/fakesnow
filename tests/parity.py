from __future__ import annotations

# ruff: noqa: ANN401
import datetime
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import snowflake.connector
from snowflake.connector.constants import FIELD_ID_TO_NAME

GOLDEN_DIR = Path(__file__).parent / "golden"


def load_fixtures() -> list[dict[str, Any]]:
    fixtures: list[dict[str, Any]] = []
    for path in sorted(GOLDEN_DIR.glob("variant_batch*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["_path"] = str(path)
        fixtures.append(payload)
    return fixtures


def normalize_cell(cell: dict[str, Any]) -> Any:
    tag = cell["type"]
    value = cell["value"]
    if tag == "NoneType":
        return None
    if tag == "Decimal":
        return Decimal(value)
    if tag == "date":
        return datetime.date.fromisoformat(value)
    if tag == "datetime":
        return datetime.datetime.fromisoformat(value)
    if tag == "time":
        return datetime.time.fromisoformat(value)
    if tag == "bytearray":
        return bytearray.fromhex(value)
    return value


def normalize_row(row: list[dict[str, Any]]) -> tuple[Any, ...]:
    return tuple(normalize_cell(cell) for cell in row)


def _description_from_cursor(cur: snowflake.connector.cursor.SnowflakeCursor) -> list[list[Any]] | None:
    if not cur.description:
        return None
    cols: list[list[Any]] = []
    for col in cur.description:
        type_name = FIELD_ID_TO_NAME.get(col.type_code, str(col.type_code))
        entry: list[Any] = [col.name, type_name, col.precision, col.scale, col.internal_size]
        nullable = getattr(col, "is_nullable", None)
        if nullable is not None:
            entry.append(nullable)
        cols.append(entry)
    return cols


def _run(sql: str, conn: snowflake.connector.SnowflakeConnection) -> dict[str, Any]:
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
        except Exception as exc:
            return {"error": str(exc), "description": None, "rows": []}
        try:
            rows = cur.fetchall() or []
        except Exception:
            rows = []
        return {
            "error": None,
            "description": _description_from_cursor(cur),
            "rows": [tuple(row) for row in rows],
        }


def run_fakesnow(sql: str, conn: snowflake.connector.SnowflakeConnection) -> dict[str, Any]:
    return _run(sql, conn)


def _load_private_key() -> Any | None:
    key_path = os.path.expanduser("~/.snowflake/test_key.p8")
    if not os.path.exists(key_path):
        return None
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
    except ImportError:
        return None
    with open(key_path, "rb") as fh:
        return serialization.load_pem_private_key(fh.read(), password=None, backend=default_backend())


def run_snowflake(sql: str, setup: str | None = None) -> dict[str, Any] | None:
    """Run SQL on live Snowflake. Returns None when key-pair auth is unavailable."""
    private_key = _load_private_key()
    if private_key is None:
        return None
    try:
        conn = snowflake.connector.connect(
            account="TEST_ACCOUNT",
            user="TEST_USER",
            private_key=private_key,
            warehouse="TEST_WAREHOUSE",
        )
    except Exception:
        return None
    try:
        if setup:
            setup_result = _run(setup, conn)
            if setup_result["error"]:
                return setup_result
        return _run(sql, conn)
    finally:
        conn.close()


def _coerce_actual_cell(value: Any) -> Any:
    if isinstance(value, bytes) and not isinstance(value, bytearray):
        return bytearray(value)
    return value


def compare_results(expected: dict[str, Any], actual: dict[str, Any]) -> None:
    exp_error = expected.get("error")
    if exp_error:
        assert actual.get("error"), f"expected error {exp_error!r}, got rows={actual.get('rows')!r}"
        assert exp_error in actual["error"] or actual["error"] in exp_error, (
            f"error mismatch\nexpected: {exp_error}\nactual:   {actual['error']}"
        )
        return

    assert actual.get("error") is None, f"unexpected error: {actual['error']}"

    exp_rows = [normalize_row(row) for row in expected.get("rows") or []]
    act_rows = [tuple(_coerce_actual_cell(c) for c in row) for row in actual.get("rows") or []]
    assert act_rows == exp_rows, f"rows mismatch\nexpected: {exp_rows!r}\nactual:   {act_rows!r}"

    exp_desc = expected.get("description")
    act_desc = actual.get("description")
    if exp_desc is None:
        return
    assert act_desc is not None, "expected description, got none"
    assert len(act_desc) == len(exp_desc), f"description length {len(act_desc)} != {len(exp_desc)}"
    for exp_col, act_col in zip(exp_desc, act_desc, strict=True):
        assert str(act_col[0]).upper() == str(exp_col[0]).upper(), f"column name {act_col[0]!r} != {exp_col[0]!r}"
        assert act_col[1] == exp_col[1], f"column type {act_col[1]!r} != {exp_col[1]!r} ({exp_col[0]})"
        # precision, scale
        if len(exp_col) > 2:
            assert act_col[2] == exp_col[2], f"precision {act_col[2]!r} != {exp_col[2]!r} ({exp_col[0]})"
        if len(exp_col) > 3:
            assert act_col[3] == exp_col[3], f"scale {act_col[3]!r} != {exp_col[3]!r} ({exp_col[0]})"
        if len(exp_col) > 4 and exp_col[4] is not None:
            assert act_col[4] == exp_col[4], f"internal_size {act_col[4]!r} != {exp_col[4]!r} ({exp_col[0]})"
