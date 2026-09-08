#!/usr/bin/env python3
"""Parse /tmp/sf_batch{1-6}.txt into tests/golden/variant_batch*.json fixtures."""

# ruff: noqa: ANN401

from __future__ import annotations

import ast
import datetime
import json
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

BATCH_DIR = Path("/tmp")
OUT_DIR = Path(__file__).resolve().parent.parent / "tests" / "golden"

WRAP_FROM = (
    "SELECT PARSE_JSON('{\"a\":1}') v2, "
    "[1,2,3]::ARRAY(INT) sa, "
    "{'a':1,'b':'x'}::OBJECT(a INT, b VARCHAR) so, "
    "{'k1':1,'k2':2}::MAP(VARCHAR, INT) sm"
)

IDENT_RE = re.compile(r"(?<![A-Za-z0-9_])(v2|sa|so|sm)(?![A-Za-z0-9_])")


def eval_repr(src: str) -> Any:
    """Evaluate a Snowflake dump Python repr (tuples/lists plus Decimal/datetime/bytearray)."""
    tree = ast.parse(src.strip(), mode="eval")
    return _eval_node(tree.body)


def _eval_node(node: ast.AST) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(elt) for elt in node.elts)
    if isinstance(node, ast.List):
        return [_eval_node(elt) for elt in node.elts]
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_eval_node(node.operand)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.UAdd):
        return _eval_node(node.operand)
    if isinstance(node, ast.Name):
        if node.id == "True":
            return True
        if node.id == "False":
            return False
        if node.id == "None":
            return None
        raise ValueError(f"unsupported name {node.id}")
    if isinstance(node, ast.Call):
        return _eval_call(node)
    if isinstance(node, ast.Attribute):
        # datetime.date / datetime.datetime used as Call.func
        raise ValueError(f"unsupported attribute {ast.dump(node)}")
    raise ValueError(f"unsupported ast {type(node).__name__}: {ast.dump(node)}")


def _eval_call(node: ast.Call) -> Any:
    func = node.func
    args = [_eval_node(a) for a in node.args]
    if isinstance(func, ast.Name) and func.id == "Decimal":
        return Decimal(args[0])
    if isinstance(func, ast.Name) and func.id == "bytearray":
        raw = args[0]
        if isinstance(raw, bytes):
            return bytearray(raw)
        if isinstance(raw, str):
            return bytearray(raw, "latin-1")
        return bytearray(raw)
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id == "datetime":
        if func.attr == "date":
            return datetime.date(*args)
        if func.attr == "datetime":
            return datetime.datetime(*args)
        if func.attr == "time":
            return datetime.time(*args)
    raise ValueError(f"unsupported call {ast.dump(node)}")


def encode_cell(cell: Any) -> dict[str, Any]:
    if not isinstance(cell, tuple) or len(cell) != 2:
        raise ValueError(f"expected tagged 2-tuple, got {cell!r}")
    tag, val = cell
    if tag == "Decimal":
        return {"type": "Decimal", "value": format(val, "f") if isinstance(val, Decimal) else str(val)}
    if tag == "date":
        assert isinstance(val, datetime.date)
        return {"type": "date", "value": val.isoformat()}
    if tag == "datetime":
        assert isinstance(val, datetime.datetime)
        return {"type": "datetime", "value": val.isoformat()}
    if tag == "time":
        assert isinstance(val, datetime.time)
        return {"type": "time", "value": val.isoformat()}
    if tag == "bytearray":
        assert isinstance(val, (bytes, bytearray))
        return {"type": "bytearray", "value": bytes(val).hex()}
    if tag in {"str", "int", "bool", "NoneType", "float"}:
        return {"type": tag, "value": val}
    raise ValueError(f"unknown value tag {tag!r}")


def encode_row(row: Any) -> list[dict[str, Any]]:
    if not isinstance(row, (list, tuple)):
        raise ValueError(f"expected row sequence, got {row!r}")
    return [encode_cell(c) for c in row]


TYPE_NAMES = {
    "OBJECT",
    "ARRAY",
    "VARIANT",
    "TEXT",
    "FIXED",
    "BOOLEAN",
    "REAL",
    "DATE",
    "BINARY",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIME",
    "MAP",
}


def encode_description(desc: Any, *, default_name: str | None = None) -> list[list[Any]] | None:
    if desc is None:
        return None
    # Format C success: ['VARIANT', 'TEXT']
    if isinstance(desc, (list, tuple)) and desc and all(isinstance(x, str) for x in desc):
        names = [default_name or "A"]
        if len(desc) == 2:
            names = [default_name or "A", f"TYPEOF({(default_name or 'A')})"]
        elif len(desc) > 2:
            names = [f"C{i}" for i in range(len(desc))]
            if default_name:
                names[0] = default_name
        return [[names[i], t, None, None, None] for i, t in enumerate(desc)]
    if isinstance(desc, tuple):
        desc = [desc]
    if not isinstance(desc, list):
        raise ValueError(f"unexpected desc {desc!r}")
    out: list[list[Any]] = []
    for i, col in enumerate(desc):
        if not isinstance(col, tuple):
            raise ValueError(f"unexpected desc col {col!r}")
        if col and isinstance(col[0], str) and col[0] in TYPE_NAMES:
            # Format B: (TYPE, prec, scale, internal_size[, structured])
            name = default_name or "A"
            if len(desc) > 1:
                name = f"{default_name or 'C'}{i}" if i else (default_name or "A")
            out.append([name, *[_jsonify(x) for x in col]])
        else:
            out.append([_jsonify(x) for x in col])
    return out


def _jsonify(x: Any) -> Any:
    if isinstance(x, Decimal):
        return str(x)
    return x


def slugify(text: str, *, max_len: int = 60) -> str:
    s = text.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:max_len].strip("_") or "case"


def unique_id(base: str, used: set[str]) -> str:
    ident = base or "case"
    if ident not in used:
        used.add(ident)
        return ident
    n = 2
    while f"{ident}_{n}" in used:
        n += 1
    ident = f"{ident}_{n}"
    used.add(ident)
    return ident


def assign_pr(title: str) -> int:
    t = title.lower()
    if "harness" in t:
        return 0
    if any(k in t for k in ("group by", "order by", "distinct")):
        return 6
    if any(k in t for k in ("structured", "ctas", "coercion")):
        return 5
    if "key order" in t:
        return 4
    if "object" in t and "array" not in t and "flatten" not in t:
        return 4
    if "array" in t:
        return 3
    if "function coverage" in t:
        return 3
    if "insert" in t:
        return 2
    return 2


def needs_wrap(expr: str) -> bool:
    if re.search(r"\bFROM\b", expr, re.I):
        return False
    if re.match(r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|MERGE|WITH)\b", expr, re.I):
        return False
    return bool(IDENT_RE.search(expr))


def wrap_expr(expr: str) -> tuple[str, str | None]:
    expr = expr.strip()
    if needs_wrap(expr):
        return f"SELECT {expr} AS a FROM ({WRAP_FROM})", None
    if re.match(r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|MERGE|WITH)\b", expr, re.I):
        return expr, None
    return f"SELECT {expr} AS a", None


def parse_ctas(lhs: str) -> tuple[str, str, str] | None:
    marker = ") AS SELECT "
    idx = lhs.find(marker)
    if idx < 0 or not lhs.startswith("("):
        return None
    colspec = lhs[1:idx].strip()
    select_sql = lhs[idx + len(marker) :].strip()
    col = colspec.split()[0]
    return col, colspec, select_sql


def make_case(
    *,
    case_id: str,
    sql: str,
    setup: str | None,
    error: str | None,
    description: list[list[Any]] | None,
    rows: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    return {
        "id": case_id,
        "sql": sql,
        "setup": setup,
        "expect": {
            "error": error,
            "description": description,
            "rows": rows,
        },
    }


def parse_format_a_probe(sql: str, result_lines: list[str], used: set[str]) -> dict[str, Any]:
    error: str | None = None
    desc_raw: Any = None
    rows_raw: list[Any] = []
    for line in result_lines:
        body = line.strip()
        if body.startswith("desc:"):
            desc_raw = eval_repr(body[len("desc:") :].strip())
        elif body.startswith("row:"):
            rows_raw.append(eval_repr(body[len("row:") :].strip()))
        elif body.startswith("ERROR:"):
            error = body[len("ERROR:") :].strip()
        else:
            raise ValueError(f"unexpected result line: {line!r}")
    case_id = unique_id(slugify(sql), used)
    description = encode_description(desc_raw) if desc_raw is not None else None
    rows = [encode_row(r) for r in rows_raw]
    return make_case(case_id=case_id, sql=sql, setup=None, error=error, description=description, rows=rows)


def parse_format_bc_line(line: str, used: set[str]) -> dict[str, Any]:
    if " => " not in line:
        raise ValueError(f"not a format B/C line: {line!r}")
    lhs, rhs = line.rsplit(" => ", 1)
    lhs = lhs.strip()
    rhs = rhs.strip()
    error: str | None = None
    desc_raw: Any = None
    rows_raw: list[Any] = []

    if rhs.startswith("ERROR"):
        error = rhs[len("ERROR") :].strip()
        if error.startswith(" "):
            error = error.strip()
    else:
        rows_idx = rhs.find(" rows=")
        if rows_idx >= 0:
            desc_src = rhs[:rows_idx].strip()
            rows_src = rhs[rows_idx + len(" rows=") :].strip()
            desc_raw = eval_repr(desc_src)
            parsed_rows = eval_repr(rows_src)
        else:
            # Format C: ['VARIANT', 'TEXT'] [[(...), (... )]]
            list_start = rhs.find("[[")
            if list_start < 0:
                # empty rows after type list: [...] rows=[] already handled
                if rhs.endswith("[]"):
                    # e.g. [('VARIANT', ...)] rows=[]  — shouldn't hit
                    desc_raw = eval_repr(rhs[: rhs.rfind("[]")].strip())
                    parsed_rows = []
                else:
                    raise ValueError(f"unparseable rhs: {rhs[:200]!r}")
            else:
                desc_raw = eval_repr(rhs[:list_start].strip())
                parsed_rows = eval_repr(rhs[list_start:].strip())
        if parsed_rows and isinstance(parsed_rows[0], tuple) and parsed_rows[0] and isinstance(parsed_rows[0][0], str):
            # single row stored as list of tagged cells
            rows_raw = [parsed_rows]
        else:
            rows_raw = parsed_rows

    ctas = parse_ctas(lhs)
    setup: str | None = None
    if ctas is not None:
        col, colspec, select_sql = ctas
        create = f"CREATE TEMP TABLE t ({colspec}) AS SELECT {select_sql}"
        if error:
            sql = create
        else:
            setup = create
            sql = f"SELECT {col}, TYPEOF({col}) FROM t"
        default_name = col.upper()
    elif re.match(r"^\s*(INSERT|UPDATE|DELETE|SELECT|CREATE)\b", lhs, re.I):
        sql, setup = lhs, None
        default_name = "A"
    else:
        sql, setup = wrap_expr(lhs)
        default_name = "A"

    case_id = unique_id(slugify(lhs), used)
    description = encode_description(desc_raw, default_name=default_name) if desc_raw is not None else None
    if ctas is not None and description and len(description) == 2 and description[1][0] == default_name:
        description[1][0] = f"TYPEOF({default_name})"
    rows = [encode_row(r) for r in rows_raw]
    return make_case(case_id=case_id, sql=sql, setup=setup, error=error, description=description, rows=rows)


def parse_batch(path: Path) -> list[tuple[str, list[dict[str, Any]]]]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    groups: list[tuple[str, list[dict[str, Any]]]] = []
    title = "PATHS / GET"
    cases: list[dict[str, Any]] = []
    used: set[str] = set()

    def flush() -> None:
        nonlocal cases, used
        if cases:
            groups.append((title, cases))
        cases = []
        used = set()

    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.startswith("#####"):
            flush()
            title = line[5:].strip() or "untitled"
            i += 1
            continue
        if line.startswith("=== "):
            sql_lines = [line[4:]]
            i += 1
            while i < len(lines):
                nxt = lines[i]
                if (
                    nxt.startswith("=== ")
                    or nxt.startswith("#####")
                    or nxt.startswith("   desc:")
                    or nxt.startswith("   row:")
                    or nxt.startswith("   ERROR:")
                ):
                    break
                sql_lines.append(nxt)
                i += 1
            result_lines: list[str] = []
            while i < len(lines):
                nxt = lines[i]
                if nxt.startswith("   desc:") or nxt.startswith("   row:") or nxt.startswith("   ERROR:"):
                    result_lines.append(nxt)
                    i += 1
                    continue
                break
            sql = "\n".join(sql_lines).rstrip()
            cases.append(parse_format_a_probe(sql, result_lines, used))
            continue
        if " => " in line:
            cases.append(parse_format_bc_line(line, used))
            i += 1
            continue
        raise ValueError(f"{path}: unhandled line {i + 1}: {line[:160]!r}")
    flush()
    return groups


def fixture_name(title: str) -> str:
    return slugify(title, max_len=80)


def write_fixtures() -> list[tuple[Path, int]]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for stale in OUT_DIR.glob("variant_batch*.json"):
        stale.unlink()
    written: list[tuple[Path, int]] = []
    for n in range(1, 7):
        src = BATCH_DIR / f"sf_batch{n}.txt"
        if not src.exists():
            print(f"missing {src}", file=sys.stderr)
            continue
        groups = parse_batch(src)
        # one file per section; prefix with batch number
        used_names: set[str] = set()
        for title, cases in groups:
            fname = unique_id(f"variant_batch{n}_{fixture_name(title)}", used_names)
            out = OUT_DIR / f"{fname}.json"
            payload = {
                "name": fixture_name(title),
                "pr": assign_pr(title),
                "cases": cases,
            }
            out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            written.append((out, len(cases)))
    return written


def main() -> None:
    written = write_fixtures()
    total = 0
    for path, count in written:
        print(f"{path.relative_to(OUT_DIR.parent.parent)}: {count} cases")
        total += count
    print(f"total: {total} cases in {len(written)} files")


if __name__ == "__main__":
    main()
