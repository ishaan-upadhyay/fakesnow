"""Snowflake's type-compatibility check for CTAS with a declared column schema.

`CREATE TABLE t (<schema>) AS SELECT ...` is validated at compile time: Snowflake
refuses to implicitly coerce most semi-structured and structured types and raises

    001010 (42846): SQL compilation error: | incompatible types: [<source>] and [<target>]

The rendered type names are part of the contract, and source and target render
differently: a VARCHAR nested inside a *value's* structured type carries the
variant text size, while a *declared* VARCHAR column carries the table default.
"""

from __future__ import annotations

import re

from sqlglot import exp

# default declared VARCHAR size, as reported for a column in a table schema
DECLARED_VARCHAR = 16777216
# VARCHAR size reported for text nested inside an already-materialised value
VALUE_VARCHAR = 134217728

_DUCKDB_SCALARS = {
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "DOUBLE": "FLOAT",
    "FLOAT": "FLOAT",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP_NTZ(9)",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ(9)",
    "BLOB": "BINARY(8388608)",
}
_DUCKDB_INTEGERS = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT", "UINTEGER"}


def _split_fields(inner: str) -> list[str]:
    """Split a comma separated type body, ignoring commas nested in parentheses."""
    fields: list[str] = []
    depth = 0
    current = ""
    for ch in inner:
        if ch == "," and depth == 0:
            fields.append(current.strip())
            current = ""
            continue
        if ch in "(":
            depth += 1
        elif ch in ")":
            depth -= 1
        current += ch
    if current.strip():
        fields.append(current.strip())
    return fields


def duckdb_to_snowflake(duckdb_type: str, varchar_size: int = VALUE_VARCHAR) -> str:
    """Render a duckdb type as the Snowflake type name used in compile errors."""
    t = duckdb_type.strip()
    upper = t.upper()

    if upper.endswith("[]"):
        element = t[:-2]
        # an array of VARIANT is Snowflake's untyped ARRAY
        if element.strip().upper() in ("VARIANT", "JSON"):
            return "ARRAY"
        return f"ARRAY({duckdb_to_snowflake(element, varchar_size)})"

    if upper.startswith("MAP("):
        key, value = _split_fields(t[4:-1])
        # MAP(VARCHAR, VARIANT) is how an untyped OBJECT is stored
        if key.strip().upper().startswith("VARCHAR") and value.strip().upper() in ("VARIANT", "JSON"):
            return "OBJECT"
        return f"MAP({duckdb_to_snowflake(key, varchar_size)}, {duckdb_to_snowflake(value, varchar_size)})"

    if upper.startswith("STRUCT("):
        rendered = []
        for field in _split_fields(t[7:-1]):
            name, _, field_type = field.partition(" ")
            unquoted = name.strip('"')
            rendered.append(f"{unquoted} {duckdb_to_snowflake(field_type, varchar_size)}")
        return f"OBJECT({', '.join(rendered)})"

    if upper in ("VARIANT", "JSON"):
        return "VARIANT"
    if upper.startswith("DECIMAL"):
        match = re.search(r"\((\d+),\s*(\d+)\)", t)
        return f"NUMBER({match[1]},{match[2]})" if match else "NUMBER(38,0)"
    if upper.startswith("VARCHAR"):
        match = re.search(r"\((\d+)\)", t)
        return f"VARCHAR({match[1] if match else varchar_size})"
    if upper in _DUCKDB_INTEGERS:
        return "NUMBER(38,0)"
    return _DUCKDB_SCALARS.get(upper, upper)


def _is_variant(dt: exp.Expression) -> bool:
    return isinstance(dt, exp.DataType) and dt.this in (exp.DataType.Type.VARIANT, exp.DataType.Type.JSON)


def declared_to_snowflake(dt: exp.DataType) -> str:
    """Render a declared column type as Snowflake reports it in compile errors."""
    kind = dt.this

    if kind == exp.DataType.Type.VARIANT:
        return "VARIANT"
    if kind == exp.DataType.Type.ARRAY:
        # an untyped ARRAY column is stored as VARIANT[], so it is not a structured type
        if not dt.expressions or _is_variant(dt.expressions[0]):
            return "ARRAY"
        return f"ARRAY({declared_to_snowflake(dt.expressions[0])})"
    if kind == exp.DataType.Type.MAP:
        key, value = dt.expressions
        # likewise an untyped OBJECT column is stored as MAP(VARCHAR, VARIANT)
        if _is_variant(value):
            return "OBJECT"
        return f"MAP({declared_to_snowflake(key)}, {declared_to_snowflake(value)})"
    if kind in (exp.DataType.Type.OBJECT, exp.DataType.Type.STRUCT):
        if not dt.expressions:
            return "OBJECT"
        fields = []
        for field in dt.expressions:
            if isinstance(field, exp.ColumnDef):
                name, field_type = field.name, field.kind
                suffix = (
                    " NOT NULL"
                    if any(isinstance(c.kind, exp.NotNullColumnConstraint) for c in field.constraints or [])
                    else ""
                )
            else:
                name, field_type, suffix = field.name, field.args.get("kind"), ""
            assert field_type is not None
            fields.append(f"{name} {declared_to_snowflake(field_type)}{suffix}")
        return f"OBJECT({', '.join(fields)})"
    if kind in (exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT, exp.DataType.Type.CHAR):
        size = dt.expressions[0].name if dt.expressions else DECLARED_VARCHAR
        return f"VARCHAR({size})"
    if kind in (exp.DataType.Type.DECIMAL, exp.DataType.Type.BIGDECIMAL):
        if len(dt.expressions) == 2:
            return f"NUMBER({dt.expressions[0].name},{dt.expressions[1].name})"
        return "NUMBER(38,0)"
    if kind in (
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.TINYINT,
    ):
        return "NUMBER(38,0)"
    if kind == exp.DataType.Type.BOOLEAN:
        return "BOOLEAN"
    if kind == exp.DataType.Type.DATE:
        return "DATE"
    if kind in (exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT):
        return "FLOAT"
    return dt.sql(dialect="snowflake").upper()


def _is_structured(rendered: str) -> bool:
    """True for ARRAY(T), MAP(K,V) and OBJECT(f T), false for bare ARRAY/OBJECT/VARIANT."""
    return rendered.startswith(("ARRAY(", "MAP(", "OBJECT("))


def _strip_not_null(rendered: str) -> str:
    return rendered.replace(" NOT NULL", "")


def _shape(rendered: str) -> str:
    """Drop scalar precision, scale and length so only the structural shape remains.

    Snowflake widens scalars inside a structured type, so ARRAY(NUMBER(10,2)) is
    accepted by an ARRAY(NUMBER(38,0)) column, but it never reshapes a container.
    """
    return re.sub(r"\((\d+)(,\s*\d+)?\)", "", _strip_not_null(rendered))


def incompatible(source: str, target: str) -> bool:
    """True when Snowflake rejects a CTAS coercion of source into target at compile time."""
    if source == target:
        return False

    semi_structured = {"VARIANT", "ARRAY", "OBJECT"}

    if _is_structured(target):
        # a structured target only accepts a value of the same shape; scalar widening is
        # allowed, and NOT NULL on a field is a runtime constraint rather than a
        # compile-time incompatibility
        return _shape(source) != _shape(target)

    if _is_structured(source):
        # a structured value is never implicitly widened, not even to VARIANT
        return True

    if target == "VARIANT":
        # a string is parsed as JSON at runtime, and the untyped containers widen cleanly
        return source not in semi_structured and not source.startswith(("VARCHAR", "NUMBER", "BOOLEAN", "FLOAT"))
    if target == "ARRAY":
        return source in ("OBJECT",) or source.startswith(("NUMBER", "BOOLEAN", "FLOAT", "DATE"))
    if target == "OBJECT":
        return source in ("ARRAY",) or source.startswith(("NUMBER", "BOOLEAN", "FLOAT", "DATE"))

    # scalar target: an untyped container never coerces, but VARIANT is cast at runtime
    return source in ("ARRAY", "OBJECT")
