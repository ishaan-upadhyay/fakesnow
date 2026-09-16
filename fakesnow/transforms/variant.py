from __future__ import annotations

import json
import re
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation
from typing import Never

import snowflake.connector
from sqlglot import Dialect, errors, exp
from sqlglot.expressions import Expr
from sqlglot.tokenizer_core import TokenType


def _has_not_null(constraints: Sequence[exp.ColumnConstraint]) -> bool:
    return any(
        isinstance(constraint, exp.ColumnConstraint) and isinstance(constraint.kind, exp.NotNullColumnConstraint)
        for constraint in constraints
    )


def _structured_type_name(data_type: exp.DataType, *, defaults: bool = True) -> str:
    if data_type.this in {
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.TINYINT,
    }:
        return "NUMBER(38,0)"
    if data_type.this == exp.DataType.Type.DECIMAL:
        precision = data_type.expressions[0].name if data_type.expressions else "38"
        scale = data_type.expressions[1].name if len(data_type.expressions) > 1 else "0"
        return f"NUMBER({precision},{scale})"
    if data_type.this in {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT}:
        if data_type.expressions:
            return f"VARCHAR({data_type.expressions[0].name})"
        return "VARCHAR(16777216)" if defaults else "VARCHAR"
    if data_type.this == exp.DataType.Type.ARRAY:
        if not data_type.expressions:
            return "ARRAY"
        inner = data_type.expressions[0]
        return (
            f"ARRAY({_structured_type_name(inner, defaults=defaults)})" if isinstance(inner, exp.DataType) else "ARRAY"
        )
    if data_type.this in {exp.DataType.Type.OBJECT, exp.DataType.Type.STRUCT}:
        if not data_type.expressions:
            return "OBJECT"
        fields = []
        for field in data_type.expressions:
            if not isinstance(field, exp.ColumnDef) or field.kind is None:
                continue
            suffix = " NOT NULL" if _has_not_null(field.constraints) else ""
            fields.append(f"{field.name} {_structured_type_name(field.kind, defaults=defaults)}{suffix}")
        return f"OBJECT({', '.join(fields)})"
    if data_type.this == exp.DataType.Type.MAP and len(data_type.expressions) == 2:
        key, value = data_type.expressions
        if isinstance(key, exp.DataType) and isinstance(value, exp.DataType):
            return (
                f"MAP({_structured_type_name(key, defaults=defaults)}, "
                f"{_structured_type_name(value, defaults=defaults)})"
            )
    if data_type.this == exp.DataType.Type.VARIANT:
        return "VARIANT"
    if data_type.this == exp.DataType.Type.BOOLEAN:
        return "BOOLEAN"
    if data_type.this in {exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT}:
        return "DOUBLE"
    if data_type.this == exp.DataType.Type.DATE:
        return "DATE"
    if data_type.this in {exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPNTZ}:
        precision = data_type.expressions[0].name if data_type.expressions else "9"
        return f"TIMESTAMP_NTZ({precision})"
    if data_type.this in {exp.DataType.Type.BINARY, exp.DataType.Type.VARBINARY}:
        return "BINARY"
    return data_type.sql(dialect="snowflake")


def _structured_type_json(data_type: exp.DataType, nullable: bool) -> dict[str, object]:
    result: dict[str, object] = {"type": "VARIANT", "nullable": nullable}
    if data_type.this in {
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.TINYINT,
    }:
        return {"type": "FIXED", "precision": 38, "scale": 0, "nullable": nullable}
    if data_type.this == exp.DataType.Type.DECIMAL:
        precision = int(data_type.expressions[0].name) if data_type.expressions else 38
        scale = int(data_type.expressions[1].name) if len(data_type.expressions) > 1 else 0
        return {"type": "FIXED", "precision": precision, "scale": scale, "nullable": nullable}
    if data_type.this in {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT}:
        length = int(data_type.expressions[0].name) if data_type.expressions else 16777216
        return {
            "type": "TEXT",
            "length": length,
            "byteLength": min(length * 4, 67108864),
            "nullable": nullable,
            "fixed": False,
        }
    if data_type.this == exp.DataType.Type.ARRAY:
        result = {"type": "ARRAY", "nullable": nullable}
        if data_type.expressions and isinstance(data_type.expressions[0], exp.DataType):
            result["elementType"] = _structured_type_json(data_type.expressions[0], True)
        return result
    if data_type.this == exp.DataType.Type.OBJECT:
        result = {"type": "OBJECT", "nullable": nullable}
        if data_type.expressions:
            result["fields"] = [
                {
                    "fieldName": field.name,
                    "fieldType": _structured_type_json(field.kind, not _has_not_null(field.constraints)),
                }
                for field in data_type.expressions
                if isinstance(field, exp.ColumnDef) and field.kind is not None
            ]
        return result
    if data_type.this == exp.DataType.Type.MAP and len(data_type.expressions) == 2:
        key, value = data_type.expressions
        if isinstance(key, exp.DataType) and isinstance(value, exp.DataType):
            return {
                "type": "MAP",
                "nullable": nullable,
                "keyType": _structured_type_json(key, False),
                "valueType": _structured_type_json(value, True),
            }
    if data_type.this == exp.DataType.Type.VARIANT:
        return result
    if data_type.this == exp.DataType.Type.BOOLEAN:
        return {"type": "BOOLEAN", "nullable": nullable}
    if data_type.this in {exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT}:
        return {"type": "REAL", "nullable": nullable}
    if data_type.this in {exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPNTZ}:
        scale = int(data_type.expressions[0].name) if data_type.expressions else 9
        return {"type": "TIMESTAMP_NTZ", "precision": 0, "scale": scale, "nullable": nullable}
    if data_type.this in {exp.DataType.Type.BINARY, exp.DataType.Type.VARBINARY}:
        return {"type": "BINARY", "length": 8388608, "byteLength": 8388608, "nullable": nullable, "fixed": True}
    return result


def capture_structured_types(expression: Expr) -> Expr:
    if not isinstance(expression, exp.Create) or not isinstance(expression.this, exp.Schema):
        return expression
    properties = expression.args.get("properties")
    if isinstance(properties, exp.Properties):
        properties.set(
            "expressions",
            [prop for prop in properties.expressions if not isinstance(prop, exp.TemporaryProperty)],
        )
    metadata: list[tuple[str, str, str, str]] = []
    for column in expression.this.expressions:
        if not isinstance(column, exp.ColumnDef) or column.kind is None:
            continue
        data_type = column.kind
        if data_type.this not in {
            exp.DataType.Type.ARRAY,
            exp.DataType.Type.OBJECT,
            exp.DataType.Type.MAP,
            exp.DataType.Type.VARIANT,
        }:
            continue
        nullable = not _has_not_null(column.constraints)
        logical = "OBJECT" if data_type.this == exp.DataType.Type.OBJECT else data_type.this.value.upper()
        metadata.append(
            (
                column.name,
                logical,
                _structured_type_name(data_type),
                json.dumps(_structured_type_json(data_type, nullable), separators=(",", ":")),
            )
        )
    if metadata:
        expression.args["_fs_structured_types"] = metadata
    return expression


def capture_source_output_names(expression: Expr, sql: str) -> None:
    """Retain top-level SELECT expressions before sqlglot normalizes paths."""
    if not isinstance(expression, exp.Select):
        return

    tokens = Dialect.get_or_raise("snowflake").tokenizer().tokenize(sql)
    select_index: int | None = None
    depth = 0
    for index, token in enumerate(tokens):
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.token_type == TokenType.SELECT:
            select_index = index
            break
    if select_index is None:
        return

    projections: list[str] = []
    start = tokens[select_index].end + 1
    depth = 0
    for token in tokens[select_index + 1 :]:
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.token_type == TokenType.COMMA:
            projections.append(sql[start : token.start].strip())
            start = token.end + 1
        elif depth == 0 and token.token_type in {TokenType.FROM, TokenType.SEMICOLON}:
            projections.append(sql[start : token.start].strip())
            break
    else:
        projections.append(sql[start:].strip())

    if len(projections) != len(expression.expressions):
        return
    for item, output_name in zip(expression.expressions, projections, strict=True):
        # store on .meta, not .args: args holding a plain string would leak into
        # SQL generation for functions rendered via the all-args fallback (eg VAR_POP)
        item.meta["_fs_source_output_name"] = output_name.upper()


def _path_cast_output_name(expression: Expr) -> str | None:
    if not isinstance(expression, exp.Cast) or not isinstance(expression.this, exp.JSONExtract):
        return None
    path = expression.this.expression
    if not isinstance(path, exp.JSONPath) or len(path.expressions) != 2:
        return None
    key = path.expressions[1]
    if not isinstance(key, exp.JSONPathKey) or key.args.get("quoted"):
        return None
    return (
        f"{expression.this.this.sql(dialect='snowflake')}:{str(key.this).upper()}"
        f"::{expression.to.sql(dialect='snowflake')}"
    )


def preserve_output_names(expression: Expr) -> Expr:
    if not isinstance(expression, exp.Select):
        return expression
    if expression.find(exp.Explode):
        for column in expression.find_all(exp.Column):
            if column.name.upper() == "VALUE":
                column.args["_fs_variant"] = True
    variant_columns: set[str] = set()
    structured_columns: dict[str, exp.DataType] = {}
    for subquery in expression.find_all(exp.Subquery):
        if not isinstance(subquery.this, exp.Select):
            continue
        for item in subquery.this.expressions:
            source = item.this if isinstance(item, exp.Alias) else item
            if source.find(exp.ParseJSON, exp.ToVariant) or isinstance(
                source,
                (exp.ParseJSON, exp.ToVariant),
            ):
                variant_columns.add(item.alias_or_name.upper())
            if isinstance(source, exp.Cast) and source.to.this in {
                exp.DataType.Type.ARRAY,
                exp.DataType.Type.OBJECT,
                exp.DataType.Type.MAP,
            }:
                structured_columns[item.alias_or_name.upper()] = source.to.copy()
    if variant_columns:
        for column in expression.find_all(exp.Column):
            if column.name.upper() in variant_columns:
                column.args["_fs_variant"] = True
    if structured_columns:
        for column in expression.find_all(exp.Column):
            if column.table.upper() in structured_columns:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error: error line 1 at position 7\ninvalid identifier '{column.sql()}'",
                    errno=904,
                    sqlstate="42000",
                )
            if data_type := structured_columns.get(column.name.upper()):
                column.args["_fs_structured_type"] = data_type.copy()

    output_names: dict[str, str] = {}
    for index, item in enumerate(list(expression.expressions)):
        if isinstance(item, (exp.Alias, exp.Star)) or item.is_star:
            continue
        if not item.find(
            exp.ParseJSON,
            exp.Bracket,
            exp.GetExtract,
            exp.JSONExtract,
            exp.Typeof,
            exp.ToVariant,
        ) and not isinstance(item, (exp.Is, exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            continue
        try:
            output_name = (
                item.meta.get("_fs_source_output_name") or _path_cast_output_name(item) or item.sql(dialect="snowflake")
            )
        except (NotImplementedError, ValueError, errors.UnsupportedError):
            continue
        internal_name = f"__FS_VARIANT_COL_{index}"
        output_names[internal_name] = output_name
        item.replace(
            exp.Alias(
                this=item.copy(),
                alias=exp.Identifier(
                    this=internal_name,
                    quoted=True,
                ),
            )
        )
    if output_names:
        expression.args["_fs_output_names"] = output_names
    return expression


def _is_variant_expression(expression: Expr) -> bool:
    if isinstance(expression, exp.Column) and expression.args.get("_fs_variant"):
        return True
    if isinstance(expression, (exp.Bracket, exp.GetExtract, exp.JSONExtract, exp.ToVariant)):
        return True
    if isinstance(expression, exp.Anonymous):
        return expression.name.upper() in {
            "_FS_PARSE_JSON",
            "_FS_TRY_PARSE_JSON",
            "_FS_AS_VARIANT",
            "_FS_TO_VARIANT_TIMESTAMP",
            "_FS_VARIANT_GET",
            "_FS_VARIANT_GET_PATH",
            "_FS_VARIANT_GET_IGNORE_CASE",
            "_FS_VARIANT_GET_INDEX",
            "_FS_MAP_GET",
            "_FS_JSON_GET",
            "_FS_VARIANT_GREATEST",
            "_FS_VARIANT_LEAST",
            "_FS_VARIANT_NULL",
        }
    return (
        isinstance(expression, exp.Cast)
        and isinstance(expression.to, exp.DataType)
        and expression.to.this == exp.DataType.Type.VARIANT
    )


def _contains_variant_expression(expression: Expr) -> bool:
    return bool(
        _is_variant_expression(expression)
        or expression.find(exp.ParseJSON, exp.Bracket, exp.GetExtract, exp.JSONExtract, exp.ToVariant)
    )


_OBJECT_MACROS = frozenset(
    {
        "_FS_OBJECT_CAT",
        "_FS_OBJECT_CONSTRUCT",
        "_FS_OBJECT_DELETE",
        "_FS_OBJECT_INSERT",
        "_FS_OBJECT_PICK",
        "_FS_VARIANT_TO_OBJECT",
    }
)


def _is_map_expression(expression: Expr) -> bool:
    if isinstance(expression, exp.Anonymous) and expression.name.upper() in _OBJECT_MACROS:
        return True
    if isinstance(expression, exp.ToMap):
        return True
    if isinstance(expression, exp.Cast) and expression.to.this == exp.DataType.Type.MAP:
        return True
    structured = expression.args.get("_fs_structured_type")
    return isinstance(structured, exp.DataType) and structured.this == exp.DataType.Type.MAP


def _json_as_variant(expression: Expr) -> Expr:
    return exp.Cast(
        this=exp.Cast(
            this=expression.copy(),
            to=exp.DataType(this=exp.DataType.Type.JSON, nested=False),
        ),
        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
    )


def _as_variant(expression: Expr) -> Expr:
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "_FS_AS_VARIANT":
        return expression.copy()
    if _is_map_expression(expression):
        return _json_as_variant(expression)
    return exp.Anonymous(this="_fs_as_variant", expressions=[expression.copy()])


def _unwrap_casts(expression: Expr) -> Expr:
    current = expression
    while isinstance(current, exp.Cast):
        current = current.this
    return current


def _value_to_compact_json(value: Expr) -> Expr:
    inner = _unwrap_casts(value)
    if _is_array_expression(inner) or (isinstance(inner, exp.Anonymous) and inner.name.upper().startswith("_FS_ARRAY")):
        return exp.Anonymous(this="_fs_to_json_element", expressions=[inner.copy()])
    if _is_map_expression(inner):
        return _as_json_compact(inner)
    return exp.Anonymous(this="_fs_to_json", expressions=[_as_variant(value)])


def _pairs_to_compact_json(pairs: list[tuple[Expr, Expr]]) -> Expr:
    if not pairs:
        return exp.Literal.string("{}")
    selects = [
        f"SELECT {key.sql(dialect='duckdb')} AS k, {json_value.sql(dialect='duckdb')} AS j" for key, json_value in pairs
    ]
    sql = (
        "(SELECT '{' || COALESCE("
        "string_agg(to_json(CAST(k AS VARCHAR)) || ':' || j, ',' ORDER BY CAST(k AS VARCHAR)), '') || '}' "
        f"FROM ({' UNION ALL '.join(selects)}) AS _fs_object_json(k, j))"
    )
    parsed = exp.maybe_parse(sql, dialect="duckdb")
    assert parsed is not None
    return parsed


def _object_construct_to_json(expression: Expr) -> Expr | None:
    if isinstance(expression, exp.ToMap) and isinstance(expression.this, exp.Struct):
        pairs = [
            (prop.this, _value_to_compact_json(prop.expression))
            for prop in expression.this.expressions
            if isinstance(prop, exp.PropertyEQ)
        ]
        return _pairs_to_compact_json(pairs)
    if not (isinstance(expression, exp.Anonymous) and expression.name.upper() == "_FS_OBJECT_CONSTRUCT"):
        return None
    if len(expression.expressions) < 3:
        return None
    keys, vals, keep = expression.expressions[:3]
    if not isinstance(keys, exp.Array) or not isinstance(vals, exp.Array):
        return None
    keep_nulls = True
    if isinstance(keep, exp.Boolean):
        keep_nulls = bool(keep.this)
    pairs: list[tuple[Expr, Expr]] = []
    for key, value in zip(keys.expressions, vals.expressions, strict=True):
        if not keep_nulls and isinstance(_unwrap_casts(value), exp.Null):
            continue
        pairs.append((key, _value_to_compact_json(value)))
    return _pairs_to_compact_json(pairs)


def _as_json_compact(expression: Expr) -> Expr:
    if converted := _object_construct_to_json(expression):
        return converted
    if _is_map_expression(expression):
        return exp.Anonymous(this="_fs_to_json", expressions=[expression.copy()])
    if _is_array_expression(expression):
        return exp.Anonymous(this="_fs_to_json_element", expressions=[expression.copy()])
    return exp.Anonymous(this="_fs_to_json", expressions=[_as_variant(expression)])


def _variant_key(expression: Expr) -> Expr:
    return exp.Anonymous(this="_fs_variant_key", expressions=[_as_variant(expression)])


def _variant_sort_key(expression: Expr) -> Expr:
    return exp.Anonymous(this="_fs_variant_sort_key", expressions=[_as_variant(expression)])


def _first_variant(expression: Expr) -> Expr:
    return exp.Anonymous(this="FIRST", expressions=[expression.copy()])


# sqlglot parses Snowflake's TRY_* conversions as these nodes with safe=True. Other nodes carry a
# safe arg for unrelated reasons, eg: DPipe for concatenation, so they must not be treated as TRY_*.
_TRY_CONVERSIONS = (
    exp.Cast,
    exp.StrToDate,
    exp.StrToTime,
    exp.ToBinary,
    exp.ToBoolean,
    exp.ToDouble,
    exp.ToNumber,
    exp.TsOrDsToDate,
    exp.TsOrDsToTime,
)


def _is_array_expression(expression: Expr) -> bool:
    return isinstance(expression, exp.Array) or (
        isinstance(expression, exp.Cast)
        and isinstance(expression.to, exp.DataType)
        and expression.to.this == exp.DataType.Type.ARRAY
    )


def _branch_values(expression: Expr) -> list[Expr]:
    """The values a CASE or IFF can return, ie: the branches Snowflake unifies into one type."""

    if isinstance(expression, exp.If):
        return [value for value in (expression.args.get("true"), expression.args.get("false")) if value is not None]
    if isinstance(expression, exp.Case):
        values = [branch.args["true"] for branch in expression.args.get("ifs") or []]
        if default := expression.args.get("default"):
            values.append(default)
        return values
    return []


def _convert_branches(expression: Expr, function: str) -> Expr:
    """Pass every variant branch of a CASE or IFF through function."""

    def converted(value: Expr) -> Expr:
        if not _contains_variant_expression(value):
            return value.copy()
        return exp.Anonymous(this=function, expressions=[_as_variant(value)])

    result = expression.copy()
    if isinstance(result, exp.If):
        for key in ("true", "false"):
            if (value := result.args.get(key)) is not None:
                result.set(key, converted(value))
        return result
    for branch in result.args.get("ifs") or []:
        branch.set("true", converted(branch.args["true"]))
    if (default := result.args.get("default")) is not None:
        result.set("default", converted(default))
    return result


def _zeroifnull_argument(expression: Expr) -> Expr | None:
    """The argument of ZEROIFNULL, which sqlglot parses as IFF(arg IS NULL, 0, arg)."""

    if not isinstance(expression, exp.If):
        return None
    predicate = expression.this
    true = expression.args.get("true")
    false = expression.args.get("false")
    if (
        isinstance(predicate, exp.Is)
        and isinstance(predicate.expression, exp.Null)
        and isinstance(true, exp.Literal)
        and not true.is_string
        and true.this == "0"
        and false is not None
        and predicate.this == false
    ):
        return false
    return None


def variant_operators(expression: Expr) -> Expr:
    if expression.meta.get("_fs_native_comparison"):
        return expression

    def numeric_value(value: Expr) -> Expr:
        if not _contains_variant_expression(value):
            return value.copy()
        return exp.Anonymous(
            this="_fs_variant_to_double",
            expressions=[_as_variant(value)],
        )

    def boolean_value(value: Expr) -> Expr:
        if not _contains_variant_expression(value):
            return value.copy()
        return exp.Anonymous(
            this="_fs_variant_to_boolean",
            expressions=[_as_variant(value)],
        )

    if isinstance(expression, (exp.Add, exp.Sub, exp.Mul, exp.Div)) and (
        _contains_variant_expression(expression.this) or _contains_variant_expression(expression.expression)
    ):
        return expression.__class__(
            this=numeric_value(expression.this),
            expression=numeric_value(expression.expression),
        )

    if isinstance(expression, exp.Neg) and _contains_variant_expression(expression.this):
        return exp.Neg(this=numeric_value(expression.this))

    if isinstance(expression, (exp.Abs, exp.Round, exp.Sum, exp.Avg)) and _contains_variant_expression(expression.this):
        result = expression.copy()
        result.set("this", numeric_value(expression.this))
        return result

    if isinstance(expression, exp.DPipe) and (
        _contains_variant_expression(expression.this) or _contains_variant_expression(expression.expression)
    ):

        def text_value(value: Expr) -> Expr:
            if not _contains_variant_expression(value):
                return value.copy()
            return exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(value)],
            )

        return exp.DPipe(
            this=text_value(expression.this),
            expression=text_value(expression.expression),
        )

    if isinstance(expression, (exp.Like, exp.ILike)) and _contains_variant_expression(expression.this):
        return expression.__class__(
            this=exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(expression.this)],
            ),
            expression=expression.expression.copy(),
        )

    if isinstance(expression, (exp.And, exp.Or)) and (
        _contains_variant_expression(expression.this) or _contains_variant_expression(expression.expression)
    ):
        return expression.__class__(
            this=boolean_value(expression.this),
            expression=boolean_value(expression.expression),
        )

    if isinstance(expression, exp.Not) and _contains_variant_expression(expression.this):
        return exp.Not(this=boolean_value(expression.this))

    if isinstance(expression, (exp.EQ, exp.NEQ)) and (
        _contains_variant_expression(expression.this)
        or _contains_variant_expression(expression.expression)
        or (_is_array_expression(expression.this) and _is_array_expression(expression.expression))
    ):
        both_variant = (
            _contains_variant_expression(expression.this) and _contains_variant_expression(expression.expression)
        ) or (_is_array_expression(expression.this) and _is_array_expression(expression.expression))
        equals = exp.Anonymous(
            this="_fs_variant_eq" if both_variant else "_fs_variant_eq_sql",
            expressions=[_as_variant(expression.this), _as_variant(expression.expression)],
        )
        return exp.Not(this=equals) if isinstance(expression, exp.NEQ) else equals

    if isinstance(expression, (exp.GT, exp.GTE, exp.LT, exp.LTE)) and (
        _contains_variant_expression(expression.this) or _contains_variant_expression(expression.expression)
    ):
        left = _as_variant(expression.this)
        right = _as_variant(expression.expression)
        if isinstance(expression, exp.LT):
            return exp.Anonymous(this="_fs_variant_lt", expressions=[left, right])
        if isinstance(expression, exp.GT):
            return exp.Anonymous(this="_fs_variant_lt", expressions=[right, left])
        left_key = _variant_key(expression.this)
        right_key = _variant_key(expression.expression)
        return expression.__class__(this=left_key, expression=right_key)

    if isinstance(expression, exp.In) and _contains_variant_expression(expression.this):
        keyed = expression.copy()
        keyed.set("this", _variant_key(expression.this))
        keyed.set("expressions", [_variant_key(item) for item in expression.expressions])
        keyed.set("not", None)
        coerced = [
            exp.Anonymous(
                this="_fs_variant_eq_sql",
                expressions=[_as_variant(expression.this), _as_variant(item)],
            )
            for item in expression.expressions
        ]
        matches: Expr = keyed
        for comparison in coerced:
            matches = exp.Or(this=matches, expression=comparison)
        return exp.Not(this=matches) if expression.args.get("not") else matches

    return expression


def variant_relational_keys(expression: Expr) -> Expr:
    """Use Snowflake VARIANT keys where DuckDB requires comparable values."""

    if isinstance(expression, exp.Window):
        result = expression.copy()
        result.set(
            "partition_by",
            [
                _variant_key(item) if _contains_variant_expression(item) else item.copy()
                for item in expression.args.get("partition_by") or []
            ],
        )
        order = expression.args.get("order")
        if isinstance(order, exp.Order):
            ordered = order.copy()
            ordered_expressions: list[Expr] = []
            for item in order.expressions:
                if not isinstance(item, exp.Ordered):
                    ordered_expressions.append(item.copy())
                    continue
                ordered_expression = item.copy()
                ordered_expression.set(
                    "this",
                    _variant_sort_key(item.this) if _contains_variant_expression(item.this) else item.this.copy(),
                )
                ordered_expressions.append(ordered_expression)
            ordered.set(
                "expressions",
                ordered_expressions,
            )
            result.set("order", ordered)
        return result

    if not isinstance(expression, exp.Select):
        return expression

    result = expression.copy()
    projections = list(result.expressions)
    representatives: dict[str, Expr] = {}
    group = result.args.get("group")
    if group:
        grouped = group.copy()
        group_expressions: list[Expr] = []
        for item in group.expressions:
            if _contains_variant_expression(item):
                representatives[item.sql()] = item.copy()
                group_expressions.append(_variant_key(item))
            else:
                group_expressions.append(item.copy())
        grouped.set("expressions", group_expressions)
        result.set("group", grouped)

        rewritten: list[Expr] = []
        for item in projections:
            source = item.this if isinstance(item, exp.Alias) else item
            representative = representatives.get(source.sql())
            if representative is None:
                output_name = item.meta.get("_fs_source_output_name")
                rewritten.append(
                    exp.Alias(this=item.copy(), alias=exp.to_identifier(output_name, quoted=True))
                    if output_name and not isinstance(item, exp.Alias)
                    else item.copy()
                )
                continue
            first = _first_variant(representative)
            alias = item.args["alias"].copy() if isinstance(item, exp.Alias) else exp.to_identifier(item.alias_or_name)
            rewritten.append(exp.Alias(this=first, alias=alias))
        projections = rewritten
        result.set("expressions", projections)

    if result.args.get("distinct"):
        distinct_groups: list[Expr] = []
        rewritten = []
        for item in projections:
            source = item.this if isinstance(item, exp.Alias) else item
            if _contains_variant_expression(source):
                representatives[item.alias_or_name.upper()] = source.copy()
                distinct_groups.append(_variant_key(source))
                first = _first_variant(source)
                alias = (
                    item.args["alias"].copy() if isinstance(item, exp.Alias) else exp.to_identifier(item.alias_or_name)
                )
                rewritten.append(exp.Alias(this=first, alias=alias))
            else:
                distinct_groups.append(source.copy())
                rewritten.append(item.copy())
        if representatives:
            result.set("distinct", None)
            result.set("expressions", rewritten)
            result.set("group", exp.Group(expressions=distinct_groups))
            projections = rewritten

    if order := result.args.get("order"):
        unsupported_order_types = {
            exp.DataType.Type.BINARY,
            exp.DataType.Type.DATE,
            exp.DataType.Type.TIME,
            exp.DataType.Type.TIMESTAMP,
            exp.DataType.Type.TIMESTAMPLTZ,
            exp.DataType.Type.TIMESTAMPNTZ,
            exp.DataType.Type.TIMESTAMPTZ,
        }
        if any(
            isinstance(value.this, (exp.Cast, exp.ToBinary))
            and (
                isinstance(value.this, exp.ToBinary)
                or (isinstance(value.this, exp.Cast) and value.this.to.this in unsupported_order_types)
            )
            for value in result.find_all(exp.ToVariant)
        ):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:",
                errno=2014,
                sqlstate="22000",
            )
        rewritten_order = order.copy()
        ordered_items: list[Expr] = []
        for item in order.expressions:
            target = item.this
            use_key = _contains_variant_expression(target)
            if isinstance(target, exp.Literal) and not target.is_string:
                index = int(target.this) - 1
                if 0 <= index < len(projections):
                    projection = projections[index]
                    source = projection.this if isinstance(projection, exp.Alias) else projection
                    if _contains_variant_expression(source):
                        target = source
                        use_key = True
            elif isinstance(target, exp.Column):
                representative = representatives.get(target.name.upper()) or representatives.get(target.sql())
                if representative is not None:
                    target = _first_variant(representative)
                    use_key = True
            if use_key:
                ordered_item = item.copy()
                ordered_item.set("this", _variant_sort_key(target))
                ordered_items.append(ordered_item)
            else:
                ordered_items.append(item.copy())
        rewritten_order.set("expressions", ordered_items)
        result.set("order", rewritten_order)

    return result


class _JsonNumber:
    __slots__ = ("token",)

    def __init__(self, token: str) -> None:
        self.token = token


class _DuplicateJsonKeyError(ValueError):
    def __init__(self, key: str, pos: int) -> None:
        super().__init__(key)
        self.key = key
        self.pos = pos


def _quote_unquoted_json_keys(text: str) -> str:
    """Quote identifier keys so `{a:1}` becomes `{"a":1}` without touching string contents."""
    out: list[str] = []
    index = 0
    in_string = False
    escape = False
    expect_key = False
    while index < len(text):
        char = text[index]
        if in_string:
            out.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            expect_key = False
            out.append(char)
            index += 1
            continue
        if char in "{[":
            out.append(char)
            expect_key = char == "{"
            index += 1
            continue
        if char == ",":
            out.append(char)
            expect_key = True
            index += 1
            continue
        if char in "}]":
            expect_key = False
            out.append(char)
            index += 1
            continue
        if expect_key and char.isspace():
            out.append(char)
            index += 1
            continue
        if expect_key and (char.isalpha() or char == "_"):
            start = index
            index += 1
            while index < len(text) and (text[index].isalnum() or text[index] == "_"):
                index += 1
            key = text[start:index]
            while index < len(text) and text[index].isspace():
                index += 1
            if index < len(text) and text[index] == ":":
                out.append(f'"{key}"')
                expect_key = False
                continue
            out.append(key)
            expect_key = False
            continue
        expect_key = False
        out.append(char)
        index += 1
    return "".join(out)


def _duplicate_key_pos(text: str, key: str) -> int:
    needle = json.dumps(key)
    first = text.find(needle)
    if first < 0:
        return 0
    second = text.find(needle, first + 1)
    return second + len(needle) if second >= 0 else first + len(needle)


def _load_snowflake_json(text: str) -> object:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        seen: dict[str, object] = {}
        for key, value in pairs:
            if key in seen:
                raise _DuplicateJsonKeyError(key, _duplicate_key_pos(text, key))
            seen[key] = value
        return seen

    last_error: json.JSONDecodeError | None = None
    candidates = [text]
    quoted = _quote_unquoted_json_keys(text)
    if quoted != text:
        candidates.append(quoted)
    for candidate in candidates:
        try:
            return json.loads(
                candidate,
                object_pairs_hook=hook,
                parse_int=_JsonNumber,
                parse_float=_JsonNumber,
            )
        except _DuplicateJsonKeyError:
            raise
        except json.JSONDecodeError as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def _raise_parse_json_error(text: str, exc: BaseException) -> Never:
    if isinstance(exc, _DuplicateJsonKeyError):
        raise snowflake.connector.errors.ProgrammingError(
            msg=f'Error parsing JSON: duplicate object attribute "{exc.key}", pos {exc.pos}',
            errno=100069,
            sqlstate="22P02",
        ) from None
    if isinstance(exc, json.JSONDecodeError):
        message = (
            "Error parsing JSON: unterminated string, line 2, pos 0"
            if "\n" in text
            else f"Error parsing JSON: {exc.msg}, line {exc.lineno}, pos {exc.colno}"
        )
        raise snowflake.connector.errors.ProgrammingError(
            msg=message,
            errno=100069,
            sqlstate="22P02",
        ) from None
    raise exc


_JSON_NUMBER = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$")


def _as_variant_cast(value: Expr) -> Expr:
    return exp.Anonymous(this="_fs_as_variant", expressions=[value])


def _json_number_to_variant_expr(token: str, *, as_variant: bool = True) -> Expr:
    stripped = token.strip()
    if re.search(r"[eE]", stripped):
        value: Expr = exp.Cast(
            this=exp.Literal.number(stripped),
            to=exp.DataType(this=exp.DataType.Type.DOUBLE, nested=False),
        )
    elif "." not in stripped:
        magnitude = abs(int(stripped))
        if magnitude <= 2**63 - 1:
            value = exp.Literal.number(stripped if stripped != "-0" else "0")
        elif magnitude <= 2**127 - 1:
            value = exp.Cast(
                this=exp.Literal.string(stripped),
                to=exp.DataType.build("HUGEINT", dialect="duckdb"),
            )
        else:
            value = exp.Cast(
                this=exp.Literal.number(stripped),
                to=exp.DataType(this=exp.DataType.Type.DOUBLE, nested=False),
            )
    else:
        negative = stripped.startswith("-")
        body = stripped[1:] if negative else stripped
        integer, frac = body.split(".", 1)
        frac = frac.rstrip("0")
        if not frac:
            integer_token = integer.lstrip("0") or "0"
            if negative and integer_token != "0":
                integer_token = f"-{integer_token}"
            return _json_number_to_variant_expr(integer_token, as_variant=as_variant)
        scale = len(frac)
        integer_digits = len(integer.lstrip("0") or "0")
        precision = integer_digits + scale
        normalized = f"{'-' if negative else ''}{integer.lstrip('0') or '0'}.{frac}"
        if precision <= 38:
            value = exp.Cast(
                this=exp.Literal.string(normalized),
                to=exp.DataType.build(f"DECIMAL({precision}, {scale})", dialect="duckdb"),
            )
        else:
            value = exp.Cast(
                this=exp.Literal.number(stripped),
                to=exp.DataType(this=exp.DataType.Type.DOUBLE, nested=False),
            )
    return _as_variant_cast(value) if as_variant else value


def _keys_collide_casefold(keys: list[str]) -> bool:
    folded = [key.casefold() for key in keys]
    return len(folded) != len(set(folded))


def _dump_json_tree(node: object) -> str:
    if node is None:
        return "null"
    if isinstance(node, _JsonNumber):
        return node.token
    if isinstance(node, bool):
        return "true" if node else "false"
    if isinstance(node, str):
        return json.dumps(node, ensure_ascii=False)
    if isinstance(node, list):
        return "[" + ",".join(_dump_json_tree(item) for item in node) + "]"
    if isinstance(node, dict):
        parts = [json.dumps(key, ensure_ascii=False) + ":" + _dump_json_tree(value) for key, value in node.items()]
        return "{" + ",".join(parts) + "}"
    return json.dumps(node, ensure_ascii=False)


def _json_object_as_variant(node: dict[str, object]) -> Expr:
    return exp.Cast(
        this=exp.Cast(
            this=exp.Literal.string(_dump_json_tree(node)),
            to=exp.DataType(this=exp.DataType.Type.JSON, nested=False),
        ),
        to=_variant_type(),
    )


def _variant_type() -> exp.DataType:
    return exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)


def _json_tree_to_sql_expr(node: object) -> Expr:
    if node is None:
        return exp.Anonymous(this="_fs_variant_null", expressions=[])
    if isinstance(node, _JsonNumber):
        return _json_number_to_variant_expr(node.token, as_variant=False)
    if isinstance(node, bool):
        return exp.Boolean(this=node)
    if isinstance(node, str):
        return exp.Literal.string(node)
    if isinstance(node, list):
        return exp.Array(expressions=[_json_tree_to_variant_expr(item) for item in node])
    if isinstance(node, dict):
        if not node:
            return _json_object_as_variant(node)
        if "" in node or _keys_collide_casefold(list(node.keys())):
            return exp.ToMap(
                this=exp.Struct(
                    expressions=[
                        exp.PropertyEQ(
                            this=exp.Literal.string(key),
                            expression=_json_tree_to_variant_expr(value),
                        )
                        for key, value in node.items()
                    ]
                )
            )
        struct = exp.Struct(
            expressions=[
                exp.PropertyEQ(
                    this=exp.Literal.string(key),
                    expression=_json_tree_to_sql_expr(value),
                )
                for key, value in node.items()
            ]
        )
        struct.set("_fs_json_literal", True)
        return struct
    return exp.convert(node)


def _json_tree_to_variant_expr(node: object) -> Expr:
    value = _json_tree_to_sql_expr(node)
    if isinstance(node, dict) and isinstance(value, exp.ToMap):
        value.args["_fs_logical_variant"] = True
        return value
    if isinstance(node, (dict, list)):
        return exp.Cast(this=value, to=_variant_type())
    if node is None:
        return value
    return _as_variant_cast(value)


def parse_json(expression: Expr) -> Expr:
    if isinstance(expression, exp.ParseJSON):
        argument = expression.this
        safe = bool(expression.args.get("safe"))
        if isinstance(argument, exp.Literal) and argument.is_string:
            if not argument.this.strip():
                return exp.Cast(
                    this=exp.Null(),
                    to=_variant_type(),
                )
            try:
                loaded = _load_snowflake_json(argument.this)
            except (_DuplicateJsonKeyError, json.JSONDecodeError) as exc:
                if safe:
                    return exp.Cast(
                        this=exp.Null(),
                        to=_variant_type(),
                    )
                _raise_parse_json_error(argument.this, exc)
            return _json_tree_to_variant_expr(loaded)
        name = "_fs_try_parse_json" if safe else "_fs_parse_json"
        return exp.Anonymous(this=name, expressions=[argument.copy()])
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "PARSE_JSON":
        return exp.Anonymous(this="_fs_parse_json", expressions=expression.expressions)
    return expression


def try_parse_json_variant(expression: Expr) -> Expr:
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TRY_PARSE_JSON":
        return exp.Anonymous(this="_fs_try_parse_json", expressions=expression.expressions)
    return expression


def _to_variant_value(value: Expr) -> Expr:
    if _is_map_expression(value):
        return _json_as_variant(value)

    timestamp_kinds = {
        exp.DataType.Type.TIMESTAMPLTZ: "LTZ",
        exp.DataType.Type.TIMESTAMPNTZ: "NTZ",
        exp.DataType.Type.TIMESTAMPTZ: "TZ",
    }
    if isinstance(value, exp.Cast) and (kind := timestamp_kinds.get(value.to.this)):
        source = (
            value.this.copy()
            if isinstance(value.this, exp.Literal) and value.this.is_string
            else exp.Cast(
                this=value.copy(),
                to=exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
            )
        )
        return exp.Anonymous(
            this="_fs_to_variant_timestamp",
            expressions=[source, exp.Literal.string(kind)],
        )
    if isinstance(value, exp.CurrentTimestamp):
        return exp.Anonymous(
            this="_fs_to_variant_timestamp",
            expressions=[
                exp.Cast(
                    this=value.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
                ),
                exp.Literal.string("LTZ"),
            ],
        )
    if isinstance(value, exp.Literal) and not value.is_string and "e" in value.this.lower():
        try:
            number = Decimal(value.this)
        except InvalidOperation:
            pass
        else:
            if number == number.to_integral_value() and len(number.as_tuple().digits) <= 38:
                value = exp.Literal.number(format(number, "f"))
    if (
        isinstance(value, exp.Div)
        and isinstance(value.this, exp.Literal)
        and isinstance(value.expression, exp.Literal)
        and not value.this.is_string
        and not value.expression.is_string
    ):
        value = exp.Cast(
            this=value.copy(),
            to=exp.DataType.build("DECIMAL(38, 6)", dialect="duckdb"),
        )
    return exp.Anonymous(this="_fs_as_variant", expressions=[value.copy()])


def to_variant(expression: Expr) -> Expr:
    if isinstance(expression, exp.ToVariant):
        if (isinstance(expression.this, exp.Struct) and not expression.this.expressions) or (
            isinstance(expression.this, exp.Anonymous)
            and expression.this.name.upper() == "_FS_OBJECT_CONSTRUCT"
            and isinstance(expression.this.expressions[0], exp.Array)
            and not expression.this.expressions[0].expressions
        ):
            return exp.Anonymous(
                this="_fs_parse_json",
                expressions=[exp.Literal.string("{}")],
            )
        return _to_variant_value(expression.this)
    if (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.VARIANT
        and (
            _is_map_expression(expression.this)
            or isinstance(expression.this, (exp.Anonymous, exp.Cast, exp.CurrentTimestamp, exp.Literal, exp.Div))
        )
    ):
        return _to_variant_value(expression.this)
    return expression


def typeof_fn(expression: Expr) -> Expr:
    if isinstance(expression, exp.Typeof):
        if (
            isinstance(expression.this, exp.Cast)
            and isinstance(expression.this.to, exp.DataType)
            and expression.this.to.args.get("_fs_structured")
        ) or (isinstance(expression.this, exp.Column) and expression.this.args.get("_fs_structured_type") is not None):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error: error line 1 at position 7",
                errno=1044,
                sqlstate="42P13",
            )
        arguments: list[Expr] = []
        if isinstance(expression.this, exp.Coalesce):
            arguments = [
                expression.this.this,
                *expression.this.expressions,
            ]
        elif isinstance(expression.this, (exp.Case, exp.If)):
            arguments = _branch_values(expression.this)
        if (
            arguments
            and any(_contains_variant_expression(argument) for argument in arguments)
            and any(isinstance(argument, exp.Literal) and argument.is_string for argument in arguments)
        ):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:",
                errno=1044,
                sqlstate="42P13",
            )
        inner = expression.this
        return exp.Anonymous(this="_fs_typeof", expressions=[inner.copy()])
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TYPEOF":
        return exp.Anonymous(this="_fs_typeof", expressions=[expression.expressions[0].copy()])
    return expression


def variant_functions(expression: Expr) -> Expr:
    value = getattr(expression, "this", None)
    structured_value = isinstance(value, exp.Column) and isinstance(value.args.get("_fs_structured_type"), exp.DataType)

    if (
        isinstance(expression, exp.Cast)
        and structured_value
        and expression.to.this in {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT}
    ):
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:",
            errno=1007,
            sqlstate="22023",
        )
    if isinstance(expression, exp.JSONFormat) and structured_value:
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error: error line 1 at position 7",
            errno=1044,
            sqlstate="42P13",
        )

    if isinstance(expression, exp.If) and _is_variant_expression(expression.this):
        errno = 1044 if expression.args.get("false") is not None else 1038
        sqlstate = "42P13" if errno == 1044 else "22023"
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:",
            errno=errno,
            sqlstate=sqlstate,
        )

    if (
        isinstance(expression, _TRY_CONVERSIONS)
        and expression.args.get("safe")
        and isinstance(value, Expr)
        and _contains_variant_expression(value)
    ):
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:",
            errno=1065,
            sqlstate="22023",
        )

    if (argument := _zeroifnull_argument(expression)) is not None and _contains_variant_expression(argument):
        return exp.Coalesce(
            this=exp.Anonymous(
                this="_fs_variant_to_double",
                expressions=[_as_variant(argument)],
            ),
            expressions=[exp.Literal.number(0)],
        )

    if branch_values := _branch_values(expression):
        if any(_is_array_expression(branch) for branch in branch_values) and any(
            isinstance(branch, exp.Literal) for branch in branch_values
        ):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:",
                errno=1038,
                sqlstate="22023",
            )
        if any(_contains_variant_expression(branch) for branch in branch_values) and any(
            isinstance(branch, exp.Literal) and branch.is_string for branch in branch_values
        ):
            return _convert_branches(expression, "_fs_variant_to_varchar")

    if (
        isinstance(expression, exp.ToBinary)
        and isinstance(
            value,
            Expr,
        )
        and _contains_variant_expression(value)
    ):
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:",
            errno=939,
            sqlstate="22023",
        )

    def strip_json_null(argument: Expr) -> Expr:
        is_json_null = exp.EQ(
            this=exp.Anonymous(this="_fs_typeof", expressions=[_as_variant(argument)]),
            expression=exp.Literal.string("NULL_VALUE"),
        )
        return exp.Case(
            ifs=[exp.If(this=is_json_null, true=exp.Null())],
            default=argument.copy(),
        )

    coalesce_arguments = [expression.this, *expression.expressions] if isinstance(expression, exp.Coalesce) else []
    if (
        isinstance(expression, exp.Coalesce)
        and any(_contains_variant_expression(argument) for argument in coalesce_arguments)
        and any(isinstance(argument, exp.Literal) and argument.is_string for argument in coalesce_arguments)
    ):

        def coalesce_value(argument: Expr) -> Expr:
            if not _contains_variant_expression(argument):
                return argument.copy()
            return exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(argument)],
            )

        return exp.Coalesce(
            this=coalesce_value(expression.this),
            expressions=[coalesce_value(argument) for argument in expression.expressions],
            is_nvl=expression.args.get("is_nvl"),
            is_null=expression.args.get("is_null"),
        )
    if isinstance(expression, (exp.Least, exp.Greatest)):
        arguments = [expression.this, *expression.expressions]
        if any(_contains_variant_expression(argument) for argument in arguments) and any(
            isinstance(argument, exp.Literal) and argument.is_string for argument in arguments
        ):

            def text_value(argument: Expr) -> Expr:
                if not _contains_variant_expression(argument):
                    return argument.copy()
                return exp.Anonymous(
                    this="_fs_variant_to_varchar",
                    expressions=[_as_variant(argument)],
                )

            result = expression.copy()
            result.set("this", text_value(expression.this))
            result.set(
                "expressions",
                [text_value(argument) for argument in expression.expressions],
            )
            return result
        if any(_contains_variant_expression(argument) for argument in arguments) and any(
            isinstance(argument, exp.Literal) and not argument.is_string for argument in arguments
        ):
            name = "_fs_variant_greatest" if isinstance(expression, exp.Greatest) else "_fs_variant_least"
            result = _as_variant(arguments[0])
            for argument in arguments[1:]:
                result = exp.Anonymous(
                    this=name,
                    expressions=[result, _as_variant(argument)],
                )
            return result
    if isinstance(expression, exp.Count) and not isinstance(expression.this, exp.Star):
        if isinstance(expression.this, exp.Distinct):
            return exp.Count(
                this=exp.Distinct(expressions=[strip_json_null(argument) for argument in expression.this.expressions])
            )
        return exp.Count(this=strip_json_null(expression.this))

    if isinstance(expression, exp.IsNullValue):
        return exp.EQ(
            this=exp.Anonymous(this="_fs_typeof", expressions=[_as_variant(expression.this)]),
            expression=exp.Literal.string("NULL_VALUE"),
        )
    if isinstance(expression, exp.StripNullValue):
        return strip_json_null(expression.this)
    if isinstance(expression, exp.ToArray):
        return exp.Anonymous(
            this="_fs_variant_to_array",
            expressions=[_as_variant(expression.this)],
        )
    if isinstance(expression, exp.JSONFormat):
        return _as_json_compact(expression.this)
    if isinstance(expression, exp.ToChar) and isinstance(value, Expr) and _contains_variant_expression(value):
        return exp.Anonymous(
            this="_fs_variant_to_varchar",
            expressions=[_as_variant(value)],
        )
    if isinstance(expression, exp.ToChar) and isinstance(value, Expr) and _is_array_expression(value):
        return _as_json_compact(value)

    if isinstance(expression, exp.GroupConcat) and _contains_variant_expression(expression.this):
        result = expression.copy()
        result.set(
            "this",
            exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(expression.this)],
            ),
        )
        return result

    if isinstance(expression, exp.Length):
        return exp.Length(
            this=exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(expression.this)],
            )
        )

    if isinstance(
        expression,
        (
            exp.Upper,
            exp.Lower,
            exp.Substring,
            exp.Replace,
            exp.SplitPart,
            exp.RegexpReplace,
        ),
    ) and _contains_variant_expression(expression.this):
        result = expression.copy()
        result.set(
            "this",
            exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(expression.this)],
            ),
        )
        return result

    if isinstance(expression, exp.Concat) and any(
        _contains_variant_expression(argument) for argument in expression.expressions
    ):
        result = expression.copy()
        result.set(
            "expressions",
            [
                exp.Anonymous(
                    this="_fs_variant_to_varchar",
                    expressions=[_as_variant(argument)],
                )
                if _contains_variant_expression(argument)
                else argument.copy()
                for argument in expression.expressions
            ],
        )
        return result

    predicate_types: dict[type[Expr], str] = {
        exp.IsArray: "ARRAY",
    }
    for klass, expected in predicate_types.items():
        if isinstance(expression, klass):
            actual = exp.Anonymous(this="_fs_typeof", expressions=[_as_variant(expression.this)])
            return exp.EQ(this=actual, expression=exp.Literal.string(expected))

    if isinstance(expression, exp.Anonymous) and expression.expressions:
        name = expression.name.upper()
        argument = expression.expressions[0]
        if name == "TO_OBJECT" and len(expression.expressions) == 1:
            if _is_map_expression(argument):
                return argument.copy()
            return exp.Anonymous(
                this="_fs_variant_to_object",
                expressions=[_as_variant(argument)],
            )
        if name == "SYSTEM$TYPEOF" and len(expression.expressions) == 1:
            if isinstance(argument, exp.Cast) and argument.to.this in {
                exp.DataType.Type.ARRAY,
                exp.DataType.Type.STRUCT,
                exp.DataType.Type.MAP,
            }:
                return exp.Literal.string(f"{_structured_type_name(argument.to, defaults=False)}[LOB]")
            if isinstance(argument, exp.Array):
                return exp.Literal.string("ARRAY[LOB]")
            if isinstance(argument, exp.Struct):
                return exp.Literal.string("OBJECT[LOB]")
            if isinstance(argument, exp.Literal):
                if argument.is_string:
                    return exp.Literal.string(f"VARCHAR({len(argument.this)})[LOB]")
                whole, _, fractional = argument.this.partition(".")
                precision = len(whole.lstrip("-")) + len(fractional)
                return exp.Literal.string(f"NUMBER({precision},{len(fractional)})[SB1]")
            return exp.Anonymous(
                this="_fs_variant_to_varchar",
                expressions=[_as_variant(exp.Literal.string("VARIANT[LOB]"))],
            )
        anonymous_predicates = {
            "IS_OBJECT": "OBJECT",
            "IS_BOOLEAN": "BOOLEAN",
            "IS_BINARY": "BINARY",
            "IS_DATE": "DATE",
            "IS_DECIMAL": "DECIMAL",
            "IS_DOUBLE": "DOUBLE",
            "IS_INTEGER": "INTEGER",
            "IS_REAL": "DOUBLE",
            "IS_TIME": "TIME",
            "IS_TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
            "IS_TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
            "IS_TIMESTAMP_TZ": "TIMESTAMP_TZ",
            "IS_VARCHAR": "VARCHAR",
        }
        if len(expression.expressions) == 1 and (expected := anonymous_predicates.get(name)):
            actual = exp.Anonymous(this="_fs_typeof", expressions=[_as_variant(argument)])
            result = exp.EQ(this=actual, expression=exp.Literal.string(expected))
            result.meta["_fs_native_comparison"] = True
            return result

        as_types = {
            "AS_VARCHAR": (exp.DataType.Type.VARCHAR, "VARCHAR"),
            "AS_CHAR": (exp.DataType.Type.VARCHAR, "VARCHAR"),
            "AS_INTEGER": (exp.DataType.Type.BIGINT, "INTEGER"),
            "AS_DOUBLE": (exp.DataType.Type.DOUBLE, "DOUBLE"),
            "AS_REAL": (exp.DataType.Type.DOUBLE, "DOUBLE"),
            "AS_BOOLEAN": (exp.DataType.Type.BOOLEAN, "BOOLEAN"),
            "AS_DATE": (exp.DataType.Type.DATE, "DATE"),
            "AS_TIME": (exp.DataType.Type.TIME, "TIME"),
            "AS_TIMESTAMP_NTZ": (exp.DataType.Type.TIMESTAMPNTZ, "TIMESTAMP_NTZ"),
            "AS_BINARY": (exp.DataType.Type.BINARY, "BINARY"),
        }
        if type_spec := as_types.get(name):
            target_type, expected = type_spec
            target = exp.DataType(this=target_type, nested=False)
            converted = exp.Cast(this=argument.copy(), to=target).transform(variant_cast)
            expected_types = ["DOUBLE", "INTEGER", "DECIMAL"] if name in {"AS_DOUBLE", "AS_REAL"} else [expected]
            type_check = exp.In(
                this=exp.Anonymous(
                    this="_fs_typeof",
                    expressions=[_as_variant(argument)],
                ),
                expressions=[exp.Literal.string(kind) for kind in expected_types],
            )
            type_check.meta["_fs_native_comparison"] = True
            return exp.Case(
                ifs=[
                    exp.If(
                        this=type_check,
                        true=converted,
                    )
                ],
                default=exp.Null(),
            )

        if name in {"AS_ARRAY", "AS_OBJECT"} and len(expression.expressions) == 1:
            expected = name.removeprefix("AS_")
            if expected == "OBJECT" and _is_map_expression(argument):
                return argument.copy()
            if expected == "OBJECT" and isinstance(argument, exp.Struct) and not argument.expressions:
                return exp.Anonymous(
                    this="_fs_variant_to_object",
                    expressions=[
                        exp.Anonymous(
                            this="_fs_parse_json",
                            expressions=[exp.Literal.string("{}")],
                        )
                    ],
                )
            converter = "_fs_variant_to_array" if expected == "ARRAY" else "_fs_variant_to_object"
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.EQ(
                            this=exp.Anonymous(
                                this="_fs_typeof",
                                expressions=[_as_variant(argument)],
                            ),
                            expression=exp.Literal.string(expected),
                        ),
                        true=exp.Anonymous(
                            this=converter,
                            expressions=[_as_variant(argument)],
                        ),
                    )
                ],
                default=exp.Null(),
            )

        if name == "AS_DECIMAL":
            precision = expression.expressions[1].copy() if len(expression.expressions) > 1 else exp.Literal.number(38)
            scale = expression.expressions[2].copy() if len(expression.expressions) > 2 else exp.Literal.number(0)
            converted = exp.Anonymous(
                this="_fs_variant_to_decimal",
                expressions=[_as_variant(argument), precision, scale],
            )
            converted = exp.Cast(
                this=converted,
                to=exp.DataType(
                    this=exp.DataType.Type.DECIMAL,
                    expressions=[precision.copy(), scale.copy()],
                    nested=False,
                ),
            )
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.In(
                            this=exp.Anonymous(
                                this="_fs_typeof",
                                expressions=[_as_variant(argument)],
                            ),
                            expressions=[
                                exp.Literal.string("DECIMAL"),
                                exp.Literal.string("INTEGER"),
                            ],
                        ),
                        true=converted,
                    )
                ],
                default=exp.Null(),
            )

    return expression


def variant_cast(expression: Expr) -> Expr:
    if isinstance(expression, exp.Cast) and expression.args.get("_fs_native_variant_container"):
        return expression

    if (
        isinstance(expression, exp.Cast)
        and expression.to.this in {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT, exp.DataType.Type.NVARCHAR}
        and isinstance(expression.this, exp.Bracket)
    ):
        return exp.Anonymous(
            this="variant_extract_string",
            expressions=[_as_variant(expression.this), exp.Literal.string("")],
        )

    object_functions = {
        "_FS_OBJECT_CAT",
        "_FS_OBJECT_CONSTRUCT",
        "_FS_OBJECT_DELETE",
        "_FS_OBJECT_INSERT",
        "_FS_OBJECT_PICK",
        "_FS_VARIANT_TO_OBJECT",
    }
    if (
        isinstance(expression, exp.Cast)
        and expression.to.this in {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT}
        and isinstance(expression.this, exp.Anonymous)
        and expression.this.name.upper() in object_functions
    ):
        return exp.Anonymous(
            this="_fs_variant_to_varchar",
            expressions=[_as_variant(expression.this)],
        )
    if (
        isinstance(expression, exp.Cast)
        and expression.to.this in {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT}
        and _is_map_expression(expression.this)
    ):
        return _as_json_compact(expression.this)
    if not isinstance(expression, exp.Cast) or not (
        _is_variant_expression(expression.this) or _is_array_expression(expression.this)
    ):
        return expression

    target = expression.to
    variant_value = _as_variant(expression.this)
    if _is_array_expression(expression.this) and target.this in {
        exp.DataType.Type.VARCHAR,
        exp.DataType.Type.TEXT,
        exp.DataType.Type.NVARCHAR,
    }:
        return _as_json_compact(expression.this)
    if target.this == exp.DataType.Type.ARRAY:
        if (
            target.expressions
            and isinstance(target.expressions[0], exp.DataType)
            and target.expressions[0].this != exp.DataType.Type.VARIANT
        ):
            return expression
        return exp.Anonymous(this="_fs_variant_to_array", expressions=[variant_value])
    if target.this == exp.DataType.Type.MAP:
        if (
            len(target.expressions) == 2
            and isinstance(target.expressions[1], exp.DataType)
            and target.expressions[1].this != exp.DataType.Type.VARIANT
        ):
            return expression
        return exp.Anonymous(this="_fs_variant_to_object", expressions=[variant_value])
    function_by_type = {
        exp.DataType.Type.VARCHAR: "_fs_variant_to_varchar",
        exp.DataType.Type.TEXT: "_fs_variant_to_varchar",
        exp.DataType.Type.NVARCHAR: "_fs_variant_to_varchar",
        exp.DataType.Type.BOOLEAN: "_fs_variant_to_boolean",
        exp.DataType.Type.DOUBLE: "_fs_variant_to_double",
        exp.DataType.Type.FLOAT: "_fs_variant_to_double",
        exp.DataType.Type.DATE: "_fs_variant_to_date",
        exp.DataType.Type.TIME: "_fs_variant_to_time",
        exp.DataType.Type.TIMESTAMP: "_fs_variant_to_timestamp",
        exp.DataType.Type.TIMESTAMPNTZ: "_fs_variant_to_timestamp",
        exp.DataType.Type.BINARY: "_fs_variant_to_binary",
        exp.DataType.Type.VARBINARY: "_fs_variant_to_binary",
    }
    if target.this in function_by_type:
        converted = exp.Anonymous(this=function_by_type[target.this], expressions=[variant_value.copy()])
        return exp.Cast(this=converted, to=target.copy()) if target.expressions else converted

    if target.this in {
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.TINYINT,
    }:
        return exp.Anonymous(this="_fs_variant_to_bigint", expressions=[variant_value.copy()])

    if target.this == exp.DataType.Type.DECIMAL:
        precision = target.expressions[0] if target.expressions else exp.Literal.number(38)
        scale = target.expressions[1] if len(target.expressions) > 1 else exp.Literal.number(0)
        converted = exp.Anonymous(
            this="_fs_variant_to_decimal",
            expressions=[variant_value, precision.copy(), scale.copy()],
        )
        return exp.Cast(this=converted, to=target.copy())

    return expression


def structured_cast(expression: Expr) -> Expr:
    if (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.STRUCT
        and isinstance(expression.this, exp.Cast)
        and expression.this.to.this == exp.DataType.Type.VARIANT
        and isinstance(expression.this.this, exp.Struct)
        and expression.this.this.args.get("_fs_json_literal")
    ):
        values = {
            prop.this.name: prop.expression.copy()
            for prop in expression.this.this.expressions
            if isinstance(prop, exp.PropertyEQ)
        }
        fields = [
            field for field in expression.to.expressions if isinstance(field, exp.ColumnDef) and field.kind is not None
        ]
        if set(values) != {field.name for field in fields}:
            raise snowflake.connector.errors.ProgrammingError(
                msg="Typed object schema mismatch in conversion",
                errno=220000,
                sqlstate="22000",
            )
        return exp.Cast(
            this=exp.Struct(
                expressions=[
                    exp.PropertyEQ(
                        this=exp.Identifier(this=field.name, quoted=False),
                        expression=exp.Cast(this=values[field.name], to=field.kind.copy()),
                    )
                    for field in fields
                    if field.kind is not None
                ]
            ),
            to=expression.to.copy(),
        )

    if (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.STRUCT
        and isinstance(expression.this, exp.Anonymous)
        and expression.this.name.upper() == "_FS_PARSE_JSON"
        and len(expression.this.expressions) == 1
        and isinstance(expression.this.expressions[0], exp.Literal)
        and expression.this.expressions[0].is_string
    ):
        try:
            value = json.loads(expression.this.expressions[0].this)
        except json.JSONDecodeError:
            value = None
        json_fields = [
            field for field in expression.to.expressions if isinstance(field, exp.ColumnDef) and field.kind is not None
        ]
        if not isinstance(value, dict) or set(value) != {field.name for field in json_fields}:
            raise snowflake.connector.errors.ProgrammingError(
                msg="Typed object schema mismatch in conversion",
                errno=220000,
                sqlstate="22000",
            )
        properties: list[Expr] = []
        for field in json_fields:
            field_kind = field.kind
            assert field_kind is not None
            properties.append(
                exp.PropertyEQ(
                    this=exp.Identifier(this=field.name, quoted=False),
                    expression=exp.Cast(this=exp.convert(value[field.name]), to=field_kind.copy()),
                )
            )
        return exp.Cast(
            this=exp.Struct(expressions=properties),
            to=expression.to.copy(),
        )

    if (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.STRUCT
        and isinstance(expression.this, exp.ToMap)
        and isinstance(expression.this.this, exp.Struct)
    ):
        values: dict[str, Expr] = {}
        for prop in expression.this.this.expressions:
            if not isinstance(prop, exp.PropertyEQ):
                continue
            key = prop.this
            if (isinstance(key, exp.Literal) and key.is_string) or isinstance(key, exp.Identifier):
                values[key.name.upper()] = prop.expression.copy()
        fields: list[Expr] = []
        for field in expression.to.expressions:
            if not isinstance(field, exp.ColumnDef) or field.kind is None or field.name.upper() not in values:
                return exp.Cast(
                    this=exp.Cast(
                        this=expression.this.copy(),
                        to=exp.DataType(this=exp.DataType.Type.JSON, nested=False),
                    ),
                    to=expression.to.copy(),
                )
            inner = values[field.name.upper()]
            while isinstance(inner, exp.Cast):
                inner = inner.this
            if isinstance(inner, exp.Null) or inner.find(exp.Null):
                raise snowflake.connector.errors.ProgrammingError(
                    msg="Typed object schema mismatch in conversion",
                    errno=220000,
                    sqlstate="22000",
                )
            fields.append(
                exp.PropertyEQ(
                    this=exp.Identifier(this=field.name, quoted=False),
                    expression=exp.Cast(this=values[field.name.upper()], to=field.kind.copy()),
                )
            )
        return exp.Cast(this=exp.Struct(expressions=fields), to=expression.to.copy())

    if (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.STRUCT
        and _is_map_expression(expression.this)
        and not (isinstance(expression.this, exp.Anonymous) and expression.this.name.upper() == "_FS_OBJECT_CONSTRUCT")
    ):
        return exp.Cast(
            this=exp.Cast(
                this=expression.this.copy(),
                to=exp.DataType(this=exp.DataType.Type.JSON, nested=False),
            ),
            to=expression.to.copy(),
        )

    if not (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.STRUCT
        and isinstance(expression.this, exp.Anonymous)
        and expression.this.name.upper() == "_FS_OBJECT_CONSTRUCT"
        and len(expression.this.expressions) >= 2
    ):
        return expression

    key_array, value_array = expression.this.expressions[:2]
    if not isinstance(key_array, exp.Array) or not isinstance(value_array, exp.Array):
        return expression

    values: dict[str, Expr] = {}
    for key, value in zip(key_array.expressions, value_array.expressions, strict=True):
        source_key = key
        if isinstance(key, exp.Anonymous) and key.name.upper() == "_FS_AS_VARIANT" and key.expressions:
            source_key = key.expressions[0]
        elif isinstance(key, exp.Cast):
            source_key = key.this
        if isinstance(source_key, exp.Literal) and source_key.is_string:
            values[source_key.name.upper()] = value.copy()
    fields: list[Expr] = []
    for field in expression.to.expressions:
        if not isinstance(field, exp.ColumnDef) or field.kind is None or field.name.upper() not in values:
            return expression
        if isinstance(values[field.name.upper()], exp.Null) or values[field.name.upper()].find(exp.Null):
            raise snowflake.connector.errors.ProgrammingError(
                msg="Typed object schema mismatch in conversion",
                errno=220000,
                sqlstate="22000",
            )
        fields.append(
            exp.PropertyEQ(
                this=exp.Identifier(this=field.name, quoted=False),
                expression=exp.Cast(this=values[field.name.upper()], to=field.kind.copy()),
            )
        )
    return exp.Cast(this=exp.Struct(expressions=fields), to=expression.to.copy())


def semi_structured_types(expression: Expr) -> Expr:
    if not isinstance(expression, exp.DataType):
        return expression

    def lower(data_type: exp.DataType) -> exp.DataType:
        if data_type.this == exp.DataType.Type.VARIANT:
            return exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)

        if data_type.this == exp.DataType.Type.ARRAY:
            inner = (
                data_type.expressions[0]
                if data_type.expressions
                else exp.DataType(
                    this=exp.DataType.Type.VARIANT,
                    nested=False,
                )
            )
            return exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[lower(inner) if isinstance(inner, exp.DataType) else inner.copy()],
                nested=False,
                _fs_structured=bool(data_type.expressions),
            )

        if data_type.this == exp.DataType.Type.OBJECT:
            if data_type.expressions:
                fields: list[Expr] = []
                for field in data_type.expressions:
                    copied = field.copy()
                    if isinstance(copied, exp.ColumnDef):
                        copied.set("constraints", [])
                        if copied.kind is not None:
                            copied.set("kind", lower(copied.kind))
                    fields.append(copied)
                return exp.DataType(
                    this=exp.DataType.Type.STRUCT,
                    expressions=fields,
                    nested=False,
                    _fs_structured=True,
                )
            return exp.DataType(
                this=exp.DataType.Type.MAP,
                expressions=[
                    exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
                    exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ],
                nested=False,
                _fs_structured=bool(data_type.expressions),
            )

        if data_type.this == exp.DataType.Type.MAP:
            return exp.DataType(
                this=exp.DataType.Type.MAP,
                expressions=[
                    lower(item) if isinstance(item, exp.DataType) else item.copy() for item in data_type.expressions
                ],
                nested=False,
                _fs_structured=True,
            )

        return data_type.copy()

    return lower(expression)
