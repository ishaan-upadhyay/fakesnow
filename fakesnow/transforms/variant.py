from __future__ import annotations

from decimal import Decimal, InvalidOperation

import snowflake.connector
from sqlglot import Dialect, errors, exp
from sqlglot.expressions import Expr
from sqlglot.tokenizer_core import TokenType


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
        item.args["_fs_source_output_name"] = output_name.upper()


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
    if variant_columns:
        for column in expression.find_all(exp.Column):
            if column.name.upper() in variant_columns:
                column.args["_fs_variant"] = True

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
                item.args.get("_fs_source_output_name") or _path_cast_output_name(item) or item.sql(dialect="snowflake")
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
            "_FS_TO_VARIANT_TIMESTAMP",
            "_FS_VARIANT_GET",
            "_FS_VARIANT_GET_PATH",
            "_FS_VARIANT_GET_IGNORE_CASE",
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


def _as_variant(expression: Expr) -> Expr:
    return exp.Cast(
        this=expression.copy(),
        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
    )


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
            _contains_variant_expression(expression.this)
            and _contains_variant_expression(expression.expression)
        ) or (_is_array_expression(expression.this) and _is_array_expression(expression.expression))
        equals = exp.Anonymous(
            this="_fs_variant_eq" if both_variant else "_fs_variant_eq_sql",
            expressions=[_as_variant(expression.this), _as_variant(expression.expression)],
        )
        return exp.Not(this=equals) if isinstance(expression, exp.NEQ) else equals

    if isinstance(expression, (exp.GT, exp.GTE, exp.LT, exp.LTE)) and (
        _contains_variant_expression(expression.this) or _contains_variant_expression(expression.expression)
    ):
        left = exp.Anonymous(this="_fs_variant_key", expressions=[_as_variant(expression.this)])
        right = exp.Anonymous(this="_fs_variant_key", expressions=[_as_variant(expression.expression)])
        return expression.__class__(this=left, expression=right)

    if isinstance(expression, exp.In) and _contains_variant_expression(expression.this):
        comparisons = [
            exp.Anonymous(
                this="_fs_variant_eq_sql",
                expressions=[_as_variant(expression.this), _as_variant(item)],
            )
            for item in expression.expressions
        ]
        if not comparisons:
            return expression
        result = comparisons[0]
        for comparison in comparisons[1:]:
            result = exp.Or(this=result, expression=comparison)
        return exp.Not(this=result) if expression.args.get("not") else result

    return expression


def parse_json(expression: Expr) -> Expr:
    if isinstance(expression, exp.ParseJSON):
        parsed = exp.Anonymous(this="_fs_parse_json", expressions=[expression.this.copy()])
        return exp.Anonymous(this="TRY", expressions=[parsed]) if expression.args.get("safe") else parsed
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "PARSE_JSON":
        return exp.Anonymous(this="_fs_parse_json", expressions=expression.expressions)
    return expression


def try_parse_json_variant(expression: Expr) -> Expr:
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TRY_PARSE_JSON":
        return exp.Anonymous(
            this="TRY",
            expressions=[
                exp.Anonymous(this="_fs_parse_json", expressions=expression.expressions),
            ],
        )
    return expression


def _to_variant_value(value: Expr) -> Expr:
    timestamp_kinds = {
        exp.DataType.Type.TIMESTAMPLTZ: "LTZ",
        exp.DataType.Type.TIMESTAMPNTZ: "NTZ",
        exp.DataType.Type.TIMESTAMPTZ: "TZ",
    }
    if isinstance(value, exp.Cast) and (kind := timestamp_kinds.get(value.to.this)):
        source = value.this.copy() if isinstance(value.this, exp.Literal) and value.this.is_string else exp.Cast(
            this=value.copy(),
            to=exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
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
    return exp.Cast(
        this=value.copy(),
        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
    )


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
        and isinstance(expression.this, (exp.Cast, exp.CurrentTimestamp, exp.Literal, exp.Div))
    ):
        return _to_variant_value(expression.this)
    return expression


def typeof_fn(expression: Expr) -> Expr:
    if isinstance(expression, exp.Typeof):
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
        return exp.Anonymous(this="_fs_typeof", expressions=[_as_variant(expression.this)])
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TYPEOF":
        return exp.Anonymous(this="_fs_typeof", expressions=[_as_variant(expression.expressions[0])])
    return expression


def variant_functions(expression: Expr) -> Expr:
    value = getattr(expression, "this", None)

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
        return exp.Anonymous(
            this="_fs_sf_json_compact",
            expressions=[_as_variant(expression.this)],
        )
    if isinstance(expression, exp.ToChar) and isinstance(value, Expr) and _contains_variant_expression(value):
        return exp.Anonymous(
            this="_fs_variant_to_varchar",
            expressions=[_as_variant(value)],
        )
    if isinstance(expression, exp.ToChar) and isinstance(value, Expr) and _is_array_expression(value):
        return exp.Anonymous(
            this="_fs_sf_json_compact",
            expressions=[_as_variant(value)],
        )

    if isinstance(
        expression,
        (
            exp.Upper,
            exp.Lower,
            exp.Length,
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
            return exp.Anonymous(
                this="_fs_variant_to_object",
                expressions=[_as_variant(argument)],
            )
        if name == "SYSTEM$TYPEOF" and len(expression.expressions) == 1:
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
            return exp.EQ(this=actual, expression=exp.Literal.string(expected))

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
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.In(
                            this=exp.Anonymous(
                                this="_fs_typeof",
                                expressions=[_as_variant(argument)],
                            ),
                            expressions=[exp.Literal.string(kind) for kind in expected_types],
                        ),
                        true=converted,
                    )
                ],
                default=exp.Null(),
            )

        if name in {"AS_ARRAY", "AS_OBJECT"} and len(expression.expressions) == 1:
            expected = name.removeprefix("AS_")
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
            expressions=[
                exp.Cast(
                    this=expression.this.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                )
            ],
        )
    if not isinstance(expression, exp.Cast) or not (
        _is_variant_expression(expression.this) or _is_array_expression(expression.this)
    ):
        return expression

    target = expression.to
    variant_value = exp.Cast(
        this=expression.this.copy(),
        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
    )
    if _is_array_expression(expression.this) and target.this in {
        exp.DataType.Type.VARCHAR,
        exp.DataType.Type.TEXT,
        exp.DataType.Type.NVARCHAR,
    }:
        return exp.Anonymous(this="_fs_sf_json_compact", expressions=[variant_value])
    if target.this == exp.DataType.Type.ARRAY:
        return exp.Anonymous(this="_fs_variant_to_array", expressions=[variant_value])
    if target.this == exp.DataType.Type.MAP:
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
        source_key = key.this if isinstance(key, exp.Cast) else key
        if isinstance(source_key, exp.Literal) and source_key.is_string:
            values[source_key.name.upper()] = value.copy()
    fields: list[Expr] = []
    for field in expression.to.expressions:
        if not isinstance(field, exp.ColumnDef) or field.kind is None or field.name.upper() not in values:
            return expression
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

    if expression.this == exp.DataType.Type.VARIANT:
        return exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)

    if expression.this == exp.DataType.Type.ARRAY:
        if expression.expressions:
            inner = expression.expressions[0]
            return exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[inner],
                nested=False,
            )
        return exp.DataType(
            this=exp.DataType.Type.ARRAY,
            expressions=[exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)],
            nested=False,
        )

    if expression.this == exp.DataType.Type.OBJECT:
        if expression.expressions:
            fields: list[Expr] = []
            for field in expression.expressions:
                copied = field.copy()
                if isinstance(copied, exp.ColumnDef):
                    copied.set("constraints", [])
                fields.append(copied)
            return exp.DataType(
                this=exp.DataType.Type.STRUCT,
                expressions=fields,
                nested=False,
            )
        return exp.DataType(
            this=exp.DataType.Type.MAP,
            expressions=[
                exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
                exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
            ],
            nested=False,
        )

    return expression
