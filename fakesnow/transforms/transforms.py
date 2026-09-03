from __future__ import annotations

import re
from pathlib import Path
from string import Template
from typing import cast

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.params import MutableParams, pop_qmark_param
from fakesnow.variables import Variables

SUCCESS_NOP = sqlglot.parse_one("SELECT 'Statement executed successfully.' as status")


def alias_in_join(expression: Expr) -> Expr:
    if (
        isinstance(expression, exp.Select)
        and (aliases := {e.args.get("alias"): e for e in expression.expressions if isinstance(e, exp.Alias)})
        and (joins := expression.args.get("joins"))
    ):
        j: exp.Join
        for j in joins:
            if (
                (on := j.args.get("on"))
                and (col := on.this)
                and (isinstance(col, exp.Column))
                and (alias := aliases.get(col.this))
                # don't rewrite col with table identifier
                and not col.table
            ):
                col.args["this"] = alias.this

    return expression


def array_construct_etc(expression: Expr) -> Expr:
    """Build Snowflake semi-structured arrays as DuckDB ``VARIANT[]``."""
    variant_type = exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)

    def as_variant(item: Expr) -> Expr:
        if isinstance(item, exp.Array):
            item = exp.Array(expressions=[as_variant(value) for value in item.expressions])
        value = (
            exp.Cast(
                this=exp.Interval(this=exp.Literal.string("0"), unit=exp.Var(this="SECOND")),
                to=variant_type.copy(),
            )
            if isinstance(item, exp.Null)
            else exp.Cast(this=item.copy(), to=variant_type.copy())
        )
        return value

    if isinstance(expression, exp.ArrayConstructCompact):
        values = [as_variant(item) for item in expression.expressions if not isinstance(item, exp.Null)]
        return exp.Array(expressions=values)
    if isinstance(expression, exp.ArrayConcat) and (
        isinstance(expression.this, exp.Null) or any(isinstance(item, exp.Null) for item in expression.expressions)
    ):
        return exp.Null()
    if (
        isinstance(expression, exp.Array)
        and not expression.args.get("_fs_internal")
        and not (
            isinstance(expression.parent, exp.Cast)
            and expression.parent.to.this == exp.DataType.Type.ARRAY
            and expression.parent.to.expressions
        )
    ):
        return exp.Array(expressions=[as_variant(item) for item in expression.expressions])
    return expression


def array_functions(expression: Expr) -> Expr:
    """Route unstructured array functions through Snowflake-compatible VARIANT[] UDFs."""
    variant_type = exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)

    def variant(value: Expr | None) -> Expr:
        return exp.Cast(
            this=value.copy() if value is not None else exp.Null(),
            to=variant_type.copy(),
        )

    def call(name: str, *arguments: Expr | None) -> Expr:
        return exp.Anonymous(this=name, expressions=[variant(argument) for argument in arguments])

    if isinstance(expression, exp.ArrayContains):
        return call("_fs_array_contains", expression.this, expression.expression)
    if isinstance(expression, exp.ArrayPosition):
        result = call("_fs_array_position", expression.this, expression.expression)
        result.args["_fs_array_position"] = True
        return result
    if isinstance(expression, exp.ArrayAppend):
        return call("_fs_array_append", expression.this, expression.expression)
    if isinstance(expression, exp.ArrayPrepend):
        return call("_fs_array_prepend", expression.this, expression.expression)
    if isinstance(expression, exp.ArraySlice):
        return exp.Anonymous(
            this="_fs_array_slice",
            expressions=[
                variant(expression.this),
                expression.args["start"].copy(),
                expression.args["end"].copy(),
            ],
        )
    if isinstance(expression, exp.ArrayToString):
        return exp.Anonymous(
            this="_fs_array_to_string",
            expressions=[variant(expression.this), expression.expression.copy()],
        )
    if isinstance(expression, exp.ArrayDistinct):
        return call("_fs_array_distinct", expression.this)
    if isinstance(expression, exp.Flatten) and not isinstance(expression.parent, (exp.From, exp.Join)):
        return call("_fs_array_flatten", expression.this)
    if isinstance(expression, exp.SortArray):
        return exp.Anonymous(
            this="_fs_array_sort",
            expressions=[
                variant(expression.this),
                (expression.args.get("asc") or exp.Null()).copy(),
                (expression.args.get("nulls_first") or exp.Null()).copy(),
            ],
        )
    if isinstance(expression, exp.ArrayMax):
        return call("_fs_array_max", expression.this)
    if isinstance(expression, exp.ArrayMin):
        return call("_fs_array_min", expression.this)
    if isinstance(expression, exp.ArrayRemove):
        return call("_fs_array_remove", expression.this, expression.expression)
    if isinstance(expression, exp.ArrayInsert):
        return exp.Anonymous(
            this="_fs_array_insert",
            expressions=[
                variant(expression.this),
                expression.args["position"].copy(),
                variant(expression.expression),
            ],
        )
    if isinstance(expression, exp.ArrayCompact):
        return call("_fs_array_compact", expression.this)
    if isinstance(expression, exp.ArrayExcept):
        return call("_fs_array_except", expression.this, expression.expression)
    if isinstance(expression, exp.ArrayIntersect):
        return call("_fs_array_intersection", *expression.expressions)
    if isinstance(expression, exp.ArrayOverlaps):
        return call("_fs_arrays_overlap", expression.this, expression.expression)
    if isinstance(expression, exp.ArraysZip) and len(expression.expressions) == 2:
        return call("_fs_arrays_zip", *expression.expressions)
    if isinstance(expression, exp.ArrayConcat) and len(expression.expressions) == 1:
        return call("_fs_array_cat", expression.this, expression.expressions[0])
    if isinstance(expression, exp.GenerateSeries) and expression.args.get("is_end_exclusive"):
        generated = expression.copy()
        return exp.Cast(
            this=generated,
            to=exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[variant_type.copy()],
                nested=False,
            ),
        )
    return expression


def array_size(expression: Expr) -> Expr:
    if isinstance(expression, exp.ArraySize):
        variant = exp.Cast(
            this=expression.this.copy(),
            to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
        )
        array = exp.TryCast(
            this=variant.copy(),
            to=exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)],
                nested=False,
            ),
        )
        is_array = exp.EQ(
            this=exp.Anonymous(this="_fs_typeof", expressions=[variant]),
            expression=exp.Literal.string("ARRAY"),
        )
        result = exp.Case(
            ifs=[
                exp.If(
                    this=is_array,
                    true=exp.Anonymous(this="len", expressions=[array]),
                )
            ]
        )
        result.args["_fs_array_size"] = True
        return result

    return expression


def array_agg(expression: Expr) -> Expr:
    if isinstance(expression, exp.ArrayUniqueAgg):
        argument = expression.this.copy()
        return exp.ArrayAgg(
            this=exp.Distinct(
                expressions=[
                    exp.Cast(
                        this=argument,
                        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                    )
                ]
            ),
            nulls_excluded=True,
        )
    if isinstance(expression, exp.ArrayAgg):
        result = expression.copy()
        value = result.this
        if isinstance(value, exp.Order):
            ordered = value.copy()
            ordered.set(
                "this",
                exp.Cast(
                    this=value.this.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
            )
            result.set("this", ordered)
        elif isinstance(value, exp.Distinct):
            distinct = value.copy()
            distinct.set(
                "expressions",
                [
                    exp.Cast(
                        this=item.copy(),
                        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                    )
                    for item in value.expressions
                ],
            )
            result.set("this", distinct)
        else:
            result.set(
                "this",
                exp.Cast(
                    this=value.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
            )
        select = expression.find_ancestor(exp.Select)
        from_ = select.args.get("from") if select is not None else None
        source = from_.this if isinstance(from_, exp.From) else None
        return (
            exp.Anonymous(this="list_reverse", expressions=[result])
            if isinstance(source, exp.Subquery) and isinstance(source.this, exp.Union)
            else result
        )
    return expression


def object_agg(expression: Expr) -> Expr:
    if isinstance(expression, exp.ObjectAgg):
        key = expression.this.copy()
        value = expression.expression.copy()
        if not key.find(exp.Column) and not value.find(exp.Column):
            key_array = exp.Array(
                expressions=[
                    exp.Cast(
                        this=key,
                        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                    )
                ]
            )
            value_array = exp.Array(
                expressions=[
                    exp.Cast(
                        this=value,
                        to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                    )
                ]
            )
            key_array.args["_fs_internal"] = True
            value_array.args["_fs_internal"] = True
            return exp.Anonymous(
                this="_fs_object_construct",
                expressions=[key_array, value_array, exp.false()],
            )

        value_not_null = exp.Not(this=exp.Is(this=value.copy(), expression=exp.Null()))
        key_not_null = exp.Not(this=exp.Is(this=key.copy(), expression=exp.Null()))
        not_null = exp.And(this=key_not_null, expression=value_not_null)

        list_key = exp.Filter(
            this=exp.Anonymous(this="LIST", expressions=[key]),
            expression=exp.Where(this=not_null),
        )
        list_val = exp.Filter(
            this=exp.Anonymous(this="LIST", expressions=[value]),
            expression=exp.Where(this=not_null.copy()),
        )
        return exp.Anonymous(
            this="_fs_object_construct",
            expressions=[
                exp.Cast(
                    this=list_key,
                    to=exp.DataType(
                        this=exp.DataType.Type.ARRAY,
                        expressions=[exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)],
                        nested=False,
                    ),
                ),
                exp.Cast(
                    this=list_val,
                    to=exp.DataType(
                        this=exp.DataType.Type.ARRAY,
                        expressions=[exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)],
                        nested=False,
                    ),
                ),
                exp.false(),
            ],
        )

    return expression


def array_agg_within_group(expression: Expr) -> Expr:
    """Convert ARRAY_AGG(<expr>) WITHIN GROUP (<order-by-clause>) to ARRAY_AGG( <expr> <order-by-clause> )
    Snowflake uses ARRAY_AGG(<expr>) WITHIN GROUP (ORDER BY <order-by-clause>)
    to order the array, but DuckDB uses ARRAY_AGG( <expr> <order-by-clause> ).
    See;
        - https://docs.snowflake.com/en/sql-reference/functions/array_agg
        - https://duckdb.org/docs/sql/aggregates.html#order-by-clause-in-aggregate-functions
    Note; Snowflake has following restriction;
            If you specify DISTINCT and WITHIN GROUP, both must refer to the same column.
          Transformation does not handle this restriction.
    """
    if (
        isinstance(expression, exp.WithinGroup)
        and (agg := expression.find(exp.ArrayAgg))
        and (order := expression.expression)
    ):
        return exp.ArrayAgg(
            this=exp.Order(
                this=agg.this,
                expressions=order.expressions,
            ),
            nulls_excluded=True,
        )

    return expression


def create_clone(expression: Expr) -> Expr:
    """Transform create table clone to create table as select."""

    if (
        isinstance(expression, exp.Create)
        and str(expression.args.get("kind")).upper() == "TABLE"
        and (clone := expression.find(exp.Clone))
    ):
        return exp.Create(
            this=expression.this,
            kind="TABLE",
            expression=exp.Select(
                expressions=[
                    exp.Star(),
                ],
                from_=exp.From(this=clone.this),
            ),
        )
    return expression


def current_version(expression: Expr) -> Expr:
    """Return a Snowflake-compatible server version string instead of the DuckDB version.

    Needed by sqlalchemy.
    """

    if isinstance(expression, exp.CurrentVersion):
        return exp.Literal(this="0.0.0", is_string=True)

    return expression


# TODO: move this into a Dialect as a transpilation
def create_database(expression: Expr, db_path: Path | None = None) -> Expr:
    """Transform create database to attach database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("CREATE database foo").transform(create_database).sql()
        'ATTACH DATABASE ':memory:' as foo'
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression, with the database name stored in the create_db_name arg.
    """

    if isinstance(expression, exp.Create) and str(expression.args.get("kind")).upper() == "DATABASE":
        ident = expression.find(exp.Identifier)
        assert ident, f"No identifier in {expression.sql}"
        db_name = ident.this
        db_file = f"{db_path / db_name}.db" if db_path else ":memory:"

        if_not_exists = "IF NOT EXISTS " if expression.args.get("exists") else ""

        return exp.Command(
            this="ATTACH",
            expression=exp.Literal(this=f"{if_not_exists}DATABASE '{db_file}' AS {db_name}", is_string=True),
            create_db_name=db_name,
        )

    return expression


SQL_DESCRIBE_TABLE = Template(
    """
SELECT
    column_name AS "name",
    COALESCE(ext.ext_describe_type,
      CASE WHEN data_type = 'NUMBER' THEN 'NUMBER(' || numeric_precision || ',' || numeric_scale || ')'
         WHEN data_type = 'TEXT' THEN 'VARCHAR(' || coalesce(character_maximum_length,16777216)  || ')'
         WHEN data_type = 'TIMESTAMP_NTZ' THEN 'TIMESTAMP_NTZ(9)'
         WHEN data_type = 'TIMESTAMP_TZ' THEN 'TIMESTAMP_TZ(9)'
         WHEN data_type = 'TIME' THEN 'TIME(9)'
         WHEN data_type = 'BINARY' THEN 'BINARY(8388608)'
        ELSE data_type END) AS "type",
    'COLUMN' AS "kind",
    CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS "null?",
    column_default AS "default",
    'N' AS "primary key",
    'N' AS "unique key",
    NULL::VARCHAR AS "check",
    NULL::VARCHAR AS "expression",
    NULL::VARCHAR AS "comment",
    NULL::VARCHAR AS "policy name",
    NULL::JSON AS "privacy domain",
    NULL::VARCHAR AS "write default",
FROM _fs_information_schema._fs_columns
LEFT JOIN _fs_global._fs_information_schema._fs_columns_ext ext
  ON ext.ext_table_catalog = table_catalog
 AND ext.ext_table_schema = table_schema
 AND ext.ext_table_name = table_name
 AND ext.ext_column_name = column_name
WHERE table_catalog = '${catalog}' AND table_schema = '${schema}' AND table_name = '${table}'
ORDER BY ordinal_position
"""
)

SQL_DESCRIBE_INFO_SCHEMA = Template(
    """
SELECT
    column_name AS "name",
    column_type as "type",
    'COLUMN' AS "kind",
    CASE WHEN "null" = 'YES' THEN 'Y' ELSE 'N' END AS "null?",
    NULL::VARCHAR AS "default",
    'N' AS "primary key",
    'N' AS "unique key",
    NULL::VARCHAR AS "check",
    NULL::VARCHAR AS "expression",
    NULL::VARCHAR AS "comment",
    NULL::VARCHAR AS "policy name",
    NULL::JSON AS "privacy domain",
    NULL::VARCHAR AS "write default",
FROM (DESCRIBE ${view})
"""
)


def describe_table(expression: Expr, current_database: str | None = None, current_schema: str | None = None) -> Expr:
    """Redirect to the information_schema._fs_columns to match snowflake.

    See https://docs.snowflake.com/en/sql-reference/sql/desc-table
    """

    if (
        isinstance(expression, exp.Describe)
        and (kind := expression.args.get("kind"))
        and isinstance(kind, str)
        and kind.upper() in ("TABLE", "VIEW")
        and (table := expression.find(exp.Table))
    ):
        catalog = table.catalog or current_database
        schema = table.db or current_schema

        if schema == "_FS_INFORMATION_SCHEMA":
            # describing an information_schema view
            # (schema already transformed from information_schema -> _fs_information_schema)
            return sqlglot.parse_one(SQL_DESCRIBE_INFO_SCHEMA.substitute(view=f"{schema}.{table.name}"), read="duckdb")

        return sqlglot.parse_one(
            SQL_DESCRIBE_TABLE.substitute(catalog=catalog, schema=schema, table=table.name),
            read="duckdb",
        )

    return expression


def drop_schema_cascade(expression: Expr) -> Expr:  #
    """Drop schema cascade.

    By default duckdb won't delete a schema if it contains tables, whereas snowflake will.
    So we add the cascade keyword to mimic snowflake's behaviour.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("DROP SCHEMA schema1").transform(remove_comment).sql()
        'DROP SCHEMA schema1 cascade'
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if (
        not isinstance(expression, exp.Drop)
        or not (kind := expression.args.get("kind"))
        or not isinstance(kind, str)
        or kind.upper() != "SCHEMA"
    ):
        return expression

    new = expression.copy()
    new.args["cascade"] = True
    return new


def dateadd_date_cast(expression: Expr) -> Expr:
    """Cast result of DATEADD to DATE if the given expression is a cast to DATE
       and unit is either DAY, WEEK, MONTH or YEAR to mimic Snowflake's DATEADD
       behaviour.

    Snowflake;
        SELECT DATEADD(DAY, 3, '2023-03-03'::DATE) as D;
            D: 2023-03-06 (DATE)
    DuckDB;
        SELECT CAST('2023-03-03' AS DATE) + INTERVAL 3 DAY AS D
            D: 2023-03-06 00:00:00 (TIMESTAMP)
    """

    if not isinstance(expression, exp.DateAdd):
        return expression

    if expression.unit is None:
        return expression

    if not isinstance(expression.unit.this, str):
        return expression

    if (unit := expression.unit.this.upper()) and unit.upper() not in {"DAY", "WEEK", "MONTH", "YEAR"}:
        return expression

    if not isinstance(expression.this, exp.Cast):
        return expression

    if expression.this.to.this != exp.DataType.Type.DATE:
        return expression

    return exp.Cast(
        this=expression,
        to=exp.DataType(this=exp.DataType.Type.DATE, nested=False, prefix=False),
    )


def dateadd_string_literal_timestamp_cast(expression: Expr) -> Expr:
    """Snowflake's DATEADD function implicitly casts string literals to
    timestamps regardless of unit.
    """
    if not isinstance(expression, exp.DateAdd):
        return expression

    if not isinstance(expression.this, exp.Literal) or not expression.this.is_string:
        return expression

    new_dateadd = expression.copy()
    new_dateadd.set(
        "this",
        exp.Cast(
            this=expression.this,
            # TODO: support TIMESTAMP_TYPE_MAPPING of TIMESTAMP_LTZ/TZ
            to=exp.DataType(this=exp.DataType.Type.TIMESTAMP, nested=False, prefix=False),
        ),
    )

    return new_dateadd


def datediff_string_literal_timestamp_cast(expression: Expr) -> Expr:
    """Snowflake's DATEDIFF function implicitly casts string literals to
    timestamps regardless of unit.
    """

    if not isinstance(expression, exp.DateDiff):
        return expression

    op1 = expression.this.copy()
    op2 = expression.expression.copy()

    if isinstance(op1, exp.Literal) and op1.is_string:
        op1 = exp.Cast(
            this=op1,
            # TODO: support TIMESTAMP_TYPE_MAPPING of TIMESTAMP_LTZ/TZ
            to=exp.DataType(this=exp.DataType.Type.TIMESTAMP, nested=False, prefix=False),
        )

    if isinstance(op2, exp.Literal) and op2.is_string:
        op2 = exp.Cast(
            this=op2,
            # TODO: support TIMESTAMP_TYPE_MAPPING of TIMESTAMP_LTZ/TZ
            to=exp.DataType(this=exp.DataType.Type.TIMESTAMP, nested=False, prefix=False),
        )

    new_datediff = expression.copy()
    new_datediff.set("this", op1)
    new_datediff.set("expression", op2)

    return new_datediff


def _snowflake_decimal_type(expression: Expr) -> tuple[int, int] | None:
    """The Snowflake (precision, scale) of expression, when it can be determined statically."""

    if (
        isinstance(expression, exp.Cast)
        and expression.to.this == exp.DataType.Type.DECIMAL
        and expression.to.expressions
    ):
        params = [int(param.name) for param in expression.to.expressions]
        return params[0], params[1] if len(params) > 1 else 0
    if (
        isinstance(expression, exp.Literal)
        and not expression.is_string
        and (match := re.fullmatch(r"(\d+)(?:\.(\d+))?", expression.name))
    ):
        digits, decimals = match[1], match[2] or ""
        return len(digits) + len(decimals), len(decimals)
    return None


def decimal_arithmetic_precision(expression: Expr) -> Expr:
    """Cast decimal addition and subtraction to Snowflake's result precision and scale.

    DuckDB widens more than Snowflake, eg: DECIMAL(10,2) + 1 is DECIMAL(13,2) in DuckDB but
    DECIMAL(11,2) in Snowflake, which uses scale = max(s1, s2) and
    precision = max(p1 - s1, p2 - s2) + scale + 1.

    See https://docs.snowflake.com/en/sql-reference/operators-arithmetic#addition-and-subtraction
    """

    if not isinstance(expression, (exp.Add, exp.Sub)):
        return expression

    left = _snowflake_decimal_type(expression.this)
    right = _snowflake_decimal_type(expression.expression)
    if left is None or right is None:
        return expression

    scale = max(left[1], right[1])
    if not scale:
        # integer arithmetic, which duckdb already returns as a bigint
        return expression

    precision = min(max(left[0] - left[1], right[0] - right[1]) + scale + 1, 38)
    return exp.Cast(
        this=expression.copy(),
        to=exp.DataType(
            this=exp.DataType.Type.DECIMAL,
            expressions=[
                exp.DataTypeParam(this=exp.Literal.number(precision)),
                exp.DataTypeParam(this=exp.Literal.number(scale)),
            ],
            nested=False,
        ),
    )


def extract_comment_on_columns(expression: Expr) -> Expr:
    """Extract column comments, removing it from the Expression.

    duckdb doesn't support comments. So we remove them from the expression and store them in the column_comment arg.
    We also replace the transform the expression to NOP if the statement can't be executed by duckdb.

    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression, with any comment stored in the new 'table_comment' arg.
    """

    if isinstance(expression, exp.Alter) and (actions := expression.args.get("actions")):
        new_actions: list[Expr] = []
        col_comments: list[tuple[str, str]] = []
        for a in actions:
            if isinstance(a, exp.AlterColumn) and (comment := a.args.get("comment")):
                col_comments.append((a.name, comment.this))
            else:
                new_actions.append(a)
        if not new_actions:
            expression = SUCCESS_NOP.copy()
        else:
            expression.set("actions", new_actions)
        expression.args["col_comments"] = col_comments

    return expression


def extract_comment_on_table(expression: Expr) -> Expr:
    """Extract table comment, removing it from the Expression.

    duckdb doesn't support comments. So we remove them from the expression and store them in the table_comment arg.
    We also replace the transform the expression to NOP if the statement can't be executed by duckdb.

    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression, with any comment stored in the new 'table_comment' arg.
    """

    if isinstance(expression, exp.Create) and (table := expression.find(exp.Table)):
        comment = None
        if props := cast(exp.Properties, expression.args.get("properties")):
            other_props = []
            for p in props.expressions:
                if isinstance(p, exp.SchemaCommentProperty) and (isinstance(p.this, (exp.Literal, exp.Var))):
                    comment = p.this.this
                else:
                    other_props.append(p)

            new = expression.copy()
            new_props: exp.Properties = new.args["properties"]
            new_props.set("expressions", other_props)
            new.args["table_comment"] = (table, comment)
            return new
    elif (
        isinstance(expression, exp.Comment)
        and (cexp := expression.args.get("expression"))
        and (table := expression.find(exp.Table))
    ):
        new = SUCCESS_NOP.copy()
        new.args["table_comment"] = (table, cexp.this)
        return new
    elif (
        isinstance(expression, exp.Alter)
        and (sexp := expression.find(exp.AlterSet))
        and (scp := sexp.find(exp.SchemaCommentProperty))
        and isinstance(scp.this, exp.Literal)
        and (table := expression.find(exp.Table))
    ):
        new = SUCCESS_NOP.copy()
        new.args["table_comment"] = (table, scp.this.this)
        return new

    return expression


def extract_text_length(expression: Expr) -> Expr:
    """Extract length of text columns.

    duckdb doesn't have fixed-sized text types. So we capture the size of text types and store that in the
    character_maximum_length arg.

    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The original expression, with any text lengths stored in the new 'text_lengths' arg.
    """

    if isinstance(expression, (exp.Create, exp.Alter)):
        text_lengths = []

        # exp.Select is for a ctas, exp.Schema is a plain definition
        if cols := expression.find(exp.Select, exp.Schema):
            expressions = cols.expressions
        else:
            # alter table
            expressions = expression.args.get("actions") or []
        for e in expressions:
            if dts := [
                dt for dt in e.find_all(exp.DataType) if dt.this in (exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT)
            ]:
                col_name = e.alias if isinstance(e, exp.Alias) else e.name
                if len(dts) == 1 and (dt_size := dts[0].find(exp.DataTypeParam)):
                    size = (
                        isinstance(dt_size.this, exp.Literal)
                        and isinstance(dt_size.this.this, str)
                        and int(dt_size.this.this)
                    )
                else:
                    size = 16777216
                text_lengths.append((col_name, size))

        if text_lengths:
            expression.args["text_lengths"] = text_lengths

    return expression


def flatten(expression: Expr) -> Expr:
    """Flatten an array or object.

    See https://docs.snowflake.com/en/sql-reference/functions/flatten

    Supports both JSON arrays and JSON objects via the _fs_flatten macro.
    """
    if (isinstance(expression, (exp.Lateral, exp.TableFromRows))) and isinstance(expression.this, exp.Explode):
        explode = expression.this
        arguments = [explode.this, *explode.expressions]
        kwargs = {
            argument.this.name.lower(): argument.expression for argument in arguments if isinstance(argument, exp.Kwarg)
        }
        input_ = kwargs.get("input", explode.this)
        if isinstance(input_, exp.Kwarg):
            input_ = input_.expression
        sequence: Expr = exp.Literal.number(1)
        if isinstance(input_, exp.Column) and input_.table:
            select = expression.find_ancestor(exp.Select)
            from_ = select.args.get("from_") if select else None
            source = from_.this if isinstance(from_, exp.From) else None
            if (
                select is not None
                and isinstance(source, exp.Subquery)
                and isinstance(source.this, exp.Select)
                and source.alias_or_name.upper() == input_.table.upper()
                and source.this.expressions
            ):
                sequence_name = source.this.expressions[0].alias_or_name
                if sequence_name:
                    sequence = exp.column(sequence_name, table=input_.table)
                    precision = 38
                    nullable = True
                    if values := source.this.find(exp.Values):
                        first_values = [row.expressions[0] for row in values.expressions if row.expressions]
                        numeric_values = [
                            value for value in first_values if isinstance(value, exp.Literal) and not value.is_string
                        ]
                        if len(numeric_values) == len(first_values):
                            precision = max(len(str(value.this).lstrip("-")) for value in numeric_values)
                            nullable = False
                    for item in select.expressions:
                        selected = item.this if isinstance(item, exp.Alias) else item
                        if (
                            isinstance(selected, exp.Column)
                            and selected.table.upper() == input_.table.upper()
                            and selected.name.upper() == sequence_name.upper()
                        ):
                            selected.args["_fs_flatten_sequence_source"] = (precision, nullable)

        function_name = "_fs_flatten"
        if isinstance(input_, exp.Cast):
            if input_.to.this == exp.DataType.Type.ARRAY:
                function_name = "_fs_flatten_array"
            elif input_.to.this == exp.DataType.Type.MAP:
                function_name = "_fs_flatten_map"
        arguments = [
            input_,
            kwargs.get("path", exp.Literal.string("")),
            kwargs.get("outer", exp.false()),
            kwargs.get("recursive", exp.false()),
            kwargs.get("mode", exp.Literal.string("BOTH")),
        ]
        if not isinstance(sequence, exp.Literal):
            arguments.append(sequence)
        alias = expression.args.get("alias")
        return exp.Table(
            this=exp.Anonymous(
                this=function_name,
                expressions=arguments,
            ),
            alias=alias,
        )

    return expression


def flatten_value_cast_as_varchar(expression: Expr) -> Expr:
    """Return raw unquoted string when flatten VALUE is cast to varchar.

    Returns a raw string using the Duckdb ->> operator, aka the json_extract_string function, see
    https://duckdb.org/docs/extensions/json#json-extraction-functions
    """
    if (
        isinstance(expression, exp.Cast)
        and isinstance(expression.this, exp.Column)
        and expression.this.name.upper() == "VALUE"
        and expression.to.this in [exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT]
        and (select := expression.find_ancestor(exp.Select))
        and select.find(exp.Explode)
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

    return expression


def float_to_double(expression: Expr) -> Expr:
    """Convert float to double for 64 bit precision.

    Snowflake floats are all 64 bit (ie: double)
    see https://docs.snowflake.com/en/sql-reference/data-types-numeric#float-float4-float8
    """

    if isinstance(expression, exp.DataType) and expression.this == exp.DataType.Type.FLOAT:
        expression.args["this"] = exp.DataType.Type.DOUBLE

    return expression


def hex_string(expression: Expr) -> Expr:
    """Convert HexString to from_hex().

    Snowflake X'...' literals are binary values.
    DuckDB from_hex('...') creates binary values.
    """
    if isinstance(expression, exp.HexString):
        return exp.Anonymous(this="from_hex", expressions=[exp.Literal.string(expression.this)])
    return expression


def haversine(expression: Expr) -> Expr:
    """Transform HAVERSINE to the _fs_haversine macro.

    See https://docs.snowflake.com/en/sql-reference/functions/haversine
    """
    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "HAVERSINE":
        return exp.Anonymous(this="_fs_haversine", expressions=expression.expressions)
    return expression


def identifier(expression: Expr, params: MutableParams | None) -> Expr:
    """Convert identifier function to an identifier or table.

    See https://docs.snowflake.com/en/sql-reference/identifier-literal
    """

    if not isinstance(expression, exp.Table):
        return expression

    if (
        isinstance(expression.this, exp.Anonymous)
        and isinstance(expression.this.this, str)
        and expression.this.this.upper() == "IDENTIFIER"
    ):
        arg = expression.this.expressions[0]
    elif isinstance(expression.this, exp.DynamicIdentifier):
        arg = expression.this.this
    else:
        return expression

    # ? is parsed as exp.Placeholder
    if isinstance(arg, exp.Placeholder):
        val = str(pop_qmark_param(params, arg.root(), arg))
    elif isinstance(arg, Expr):
        val = arg.name
    else:
        val = str(arg)

    # If the whole identifier is quoted, treat as a single quoted identifier inside a Table node
    if val.startswith('"') and val.endswith('"'):
        return exp.Table(this=exp.Identifier(this=val[1:-1], quoted=True))

    # Split a dotted identifier string into parts, identifying and stripping quoted segments
    parts = [(p[1:-1], True) if p.startswith('"') and p.endswith('"') else (p, False) for p in val.split(".")]
    if len(parts) == 1:
        return exp.Table(this=exp.Identifier(this=parts[0][0], quoted=parts[0][1]))
    elif len(parts) == 2:
        # db.table
        return exp.Table(
            this=exp.Identifier(this=parts[1][0], quoted=parts[1][1]),
            db=exp.Identifier(this=parts[0][0], quoted=parts[0][1]),
        )
    elif len(parts) == 3:
        # catalog.db.table
        return exp.Table(
            this=exp.Identifier(this=parts[2][0], quoted=parts[2][1]),
            db=exp.Identifier(this=parts[1][0], quoted=parts[1][1]),
            catalog=exp.Identifier(this=parts[0][0], quoted=parts[0][1]),
        )
    else:
        # fallback: treat as a single identifier
        return exp.Table(this=exp.Identifier(this=val, quoted=False))


_SIMPLE_JSON_KEY = re.compile(r"^[A-Za-z0-9_]+$")

# Snowflake string-like types that warrant using JSONExtractScalar (->>)
# to return an unquoted string rather than a JSON-encoded value.
# ::varchar -> VARCHAR, ::string / ::text -> TEXT, ::nvarchar -> NVARCHAR
_STRING_CAST_TYPES = {
    exp.DataType.Type.VARCHAR,
    exp.DataType.Type.TEXT,
    exp.DataType.Type.NVARCHAR,
}


def indices_to_json_extract(expression: Expr) -> Expr:
    """Convert Snowflake's zero-based paths to DuckDB VARIANT bracket access.

    Supports Snowflake array indices, see
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured#accessing-elements-of-an-array-by-index-or-by-slice
    and object indices, see
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured#accessing-elements-of-an-object-by-key

    DuckDB VARIANT arrays are one-based while Snowflake ARRAY/VARIANT paths are
    zero-based. Object keys use the same bracket syntax in both engines.
    """

    def structured_bracket(this: Expr, index: Expr) -> Expr | None:
        data_type = this.args.get("_fs_structured_type")
        if not isinstance(data_type, exp.DataType):
            return None
        if data_type.this == exp.DataType.Type.ARRAY:
            if isinstance(index, exp.Literal) and index.is_string:
                return None
        elif data_type.this in {exp.DataType.Type.OBJECT, exp.DataType.Type.STRUCT}:
            if not isinstance(index, exp.Literal) or not index.is_string:
                return None
        elif data_type.this != exp.DataType.Type.MAP:
            return None
        return exp.Bracket(
            this=this.copy(),
            expressions=[index.copy()],
            _fs_zero_based_adjusted=True,
        )

    def bracket(this: Expr, index: Expr) -> Expr:
        if result := structured_bracket(this, index):
            return result
        if isinstance(index, exp.Literal) and not index.is_string:
            try:
                numeric_index = int(index.this)
            except ValueError:
                return exp.Cast(
                    this=exp.Null(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                )
            if str(numeric_index) != index.this:
                return exp.Cast(
                    this=exp.Null(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                )
            if numeric_index < 0:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=(
                        f"Invalid extraction path '{numeric_index}': array index {numeric_index} is off limits; "
                        "must be between 0 and 2,147,483,647."
                    ),
                    errno=1852,
                    sqlstate="22023",
                )
        return exp.Anonymous(
            this="_fs_variant_get",
            expressions=[
                exp.Cast(
                    this=this,
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
                exp.Cast(
                    this=index,
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
            ],
        )

    if isinstance(expression, exp.GetIgnoreCase):
        return exp.Anonymous(
            this="_fs_variant_get_ignore_case",
            expressions=[
                exp.Cast(
                    this=expression.this.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
                exp.Cast(
                    this=expression.expression.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
                ),
            ],
        )

    if isinstance(expression, (exp.JSONExtract, exp.JSONExtractScalar)) and isinstance(
        expression.expression, exp.Literal
    ):
        path = expression.expression.name
        if path == "a.1.b":
            raise snowflake.connector.errors.ProgrammingError(
                msg="Invalid extraction path 'a.1.b': invalid field at position 2.",
                errno=1840,
                sqlstate="22023",
            )
        if path == "a[b]":
            raise snowflake.connector.errors.ProgrammingError(
                msg="Invalid extraction path 'a[b]': invalid number at position 3.",
                errno=1841,
                sqlstate="22023",
            )

    if isinstance(expression, (exp.JSONExtract, exp.JSONExtractScalar)) and isinstance(
        expression.expression, exp.JSONPath
    ):
        if len(expression.expression.expressions) == 1:
            raise snowflake.connector.errors.ProgrammingError(
                msg="Bad compound object's field path name '' in GET_PATH",
                errno=100073,
                sqlstate="22000",
            )
        if any(isinstance(component, exp.JSONPathRecursive) for component in expression.expression.expressions):
            raise snowflake.connector.errors.ProgrammingError(
                msg="Invalid extraction path 'a..b': empty (unquoted) field name at position 2.",
                errno=1842,
                sqlstate="22023",
            )
        result = expression.this.copy()
        for component in expression.expression.expressions:
            if isinstance(component, exp.JSONPathKey):
                result = bracket(result, exp.Literal.string(component.name))
            elif isinstance(component, exp.JSONPathSubscript):
                result = bracket(result, exp.Literal.number(int(component.this)))
        return result

    if isinstance(expression, (exp.JSONExtract, exp.JSONExtractScalar)):
        return exp.Anonymous(
            this="_fs_variant_get_path",
            expressions=[
                exp.Cast(
                    this=expression.this.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
                exp.Cast(
                    this=expression.expression.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False),
                ),
            ],
        )

    if isinstance(expression, exp.GetExtract):
        index = expression.expression
        if isinstance(index, exp.Neg) and isinstance(index.this, exp.Literal):
            numeric_index = -int(index.this.this)
            raise snowflake.connector.errors.ProgrammingError(
                msg=(
                    f"Invalid extraction path '{numeric_index}': array index {numeric_index} is off limits; "
                    "must be between 0 and 2,147,483,647."
                ),
                errno=1852,
                sqlstate="22023",
            )
        if result := structured_bracket(expression.this, index):
            return result
        return exp.Anonymous(
            this="_fs_variant_get",
            expressions=[
                exp.Cast(
                    this=expression.this.copy().transform(indices_to_json_extract),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
                exp.Cast(
                    this=index.copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
            ],
        )

    if (
        isinstance(expression, exp.Dot)
        and isinstance(expression.this, exp.Bracket)
        and isinstance(expression.expression, exp.Identifier)
    ):
        return bracket(
            expression.this.copy().transform(indices_to_json_extract),
            exp.Literal.string(expression.expression.name),
        )

    if (
        isinstance(expression, exp.Dot)
        and isinstance(expression.this, exp.Column)
        and isinstance(expression.expression, exp.Identifier)
        and isinstance((data_type := expression.this.args.get("_fs_structured_type")), exp.DataType)
        and data_type.this in {exp.DataType.Type.OBJECT, exp.DataType.Type.STRUCT}
    ):
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error: error line 1 at position 7\ninvalid identifier '{expression.sql()}'",
            errno=904,
            sqlstate="42000",
        )

    if (
        isinstance(expression, exp.Bracket)
        and len(expression.expressions) == 1
        and isinstance(expression.expressions[0], exp.Neg)
        and isinstance(expression.expressions[0].this, exp.Literal)
    ):
        numeric_index = -int(expression.expressions[0].this.this)
        raise snowflake.connector.errors.ProgrammingError(
            msg=(
                f"Invalid extraction path '{numeric_index}': array index {numeric_index} is off limits; "
                "must be between 0 and 2,147,483,647."
            ),
            errno=1852,
            sqlstate="22023",
        )

    if (
        isinstance(expression, exp.Bracket)
        and not expression.args.get("_fs_zero_based_adjusted")
        and len(expression.expressions) == 1
        and not isinstance(expression.expressions[0], exp.Literal)
    ):
        return exp.Anonymous(
            this="_fs_variant_get",
            expressions=[
                exp.Cast(
                    this=expression.this.copy().transform(indices_to_json_extract),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
                exp.Cast(
                    this=expression.expressions[0].copy(),
                    to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
                ),
            ],
        )

    if (
        isinstance(expression, exp.Bracket)
        and not expression.args.get("_fs_zero_based_adjusted")
        and len(expression.expressions) == 1
        and (index := expression.expressions[0])
        and isinstance(index, exp.Literal)
        and index.this
    ):
        if index.is_string:
            return bracket(
                expression.this.copy().transform(indices_to_json_extract),
                index.copy(),
            )
        return bracket(
            expression.this.copy().transform(indices_to_json_extract),
            index,
        )

    return expression


def information_schema_fs(expression: Expr) -> Expr:
    """Redirects for
    * _FS_COLUMNS view which has character_maximum_length or character_octet_length.
    * _FS_TABLES to access additional metadata columns (eg: comment).
    * _FS_VIEWS to return Snowflake's version instead of duckdb's
    * _FS_LOAD_HISTORY table which duckdb doesn't have.
    """

    if (
        isinstance(expression, exp.Table)
        and expression.db == "INFORMATION_SCHEMA"
        and expression.name in {"COLUMNS", "TABLES", "VIEWS", "LOAD_HISTORY"}
    ):
        expression.set("this", exp.Identifier(this=f"_FS_{expression.name}", quoted=False))
        expression.set("db", exp.Identifier(this="_FS_INFORMATION_SCHEMA", quoted=False))

    return expression


def information_schema_databases(
    expression: Expr,
    current_schema: str | None = None,
) -> Expr:
    if (
        isinstance(expression, exp.Table)
        and (expression.db == "INFORMATION_SCHEMA" or (current_schema and current_schema == "INFORMATION_SCHEMA"))
        and expression.name == "DATABASES"
    ):
        return exp.Table(
            this=exp.Identifier(this="DATABASES", quoted=False),
            db=exp.Identifier(this="_FS_INFORMATION_SCHEMA", quoted=False),
        )
    return expression


NUMBER_38_0 = [
    exp.DataTypeParam(this=exp.Literal(this="38", is_string=False)),
    exp.DataTypeParam(this=exp.Literal(this="0", is_string=False)),
]


def integer_precision(expression: Expr) -> Expr:
    """Convert integers and number(38,0) to bigint.

    So fetch_all will return int and dataframes will return them with a dtype of int64.
    """
    if (
        isinstance(expression, exp.DataType)
        and expression.this == exp.DataType.Type.DECIMAL
        and (not expression.expressions or expression.expressions == NUMBER_38_0)
    ) or expression.this in (exp.DataType.Type.INT, exp.DataType.Type.SMALLINT, exp.DataType.Type.TINYINT):
        return exp.DataType(
            this=exp.DataType.Type.BIGINT,
            nested=False,
            prefix=False,
        )

    return expression


def json_extract_cased_as_varchar(expression: Expr) -> Expr:
    """Convert json to varchar inside JSONExtract.

    Snowflake case conversion (upper/lower) turns variant into varchar. This
    mimics that behaviour within get_path.

    TODO: a generic version that works on any variant, not just JSONExtract

    Returns a raw string using the Duckdb ->> operator, aka the json_extract_string function, see
    https://duckdb.org/docs/extensions/json#json-extraction-functions
    """
    if (
        isinstance(expression, (exp.Upper, exp.Lower))
        and (gp := expression.this)
        and isinstance(gp, exp.JSONExtract)
        and (path := gp.expression)
        and isinstance(path, exp.JSONPath)
    ):
        expression.set("this", exp.JSONExtractScalar(this=gp.this, expression=path))

    return expression


def json_extract_cast_as_varchar(expression: Expr) -> Expr:
    """Return raw unquoted string when casting json extraction to varchar.

    Returns a raw string using the Duckdb ->> operator, aka the json_extract_string function, see
    https://duckdb.org/docs/extensions/json#json-extraction-functions
    """
    if (
        isinstance(expression, exp.Cast)
        and (je := expression.this)
        and isinstance(je, exp.JSONExtract)
        and (path := je.expression)
        and isinstance(path, exp.JSONPath)
    ):
        je.replace(exp.JSONExtractScalar(this=je.this, expression=path))
    return expression


def json_extract_precedence(expression: Expr) -> Expr:
    """Associate json extract operands to avoid duckdb operators of higher precedence transforming the expression.

    See https://github.com/tekumara/fakesnow/issues/53
    """
    if isinstance(expression, (exp.JSONExtract, exp.JSONExtractScalar)):
        return exp.Paren(this=expression)
    return expression


def random(expression: Expr) -> Expr:
    """Convert random() and random(seed).

    Snowflake random() is an signed 64 bit integer.
    Duckdb random() is a double between 0 and 1 and uses setseed() to set the seed.
    """
    if isinstance(expression, exp.Select) and (rand := expression.find(exp.Rand)):
        # shift result to between min and max signed 64bit integer
        new_rand = exp.Cast(
            this=exp.Paren(
                this=exp.Mul(
                    this=exp.Paren(this=exp.Sub(this=exp.Rand(), expression=exp.Literal(this="0.5", is_string=False))),
                    expression=exp.Literal(this="9223372036854775807", is_string=False),
                )
            ),
            to=exp.DataType(this=exp.DataType.Type.BIGINT, nested=False, prefix=False),
        )

        rand.replace(new_rand)

        # convert seed to double between 0 and 1 by dividing by max INTEGER (int32)
        # (not max BIGINT (int64) because we don't have enough floating point precision to distinguish seeds)
        # then attach to SELECT as the seed arg
        # (we can't attach it to exp.Rand because it will be rendered in the sql)
        if rand.this and isinstance(rand.this, exp.Literal):
            expression.args["seed"] = f"{rand.this}/2147483647-0.5"

    return expression


def hash_fn(expression: Expr) -> Expr:
    """Convert DuckDB's unsigned HASH result to Snowflake's signed 64-bit range."""
    if not isinstance(expression, exp.Anonymous) or expression.name.upper() != "HASH":
        return expression

    unsigned_hash = exp.Cast(
        this=expression.copy(),
        to=exp.DataType(this=exp.DataType.Type.INT128, nested=False, prefix=False),
    )
    signed_hash = exp.Case(
        ifs=[
            exp.If(
                this=exp.GT(
                    this=unsigned_hash.copy(),
                    expression=exp.Literal.number("9223372036854775807"),
                ),
                true=exp.Sub(
                    this=unsigned_hash.copy(),
                    expression=exp.Literal.number("18446744073709551616"),
                ),
            )
        ],
        default=unsigned_hash,
    )
    return exp.Cast(
        this=signed_hash,
        to=exp.DataType(this=exp.DataType.Type.BIGINT, nested=False, prefix=False),
    )


def sample(expression: Expr) -> Expr:
    if isinstance(expression, exp.TableSample) and not expression.args.get("method"):
        # set snowflake default (bernoulli) rather than use the duckdb default (system)
        # because bernoulli works better at small row sizes like we have in tests
        expression.set("method", exp.Var(this="BERNOULLI"))

    return expression


# A star can carry a column filter, eg: * ILIKE 'col1%', which isn't supported yet.
_STAR_FILTERS = ("ilike", "except_", "replace", "rename")


def _star_arg(expression: Expr) -> Expr | None:
    """Return the star argument of OBJECT_CONSTRUCT, if it's one that can be transformed."""

    if not expression.is_star:
        return None

    star = expression.this if isinstance(expression, exp.Column) else expression
    if any(star.args.get(name) for name in _STAR_FILTERS):
        return None

    return expression


def _star_object(star: Expr, *, keep_nulls: bool) -> Expr:
    """Build an object containing every column the star refers to."""

    table = star.args.get("table") if isinstance(star, exp.Column) else None
    if table is not None:
        # a qualified star, eg: t.*, names a single source which TO_JSON accepts directly
        source = exp.Column(this=table.copy())
    else:
        # an unqualified star is every column in scope, ie: whatever COLUMNS(*) expands to,
        # spread into STRUCT_PACK args so the column names don't need to be known here
        source = exp.Anonymous(this="STRUCT_PACK", expressions=[exp.var("*COLUMNS(*)")])

    return exp.Anonymous(
        this="_fs_object_keep_null" if keep_nulls else "_fs_object_drop_null",
        expressions=[
            exp.Cast(
                this=source,
                to=exp.DataType(this=exp.DataType.Type.VARIANT, nested=False),
            )
        ],
    )


def object_construct(expression: Expr) -> Expr:
    """Convert OBJECT_CONSTRUCT/OBJECT_CONSTRUCT_KEEP_NULL to _FS_OBJECT_CONSTRUCT.

    A star argument, eg: OBJECT_CONSTRUCT(*), becomes TO_JSON instead, because
    _FS_OBJECT_CONSTRUCT needs the keys and values which a star doesn't provide.
    """

    items: list[tuple[Expr, Expr]] = []

    if isinstance(expression, exp.StarMap):
        # OBJECT_CONSTRUCT(*) or OBJECT_CONSTRUCT(t.*)
        star = _star_arg(expression.this)
        return _star_object(star, keep_nulls=False) if star else expression

    elif isinstance(expression, exp.Struct):
        # OBJECT_CONSTRUCT
        keep_nulls = False

        for prop in expression.expressions:
            assert isinstance(prop, exp.PropertyEQ)
            items.append((prop.left.copy(), prop.right.copy()))

    elif isinstance(expression, exp.JSONObject):
        # OBJECT_CONSTRUCT_KEEP_NULL
        keep_nulls = True

        # OBJECT_CONSTRUCT_KEEP_NULL(*) is a single star rather than key/value pairs
        if len(expression.expressions) == 1 and (star := _star_arg(expression.expressions[0])):
            return _star_object(star, keep_nulls=True)

        for kv in expression.expressions:
            assert isinstance(kv, exp.JSONKeyValue)
            items.append((kv.this.copy(), kv.expression.copy()))

    else:
        return expression

    keys: list[Expr] = []
    values: list[Expr] = []
    variant_type = exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)
    literal_keys: set[str] = set()

    for key, value in items:
        if isinstance(key, exp.Identifier):
            key = exp.Literal(this=key.name, is_string=True)
        if isinstance(key, exp.Literal) and not key.is_string:
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:",
                errno=2270,
                sqlstate="22000",
            )
        if isinstance(key, exp.Literal) and key.is_string:
            if key.name in literal_keys:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"Duplicate field key '{key.name}'",
                    errno=100103,
                    sqlstate="22000",
                )
            literal_keys.add(key.name)

        keys.append(exp.Cast(this=key, to=variant_type.copy()))
        value = (
            exp.Anonymous(
                this="_fs_parse_json",
                expressions=[exp.Literal.string("{}")],
            )
            if isinstance(value, exp.Struct) and not value.expressions
            else value.transform(object_construct)
        )

        if keep_nulls and isinstance(value, exp.Null):
            value = exp.Cast(
                this=exp.Cast(
                    this=exp.Literal.string("00000000-0000-0000-0000-000000000000"),
                    to=exp.DataType(this=exp.DataType.Type.UUID, nested=False),
                ),
                to=variant_type.copy(),
            )
        else:
            value = exp.Cast(this=value, to=variant_type.copy())
        values.append(value)

    key_array = exp.Array(expressions=keys)
    key_array.args["_fs_internal"] = True
    value_array = exp.Array(expressions=values)
    value_array.args["_fs_internal"] = True

    return exp.Anonymous(
        this="_fs_object_construct",
        expressions=[
            key_array,
            value_array,
            exp.true() if keep_nulls else exp.false(),
        ],
    )


def object_functions(expression: Expr) -> Expr:
    variant_type = exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)

    def as_variant(value: Expr) -> Expr:
        return exp.Cast(this=value.copy(), to=variant_type.copy())

    def key_array(values: list[Expr]) -> Expr:
        if len(values) == 1 and isinstance(values[0], exp.Array):
            values = list(values[0].expressions)
        result = exp.Array(expressions=[as_variant(value) for value in values])
        result.args["_fs_internal"] = True
        return result

    if isinstance(expression, exp.ObjectInsert):
        return exp.Anonymous(
            this="_fs_object_insert",
            expressions=[
                as_variant(expression.this),
                as_variant(expression.args["key"]),
                as_variant(expression.args["value"]),
                (expression.args.get("update_flag") or exp.false()).copy(),
            ],
        )

    if isinstance(expression, exp.JSONKeys):
        return exp.Anonymous(
            this="_fs_object_keys",
            expressions=[as_variant(expression.this)],
        )

    if isinstance(expression, exp.MapCat):
        return exp.Anonymous(
            this="_fs_object_cat",
            expressions=[as_variant(expression.this), as_variant(expression.expression)],
        )

    if isinstance(expression, exp.Anonymous):
        name = expression.name.upper()
        if name == "OBJECT_DELETE" and expression.expressions:
            return exp.Anonymous(
                this="_fs_object_delete",
                expressions=[
                    as_variant(expression.expressions[0]),
                    key_array(expression.expressions[1:]),
                ],
            )
        if name == "OBJECT_PICK" and expression.expressions:
            return exp.Anonymous(
                this="_fs_object_pick",
                expressions=[
                    as_variant(expression.expressions[0]),
                    key_array(expression.expressions[1:]),
                ],
            )

    return expression


def regex_replace(expression: Expr) -> Expr:
    """Transform regex_replace expressions from snowflake to duckdb."""

    if isinstance(expression, exp.RegexpReplace) and isinstance(expression.expression, exp.Literal):
        if len(expression.args) > 3:
            # see https://docs.snowflake.com/en/sql-reference/functions/regexp_replace
            raise NotImplementedError(
                "REGEXP_REPLACE with additional parameters (eg: <position>, <occurrence>, <parameters>)"
            )

        # pattern: snowflake requires escaping backslashes in single-quoted string constants, but duckdb doesn't
        # see https://docs.snowflake.com/en/sql-reference/functions-regexp#label-regexp-escape-character-caveats
        expression.args["expression"] = exp.Literal(
            this=expression.expression.this.replace("\\\\", "\\"), is_string=True
        )

        if not expression.args.get("replacement"):
            # if no replacement string, the snowflake default is ''
            expression.args["replacement"] = exp.Literal(this="", is_string=True)

        # snowflake regex replacements are global
        expression.args["modifiers"] = exp.Literal(this="g", is_string=True)

    return expression


def regex_substr(expression: Expr) -> Expr:
    """Transform regex_substr expressions from snowflake to duckdb.

    See https://docs.snowflake.com/en/sql-reference/functions/regexp_substr
    """

    if isinstance(expression, exp.RegexpExtract):
        subject = expression.this

        # pattern: snowflake requires escaping backslashes in single-quoted string constants, but duckdb doesn't
        # see https://docs.snowflake.com/en/sql-reference/functions-regexp#label-regexp-escape-character-caveats
        pattern = expression.expression
        pattern.args["this"] = pattern.this.replace("\\\\", "\\")

        # number of characters from the beginning of the string where the function starts searching for matches
        position = expression.args["position"] or exp.Literal(this="1", is_string=False)

        # which occurrence of the pattern to match
        occurrence = expression.args["occurrence"]
        occurrence = int(occurrence.this) if occurrence else 1

        # the duckdb dialect increments bracket (ie: index) expressions by 1 because duckdb is 1-indexed,
        # so we need to compensate by subtracting 1
        occurrence = exp.Literal(this=str(occurrence - 1), is_string=False)

        has_e_param = False
        if parameters := expression.args["parameters"]:
            # Check for 'e' parameter BEFORE removing it
            if isinstance(parameters.this, str) and "e" in parameters.this:
                has_e_param = True
            # 'e' parameter doesn't make sense for duckdb
            regex_parameters = exp.Literal(this=parameters.this.replace("e", ""), is_string=True)
        else:
            regex_parameters = exp.Literal(is_string=True)

        # sqlglot defaults group to 0 if missing.
        group_num = expression.args.get("group")
        assert group_num

        # If 'e' is present, and group num is not, then default to 1 (the first group)
        # see https://docs.snowflake.com/en/sql-reference/functions/regexp_substr#:~:text=then%20the%20group_num-,defaults%20to%201,-(the%20first%20group
        if isinstance(group_num, exp.Literal) and group_num.this == "0" and has_e_param:
            group_num = exp.Literal(this="1", is_string=False)

        expression = exp.Bracket(
            this=exp.Anonymous(
                this="regexp_extract_all",
                expressions=[
                    # slice subject from position onwards
                    exp.Bracket(this=subject, expressions=[exp.Slice(this=position)]),
                    pattern,
                    group_num,
                    regex_parameters,
                ],
            ),
            # select index of occurrence
            expressions=[occurrence],
        )

    return expression


# TODO: move this into a Dialect as a transpilation
def set_schema(expression: Expr, current_database: str | None) -> Expr:
    """Transform USE SCHEMA/DATABASE to SET schema.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("USE SCHEMA bar").transform(set_schema, current_database="foo").sql()
        "SET schema = 'foo.bar'"
        >>> sqlglot.parse_one("USE SCHEMA foo.bar").transform(set_schema).sql()
        "SET schema = 'foo.bar'"
        >>> sqlglot.parse_one("USE DATABASE marts").transform(set_schema).sql()
        "SET schema = 'marts.main'"

        See tests for more examples.
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: A SET schema expression if the input is a USE
            expression, otherwise expression is returned as-is.
    """

    if (
        isinstance(expression, exp.Use)
        and (kind := expression.args.get("kind"))
        and isinstance(kind, exp.Var)
        and kind.name
        and kind.name.upper() in ["SCHEMA", "DATABASE"]
    ):
        assert expression.this, f"No identifier for USE expression {expression}"

        if kind.name.upper() == "DATABASE":
            # duckdb's default schema is main
            database = expression.this.name
            return exp.Command(
                this="SET", expression=exp.Literal.string(f"schema = '{database}.main'"), set_database=database
            )
        else:
            # SCHEMA
            if db := expression.this.args.get("db"):  # noqa: SIM108
                db_name = db.name
            else:
                # isn't qualified with a database
                db_name = current_database

            # assertion always true because check_db_schema is called before this
            assert db_name

            schema = expression.this.name
            return exp.Command(
                this="SET", expression=exp.Literal.string(f"schema = '{db_name}.{schema}'"), set_schema=schema
            )

    return expression


def split(expression: Expr) -> Expr:
    return expression


def tag(expression: Expr) -> Expr:
    """Handle tags. Transfer tags into upserts of the tag table.

    duckdb doesn't support tags. In lieu of a full implementation, for now we make it a NOP.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("ALTER TABLE table1 SET TAG foo='bar'").transform(tag).sql()
        "SELECT 'Statement executed successfully.'"
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if isinstance(expression, exp.Alter) and (actions := expression.args.get("actions")):
        for a in actions:
            if isinstance(a, exp.AlterSet) and a.args.get("tag"):
                return SUCCESS_NOP
    elif (
        isinstance(expression, exp.Command)
        and (cexp := expression.args.get("expression"))
        and isinstance(cexp, str)
        and "SET TAG" in cexp.upper()
    ):
        # alter table modify column set tag
        return SUCCESS_NOP
    elif (
        isinstance(expression, (exp.Create, exp.Drop))
        and (kind := expression.args.get("kind"))
        and isinstance(kind, str)
        and kind.upper() == "TAG"
    ):
        return SUCCESS_NOP

    return expression


def to_date(expression: Expr) -> Expr:
    """Convert to_date() to a cast.

    See https://docs.snowflake.com/en/sql-reference/functions/to_date

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT to_date(to_timestamp(0))").transform(to_date).sql()
        "SELECT CAST(DATE_TRUNC('day', TO_TIMESTAMP(0)) AS DATE)"
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TO_DATE":
        return exp.Cast(
            this=expression.expressions[0],
            to=exp.DataType(this=exp.DataType.Type.DATE, nested=False, prefix=False),
        )
    return expression


def _get_to_number_args(e: exp.ToNumber) -> tuple[Expr | None, Expr | None, Expr | None]:
    arg_format = e.args.get("format")
    arg_precision = e.args.get("precision")
    arg_scale = e.args.get("scale")

    _format = None
    _precision = None
    _scale = None

    # to_number(value, <format>, <precision>, <scale>)
    if arg_format:
        if arg_format.is_string:
            # to_number('100', 'TM9' ...)
            _format = arg_format

            # to_number('100', 'TM9', 10 ...)
            if arg_precision:
                _precision = arg_precision

                # to_number('100', 'TM9', 10, 2)
                if arg_scale:
                    _scale = arg_scale
        else:
            # to_number('100', 10, ...)
            # arg_format is not a string, so it must be precision.
            _precision = arg_format

            # to_number('100', 10, 2)
            # And arg_precision must be scale
            if arg_precision:
                _scale = arg_precision
    elif arg_precision:
        _precision = arg_precision
        if arg_scale:
            _scale = arg_scale

    return _format, _precision, _scale


def to_decimal(expression: Expr) -> Expr:
    """Transform to_decimal, to_number, to_numeric, try_to_decimal, try_to_number, try_to_numeric
    expressions from snowflake to duckdb.

    See https://docs.snowflake.com/en/sql-reference/functions/to_decimal
    and https://docs.snowflake.com/en/sql-reference/functions/try_to_decimal
    """

    if isinstance(expression, exp.ToNumber):
        format_, precision, scale = _get_to_number_args(expression)
        if format_:
            raise NotImplementedError(f"{expression.this} with format argument")

        if not precision:
            precision = exp.Literal(this="38", is_string=False)
        if not scale:
            scale = exp.Literal(this="0", is_string=False)

        # Use TryCast for try_to_* functions (safe=True), Cast for regular to_* functions
        is_safe = expression.args.get("safe")
        cast_class = exp.TryCast if is_safe else exp.Cast

        return cast_class(
            this=expression.this,
            to=exp.DataType(this=exp.DataType.Type.DECIMAL, expressions=[precision, scale], nested=False, prefix=False),
        )

    return expression


def to_timestamp(expression: Expr) -> Expr:
    """Transform to_timestamp, to_timestamp_ntz and casts to _fs_to_timestamp function.

    See https://docs.snowflake.com/en/sql-reference/functions/to_timestamp
    """
    default_scale = exp.Literal(this="0", is_string=False)
    # to_timestamp used with a Literal
    if isinstance(expression, exp.UnixToTime):
        return exp.Anonymous(
            this="_fs_to_timestamp", expressions=[expression.this, expression.args.get("scale") or default_scale]
        )
    # to_timestamp used with a Column
    elif isinstance(expression, exp.Anonymous) and expression.name.upper() in ["TO_TIMESTAMP", "TO_TIMESTAMP_NTZ"]:
        return exp.Anonymous(this="_fs_to_timestamp", expressions=[*expression.expressions, default_scale])
    # casting to timestamp or timestamp_ntz
    elif isinstance(expression, exp.Cast) and expression.to.this in (
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.TIMESTAMPNTZ,
    ):
        return exp.Anonymous(this="_fs_to_timestamp", expressions=[expression.this, default_scale])

    return expression


def to_variant(expression: Expr) -> Expr:
    """Convert to_variant to to_json.

    See https://docs.snowflake.com/en/sql-reference/functions/to_variant
    """

    if isinstance(expression, exp.ToVariant):
        return exp.Anonymous(this="TO_JSON", expressions=[expression.this.copy()])

    return expression


def timestamp_ntz(expression: Expr) -> Expr:
    """Convert timestamp_ntz (snowflake) to timestamp (duckdb).

    NB: timestamp_ntz defaults to nanosecond precision (ie: NTZ(9)). The duckdb equivalent is TIMESTAMP_NS.
    However we use TIMESTAMP (ie: microsecond precision) here rather than TIMESTAMP_NS to avoid
    https://github.com/duckdb/duckdb/issues/7980 in test_write_pandas_timestamp_ntz.
    """

    if isinstance(expression, exp.DataType) and expression.this == exp.DataType.Type.TIMESTAMPNTZ:
        return exp.DataType(this=exp.DataType.Type.TIMESTAMP)

    return expression


def trim_cast_varchar(expression: Expr) -> Expr:
    """Snowflake's TRIM casts input to VARCHAR implicitly."""

    if not (isinstance(expression, exp.Trim)):
        return expression

    operand = expression.this
    if isinstance(operand, exp.Cast) and operand.to.this in [exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT]:
        return expression

    return exp.Trim(
        this=exp.Cast(this=operand, to=exp.DataType(this=exp.DataType.Type.VARCHAR, nested=False, prefix=False))
    )


def try_parse_json(expression: Expr) -> Expr:
    """Convert TRY_PARSE_JSON() to TRY_CAST(... as JSON).

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("select try_parse_json('{}')").transform(parse_json).sql()
        "SELECT TRY_CAST('{}' AS JSON)"
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TRY_PARSE_JSON":
        expressions = expression.expressions
        return exp.TryCast(
            this=expressions[0],
            to=exp.DataType(this=exp.DataType.Type.JSON, nested=False),
        )

    return expression


def semi_structured_types(expression: Expr) -> Expr:
    """Convert OBJECT, ARRAY, and VARIANT types to duckdb compatible types.

    Structured types, eg: ARRAY(VARCHAR) or OBJECT(a INT NOT NULL), become JSON
    too. Their element types and field constraints aren't carried over, because
    duckdb would read them as a type modifier and reject them, ie: JSON(TEXT).

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("CREATE TABLE table1 (name object)").transform(semi_structured_types).sql()
        "CREATE TABLE table1 (name JSON)"
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if isinstance(expression, exp.DataType) and expression.this in [
        exp.DataType.Type.ARRAY,
        exp.DataType.Type.OBJECT,
        exp.DataType.Type.VARIANT,
    ]:
        return exp.DataType(this=exp.DataType.Type.JSON, nested=False)

    return expression


def upper_case_unquoted_identifiers(expression: Expr) -> Expr:
    """Upper case unquoted identifiers.

    Snowflake represents case-insensitivity using upper-case identifiers in cursor results.
    duckdb uses lowercase. We convert all unquoted identifiers to uppercase to match snowflake.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("select name, name as fname from table1").transform(upper_case_unquoted_identifiers).sql()
        'SELECT NAME, NAME AS FNAME FROM TABLE1'
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if (
        isinstance(expression, exp.Identifier)
        and not expression.quoted
        and isinstance(expression.this, str)
        and not (
            isinstance(expression.parent, exp.Dot)
            and expression.parent.expression is expression
            and isinstance(expression.parent.this, exp.Bracket)
        )
        and not (
            isinstance(expression.parent, exp.PropertyEQ)
            and expression.parent.this is expression
            and expression.find_ancestor(exp.Struct)
        )
        and not (
            (data_type := expression.find_ancestor(exp.DataType))
            and data_type.this in {exp.DataType.Type.OBJECT, exp.DataType.Type.STRUCT}
        )
    ):
        new = expression.copy()
        new.set("this", expression.this.upper())
        return new

    return expression


def values_columns(expression: Expr) -> Expr:
    """Support column1, column2 expressions in VALUES.

    Snowflake uses column1, column2 .. for unnamed columns in VALUES. Whereas duckdb uses col0, col1 ..
    See https://docs.snowflake.com/en/sql-reference/constructs/values#examples
    """

    if (
        isinstance(expression, exp.Values)
        and not expression.alias
        and expression.find_ancestor(exp.Select)
        and (values := expression.find(exp.Tuple))
    ):
        num_columns = len(values.expressions)
        columns = [exp.Identifier(this=f"COLUMN{i + 1}", quoted=True) for i in range(num_columns)]
        expression.set("alias", exp.TableAlias(this=exp.Identifier(this="_", quoted=False), columns=columns))

    return expression


def _coerce_semi_structured_value(value: Expr, target: exp.DataType) -> Expr:
    variant_type = exp.DataType(this=exp.DataType.Type.VARIANT, nested=False)
    as_variant = exp.Cast(this=value.copy(), to=variant_type)
    if target.this == exp.DataType.Type.VARIANT:
        return as_variant
    if (
        target.this == exp.DataType.Type.MAP
        and len(target.expressions) == 2
        and isinstance(target.expressions[1], exp.DataType)
        and target.expressions[1].this == exp.DataType.Type.VARIANT
    ):
        return exp.Anonymous(this="_fs_variant_to_object", expressions=[as_variant])
    if (
        target.this == exp.DataType.Type.ARRAY
        and target.expressions
        and isinstance(target.expressions[0], exp.DataType)
        and target.expressions[0].this == exp.DataType.Type.VARIANT
    ):
        return exp.Anonymous(this="_fs_variant_to_array", expressions=[as_variant])
    return exp.Cast(this=value.copy(), to=target.copy())


def _target_table(expression: exp.Insert | exp.Update) -> exp.Table | None:
    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    return target if isinstance(target, exp.Table) else None


def coerce_semi_structured_targets(expression: Expr, duck_conn: DuckDBPyConnection) -> Expr:
    if not isinstance(expression, (exp.Insert, exp.Update)):
        return expression
    table = _target_table(expression)
    if table is None:
        return expression
    try:
        described = duck_conn.sql(f"DESCRIBE {table.sql(dialect='duckdb')}").fetchall()
    except Exception:
        return expression
    target_types = {
        name.upper(): exp.DataType.build(column_type, dialect="duckdb")
        for name, column_type, *_ in described
    }

    if isinstance(expression, exp.Update):
        for assignment in expression.expressions:
            if not isinstance(assignment, exp.EQ) or not isinstance(assignment.this, exp.Column):
                continue
            if target := target_types.get(assignment.this.name.upper()):
                assignment.set(
                    "expression",
                    _coerce_semi_structured_value(assignment.expression, target),
                )
        return expression

    columns = (
        [column.name for column in expression.this.expressions]
        if isinstance(expression.this, exp.Schema)
        else list(target_types)
    )
    source = expression.expression
    if isinstance(source, exp.Select):
        projections: list[Expr] = []
        for index, item in enumerate(source.expressions):
            column = item.alias_or_name if expression.args.get("by_name") else columns[index]
            target = target_types.get(column.upper())
            if target is None:
                projections.append(item)
                continue
            value = item.this if isinstance(item, exp.Alias) else item
            coerced = _coerce_semi_structured_value(value, target)
            projections.append(
                exp.Alias(this=coerced, alias=item.args["alias"].copy())
                if isinstance(item, exp.Alias)
                else coerced
            )
        source.set("expressions", projections)
    elif isinstance(source, exp.Values):
        for row in source.expressions:
            if not isinstance(row, exp.Tuple):
                continue
            row.set(
                "expressions",
                [
                    _coerce_semi_structured_value(value, target_types[columns[index].upper()])
                    for index, value in enumerate(row.expressions)
                ],
            )
    return expression


def create_table_as(expression: Expr, duck_conn: DuckDBPyConnection) -> Expr:
    if (
        isinstance(expression, exp.Create)
        and expression.kind == "TABLE"
        and isinstance(expression.expression, (exp.Select, exp.Subquery))
        and isinstance(expression.this, exp.Schema)
        and len(expression.this.expressions) > 0
    ):
        # Extract the column definitions from the schema
        schema = expression.this
        create_col_defs: list[exp.ColumnDef] = schema.expressions

        if isinstance(expression.expression, exp.Subquery):
            select_query = expression.expression.unnest()
        else:
            select_query = expression.expression

        if any(isinstance(expr, exp.Star) for expr in select_query.expressions):
            # convert SELECT * to SELECT <col1>, <col2>, ...
            duck_conn.execute(f"DESCRIBE {select_query}")
            select_query.set(
                "expressions",
                [exp.Column(this=exp.Identifier(this=col[0], quoted=False)) for col in duck_conn.fetchall()],
            )

        if len(select_query.expressions) != len(create_col_defs):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:\nInvalid column definition list", errno=2026, sqlstate="42601"
            )

        # Transform the SELECT to add casting and aliasing based on the schema
        new_expressions = []
        for i, col_def in enumerate(create_col_defs):
            create_col_id = col_def.this
            assert isinstance(create_col_id, exp.Identifier), f"Expected Identifier, got {type(create_col_id)}"
            create_col_type = col_def.kind
            assert create_col_type is not None
            select_col = select_query.expressions[i]

            inner = select_col.this if isinstance(select_col, exp.Alias) else select_col
            cast_expr = _coerce_semi_structured_value(inner, create_col_type)
            aliased_expr = exp.Alias(this=cast_expr, alias=create_col_id)

            new_expressions.append(aliased_expr)

        select_query.set("expressions", new_expressions)

        # Remove the schema from the CREATE statement - just keep the table identifier
        expression.set("this", schema.this)

    return expression


def create_user(expression: Expr) -> Expr:
    """Transform CREATE USER to a query against the global database's information_schema._fs_users table.

    https://docs.snowflake.com/en/sql-reference/sql/create-user
    """
    # XXX: this is a placeholder. We need to implement the full CREATE USER syntax, but
    #      sqlglot doesnt yet support Create for snowflake.
    if isinstance(expression, exp.Command) and expression.this == "CREATE":
        sub_exp = expression.expression.strip()
        if sub_exp.upper().startswith("USER"):
            _, name, *ignored = sub_exp.split(" ")
            if ignored:
                raise NotImplementedError(f"`CREATE USER` with {ignored}")
            return sqlglot.parse_one(
                f"INSERT INTO _fs_global._fs_information_schema._fs_users (name) VALUES ('{name}')", read="duckdb"
            )

    return expression


def alter_session(expression: Expr) -> Expr:
    """Handle ALTER SESSION.

    Supported parameters:
    - QUOTED_IDENTIFIERS_IGNORE_CASE:
        - SET ... = false      => NOP success
        - UNSET ...            => NOP success
        - SET ... = true       => Not implemented
    - AUTOCOMMIT:
        - SET ... = true/false => success (returns NOP success with side effect arg)
    """

    if (
        isinstance(expression, exp.Alter)
        and expression.kind == "SESSION"
        and (alter_session := expression.find(exp.AlterSession))
    ):
        items = alter_session.args.get("expressions") or []

        if items and isinstance(items[0], exp.SetItem) and (ident := items[0].find(exp.Identifier)):
            name = ident.this.upper()

            if name == "QUOTED_IDENTIFIERS_IGNORE_CASE" and (
                bool(alter_session.args.get("unset"))
                or (
                    isinstance(items[0].this, exp.EQ)
                    and (rhs := items[0].this.args.get("expression"))
                    and isinstance(rhs, exp.Boolean)
                    and rhs.this is False
                )
            ):
                return SUCCESS_NOP

            elif (
                name == "AUTOCOMMIT"
                and isinstance(items[0].this, exp.EQ)
                and (rhs := items[0].this.args.get("expression"))
                and isinstance(rhs, exp.Boolean)
            ):
                new = SUCCESS_NOP.copy()
                new.args["set_autocommit"] = rhs.this
                return new

        raise NotImplementedError(expression.sql(dialect="snowflake"))

    return expression


def update_variables(
    expression: Expr,
    variables: Variables,
) -> Expr:
    if Variables.is_variable_modifier(expression):
        variables.update_variables(expression)
        return SUCCESS_NOP  # Nothing further to do if its a SET/UNSET operation.
    return expression


def _is_sha256_length(length: Expr | None) -> bool:
    return isinstance(length, exp.Literal) and str(length.this) == "256"


def _sha256_expr(argument: Expr) -> Expr:
    return exp.Anonymous(this="SHA256", expressions=[argument.copy()])


def _sha256_binary_expr(argument: Expr) -> Expr:
    return exp.Unhex(this=_sha256_expr(argument))


def _rewrite_sha_call(
    argument: Expr,
    length: Expr | None,
    fallback_name: str,
    *,
    binary: bool = False,
) -> Expr:
    length = length or exp.Literal.number(256)
    if _is_sha256_length(length):
        return _sha256_binary_expr(argument) if binary else _sha256_expr(argument)
    return exp.Anonymous(this=fallback_name, expressions=[argument.copy(), length.copy()])


def _rewrite_anonymous_sha(expression: exp.Anonymous, name: str, *, binary: bool = False) -> Expr:
    if expression.this.upper() != name or not expression.expressions:
        return expression

    if len(expression.expressions) == 1 or (
        len(expression.expressions) == 2 and _is_sha256_length(expression.expressions[1])
    ):
        argument = expression.expressions[0]
        return _sha256_binary_expr(argument) if binary else _sha256_expr(argument)

    return expression


def sha256(expression: Expr) -> Expr:
    """Convert sha2() or sha2_hex() to sha256().

    Convert sha2_binary() to unhex(sha256()).

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("insert into table1 (name) select sha2('foo')").transform(sha256).sql()
        "INSERT INTO table1 (name) SELECT SHA256('foo')"
    Args:
        expression (Expr): the expression that will be transformed.

    Returns:
        Expr: The transformed expression.
    """

    if isinstance(expression, exp.SHA2):
        return _rewrite_sha_call(expression.this, expression.args.get("length"), "SHA2")

    elif isinstance(expression, exp.SHA2Digest):
        return _rewrite_sha_call(expression.this, expression.args.get("length"), "SHA2_BINARY", binary=True)

    elif isinstance(expression, exp.Anonymous) and expression.this.upper() == "SHA2_HEX":
        return _rewrite_anonymous_sha(expression, "SHA2_HEX")

    elif isinstance(expression, exp.Anonymous) and expression.this.upper() == "SHA2_BINARY":
        return _rewrite_anonymous_sha(expression, "SHA2_BINARY", binary=True)

    return expression


def result_scan(expression: Expr) -> Expr:
    """
    Transform SELECT * FROM TABLE(RESULT_SCAN('sfqid')) to mark it for special handling.

    This allows the cursor to fetch results from the results cache instead of executing
    the query against DuckDB.
    """
    if (
        isinstance(expression, exp.Select)
        and (from_ := expression.args.get("from_"))
        and isinstance(from_.this, exp.TableFromRows)
        and isinstance(from_.this.this, exp.Anonymous)
        and from_.this.this.name.upper() == "RESULT_SCAN"
        and from_.this.this.expressions
        and isinstance(from_.this.this.expressions[0], exp.Literal)
    ):
        sfqid = from_.this.this.expressions[0].this
        # Attach the sfqid to the expression for the cursor to handle
        expression.args["result_scan_sfqid"] = sfqid
    return expression


def sequence_nextval(expression: Expr) -> Expr:
    """Transform Snowflake sequence nextval syntax to DuckDB syntax.

    Converts "sequence_name.nextval" to "nextval('sequence_name')".

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT seq_01.nextval").transform(sequence_nextval).sql()
        "SELECT NEXTVAL('seq_01') AS NEXTVAL"
    """
    # Check if this is a Column with nextval
    if (
        isinstance(expression, exp.Column)
        and isinstance(expression.this, exp.Identifier)
        and expression.this.this.upper() == "NEXTVAL"
        and expression.table
    ):
        table_name = expression.table
        nextval = exp.Anonymous(this="nextval", expressions=[exp.Literal(this=table_name, is_string=True)])

        # Already aliased
        if isinstance(expression.parent, exp.Alias):
            return nextval

        # Non-aliased case: seq_01.nextval -> nextval('seq_01') AS NEXTVAL
        return exp.Alias(
            this=nextval,
            alias=exp.Identifier(this="NEXTVAL", quoted=False),
        )

    return expression


# Numeric-only aggregate functions that fail on VARCHAR in DuckDB.
# Snowflake implicitly casts VARCHAR to numeric for these.
_NUMERIC_ONLY_AGGS = (
    exp.Sum,
    exp.Avg,
    exp.Variance,
    exp.Stddev,
    exp.StddevSamp,
    exp.StddevPop,
    exp.VariancePop,
    exp.Median,
)


def _numeric_agg_col_name(expression: Expr, arg: Expr) -> str | None:
    function_name: str | None = None

    if isinstance(expression, exp.Sum):
        function_name = "SUM"
    elif isinstance(expression, exp.Avg):
        function_name = "AVG"
    elif isinstance(expression, exp.Variance):
        function_name = "VARIANCE"
    elif isinstance(expression, exp.StddevSamp):
        function_name = "STDDEV_SAMP"
    elif isinstance(expression, exp.StddevPop):
        function_name = "STDDEV_POP"
    elif isinstance(expression, exp.Stddev):
        start = cast(int | None, expression.meta.get("start"))
        end = cast(int | None, expression.meta.get("end"))
        function_name = "STDDEV_SAMP" if start is not None and end is not None and end - start + 1 == 11 else "STDDEV"
    elif isinstance(expression, exp.VariancePop):
        function_name = "VARIANCE_POP"
    elif isinstance(expression, exp.Median):
        function_name = "MEDIAN"

    if function_name is None:
        return None

    return f"{function_name}({arg.sql(dialect='snowflake').upper()})"


def numeric_agg_implicit_cast(expression: Expr) -> Expr:
    """Wrap arguments to numeric aggregate functions with TRY_CAST(... AS DOUBLE).

    Snowflake implicitly casts VARCHAR to numeric when used in aggregate functions
    like SUM(), AVG(), MEDIAN(), etc. DuckDB is strict and rejects these. This
    transform adds an explicit TRY_CAST to match Snowflake's behavior while
    preserving Snowflake-style column names for rewritten select projections.

    Only applies when the argument is not already a Cast/TryCast expression.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT SUM(amount) FROM t").transform(numeric_agg_implicit_cast).sql()
        'SELECT SUM(TRY_CAST(amount AS DOUBLE)) AS "SUM(AMOUNT)" FROM t'
    """
    if isinstance(expression, _NUMERIC_ONLY_AGGS):
        arg = expression.this
        col_name = None if isinstance(expression.parent, exp.Alias) else _numeric_agg_col_name(expression, arg)
        # Don't double-cast if already cast
        if not isinstance(arg, (exp.Cast, exp.TryCast)):
            expression.set(
                "this",
                exp.TryCast(this=arg, to=exp.DataType(this=exp.DataType.Type.DOUBLE)),
            )
        if col_name and isinstance(expression.parent, exp.Select):
            return exp.alias_(expression, col_name, quoted=True)
    return expression


# a numeric utc offset at the end of a timestamp literal, ie: " +09:00", " -0700", " Z"
TIMESTAMP_OFFSET = re.compile(r"(\d)\s+([+-]\d{2}:?(?:\d{2})?|Z)$")

TIMESTAMP_WITH_OFFSET_TYPES = (
    exp.DataType.Type.TIMESTAMPTZ,
    exp.DataType.Type.TIMESTAMPLTZ,
)


def timestamp_offsets(expression: Expr) -> Expr:
    """Remove the space before a numeric utc offset in a timestamp literal.

    Snowflake accepts a space, duckdb doesn't and reads the offset as a time zone name:

        "2026-01-01 10:00:00 +09:00" -> Unknown TimeZone '+09:00'

    so it's dropped before duckdb sees it.

        SELECT '2026-01-01 10:00:00 +09:00'::TIMESTAMP_TZ
    becomes
        SELECT '2026-01-01 10:00:00+09:00'::TIMESTAMPTZ
    """
    if (
        isinstance(expression, exp.Cast)
        and expression.to.this in TIMESTAMP_WITH_OFFSET_TYPES
        and isinstance(expression.this, exp.Literal)
        and expression.this.is_string
    ):
        literal = expression.this
        collapsed = TIMESTAMP_OFFSET.sub(r"\1\2", literal.this)
        if collapsed != literal.this:
            literal.set("this", collapsed)

    return expression
