from __future__ import annotations

_VARIANT_NULL = "variant_bytes_to_variant(from_hex('01000000'))"

_HELPERS = """
LOAD parquet;

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_null() AS (${variant_null});

CREATE OR REPLACE MACRO ${catalog}.main._fs_as_variant(v) AS (
    CAST((
        CASE
            WHEN v IS NULL THEN NULL
            WHEN typeof(v) = 'VARIANT' THEN v
            WHEN typeof(v) = 'JSON' THEN CAST(v AS VARIANT)
            WHEN typeof(v) IN ('HUGEINT', 'UHUGEINT') AND TRY_CAST(v AS DECIMAL(38, 0)) IS NOT NULL
                THEN CAST(v AS DECIMAL(38, 0))::VARIANT
            WHEN typeof(v) IN ('HUGEINT', 'UHUGEINT') THEN v::VARIANT
            ELSE TRY_CAST(v AS VARIANT)
        END
    ) AS VARIANT)
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_as_list(v) AS (
    CASE
        WHEN v IS NULL THEN NULL
        WHEN typeof(v) LIKE 'MAP(%' OR typeof(v) LIKE 'STRUCT(%' THEN NULL
        WHEN typeof(v) LIKE '%[]' THEN try_cast(v AS VARIANT[])
        WHEN typeof(v) = 'VARIANT' AND ${catalog}.main._fs_variant_typeof(v) LIKE 'ARRAY%'
            THEN try_cast(v AS VARIANT[])
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_typeof(v) AS (
    variant_typeof(CASE WHEN false THEN NULL::VARIANT ELSE TRY_CAST(v AS VARIANT) END)
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_json_get(j, key) AS (
    CASE _fs_map_get_kind_py(j, key)
        WHEN 'json_null' THEN ${catalog}.main._fs_variant_null()
        WHEN 'value' THEN _fs_map_get_py(j, key)
        ELSE NULL::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_as_map(v) AS (
    CASE
        WHEN v IS NULL THEN NULL
        WHEN typeof(v) LIKE 'MAP(%' THEN TRY_CAST(v AS MAP(VARCHAR, VARIANT))
        WHEN typeof(v) LIKE 'STRUCT(%' THEN TRY_CAST(v AS MAP(VARCHAR, VARIANT))
        WHEN typeof(v) = 'VARIANT' AND ${catalog}.main._fs_variant_typeof(v) LIKE 'OBJECT%'
            THEN TRY_CAST(v AS MAP(VARCHAR, VARIANT))
        WHEN typeof(v) = 'JSON' AND json_type(v) = 'OBJECT'
            THEN TRY_CAST(TRY_CAST(v AS VARIANT) AS MAP(VARCHAR, VARIANT))
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_map_as_object(m) AS (m);

CREATE OR REPLACE MACRO ${catalog}.main._fs_is_json_null(v) AS (
    _fs_is_json_null_py(
        typeof(v),
        CASE
            WHEN typeof(v) = 'VARIANT' THEN variant_typeof(TRY_CAST(v AS VARIANT))
            ELSE NULL
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_typeof_tag(v, t) AS (
    CASE
        WHEN v IS NULL THEN NULL
        WHEN t = 'VARIANT_NULL' THEN 'NULL_VALUE'
        WHEN t LIKE 'INT%' OR t LIKE 'UINT%' OR t LIKE 'HUGEINT%' THEN 'INTEGER'
        WHEN t LIKE 'BOOL%' THEN 'BOOLEAN'
        WHEN t LIKE 'ARRAY%' THEN 'ARRAY'
        WHEN t LIKE 'OBJECT%' THEN 'OBJECT'
        WHEN t LIKE 'VARCHAR%' THEN
            CASE
                WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_LTZ__%' THEN 'TIMESTAMP_LTZ'
                WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_TZ__%' THEN 'TIMESTAMP_TZ'
                WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_NTZ__%' THEN 'TIMESTAMP_NTZ'
                ELSE 'VARCHAR'
            END
        WHEN t LIKE 'DOUBLE%' OR t LIKE 'FLOAT%' THEN 'DOUBLE'
        WHEN regexp_matches(t, '^DECIMAL\\([^,]+,\\s*0\\)$') THEN 'INTEGER'
        WHEN t LIKE 'DECIMAL%' THEN 'DECIMAL'
        WHEN t LIKE 'BLOB%' OR t LIKE 'BINARY%' THEN 'BINARY'
        WHEN t LIKE 'DATE%' THEN 'DATE'
        WHEN t LIKE 'TIMESTAMP WITH TIME ZONE%' OR t LIKE 'TIMESTAMP%TZ%' THEN 'TIMESTAMP_TZ'
        WHEN t LIKE 'TIMESTAMP%' THEN 'TIMESTAMP_NTZ'
        WHEN t LIKE 'TIME%' THEN 'TIME'
        ELSE t
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_typeof(v) AS (
    _fs_typeof_py(
        v,
        typeof(v),
        CASE
            WHEN v IS NULL THEN NULL
            WHEN typeof(v) IN ('VARIANT', 'JSON') THEN variant_typeof(TRY_CAST(v AS VARIANT))
            ELSE NULL
        END
    )
);
"""

_CORE = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_try_parse_json(val) AS (
    CASE
        WHEN val IS NULL THEN NULL
        WHEN trim(CAST(val AS VARCHAR)) = '' THEN NULL
        WHEN regexp_matches(trim(CAST(val AS VARCHAR)), '^-?[0-9]+$') THEN
            CASE
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT) IS NOT NULL
                    THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT)::VARIANT
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0)) IS NOT NULL
                    THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0))::VARIANT
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS HUGEINT) IS NOT NULL
                    THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS HUGEINT)::VARIANT
                ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS DOUBLE)::VARIANT
            END
        WHEN regexp_matches(trim(CAST(val AS VARCHAR)), '^-?[0-9]+\\.[0-9]+$') THEN
            CASE
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 18))
                    = CAST(TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 18)) AS DECIMAL(38, 0))
                    THEN CASE
                        WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT) IS NOT NULL
                            THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT)::VARIANT
                        WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0)) IS NOT NULL
                            THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0))::VARIANT
                        ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS DOUBLE)::VARIANT
                    END
                ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 18))::VARIANT
            END
        WHEN regexp_matches(trim(CAST(val AS VARCHAR)), '^-?[0-9]+(\\.[0-9]+)?[eE][+-]?[0-9]+$') THEN
            TRY_CAST(trim(CAST(val AS VARCHAR)) AS DOUBLE)::VARIANT
        WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS JSON) IS NULL THEN NULL
        WHEN json_type(TRY_CAST(trim(CAST(val AS VARCHAR)) AS JSON)) = 'NULL'
            THEN ${catalog}.main._fs_variant_null()
        ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS JSON)::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_parse_json(val) AS (
    CASE
        WHEN val IS NULL THEN NULL
        WHEN trim(CAST(val AS VARCHAR)) = '' THEN NULL
        WHEN regexp_matches(trim(CAST(val AS VARCHAR)), '^-?[0-9]+$') THEN
            CASE
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT) IS NOT NULL
                    THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT)::VARIANT
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0)) IS NOT NULL
                    THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0))::VARIANT
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS HUGEINT) IS NOT NULL
                    THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS HUGEINT)::VARIANT
                ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS DOUBLE)::VARIANT
            END
        WHEN regexp_matches(trim(CAST(val AS VARCHAR)), '^-?[0-9]+\\.[0-9]+$') THEN
            CASE
                WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 18))
                    = CAST(TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 18)) AS DECIMAL(38, 0))
                    THEN CASE
                        WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT) IS NOT NULL
                            THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS BIGINT)::VARIANT
                        WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0)) IS NOT NULL
                            THEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 0))::VARIANT
                        ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS DOUBLE)::VARIANT
                    END
                ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS DECIMAL(38, 18))::VARIANT
            END
        WHEN regexp_matches(trim(CAST(val AS VARCHAR)), '^-?[0-9]+(\\.[0-9]+)?[eE][+-]?[0-9]+$') THEN
            TRY_CAST(trim(CAST(val AS VARCHAR)) AS DOUBLE)::VARIANT
        WHEN TRY_CAST(trim(CAST(val AS VARCHAR)) AS JSON) IS NULL THEN NULL
        WHEN json_type(TRY_CAST(trim(CAST(val AS VARCHAR)) AS JSON)) = 'NULL'
            THEN ${catalog}.main._fs_variant_null()
        ELSE TRY_CAST(trim(CAST(val AS VARCHAR)) AS JSON)::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_to_variant_timestamp(val, kind) AS (
    CASE upper(kind)
        WHEN 'LTZ' THEN CAST('__FAKESNOW_TIMESTAMP_LTZ__' || CAST(val AS VARCHAR) || ' Z' AS VARCHAR)::VARIANT
        WHEN 'TZ' THEN CAST('__FAKESNOW_TIMESTAMP_TZ__' || CAST(val AS VARCHAR) AS VARCHAR)::VARIANT
        ELSE CAST('__FAKESNOW_TIMESTAMP_NTZ__' || CAST(val AS VARCHAR) AS VARCHAR)::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_cmp_variant(v) AS (
    CAST(
        CASE
            WHEN typeof(v) LIKE 'MAP(%' THEN CAST(v AS JSON)
            WHEN typeof(v) = 'JSON' THEN v
            ELSE ${catalog}.main._fs_as_variant(v)
        END
        AS VARIANT
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_key(v) AS (
    _fs_variant_group_key_py(
        v,
        typeof(v),
        CASE WHEN v IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(v AS VARIANT)) END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_sort_rank(v) AS (
    _fs_variant_sort_rank_py(
        typeof(v),
        CASE
            WHEN v IS NULL THEN NULL
            ELSE variant_typeof(TRY_CAST(v AS VARIANT))
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_sort_key(v) AS (
    struct_pack(
        rank := ${catalog}.main._fs_variant_sort_rank(v),
        num := CASE
            WHEN ${catalog}.main._fs_is_numeric_variant(v) THEN TRY_CAST(v AS DOUBLE)
            ELSE NULL::DOUBLE
        END,
        cmp := _fs_variant_order_key_py(v)
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_lt(a, b) AS (
    _fs_variant_lt_py(
        a,
        typeof(a),
        CASE WHEN a IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(a AS VARIANT)) END,
        b,
        typeof(b),
        CASE WHEN b IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(b AS VARIANT)) END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_is_numeric_variant(v) AS (
    _fs_is_numeric_variant_py(
        CASE
            WHEN v IS NULL THEN NULL
            ELSE variant_typeof(TRY_CAST(v AS VARIANT))
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_eq(a, b) AS (
    _fs_variant_eq_py(
        a,
        typeof(a),
        CASE WHEN a IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(a AS VARIANT)) END,
        b,
        typeof(b),
        CASE WHEN b IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(b AS VARIANT)) END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_eq_sql(a, b) AS (
    _fs_variant_eq_sql_py(
        a,
        typeof(a),
        CASE WHEN a IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(a AS VARIANT)) END,
        b,
        typeof(b),
        CASE WHEN b IS NULL THEN NULL ELSE variant_typeof(TRY_CAST(b AS VARIANT)) END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get_index(v, key) AS (
    _fs_variant_get_index_py(v, key)
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_map_get(m, key) AS (
    list_element(
        map_extract(TRY_CAST(m AS MAP(VARCHAR, VARIANT)), TRY_CAST(key AS VARCHAR)),
        1
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_greatest(a, b) AS (
    CASE
        WHEN a IS NULL OR b IS NULL THEN NULL::VARIANT
        WHEN TRY_CAST(a AS DOUBLE) >= TRY_CAST(b AS DOUBLE) THEN a::VARIANT
        ELSE b::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_least(a, b) AS (
    CASE
        WHEN a IS NULL OR b IS NULL THEN NULL::VARIANT
        WHEN TRY_CAST(a AS DOUBLE) <= TRY_CAST(b AS DOUBLE) THEN a::VARIANT
        ELSE b::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_to_json_element(x) AS (
    CASE
        WHEN x IS NULL THEN 'undefined'
        WHEN ${catalog}.main._fs_is_json_null(x) THEN 'null'
        WHEN ${catalog}.main._fs_variant_typeof(x) LIKE 'DOUBLE%'
            AND isfinite(TRY_CAST(x AS DOUBLE))
            THEN printf('%.15e', TRY_CAST(x AS DOUBLE))
        WHEN ${catalog}.main._fs_variant_typeof(x) LIKE 'TIMESTAMP%'
            THEN '"' || left(strftime(TRY_CAST(x AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f'), 23) || '"'
        WHEN ${catalog}.main._fs_variant_typeof(x) LIKE 'BLOB%'
            OR ${catalog}.main._fs_variant_typeof(x) LIKE 'BINARY%'
            THEN '"' || upper(hex(TRY_CAST(x AS BLOB))) || '"'
        WHEN TRY_CAST(x AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_NTZ__%' THEN
            '"' || CASE
                WHEN length(replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_NTZ__', '')) <= 19
                    THEN replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_NTZ__', '') || '.000'
                ELSE left(replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_NTZ__', ''), 23)
            END || '"'
        WHEN TRY_CAST(x AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_TZ__%' THEN
            '"' || left(
                replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_TZ__', ''),
                length(replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_TZ__', '')) - 6
            ) || '.000 ' || replace(
                right(replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_TZ__', ''), 6),
                ':',
                ''
            ) || '"'
        WHEN TRY_CAST(x AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_LTZ__%' THEN
            '"' || replace(
                replace(TRY_CAST(x AS VARCHAR), '__FAKESNOW_TIMESTAMP_LTZ__', ''),
                ' Z',
                '.000 Z'
            ) || '"'
        WHEN typeof(x) LIKE 'MAP(%'
            OR typeof(x) LIKE 'STRUCT(%'
            OR ${catalog}.main._fs_variant_typeof(x) LIKE 'OBJECT%' THEN
            ${catalog}.main._fs_to_json_object(${catalog}.main._fs_as_map(x))
        WHEN typeof(x) LIKE '%[]' OR ${catalog}.main._fs_variant_typeof(x) LIKE 'ARRAY%' THEN
            '[' || COALESCE(
                list_aggr(
                    list_transform(
                        ${catalog}.main._fs_as_list(x),
                        y -> CASE
                            WHEN y IS NULL THEN 'undefined'
                            WHEN ${catalog}.main._fs_is_json_null(y) THEN 'null'
                            WHEN ${catalog}.main._fs_variant_typeof(y) LIKE 'DOUBLE%'
                                AND isfinite(TRY_CAST(y AS DOUBLE))
                                THEN printf('%.15e', TRY_CAST(y AS DOUBLE))
                            WHEN ${catalog}.main._fs_variant_typeof(y) LIKE 'DECIMAL%'
                                THEN regexp_replace(
                                    regexp_replace(CAST(y AS VARCHAR), '0+$', ''),
                                    '\\.$',
                                    ''
                                )
                            WHEN ${catalog}.main._fs_variant_typeof(y) LIKE 'TIMESTAMP%'
                                THEN '"' || left(strftime(TRY_CAST(y AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f'), 23) || '"'
                            WHEN ${catalog}.main._fs_variant_typeof(y) LIKE 'BLOB%'
                                OR ${catalog}.main._fs_variant_typeof(y) LIKE 'BINARY%'
                                THEN '"' || upper(hex(TRY_CAST(y AS BLOB))) || '"'
                            WHEN TRY_CAST(y AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_NTZ__%' THEN
                                '"' || CASE
                                    WHEN length(
                                        replace(TRY_CAST(y AS VARCHAR), '__FAKESNOW_TIMESTAMP_NTZ__', '')
                                    ) <= 19
                                        THEN replace(
                                            TRY_CAST(y AS VARCHAR),
                                            '__FAKESNOW_TIMESTAMP_NTZ__',
                                            ''
                                        ) || '.000'
                                    ELSE left(
                                        replace(TRY_CAST(y AS VARCHAR), '__FAKESNOW_TIMESTAMP_NTZ__', ''),
                                        23
                                    )
                                END || '"'
                            WHEN TRY_CAST(y AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_TZ__%' THEN
                                '"' || left(
                                    replace(TRY_CAST(y AS VARCHAR), '__FAKESNOW_TIMESTAMP_TZ__', ''),
                                    length(
                                        replace(TRY_CAST(y AS VARCHAR), '__FAKESNOW_TIMESTAMP_TZ__', '')
                                    ) - 6
                                ) || '.000 ' || replace(
                                    right(
                                        replace(TRY_CAST(y AS VARCHAR), '__FAKESNOW_TIMESTAMP_TZ__', ''),
                                        6
                                    ),
                                    ':',
                                    ''
                                ) || '"'
                            WHEN TRY_CAST(y AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_LTZ__%' THEN
                                '"' || replace(
                                    replace(TRY_CAST(y AS VARCHAR), '__FAKESNOW_TIMESTAMP_LTZ__', ''),
                                    ' Z',
                                    '.000 Z'
                                ) || '"'
                            WHEN ${catalog}.main._fs_variant_typeof(y) LIKE 'OBJECT%' THEN
                                CAST(y AS JSON)::VARCHAR
                            ELSE _fs_to_json_py(y)
                        END
                    ),
                    'string_agg',
                    ','
                ),
                ''
            ) || ']'
        ELSE CAST(x AS JSON)::VARCHAR
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_to_json_object(m) AS (
    '{' || COALESCE(
        list_aggr(
            list_transform(
                list_sort(map_keys(CAST(m AS MAP(VARCHAR, VARIANT)))),
                k -> to_json(k::VARCHAR) || ':' || COALESCE(
                    CASE
                        WHEN list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1) IS NULL
                            THEN 'null'
                        WHEN ${catalog}.main._fs_is_json_null(
                            list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                        ) THEN 'null'
                        WHEN ${catalog}.main._fs_variant_typeof(
                            list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                        ) LIKE 'DOUBLE%'
                            AND isfinite(
                                TRY_CAST(
                                    list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                                    AS DOUBLE
                                )
                            )
                            THEN printf(
                                '%.15e',
                                TRY_CAST(
                                    list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                                    AS DOUBLE
                                )
                            )
                        WHEN ${catalog}.main._fs_variant_typeof(
                            list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                        ) LIKE 'BLOB%'
                            OR ${catalog}.main._fs_variant_typeof(
                                list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                            ) LIKE 'BINARY%'
                            THEN '"' || upper(hex(TRY_CAST(
                                list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                                AS BLOB
                            ))) || '"'
                        WHEN ${catalog}.main._fs_variant_typeof(
                            list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                        ) LIKE 'TIMESTAMP%'
                            THEN '"' || left(strftime(TRY_CAST(
                                list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                                AS TIMESTAMP
                            ), '%Y-%m-%d %H:%M:%S.%f'), 23) || '"'
                        ELSE CAST(
                            list_element(map_extract(CAST(m AS MAP(VARCHAR, VARIANT)), k), 1)
                            AS JSON
                        )::VARCHAR
                    END,
                    'null'
                )
            ),
            'string_agg',
            ','
        ),
        ''
    ) || '}'
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_to_json(v) AS (
    CAST((
        CASE
            WHEN v IS NULL THEN NULL
            WHEN ${catalog}.main._fs_is_json_null(v) THEN 'null'
            WHEN typeof(v) LIKE '%[]'
                OR (typeof(v) = 'VARIANT' AND ${catalog}.main._fs_variant_typeof(v) LIKE 'ARRAY%')
                THEN ${catalog}.main._fs_to_json_element(v)
            WHEN typeof(v) LIKE 'MAP(%'
                OR (typeof(v) = 'VARIANT' AND ${catalog}.main._fs_variant_typeof(v) LIKE 'OBJECT%')
                THEN ${catalog}.main._fs_to_json_object(${catalog}.main._fs_as_map(v))
            WHEN typeof(v) IN ('DOUBLE', 'FLOAT') THEN printf('%.15e', TRY_CAST(v AS DOUBLE))
            WHEN typeof(v) = 'VARIANT' THEN
                CASE
                    WHEN ${catalog}.main._fs_variant_typeof(v) LIKE 'DECIMAL%' THEN
                        CAST(v AS VARCHAR)
                    WHEN ${catalog}.main._fs_variant_typeof(v) LIKE 'DOUBLE%' THEN
                        CASE
                            WHEN NOT isfinite(TRY_CAST(v AS DOUBLE)) THEN
                                CASE
                                    WHEN TRY_CAST(v AS DOUBLE) > 0 THEN 'Infinity'
                                    WHEN TRY_CAST(v AS DOUBLE) < 0 THEN '-Infinity'
                                    ELSE 'NaN'
                                END
                            ELSE printf('%.15e', TRY_CAST(v AS DOUBLE))
                        END
                    WHEN ${catalog}.main._fs_variant_typeof(v) LIKE 'HUGEINT%'
                        OR ${catalog}.main._fs_variant_typeof(v) LIKE 'UHUGEINT%' THEN
                        CAST(v AS VARCHAR)
                    WHEN ${catalog}.main._fs_variant_typeof(v) LIKE 'BLOB%'
                        OR ${catalog}.main._fs_variant_typeof(v) LIKE 'BINARY%' THEN
                        '"' || upper(hex(TRY_CAST(v AS BLOB))) || '"'
                    WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_%' THEN
                        ${catalog}.main._fs_to_json_element(v)
                    ELSE CAST(v AS JSON)::VARCHAR
                END
            ELSE CAST(v AS JSON)::VARCHAR
        END
    ) AS VARCHAR)
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get(v, key) AS (
    _fs_variant_get_py(v, key)
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get_ignore_case(v, key) AS (
    CASE
        WHEN v IS NULL OR key IS NULL THEN NULL::VARIANT
        WHEN typeof(v) LIKE 'MAP(%' THEN ${catalog}.main._fs_map_get(
            v,
            list_element(
                list_filter(
                    map_keys(CAST(v AS MAP(VARCHAR, VARIANT))),
                    k -> lower(k) = lower(key)
                ),
                len(
                    list_filter(
                        map_keys(CAST(v AS MAP(VARCHAR, VARIANT))),
                        k -> lower(k) = lower(key)
                    )
                )
            )
        )
        ELSE ${catalog}.main._fs_json_get(
            TRY_CAST(v AS JSON),
            list_element(
                list_filter(
                    COALESCE(json_keys(TRY_CAST(v AS JSON)), []::VARCHAR[]),
                    k -> lower(k) = lower(key)
                ),
                len(
                    list_filter(
                        COALESCE(json_keys(TRY_CAST(v AS JSON)), []::VARCHAR[]),
                        k -> lower(k) = lower(key)
                    )
                )
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get_path(v, path) AS (
    CASE
        WHEN v IS NULL OR path IS NULL THEN NULL
        WHEN path = '' THEN error('[FAKESNOW:100073:22000] Bad compound object''s field path name '''' in GET_PATH')
        WHEN json_type(json_extract(CAST(v AS JSON), '$.' || path)) = 'NULL'
            THEN ${catalog}.main._fs_variant_null()
        ELSE TRY_CAST(json_extract(CAST(v AS JSON), '$.' || path) AS VARIANT)
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_object_entries(v) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(v) IS NULL THEN []::STRUCT(key VARCHAR, value VARIANT)[]
        ELSE list_transform(
            map_entries(${catalog}.main._fs_as_map(v)),
            e -> struct_pack(key := e.key, value := e.value::VARIANT)
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_array(v) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL
        WHEN typeof(v) LIKE '%[]' THEN try_cast(v AS VARIANT[])
        WHEN typeof(v) = 'VARIANT' AND ${catalog}.main._fs_variant_typeof(v) LIKE 'ARRAY%'
            THEN ${catalog}.main._fs_as_list(v)
        ELSE list_append([]::VARIANT[], v::VARIANT)
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_object(v) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL::MAP(VARCHAR, VARIANT)
        WHEN typeof(v) LIKE 'MAP(%' THEN TRY_CAST(v AS MAP(VARCHAR, VARIANT))
        WHEN typeof(v) = 'VARIANT' AND ${catalog}.main._fs_variant_typeof(v) LIKE 'OBJECT%'
            THEN TRY_CAST(v AS MAP(VARCHAR, VARIANT))
        WHEN json_type(TRY_CAST(v AS JSON)) = 'OBJECT'
            THEN TRY_CAST(TRY_CAST(v AS VARIANT) AS MAP(VARCHAR, VARIANT))
        ELSE error(
            '[FAKESNOW:100071:22000] Failed to cast variant value '
            || COALESCE(
                _fs_cast_error_value_py(
                    v,
                    typeof(v),
                    CASE
                        WHEN v IS NULL THEN NULL
                        WHEN typeof(v) IN ('VARIANT', 'JSON') THEN variant_typeof(TRY_CAST(v AS VARIANT))
                        ELSE NULL
                    END
                ),
                'null'
            )
            || ' to OBJECT'
        )
    END
);
"""

_OBJECTS = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_object_validate_keys(keys) AS (
    CASE
        WHEN keys IS NULL THEN NULL::VARCHAR[]
        WHEN list_any_value(
            list_transform(
                keys,
                k -> CASE
                    WHEN k IS NOT NULL AND try_cast(k AS VARCHAR) IS NULL
                        THEN error('[FAKESNOW:2270:22000] SQL compilation error:')
                    ELSE NULL
                END
            )
        ) IS NOT NULL THEN NULL::VARCHAR[]
        WHEN len(list_filter(keys, k -> k IS NOT NULL))
            != len(list_distinct(list_filter(keys, k -> k IS NOT NULL)))
            THEN error('[FAKESNOW:100103:22000] Duplicate field key')
        ELSE keys::VARCHAR[]
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_drop_null(v) AS (
    ${catalog}.main._fs_map_as_object(
        CASE
            WHEN ${catalog}.main._fs_as_map(v) IS NULL THEN NULL
            ELSE map_from_entries(
                list_filter(
                    map_entries(${catalog}.main._fs_as_map(v)),
                    e -> e.value IS NOT NULL
                )
            )
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_keep_null(v) AS (
    ${catalog}.main._fs_map_as_object(
        CASE
            WHEN ${catalog}.main._fs_as_map(v) IS NULL THEN NULL
            ELSE map_from_entries(
                list_transform(
                    map_entries(${catalog}.main._fs_as_map(v)),
                    e -> struct_pack(
                        key := e.key,
                        value := CASE
                            WHEN e.value IS NULL THEN ${catalog}.main._fs_variant_null()
                            ELSE e.value
                        END
                    )
                )
            )
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_insert(obj, key, val, update_flag) AS (
    CASE
        WHEN obj IS NULL OR ${catalog}.main._fs_is_json_null(obj) THEN NULL
        WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN error(
            '[FAKESNOW:100071:22000] Failed to cast variant value '
            || COALESCE(TRY_CAST(obj AS JSON)::VARCHAR, 'null')
            || ' to OBJECT'
        )
        WHEN key IS NULL OR val IS NULL THEN ${catalog}.main._fs_map_as_object(${catalog}.main._fs_as_map(obj))
        WHEN try_cast(key AS VARCHAR) IS NULL
            THEN error('[FAKESNOW:2270:22000] SQL compilation error:')
        WHEN map_contains(${catalog}.main._fs_as_map(obj), CAST(key AS VARCHAR))
            AND NOT COALESCE(update_flag, false)
            THEN error(
                '[FAKESNOW:100103:22000] Duplicate field key '''
                || CAST(key AS VARCHAR)
                || ''''
            )
        ELSE ${catalog}.main._fs_map_as_object(
            map_concat(
                CASE
                    WHEN COALESCE(update_flag, false) THEN map_from_entries(
                        list_filter(
                            map_entries(${catalog}.main._fs_as_map(obj)),
                            e -> e.key != CAST(key AS VARCHAR)
                        )
                    )
                    ELSE ${catalog}.main._fs_as_map(obj)
                END,
                MAP {CAST(key AS VARCHAR): val::VARIANT}
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_delete(obj, keys) AS (
    ${catalog}.main._fs_map_as_object(
        CASE
            WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN NULL
            ELSE map_from_entries(
                list_filter(
                    map_entries(${catalog}.main._fs_as_map(obj)),
                    e -> NOT list_contains(COALESCE(keys, []::VARCHAR[]), e.key)
                )
            )
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_pick(obj, keys) AS (
    ${catalog}.main._fs_map_as_object(
        CASE
            WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN NULL
            ELSE map_from_entries(
                list_filter(
                    map_entries(${catalog}.main._fs_as_map(obj)),
                    e -> list_contains(COALESCE(keys, []::VARCHAR[]), e.key)
                )
            )
        END
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_keys(obj) AS (
    CASE
        WHEN obj IS NULL OR ${catalog}.main._fs_is_json_null(obj) THEN NULL::VARIANT[]
        WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN error(
            '[FAKESNOW:100071:22000] Failed to cast variant value '
            || COALESCE(TRY_CAST(obj AS JSON)::VARCHAR, 'null')
            || ' to OBJECT'
        )
        ELSE list_transform(list_sort(map_keys(${catalog}.main._fs_as_map(obj))), k -> k::VARIANT)
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_cat(obj_left, obj_right) AS (
    ${catalog}.main._fs_map_as_object(
        CASE
            WHEN ${catalog}.main._fs_as_map(obj_left) IS NULL OR ${catalog}.main._fs_as_map(obj_right) IS NULL THEN NULL
            ELSE map_concat(${catalog}.main._fs_as_map(obj_left), ${catalog}.main._fs_as_map(obj_right))
        END
    )
);
"""

_ARRAYS = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_array_contains(arr, value) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN CASE WHEN arr IS NULL THEN NULL ELSE false END
        WHEN value IS NULL THEN CASE
            WHEN COALESCE(
                list_bool_or(list_transform(${catalog}.main._fs_as_list(arr), x -> x IS NULL)),
                false
            ) THEN true
            ELSE NULL
        END
        ELSE COALESCE(
            list_bool_or(
                list_transform(
                    ${catalog}.main._fs_as_list(arr),
                    x -> ${catalog}.main._fs_variant_eq(x, value)
                )
            ),
            false
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_position(arr, value) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        ELSE (
            list_first(
                list_filter(
                    range(1, len(${catalog}.main._fs_as_list(arr)) + 1),
                    i -> CASE
                        WHEN value IS NULL THEN list_element(${catalog}.main._fs_as_list(arr), i) IS NULL
                        ELSE ${catalog}.main._fs_variant_eq(
                            list_element(${catalog}.main._fs_as_list(arr), i),
                            value
                        )
                    END
                )
            ) - 1
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_append(arr, value) AS (
    CASE
        WHEN arr IS NULL THEN NULL
        ELSE list_concat(${catalog}.main._fs_variant_to_array(arr), [value])
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_prepend(arr, value) AS (
    CASE
        WHEN arr IS NULL THEN NULL
        ELSE list_concat([value], ${catalog}.main._fs_variant_to_array(arr))
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_slice(arr, start, end_pos) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL OR start IS NULL OR end_pos IS NULL THEN NULL
        ELSE list_slice(
            ${catalog}.main._fs_as_list(arr),
            CASE WHEN start < 0 THEN greatest(1, len(${catalog}.main._fs_as_list(arr)) + start + 1) ELSE start + 1 END,
            CASE WHEN end_pos < 0 THEN greatest(0, len(${catalog}.main._fs_as_list(arr)) + end_pos) ELSE end_pos END
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_to_string(arr, delimiter) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL OR delimiter IS NULL THEN NULL
        ELSE COALESCE(
            list_aggr(
                list_transform(
                    ${catalog}.main._fs_as_list(arr),
                    x -> CASE
                        WHEN x IS NULL THEN ''
                        WHEN ${catalog}.main._fs_is_json_null(x)
                            THEN error('[FAKESNOW:100071:22000] Failed to cast variant value from array to string')
                        WHEN ${catalog}.main._fs_as_list(x) IS NOT NULL OR ${catalog}.main._fs_as_map(x) IS NOT NULL
                            THEN COALESCE(TRY_CAST(x AS JSON)::VARCHAR, '')
                        ELSE COALESCE(TRY_CAST(x AS VARCHAR), '')
                    END
                ),
                'string_agg',
                delimiter
            ),
            ''
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_distinct(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        ELSE list_concat(
            list_reduce(
                ${catalog}.main._fs_as_list(arr),
                (acc, x) -> CASE
                    WHEN x IS NULL THEN acc
                    WHEN COALESCE(
                        list_bool_or(list_transform(acc, y -> ${catalog}.main._fs_variant_eq(x, y))),
                        false
                    ) THEN acc
                    WHEN ${catalog}.main._fs_is_json_null(x) THEN
                        list_append(acc, ${catalog}.main._fs_variant_null())
                    ELSE list_append(acc, x)
                END,
                []::VARIANT[]
            ),
            CASE
                WHEN COALESCE(
                    list_bool_or(
                        list_transform(
                            ${catalog}.main._fs_as_list(arr),
                            x -> x IS NULL
                        )
                    ),
                    false
                ) THEN [NULL]::VARIANT[]
                ELSE []::VARIANT[]
            END
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_flatten(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        WHEN COALESCE(
            list_bool_or(
                list_transform(
                    ${catalog}.main._fs_as_list(arr),
                    x -> x IS NULL
                )
            ),
            false
        ) THEN NULL
        WHEN COALESCE(
            list_bool_or(
                list_transform(
                    ${catalog}.main._fs_as_list(arr),
                    x -> ${catalog}.main._fs_as_list(x) IS NULL
                )
            ),
            false
        ) THEN error(
            '[FAKESNOW:100107:22000] Not an array: ''Input argument to ARRAY_FLATTEN is not an array of arrays'''
        )
        ELSE list_reduce(
            list_transform(${catalog}.main._fs_as_list(arr), x -> ${catalog}.main._fs_as_list(x)),
            (a, b) -> list_concat(a, b),
            []::VARIANT[]
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_sort(arr, ascending, nulls_first) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        WHEN COALESCE(ascending, true) THEN list_transform(
            list_sort(
                list_transform(
                    ${catalog}.main._fs_as_list(arr),
                    x -> struct_pack(key := ${catalog}.main._fs_variant_sort_key(x), value := x)
                )
            ),
            x -> x.value
        )
        ELSE list_transform(
            list_reverse(
                list_sort(
                    list_transform(
                        ${catalog}.main._fs_as_list(arr),
                        x -> struct_pack(key := ${catalog}.main._fs_variant_sort_key(x), value := x)
                    )
                )
            ),
            x -> x.value
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_max(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        WHEN len(
            list_filter(
                ${catalog}.main._fs_as_list(arr),
                x -> x IS NOT NULL AND NOT ${catalog}.main._fs_is_json_null(x)
            )
        ) = 0 THEN NULL
        ELSE list_reduce(
            list_filter(
                ${catalog}.main._fs_as_list(arr),
                x -> x IS NOT NULL AND NOT ${catalog}.main._fs_is_json_null(x)
            ),
            (a, b) -> CASE
                WHEN variant_comparator(a) > variant_comparator(b) THEN a
                ELSE b
            END
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_min(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        WHEN len(
            list_filter(
                ${catalog}.main._fs_as_list(arr),
                x -> x IS NOT NULL AND NOT ${catalog}.main._fs_is_json_null(x)
            )
        ) = 0 THEN NULL
        ELSE list_reduce(
            list_filter(
                ${catalog}.main._fs_as_list(arr),
                x -> x IS NOT NULL AND NOT ${catalog}.main._fs_is_json_null(x)
            ),
            (a, b) -> CASE
                WHEN variant_comparator(a) < variant_comparator(b) THEN a
                ELSE b
            END
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_remove(arr, value) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL OR value IS NULL THEN NULL
        ELSE list_filter(
            ${catalog}.main._fs_as_list(arr),
            x -> NOT ${catalog}.main._fs_variant_eq(x, value)
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_insert(arr, position, value) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL OR position IS NULL THEN NULL
        WHEN position >= len(${catalog}.main._fs_as_list(arr)) THEN list_concat(
            ${catalog}.main._fs_as_list(arr),
            list_transform(
                range(1, (position - len(${catalog}.main._fs_as_list(arr))) + 1),
                i -> NULL::VARIANT
            ),
            [value]
        )
        ELSE list_concat(
            list_slice(
                ${catalog}.main._fs_as_list(arr),
                1,
                CASE
                    WHEN position < 0 THEN greatest(0, len(${catalog}.main._fs_as_list(arr)) + position)
                    ELSE position
                END
            ),
            [value],
            list_slice(
                ${catalog}.main._fs_as_list(arr),
                CASE
                    WHEN position < 0 THEN greatest(1, len(${catalog}.main._fs_as_list(arr)) + position + 1)
                    ELSE position + 1
                END,
                len(${catalog}.main._fs_as_list(arr))
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_compact(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        ELSE list_filter(
            ${catalog}.main._fs_as_list(arr),
            x -> x IS NOT NULL AND NOT ${catalog}.main._fs_is_json_null(x)
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_cat(arr_left, arr_right) AS (
    CASE
        WHEN arr_left IS NULL OR arr_right IS NULL THEN NULL
        WHEN ${catalog}.main._fs_as_list(arr_left) IS NULL
            THEN error('[FAKESNOW:100098:22000] Left argument of ARRAY_CAT is not an array')
        WHEN ${catalog}.main._fs_as_list(arr_right) IS NULL
            THEN error('[FAKESNOW:100098:22000] Right argument of ARRAY_CAT is not an array')
        ELSE list_concat(${catalog}.main._fs_as_list(arr_left), ${catalog}.main._fs_as_list(arr_right))
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_except(arr_left, arr_right) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr_left) IS NULL OR ${catalog}.main._fs_as_list(arr_right) IS NULL THEN NULL
        ELSE list_filter(
            ${catalog}.main._fs_as_list(arr_left),
            (l, i) -> len(
                list_filter(
                    list_slice(${catalog}.main._fs_as_list(arr_left), 1, i),
                    x -> ${catalog}.main._fs_variant_eq(x, l)
                )
            ) > len(
                list_filter(
                    ${catalog}.main._fs_as_list(arr_right),
                    r -> ${catalog}.main._fs_variant_eq(r, l)
                )
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_intersection(arr_left, arr_right) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr_left) IS NULL OR ${catalog}.main._fs_as_list(arr_right) IS NULL THEN NULL
        ELSE list_filter(
            ${catalog}.main._fs_as_list(arr_left),
            (l, i) -> len(
                list_filter(
                    list_slice(${catalog}.main._fs_as_list(arr_left), 1, i),
                    x -> ${catalog}.main._fs_variant_eq(x, l)
                )
            ) <= len(
                list_filter(
                    ${catalog}.main._fs_as_list(arr_right),
                    r -> ${catalog}.main._fs_variant_eq(r, l)
                )
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_arrays_overlap(arr_left, arr_right) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr_left) IS NULL OR ${catalog}.main._fs_as_list(arr_right) IS NULL THEN NULL
        ELSE COALESCE(
            list_bool_or(
                list_transform(
                    ${catalog}.main._fs_as_list(arr_left),
                    l -> COALESCE(
                        list_bool_or(
                            list_transform(
                                ${catalog}.main._fs_as_list(arr_right),
                                r -> ${catalog}.main._fs_variant_eq(l, r)
                            )
                        ),
                        false
                    )
                )
            ),
            false
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_arrays_zip(arr_left, arr_right) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr_left) IS NULL
            OR ${catalog}.main._fs_as_list(arr_right) IS NULL THEN NULL
        ELSE list_transform(
            range(
                1,
                greatest(
                    len(${catalog}.main._fs_as_list(arr_left)),
                    len(${catalog}.main._fs_as_list(arr_right))
                ) + 1
            ),
            i -> CAST(to_json(MAP {
                '$1': CASE
                    WHEN i <= len(${catalog}.main._fs_as_list(arr_left))
                        THEN list_element(${catalog}.main._fs_as_list(arr_left), i)
                    ELSE ${catalog}.main._fs_variant_null()
                END,
                '$2': CASE
                    WHEN i <= len(${catalog}.main._fs_as_list(arr_right))
                        THEN list_element(${catalog}.main._fs_as_list(arr_right), i)
                    ELSE ${catalog}.main._fs_variant_null()
                END
            }) AS VARIANT)
        )
    END
);
"""

_CASTS = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_cast_failed(v, target) AS (
    error(
        '[FAKESNOW:100071:22000] Failed to cast variant value '
        || COALESCE(
            _fs_cast_error_value_py(
                v,
                typeof(v),
                CASE
                    WHEN v IS NULL THEN NULL
                    WHEN typeof(v) IN ('VARIANT', 'JSON') THEN variant_typeof(TRY_CAST(v AS VARIANT))
                    ELSE NULL
                END
            ),
            'null'
        )
        || ' to '
        || target
    )
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_varchar(v) AS (
    _fs_variant_to_varchar_py(
        v,
        typeof(v),
        CASE
            WHEN v IS NULL THEN NULL
            WHEN typeof(v) IN ('VARIANT', 'JSON') THEN variant_typeof(TRY_CAST(v AS VARIANT))
            ELSE NULL
        END
    )
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_boolean(v) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL
        WHEN ${catalog}.main._fs_typeof(v) = 'BOOLEAN' THEN TRY_CAST(v AS BOOLEAN)
        WHEN ${catalog}.main._fs_typeof(v) = 'VARCHAR'
            AND TRY_CAST(v AS BOOLEAN) IS NOT NULL THEN TRY_CAST(v AS BOOLEAN)
        ELSE ${catalog}.main._fs_variant_cast_failed(v, 'BOOLEAN')
    END
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_decimal(v, precision, scale) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL
        WHEN ${catalog}.main._fs_typeof(v) IN ('INTEGER', 'DECIMAL', 'DOUBLE', 'BOOLEAN')
            AND TRY_CAST(round(TRY_CAST(v AS DOUBLE), COALESCE(scale, 0)) AS DECIMAL(38, 18)) IS NOT NULL
            THEN TRY_CAST(round(TRY_CAST(v AS DOUBLE), COALESCE(scale, 0)) AS DECIMAL(38, 18))
        WHEN ${catalog}.main._fs_typeof(v) = 'VARCHAR'
            AND TRY_CAST(TRY_CAST(v AS VARCHAR) AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(round(TRY_CAST(v AS DOUBLE), COALESCE(scale, 0)) AS DECIMAL(38, 18))
        ELSE ${catalog}.main._fs_variant_cast_failed(v, 'FIXED')
    END
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_bigint(v) AS (
    _fs_variant_to_bigint_py(
        v,
        typeof(v),
        CASE
            WHEN v IS NULL THEN NULL
            WHEN typeof(v) IN ('VARIANT', 'JSON') THEN variant_typeof(TRY_CAST(v AS VARIANT))
            ELSE NULL
        END
    )
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_double(v) AS (
    _fs_variant_to_double_py(
        v,
        typeof(v),
        CASE
            WHEN v IS NULL THEN NULL
            WHEN typeof(v) IN ('VARIANT', 'JSON') THEN variant_typeof(TRY_CAST(v AS VARIANT))
            ELSE NULL
        END
    )
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_date(v) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL
        WHEN TRY_CAST(v AS DATE) IS NOT NULL
            AND ${catalog}.main._fs_typeof(v) IN ('DATE', 'VARCHAR', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ')
            THEN TRY_CAST(v AS DATE)
        ELSE ${catalog}.main._fs_variant_cast_failed(v, 'DATE')
    END
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_time(v) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL
        WHEN TRY_CAST(v AS TIME) IS NOT NULL THEN TRY_CAST(v AS TIME)
        ELSE ${catalog}.main._fs_variant_cast_failed(v, 'TIME')
    END
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_timestamp(v) AS (
    CASE
        WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_%' THEN TRY_CAST(
            regexp_replace(
                regexp_replace(
                    regexp_replace(TRY_CAST(v AS VARCHAR), '^__FAKESNOW_TIMESTAMP_[A-Z]+__', ''),
                    ' Z$',
                    ''
                ),
                ' ([+-][0-9]{4})$',
                ''
            ) AS TIMESTAMP
        )
        -- a numeric variant is epoch seconds, as it is for a cast from a number or numeric string
        WHEN TRY_CAST(TRY_CAST(v AS VARCHAR) AS BIGINT) IS NOT NULL
            THEN CAST(to_timestamp(TRY_CAST(TRY_CAST(v AS VARCHAR) AS BIGINT)) AS TIMESTAMP)
        ELSE TRY_CAST(v AS TIMESTAMP)
    END
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_binary(v) AS (
    _fs_variant_to_binary_py(v)
);
"""

_FLATTEN = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_flatten_json_object_level(v, prefix, seq) AS (
    list_transform(
        list_sort(COALESCE(json_keys(TRY_CAST(v AS JSON)), []::VARCHAR[])),
        k -> struct_pack(
            seq := seq,
            key := k,
            path := CASE WHEN prefix = '' THEN k ELSE prefix || '.' || k END,
            index := NULL::BIGINT,
            value := ${catalog}.main._fs_json_get(TRY_CAST(v AS JSON), k),
            this := TRY_CAST(v AS VARIANT)
        )
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_flatten_map_level(m, prefix, seq) AS (
    list_transform(
        list_sort(map_keys(m)),
        k -> struct_pack(
            seq := seq,
            key := k,
            path := CASE WHEN prefix = '' THEN k ELSE prefix || '.' || k END,
            index := NULL::BIGINT,
            value := list_element(map_extract(m, k), 1),
            this := CAST(to_json(m) AS VARIANT)
        )
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_flatten_array_level(a, prefix, seq) AS (
    list_filter(
        list_transform(
            range(1, len(a) + 1),
            i -> struct_pack(
                seq := seq,
                key := NULL::VARCHAR,
                path := prefix || '[' || (i - 1)::VARCHAR || ']',
                index := (i - 1)::BIGINT,
                value := list_element(a, i),
                this := a::VARIANT
            )
        ),
        r -> NOT (r.value IS NULL AND NOT ${catalog}.main._fs_is_json_null(r.value))
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_flatten_outer_row(prefix, seq, target) AS (
    struct_pack(
        seq := seq,
        key := NULL::VARCHAR,
        path := CASE WHEN prefix = '' THEN '' ELSE prefix END,
        index := NULL::BIGINT,
        value := NULL::VARIANT,
        this := target
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_flatten_rows_impl(
    target, prefix, is_outer, is_recursive, mode, seq
) AS (
    CASE
        WHEN target IS NULL THEN
            CASE
                WHEN COALESCE(is_outer, false) THEN [${catalog}.main._fs_flatten_outer_row(prefix, seq, target)]
                ELSE []::STRUCT(
                    seq UBIGINT,
                    key VARCHAR,
                    path VARCHAR,
                    index BIGINT,
                    value VARIANT,
                    this VARIANT
                )[]
            END
        WHEN typeof(target) LIKE 'MAP(%' AND mode IN ('BOTH', 'OBJECT') THEN
            ${catalog}.main._fs_flatten_map_level(CAST(target AS MAP(VARCHAR, VARIANT)), prefix, seq)
        WHEN ${catalog}.main._fs_variant_typeof(target) LIKE 'OBJECT%' AND mode IN ('BOTH', 'OBJECT') THEN
            ${catalog}.main._fs_flatten_json_object_level(target, prefix, seq)
        WHEN ${catalog}.main._fs_as_list(target) IS NOT NULL AND mode IN ('BOTH', 'ARRAY') THEN
            ${catalog}.main._fs_flatten_array_level(${catalog}.main._fs_as_list(target), prefix, seq)
        WHEN COALESCE(is_outer, false) THEN [${catalog}.main._fs_flatten_outer_row(prefix, seq, target)]
        ELSE []::STRUCT(
            seq UBIGINT,
            key VARCHAR,
            path VARCHAR,
            index BIGINT,
            value VARIANT,
            this VARIANT
        )[]
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_flatten_map_rows(
    value, path_arg, mode_arg, seq
) AS (
    CASE
        WHEN value IS NULL OR upper(COALESCE(mode_arg, 'BOTH')) NOT IN ('BOTH', 'OBJECT') THEN
            []::STRUCT(
                seq UBIGINT,
                key VARCHAR,
                path VARCHAR,
                index BIGINT,
                value BIGINT,
                this MAP(VARCHAR, BIGINT)
            )[]
        ELSE list_transform(
            list_sort(map_keys(value)),
            k -> struct_pack(
                seq := seq,
                key := k,
                path := CASE
                    WHEN path_arg IS NULL OR path_arg = '' THEN k
                    ELSE path_arg || '.' || k
                END,
                index := NULL::BIGINT,
                value := list_element(map_extract(value, k), 1),
                this := value
            )
        )
    END
);
"""


def _macro_sql(catalog: str) -> str:
    sections = (_HELPERS, _CORE, _OBJECTS, _ARRAYS, _CASTS, _FLATTEN)
    return "\n".join(sections).replace("${catalog}", catalog).replace("${variant_null}", _VARIANT_NULL)


def creation_sql(catalog: str) -> str:
    return _macro_sql(catalog)


def global_creation_sql() -> str:
    return _macro_sql("_fs_global")
