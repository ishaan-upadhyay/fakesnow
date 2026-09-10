from __future__ import annotations

_VARIANT_NULL = "variant_bytes_to_variant(from_hex('01000000'))"

_HELPERS = """
LOAD parquet;

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_null() AS (${variant_null});

CREATE OR REPLACE MACRO ${catalog}.main._fs_as_variant(v) AS (
    CASE
        WHEN v IS NULL THEN NULL::VARIANT
        WHEN typeof(v) LIKE 'MAP(%' THEN v::MAP(VARCHAR, VARIANT)::VARIANT
        WHEN typeof(v) LIKE '%[]' THEN v::VARIANT[]::VARIANT
        ELSE v::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_as_list(v) AS (
    CASE
        WHEN v IS NULL THEN NULL
        WHEN typeof(v) LIKE '%[]' THEN v::VARIANT[]
        WHEN try_cast(v AS VARIANT[]) IS NOT NULL THEN try_cast(v AS VARIANT[])
        WHEN typeof(v) = 'VARIANT' AND variant_typeof(v::VARIANT) LIKE 'ARRAY%' THEN try_cast(v AS VARIANT[])
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_as_map(v) AS (
    CASE
        WHEN v IS NULL THEN NULL
        WHEN typeof(v) LIKE 'MAP(%' THEN v::MAP(VARCHAR, VARIANT)
        WHEN try_cast(v AS MAP(VARCHAR, VARIANT)) IS NOT NULL THEN try_cast(v AS MAP(VARCHAR, VARIANT))
        WHEN typeof(v) = 'VARIANT' AND variant_typeof(v::VARIANT) LIKE 'OBJECT%'
            THEN try_cast(v AS MAP(VARCHAR, VARIANT))
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_is_json_null(v) AS (
    v IS NOT NULL
    AND typeof(v) = 'VARIANT'
    AND variant_typeof(v::VARIANT) = 'VARIANT_NULL'
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_typeof(v) AS (
    CASE
        WHEN v IS NULL THEN NULL
        WHEN variant_typeof(v) = 'VARIANT_NULL' THEN 'NULL_VALUE'
        WHEN variant_typeof(v) LIKE 'INT32%' OR variant_typeof(v) LIKE 'INT64%' THEN 'INTEGER'
        WHEN variant_typeof(v) LIKE 'BOOL%' THEN 'BOOLEAN'
        WHEN variant_typeof(v) LIKE 'ARRAY%' THEN 'ARRAY'
        WHEN typeof(v) LIKE 'MAP(%' OR variant_typeof(v) LIKE 'OBJECT%' THEN 'OBJECT'
        WHEN variant_typeof(v) LIKE 'VARCHAR%' THEN 'VARCHAR'
        WHEN variant_typeof(v) LIKE 'DOUBLE%' OR variant_typeof(v) LIKE 'FLOAT%' THEN 'DOUBLE'
        WHEN variant_typeof(v) LIKE 'DECIMAL%' THEN 'DECIMAL'
        WHEN variant_typeof(v) LIKE 'BLOB%' OR variant_typeof(v) LIKE 'BINARY%' THEN 'BINARY'
        WHEN variant_typeof(v) LIKE 'DATE%' THEN 'DATE'
        WHEN variant_typeof(v) LIKE 'TIME%' THEN 'TIME'
        WHEN variant_typeof(v) LIKE 'TIMESTAMP WITH TIME ZONE%' THEN 'TIMESTAMP_TZ'
        WHEN variant_typeof(v) LIKE 'TIMESTAMP%' THEN
            CASE
                WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_LTZ__%' THEN 'TIMESTAMP_LTZ'
                WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_TZ__%' THEN 'TIMESTAMP_TZ'
                WHEN TRY_CAST(v AS VARCHAR) LIKE '__FAKESNOW_TIMESTAMP_NTZ__%' THEN 'TIMESTAMP_NTZ'
                ELSE 'TIMESTAMP_NTZ'
            END
        ELSE variant_typeof(v)
    END
);
"""

_CORE = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_parse_json(val) AS (
    CASE
        WHEN val IS NULL THEN NULL
        WHEN TRY_CAST(val AS JSON) IS NULL THEN NULL
        WHEN json_type(TRY_CAST(val AS JSON)) = 'NULL' THEN ${catalog}.main._fs_variant_null()
        ELSE TRY_CAST(val AS JSON)::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_to_variant_timestamp(val, kind) AS (
    CASE upper(kind)
        WHEN 'LTZ' THEN CAST('__FAKESNOW_TIMESTAMP_LTZ__' || CAST(val AS VARCHAR) || ' Z' AS VARCHAR)::VARIANT
        WHEN 'TZ' THEN CAST('__FAKESNOW_TIMESTAMP_TZ__' || CAST(val AS VARCHAR) AS VARCHAR)::VARIANT
        ELSE CAST('__FAKESNOW_TIMESTAMP_NTZ__' || CAST(val AS VARCHAR) AS VARCHAR)::VARIANT
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_key(v) AS (variant_comparator(v));

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_eq(a, b) AS (
    (a IS NULL AND b IS NULL)
    OR (
        a IS NOT NULL
        AND b IS NOT NULL
        AND variant_comparator(a) = variant_comparator(b)
    )
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_eq_sql(a, b) AS (${catalog}.main._fs_variant_eq(a, b));

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get_index(v, key) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(v) IS NULL OR try_cast(key AS UINTEGER) IS NULL THEN NULL
        WHEN CAST(key AS UINTEGER) < 0 THEN NULL
        WHEN CAST(key AS UINTEGER) + 1 > len(${catalog}.main._fs_as_list(v)) THEN NULL
        ELSE list_element(${catalog}.main._fs_as_list(v), CAST(key AS UINTEGER) + 1)
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get(v, key) AS (
    CASE
        WHEN v IS NULL OR key IS NULL THEN NULL
        WHEN ${catalog}.main._fs_as_map(v) IS NOT NULL THEN
            list_element(
                map_extract(
                    ${catalog}.main._fs_as_map(v),
                    TRY_CAST(key AS VARCHAR)
                ),
                1
            )
        WHEN ${catalog}.main._fs_as_list(v) IS NOT NULL THEN
            ${catalog}.main._fs_variant_get_index(v, key)
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_get_ignore_case(v, key) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(v) IS NULL OR key IS NULL THEN NULL
        ELSE map_extract(
            ${catalog}.main._fs_as_map(v),
            list_any_value(
                list_filter(
                    map_keys(${catalog}.main._fs_as_map(v)),
                    k -> lower(k) = lower(key)
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
        WHEN ${catalog}.main._fs_as_list(v) IS NOT NULL THEN ${catalog}.main._fs_as_list(v)
        WHEN ${catalog}.main._fs_as_map(v) IS NOT NULL THEN [v::VARIANT]
        ELSE [v]
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_object(v) AS (
    CASE
        WHEN v IS NULL OR ${catalog}.main._fs_is_json_null(v) THEN NULL
        WHEN ${catalog}.main._fs_as_map(v) IS NOT NULL THEN ${catalog}.main._fs_as_map(v)
        ELSE error(
            '[FAKESNOW:100071:22000] Failed to cast variant value '
            || COALESCE(TRY_CAST(v AS JSON)::VARCHAR, 'null')
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
    CASE
        WHEN ${catalog}.main._fs_as_map(v) IS NULL THEN NULL
        ELSE map_from_entries(
            list_filter(
                map_entries(${catalog}.main._fs_as_map(v)),
                e -> e.value IS NOT NULL
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_keep_null(v) AS (
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
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_insert(obj, key, val, update_flag) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN NULL
        WHEN key IS NULL OR val IS NULL THEN ${catalog}.main._fs_as_map(obj)
        WHEN try_cast(key AS VARCHAR) IS NULL
            THEN error('[FAKESNOW:2270:22000] SQL compilation error:')
        WHEN map_contains(${catalog}.main._fs_as_map(obj), CAST(key AS VARCHAR))
            AND NOT COALESCE(update_flag, false)
            THEN error(
                '[FAKESNOW:100103:22000] Duplicate field key '''
                || CAST(key AS VARCHAR)
                || ''''
            )
        ELSE map_concat(
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
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_delete(obj, keys) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN NULL
        ELSE map_from_entries(
            list_filter(
                map_entries(${catalog}.main._fs_as_map(obj)),
                e -> NOT list_contains(COALESCE(keys, []::VARCHAR[]), e.key)
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_pick(obj, keys) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN NULL
        ELSE map_from_entries(
            list_filter(
                map_entries(${catalog}.main._fs_as_map(obj)),
                e -> list_contains(COALESCE(keys, []::VARCHAR[]), e.key)
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_keys(obj) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(obj) IS NULL THEN NULL::VARIANT[]
        ELSE list_transform(list_sort(map_keys(${catalog}.main._fs_as_map(obj))), k -> k::VARIANT)
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_object_cat(obj_left, obj_right) AS (
    CASE
        WHEN ${catalog}.main._fs_as_map(obj_left) IS NULL OR ${catalog}.main._fs_as_map(obj_right) IS NULL THEN NULL
        ELSE map_concat(${catalog}.main._fs_as_map(obj_left), ${catalog}.main._fs_as_map(obj_right))
    END
);
"""

_ARRAYS = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_array_contains(arr, value) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN CASE WHEN arr IS NULL THEN NULL ELSE false END
        WHEN value IS NULL THEN list_any_value(
            list_transform(${catalog}.main._fs_as_list(arr), x -> x IS NULL)
        )
        ELSE list_any_value(
            list_transform(
                ${catalog}.main._fs_as_list(arr),
                x -> ${catalog}.main._fs_variant_eq(x, value)
            )
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
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        ELSE list_concat(${catalog}.main._fs_as_list(arr), [value])
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_prepend(arr, value) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        ELSE list_concat([value], ${catalog}.main._fs_as_list(arr))
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_slice(arr, start, end_pos) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL OR start IS NULL OR end_pos IS NULL THEN NULL
        ELSE list_slice(
            ${catalog}.main._fs_as_list(arr),
            CASE WHEN start < 0 THEN greatest(1, len(${catalog}.main._fs_as_list(arr)) + start + 1) ELSE start + 1 END,
            CASE WHEN end_pos < 0 THEN greatest(0, len(${catalog}.main._fs_as_list(arr)) + end_pos + 1) ELSE end_pos END
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_to_string(arr, delimiter) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL OR delimiter IS NULL THEN NULL
        ELSE list_aggr(
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
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_distinct(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        ELSE list_distinct(${catalog}.main._fs_as_list(arr))
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_flatten(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
        WHEN list_any_value(
            list_transform(
                ${catalog}.main._fs_as_list(arr),
                x -> x IS NULL OR ${catalog}.main._fs_as_list(x) IS NULL
            )
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
        WHEN COALESCE(ascending, true) THEN list_sort(${catalog}.main._fs_as_list(arr))
        ELSE list_reverse(list_sort(${catalog}.main._fs_as_list(arr)))
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_array_max(arr) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr) IS NULL THEN NULL
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
            l -> NOT list_any_value(
                list_transform(
                    ${catalog}.main._fs_as_list(arr_right),
                    r -> ${catalog}.main._fs_variant_eq(l, r)
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
            l -> list_any_value(
                list_transform(
                    ${catalog}.main._fs_as_list(arr_right),
                    r -> ${catalog}.main._fs_variant_eq(l, r)
                )
            )
        )
    END
);

CREATE OR REPLACE MACRO ${catalog}.main._fs_arrays_overlap(arr_left, arr_right) AS (
    CASE
        WHEN ${catalog}.main._fs_as_list(arr_left) IS NULL OR ${catalog}.main._fs_as_list(arr_right) IS NULL THEN NULL
        ELSE list_any_value(
            list_transform(
                ${catalog}.main._fs_as_list(arr_left),
                l -> list_any_value(
                    list_transform(
                        ${catalog}.main._fs_as_list(arr_right),
                        r -> ${catalog}.main._fs_variant_eq(l, r)
                    )
                )
            )
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
            i -> MAP {
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
            }::VARIANT
        )
    END
);
"""

_CASTS = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_varchar(v) AS (
    CASE
        WHEN ${catalog}.main._fs_typeof(v) IN ('ARRAY', 'OBJECT')
            THEN json_pretty(TRY_CAST(v AS JSON))
        ELSE TRY_CAST(v AS VARCHAR)
    END
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_boolean(v) AS (TRY_CAST(v AS BOOLEAN));
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_decimal(v, precision, scale) AS (
    TRY_CAST(round(TRY_CAST(v AS DOUBLE), COALESCE(scale, 0)) AS DECIMAL(38, 18))
);
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_bigint(v) AS (TRY_CAST(v AS BIGINT));
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_double(v) AS (TRY_CAST(v AS DOUBLE));
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_date(v) AS (TRY_CAST(v AS DATE));
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_time(v) AS (TRY_CAST(v AS TIME));
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
CREATE OR REPLACE MACRO ${catalog}.main._fs_variant_to_binary(v) AS (TRY_CAST(v AS BLOB));
"""

_FLATTEN = """
CREATE OR REPLACE MACRO ${catalog}.main._fs_flatten_map_level(m, prefix, seq) AS (
    list_transform(
        list_sort(map_keys(m)),
        k -> struct_pack(
            seq := seq,
            key := k,
            path := CASE WHEN prefix = '' THEN k ELSE prefix || '.' || k END,
            index := NULL::BIGINT,
            value := list_element(map_extract(m, k), 1),
            this := m::VARIANT
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
        WHEN ${catalog}.main._fs_as_map(target) IS NOT NULL AND mode IN ('BOTH', 'OBJECT') THEN
            ${catalog}.main._fs_flatten_map_level(${catalog}.main._fs_as_map(target), prefix, seq)
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
                value := map_extract(value, k),
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
