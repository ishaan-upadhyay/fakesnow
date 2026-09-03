from string import Template

# emulate the Snowflake FLATTEN function for ARRAYs and OBJECTTs
# see https://docs.snowflake.com/en/sql-reference/functions/flatten.html
FS_FLATTEN = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_flatten(input, path_arg, is_outer, is_recursive, mode_arg, seq_arg := 1) AS TABLE
    SELECT
        e.seq AS SEQ,
        e.key AS KEY,
        e.path AS PATH,
        e.index AS INDEX,
        e.value AS VALUE,
        e.this AS THIS
    FROM (
        SELECT UNNEST(
            _fs_variant_flatten_rows(
                CAST(input AS VARIANT),
                path_arg,
                is_outer,
                is_recursive,
                mode_arg,
                seq_arg::UBIGINT
            ),
            recursive := true
        )
    ) AS e(seq, key, path, index, value, this)
    """
)

FS_FLATTEN_ARRAY = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_flatten_array(
    input, path_arg, is_outer, is_recursive, mode_arg, seq_arg := 1
) AS TABLE
    SELECT
        seq_arg::UBIGINT AS SEQ,
        NULL::VARCHAR AS KEY,
        (CASE WHEN path_arg = '' THEN '' ELSE path_arg END) || '[' || (e.index - 1) || ']' AS PATH,
        (e.index - 1)::BIGINT AS INDEX,
        e.value AS VALUE,
        input AS THIS
    FROM UNNEST(input) WITH ORDINALITY AS e(value, index)
    WHERE UPPER(mode_arg) IN ('ARRAY', 'BOTH')
    """
)

FS_FLATTEN_MAP = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_flatten_map(
    input, path_arg, is_outer, is_recursive, mode_arg, seq_arg := 1
) AS TABLE
    SELECT
        e.seq AS SEQ,
        e.key AS KEY,
        e.path AS PATH,
        e.index AS INDEX,
        e.value AS VALUE,
        e.this AS THIS
    FROM (
        SELECT UNNEST(
            _fs_variant_flatten_map_rows(
                input,
                path_arg,
                mode_arg,
                seq_arg::UBIGINT
            ),
            recursive := true
        )
    ) AS e(seq, key, path, index, value, this)
    """
)

FS_OBJECT_CONSTRUCT = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_object_construct(keys, vals, keep_nulls) AS (
    WITH kv AS (
        SELECT key, list_extract(vals, idx) AS value
        FROM UNNEST(_fs_object_validate_keys(keys)) WITH ORDINALITY AS u(key, idx)
        ORDER BY idx
    )
    SELECT CASE
        WHEN count(*) FILTER (
            WHERE key IS NOT NULL AND (keep_nulls OR value IS NOT NULL)
        ) = 0 THEN map()
        ELSE map_from_entries(
            list(struct_pack(key := key, value := value::VARIANT) ORDER BY key)
            FILTER (WHERE key IS NOT NULL AND (keep_nulls OR value IS NOT NULL))
        )
    END
    FROM kv
);
"""
)

FS_TO_TIMESTAMP = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_to_timestamp(val, scale) AS (
    CASE
        WHEN try_cast(val AS BIGINT) IS NOT NULL
            THEN
                CASE
                    WHEN scale = 0 THEN cast(to_timestamp(val::BIGINT) as TIMESTAMP)
                    WHEN scale = 3 THEN cast(to_timestamp(val::BIGINT / 1000) as TIMESTAMP)
                    WHEN scale = 6 THEN cast(to_timestamp(val::BIGINT / 1000000) as TIMESTAMP)
                    WHEN scale = 9 THEN cast(to_timestamp(val::BIGINT / 1000000000) as TIMESTAMP)
                    ELSE NULL
                END
        ELSE CAST(val AS TIMESTAMP)
    END
);
"""
)


FS_HAVERSINE = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_haversine(lat1, lon1, lat2, lon2) AS (
    2 * 6371 * ASIN(SQRT(
        POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
        COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
    ))
);
"""
)


def creation_sql(catalog: str) -> str:
    return f"""
        {FS_FLATTEN.substitute(catalog=catalog)};
        {FS_FLATTEN_ARRAY.substitute(catalog=catalog)};
        {FS_FLATTEN_MAP.substitute(catalog=catalog)};
        {FS_HAVERSINE.substitute(catalog=catalog)};
        {FS_OBJECT_CONSTRUCT.substitute(catalog=catalog)};
        {FS_TO_TIMESTAMP.substitute(catalog=catalog)};
    """
