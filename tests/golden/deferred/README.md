# Deferred parity cases

Golden cases captured from live Snowflake that no branch in the native-VARIANT stack
makes pass. They are kept here so the capture isn't lost and the remaining gap is
concrete, but the loader in `tests/parity.py` only globs `pr*/pr*.json`, so nothing
here runs.

When a case starts passing, move it into the `tests/golden/prNN/` directory of the
branch that makes it pass. Nothing should be added here to silence a failure.

| file | cases | what's missing |
| --- | --- | --- |
| `function_coverage_values_individual` | 34 | scattered function-level value gaps |
| `structured_expressions_individual` | 18 | structured `ARRAY(T)` / `OBJECT(...)` expression results |
| `structured_types_expressions` | 16 | structured type expression results and metadata |
| `coercion_via_ctas_with_schema_full_errors` | 11 | 5 hardcode the capture database in the DML error envelope; the rest are message-text gaps |
| `insert_coercion_semantics` | 11 | the batch is stateful and its setup cases were themselves deferred |
| `structured_types_ddl_metadata_temp_table` | 9 | `NOT NULL` is dropped when `OBJECT(...)` is rewritten to `STRUCT(...)` |
| `function_coverage_values` | 6 | scattered function-level value gaps |
| `description_type_codes_individual` | 1 | `HASH()` is a proprietary algorithm, so values can't match |
| `comparisons_arithmetic_individually` | 1 | `HASH()`, as above |
| `description_type_codes_for_expressions` | 1 | fakesnow raises a GROUP BY binder error where Snowflake raises a compile error |

Two of these need a fixture change rather than a code change: the `HASH()` cases can
only ever assert `.description`, not the value, and the five CTAS cases need the error
envelope to stop naming `TEST_DB.PUBLIC.FAKESNOW_C20`.
