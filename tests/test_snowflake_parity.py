from __future__ import annotations

from typing import Any

import pytest
import snowflake.connector

from tests.parity import compare_results, load_fixtures, run_fakesnow, run_snowflake

_ACCEPTED_FAKESNOW_GAPS = {
    (
        "pr07_misc",
        "array_construct_array_construct_null",
    ): "DuckDB collapses nested undefined when VARIANT[] is cast to VARIANT for nesting",
    (
        "pr09_arrays_null_vs_undefined",
        "select_array_construct_null_0_e0_array_construct_null_0_is_n",
    ): "DuckDB collapses nested undefined when VARIANT[] is cast to VARIANT for nesting",
    (
        "pr09_arrays_null_vs_undefined",
        "select_f_from_table_flatten_input_array_construct_null_1_par",
    ): "DuckDB collapses nested undefined when VARIANT[] is cast to VARIANT for nesting",
    (
        "pr09_function_coverage_values_individual",
        "object_construct_a_array_construct_null_b_object_construct",
    ): "DuckDB collapses nested undefined when VARIANT[] is cast to VARIANT for nesting",
    (
        "pr07_misc",
        "parse_json_123456789012345678901234567890_123456789",
    ): "DuckDB VARIANT cannot exactly represent a 39-digit fixed-point number; DECIMAL is limited to precision 38",
}


def _params() -> list[Any]:
    return [
        pytest.param(
            fixture,
            case,
            id=f"{fixture['name']}::{case['id']}",
        )
        for fixture in load_fixtures()
        for case in fixture["cases"]
    ]


@pytest.mark.parametrize(("fixture", "case"), _params())
def test_fakesnow_parity(
    conn: snowflake.connector.SnowflakeConnection,
    fixture: dict[str, Any],
    case: dict[str, Any],
) -> None:
    if reason := _ACCEPTED_FAKESNOW_GAPS.get((fixture["name"], case["id"])):
        pytest.xfail(reason)
    if case.get("setup"):
        setup_result = run_fakesnow(case["setup"], conn)
        if case["expect"]["error"] is None:
            assert setup_result["error"] is None, setup_result["error"]
        elif setup_result["error"]:
            compare_results(case["expect"], setup_result)
            return
    actual = run_fakesnow(case["sql"], conn)
    compare_results(case["expect"], actual)


@pytest.mark.live_snowflake
@pytest.mark.parametrize(("fixture", "case"), _params())
def test_live_snowflake_parity(fixture: dict[str, Any], case: dict[str, Any]) -> None:
    actual = run_snowflake(case["sql"], setup=case.get("setup"))
    if actual is None:
        pytest.skip("live Snowflake key-pair auth unavailable")
    compare_results(case["expect"], actual)
