from __future__ import annotations

from typing import Any

import pytest
import snowflake.connector

from tests.parity import compare_results, load_fixtures, run_fakesnow, run_snowflake

# Golden batches this branch implements. A set rather than a high-water mark because the
# stack does not land the batches in numeric order: grouping (6) ships before structured
# types (5). Batches 3 and 5 are only partially implemented and stay off until complete.
IMPLEMENTED_BATCHES: frozenset[int] = frozenset()

# Known parity gaps that aren't code-fixable in fakesnow today.
_KNOWN_GAPS = {
    "description_type_codes_individual::hash_v2": "Snowflake HASH() is a proprietary algorithm; values don't match",
    "comparisons_arithmetic_individually::select_hash_v_a_as_r_from_select_parse_json_a_1_s_x_o_k_1_l": (
        "Snowflake HASH() is a proprietary algorithm; values don't match"
    ),
    "description_type_codes_for_expressions::select_object_construct_a_1_o_array_construct_1_a_parse_json": (
        "error-message parity: fakesnow raises a GROUP BY binder error vs Snowflake's compile error"
    ),
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
    if fixture["pr"] not in IMPLEMENTED_BATCHES:
        pytest.xfail(f"batch {fixture['pr']} ({fixture['name']}) not implemented on this branch")

    case_id = f"{fixture['name']}::{case['id']}"
    if case_id in _KNOWN_GAPS:
        pytest.xfail(_KNOWN_GAPS[case_id])

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
