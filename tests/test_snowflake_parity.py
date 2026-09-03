from __future__ import annotations

from typing import Any

import pytest
import snowflake.connector

from tests.parity import compare_results, load_fixtures, run_fakesnow, run_snowflake

CURRENT_PR = 3


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
    if fixture["pr"] > CURRENT_PR:
        pytest.xfail(f"PR {fixture['pr']} not implemented (CURRENT_PR={CURRENT_PR})")

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
