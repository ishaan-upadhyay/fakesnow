from __future__ import annotations

from decimal import Decimal

import pytest

from fakesnow.variant.cast import to_boolean, to_decimal
from fakesnow.variant.compare import variant_eq, variant_key
from fakesnow.variant.errors import VariantRuntimeError
from fakesnow.variant.parser import parse_json
from fakesnow.variant.render import sf_json, sf_json_compact
from fakesnow.variant.sentinels import JSON_NULL, UNDEFINED
from fakesnow.variant.typeof import typeof


def test_parse_json_number_typing() -> None:
    assert sf_json_compact(parse_json("1.50")) == "1.5"
    assert typeof(parse_json("1.50")) == "DECIMAL"
    assert typeof(parse_json("1.000")) == "INTEGER"
    assert typeof(parse_json("1e10")) == "DOUBLE"
    assert sf_json_compact(parse_json("1e10")) == "1.000000000000000e+10"
    assert sf_json_compact(parse_json("123456789012345678901234567890.123456789")) == (
        "123456789012345678901234567890.123456789"
    )
    assert typeof(parse_json("123456789012345678901234567890.123456789")) == "DECIMAL"
    assert sf_json_compact(parse_json("9999999999999999999999999999999999999999")) == "1.000000000000000e+40"
    assert typeof(parse_json("9999999999999999999999999999999999999999")) == "DOUBLE"


def test_object_key_sort_order() -> None:
    value = parse_json('{"b":1,"a":2,"B":3,"1":5}')
    rendered = sf_json(value)
    assert rendered is not None
    assert rendered.index('"1"') < rendered.index('"B"') < rendered.index('"a"') < rendered.index('"b"')


def test_three_null_states_rendering() -> None:
    assert sf_json(parse_json("null")) == "null"
    assert sf_json_compact([UNDEFINED]) == "[undefined]"
    assert sf_json_compact([JSON_NULL]) == "[null]"


def test_variant_numeric_equality() -> None:
    assert variant_eq(parse_json("1"), parse_json("1.0")) is True
    assert variant_eq(parse_json("1.5"), parse_json("1.50")) is True
    assert variant_eq(parse_json("null"), JSON_NULL) is True
    assert variant_eq([UNDEFINED], [JSON_NULL]) is False


def test_variant_ordering_key() -> None:
    assert variant_key(parse_json("-10.1")) < variant_key(parse_json("-10"))
    assert variant_key(parse_json("-1")) < variant_key(parse_json("0"))
    assert variant_key(parse_json("1")) == variant_key(parse_json("1.0"))
    assert variant_key(parse_json("1")) < variant_key(parse_json('"a"'))


def test_variant_casts() -> None:
    assert to_decimal(parse_json("1.50"), 38, 2) == Decimal("1.50")
    assert to_boolean(parse_json("true")) is True
    assert to_decimal(JSON_NULL) is None

    with pytest.raises(VariantRuntimeError, match='Failed to cast variant value "no" to BOOLEAN') as exc:
        to_boolean(parse_json('"no"'))
    assert exc.value.errno == 100071
