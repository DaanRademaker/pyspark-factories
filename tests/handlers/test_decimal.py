import random
from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType, StructField

from pyspark_factories.handlers.decimal import handle_decimal


def test_handle_decimal():
    random.seed(10)

    value = handle_decimal(StructField("some_decimal", DecimalType(precision=10, scale=4), False))

    assert value == Decimal("4270.7906")

    # never generate a number with more decimals than the scale
    for _ in range(0, 1000):
        scale = 5
        precision = 8
        value = handle_decimal(StructField("some_decimal", DecimalType(precision=precision, scale=scale), False))

        assert abs(value.as_tuple().exponent) <= scale
        assert isinstance(value, Decimal)


def test_handle_decimal_0_scale():
    random.seed(10)
    # precision of 0 should have Decimal with no value after the .
    value = handle_decimal(StructField("some_decimal", DecimalType(precision=8, scale=0), False))

    assert abs(value.as_tuple().exponent) == 0
    assert value == Decimal("6")


def test_handle_decimal_precision_lower_than_scale():
    with pytest.raises(ValueError):
        handle_decimal(StructField("some_decimal", DecimalType(precision=10, scale=10), False))
