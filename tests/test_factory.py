from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pyspark_factories.factory import ModelFactory, Overwrite

some_schema_with_all_types = StructType(
    [
        StructField("numbers", ArrayType(IntegerType(), True), True),
        StructField("firstname", StringType(), True),
        StructField("some_byte_type", ByteType(), True),
        StructField("some_short_type", ShortType(), True),
        StructField("some_float_type", FloatType(), True),
        StructField("some_double_type", DoubleType(), True),
        StructField("some_binary_type", BinaryType(), True),
        StructField("some_long_type", LongType(), True),
        StructField("married", BooleanType(), False),
        StructField("today_date", DateType(), False),
        StructField("time_now", TimestampType(), False),
        StructField("some_decimal", DecimalType(precision=8, scale=5), False),
        StructField(
            "nested",
            StructType(
                [
                    StructField("nested_deep", IntegerType(), False),
                    StructField("nested_deep2", IntegerType(), False),
                    StructField(
                        "nested_deeper",
                        StructType([StructField("nested_deep2", IntegerType(), False)]),
                        False,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)


def test_create(spark):
    """Happy flow test, make sure fake DF can be build with all available types"""

    class TestFactory(ModelFactory):
        __model__ = some_schema_with_all_types

    TestFactory.seed_random(5)

    test_factory = TestFactory()
    test_rows = test_factory.create(spark=spark, nr_of_rows=10).collect()

    for row in test_rows:
        assert len(row) == 13  # make sure all the types above have a row


def test_overwrite_fields():

    generated_fields = {
        "test": "overwrite_me",
        "test2": {"test3": "overwrite_me"},
        "test4": "stay_the_same",
    }

    class TestFactory(ModelFactory):
        __model__ = some_schema_with_all_types
        __overwrite__ = [
            Overwrite("test", "overwritten"),
            Overwrite("test2.test3", "overwritten"),
        ]

    fields = TestFactory.overwrite_fields(generated_fields=generated_fields)
    assert fields["test"] == "overwritten"
    assert fields["test2"]["test3"] == "overwritten"
    assert fields["test4"] == "stay_the_same"


def test_get_field_value():
    """
    Tests all of the possible Pyspark SQl types return an appropriate value
    """

    class TestFactory(ModelFactory):
        pass

    # make sure to seed so test results are always the same
    TestFactory.seed_random(10)

    value = TestFactory.get_field_value(StructField("decimal", DecimalType(), False))
    assert value == Decimal("4434919089")

    value = TestFactory.get_field_value(StructField("string", StringType(), False))
    assert value == "DwEkQQHiBrmXZcSFtoJx"

    value = TestFactory.get_field_value(StructField("bool", BooleanType(), False))
    assert value

    value = TestFactory.get_field_value(StructField("date", DateType(), False))
    assert value == date(2022, 10, 25)

    value = TestFactory.get_field_value(StructField("datetime", TimestampType(), False))
    assert value == datetime(1994, 9, 4, 3, 49, 3)

    value = TestFactory.get_field_value(StructField("binary", BinaryType(), False))
    assert value == (
        b"D\xe9z\xe1\xc0\x16\x02xDc\x9b\xbbz\xa0\xe6\xdf\xf0!\xa6Pr\xd3z\x12\x10\xfe\x9a$)L\xc4\xbfL91\xe2Ua\xb2\xdd\xd4\xe4}"
        b"\x8cI[=\x88\xe9\x9aTYZ\xf5\xb1\xa7\xder\x02\x16\xa9\xa3|("
    )

    value = TestFactory.get_field_value(StructField("byte", ByteType(), False))
    assert value == 119

    value = TestFactory.get_field_value(StructField("short", ShortType(), False))
    assert value == -30824

    value = TestFactory.get_field_value(StructField("integer_type", IntegerType(), False))
    assert value == -1262298496

    value = TestFactory.get_field_value(StructField("long", LongType(), False))
    assert value == -3177030862009647175

    value = TestFactory.get_field_value(StructField("float", FloatType(), False))
    assert value == -4974568753.6498

    value = TestFactory.get_field_value(StructField("double", DoubleType(), False))
    assert value == 8607.69886387695

    value = TestFactory.get_field_value(StructField("array", ArrayType(IntegerType(), False), False))
    assert value == [-596319138, -622419416, -338168573, -1020823058]

    value = TestFactory.get_field_value(
        StructField(
            "nested",
            StructType(
                [
                    StructField("nested_deep", IntegerType(), False),
                ]
            ),
            False,
        ),
    )
    assert value == {"nested_deep": -590182562}

    TestFactory.should_set_none_value = lambda _: True
    value = TestFactory.get_field_value(StructField("integer_type", IntegerType(), True))
    assert not value


def test_should_set_none_value():
    class TestFactory(ModelFactory):
        pass

    TestFactory.__allow_none_optionals__ = False
    set_none_value = TestFactory.should_set_none_value(model_field=StructField("integer_type", IntegerType(), True))

    assert not set_none_value

    with patch("pyspark_factories.factory.create_random_boolean") as patched_created_random_boolean:

        patched_created_random_boolean.return_value = True

        # Structfield allows nullable so should be True
        TestFactory.__allow_none_optionals__ = True
        set_none_value = TestFactory.should_set_none_value(model_field=StructField("integer_type", IntegerType(), True))
        assert set_none_value

        # Structfield does not allow nullable so should always be False
        TestFactory.__allow_none_optionals__ = True

        set_none_value = TestFactory.should_set_none_value(
            model_field=StructField("integer_type", IntegerType(), False)
        )

        assert not set_none_value
