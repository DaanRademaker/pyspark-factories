import random
from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, cast

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
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

from pyspark_factories.fields import Use
from pyspark_factories.handlers.decimal import handle_decimal
from pyspark_factories.value_generators.primitives import create_random_boolean

default_faker = Faker()


class OverwriteException(Exception):
    pass


@dataclass
class Overwrite:
    key: str
    value: Any


class ModelFactory(ABC):
    __model__: StructType
    __faker__: Optional[Faker]
    __allow_none_optionals__: bool = True
    __max_array_length__: int = 10
    __overwrite__: List[Overwrite] = []

    @classmethod
    def _get_faker(cls) -> Faker:
        """
        Returns an instance of faker
        """
        if hasattr(cls, "__faker__") and cls.__faker__:
            return cls.__faker__
        return default_faker

    @classmethod
    def seed_random(cls, seed: int) -> None:
        """
        Seeds Fake and random methods with seed
        """
        random.seed(seed)
        Faker.seed(seed)

    @classmethod
    def get_provider_map(cls) -> Dict[Any, callable]:
        faker = cls._get_faker()

        return {
            StringType: faker.pystr,
            BooleanType: faker.pybool,
            DateType: faker.date_this_decade,
            TimestampType: faker.date_time,
            BinaryType: lambda: faker.binary(length=64),
            ByteType: lambda: random.randint(-128, 127),
            ShortType: lambda: random.randint(-32768, 32767),
            IntegerType: lambda: random.randint(-2147483648, 2147483647),
            LongType: lambda: random.randint(-9223372036854775808, 9223372036854775807),
            FloatType: faker.pyfloat,
            DoubleType: faker.pyfloat,
        }

    @classmethod
    def get_mock_value(cls, field: Any) -> Any:
        handler = cls.get_provider_map().get(field.__class__)

        if handler is not None:
            return handler()

        else:
            raise ValueError(f"No handler can be found for {field} given pyspark type")

    @classmethod
    def create_factory(
        cls,
        model: StructType,
        base: Optional[Type["ModelFactory"]] = None,
        **kwargs: Any,
    ) -> "ModelFactory":  # pragma: no cover
        """Dynamically generates a factory given a model"""

        return cast(
            ModelFactory,
            type(
                "test",
                (base or cls,),
                {"__model__": model, **kwargs},
            ),
        )

    @classmethod
    def should_set_none_value(cls, model_field) -> bool:
        """
        Determines whether a given model field should be set to None.
        Separated to its own method to allow easy overriding
        """
        if cls.__allow_none_optionals__:
            return model_field.nullable and create_random_boolean()
        return False

    @classmethod
    def get_field_value(cls, field: StructField) -> Any:
        """
        Get's a generated value based on the StructField value in a StructType.

        :param field: Spark sql type
        :return: A mock value
        """
        if cls.should_set_none_value(field):
            return None
        if isinstance(field.dataType, StructType):
            return cls.create_factory(model=field.dataType).build()
        if isinstance(field.dataType, DecimalType):
            return handle_decimal(field)
        if isinstance(field.dataType, ArrayType):
            return [
                cls.get_mock_value(field.dataType.elementType)
                for _ in range(0, random.randint(1, cls.__max_array_length__))
            ]

        return cls.get_mock_value(field.dataType)

    @classmethod
    def build(cls) -> Dict[str, Any]:
        """
        Build the model by generating all the model fields based on the __model__ / schema provided
        :return: All of the generated fields
        """
        model_fields = cls.__model__.fields

        build_fields = [
            {field.name: cls.get_field_value(field)} for field in model_fields
        ]

        dict_fields = {}

        for field in build_fields:
            for key, value in field.items():
                dict_fields[key] = value

        return dict_fields

    @classmethod
    def write_recursive_keys(cls, dictionary: dict, keys: List[str], value: Any):
        """
        Recusively overwrite values in a a dictionary

        :param dictionary: Original dictionary to overwrite values in
        :param keys: A list of (nested) keys to do overwrite for
        :param value: Value to overwrite
        """

        values = dictionary.get(keys[0])

        if values is None:
            return None

        if len(keys) == 1:
            if isinstance(value, Use):
                value = value.to_value()
            dictionary[keys[0]] = value
            return

        # go down recursion with next key in line
        keys.pop(0)

        cls.write_recursive_keys(values, keys, value)

    @classmethod
    def overwrite_fields(cls, generated_fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Overwrite fields for which the __overwrite__ attribute is set

        :param generated_fields: Generated fields dictionary without overwrites
        :raises OverwriteException: _description_
        :return: Same generated fields dictionary but now with overwrites
        """

        for overwrite in cls.__overwrite__:
            overwrite_keys = overwrite.key.split(".")

            try:
                cls.write_recursive_keys(
                    generated_fields, overwrite_keys, overwrite.value
                )
            except KeyError:
                raise OverwriteException(
                    "This key does not exist spark schema so there is no value to overwrite"
                )

        return generated_fields

    @classmethod
    def create_single_row(cls) -> Dict[str, Any]:
        """
        Creates a single dictionary row that can be used for a Spark dataframe row.
        First the dict is build based on the spark schema fields. After this is created
        the function checks to see if any overwrites are provided, and is so overwrites the rows.

        :return: A Dict with the field values of the schema as key and the value's are either generated
            using faker or overwriten by the factory defaults.
        """
        dict_fields = cls.build()

        if len(cls.__overwrite__) > 0:
            cls.overwrite_fields(dict_fields)

        return dict_fields

    @classmethod
    def create(cls, spark: SparkSession, nr_of_rows: int = 1) -> DataFrame:
        """
        Creates a pyspark dataframe based on the created factory class

        :param spark: Spark session to use when creating the Spark dataframe
        :param nr_of_rows: Number of rows to gererate in the Spark dataframe, defaults to 1
        :return: A Spark dataframe containing randomly generated rows based on the provided
            spark schema in the factory class.
        """

        all_rows = [cls.create_single_row() for _ in range(0, nr_of_rows)]

        return spark.createDataFrame(all_rows, schema=cls.__model__)
