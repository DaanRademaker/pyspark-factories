import random
from decimal import Decimal

from pyspark.sql.types import StructField


def handle_decimal(field: StructField) -> Decimal:
    """
    Randomly generates a Decimal object based on the DecimalType and it's specifications in
    the provide StructField

    :param field: A pyspark StructField containing a Decimal type
    :return: A randomly generated Decimal object
    """
    # fist generate a random integerer not larger than max digits e.g. precision
    precision = field.dataType.precision
    scale = field.dataType.scale

    max_digits_before_dot = precision - scale

    if max_digits_before_dot <= 0:
        raise ValueError(
            "Cannot generate a Decimal for which the scale is higher than or equal to the precision"
        )

    random_digits_before_dot = random.randint(1, max_digits_before_dot)
    max_integer_range_before_dot = 10**random_digits_before_dot - 1
    number_before_dot = random.randint(0, max_integer_range_before_dot)

    max_integer_range_scale = 10**scale - 1

    # if there are digits after . generate different number
    if scale > 0:
        random_scale_range = random.randint(1, scale)
        random_scale_integer_range = int(
            str(max_integer_range_scale)[0:random_scale_range]
        )
        number_after_dot = random.randint(0, random_scale_integer_range)
        return Decimal(f"{number_before_dot}.{number_after_dot}")

    return Decimal(f"{number_before_dot}")
