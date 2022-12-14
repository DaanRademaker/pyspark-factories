import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.config(
        "spark.sql.shuffle.partitions", "1"
    ).getOrCreate()
