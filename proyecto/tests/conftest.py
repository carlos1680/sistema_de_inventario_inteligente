import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_inventario")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
