import pytest
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


@pytest.fixture(scope="session")
def glueContext() -> GlueContext:
    gc = GlueContext(SparkContext.getOrCreate())
    gc.spark_session.sparkContext.setLogLevel("WARN")
    return gc


@pytest.fixture(scope="session")
def dyfSimple(glueContext: GlueContext) -> DynamicFrame:
    ### A simple DynamicFrame for testing###

    spark = glueContext.spark_session
    df = spark.createDataFrame([("a", "b"), ("c", "d")], ["c1", "c2"])
    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
    return dyf
