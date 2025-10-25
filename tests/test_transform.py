import pytest
from pyspark.sql import SparkSession
from transform import transform_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestTransform").getOrCreate()

def test_transform_creates_features_and_label(spark):
    data = [("NY", 100, 415, "No", "No", 0, 100.0, 80, 17.0, 200.0, 90, 16.0, 150.0, 100, 7.0, 10.0, 5, 2.5, 1, "False")]
    columns = ["State", "Account length", "Area code", "International plan", "Voice mail plan",
               "Number vmail messages", "Total day minutes", "Total day calls", "Total day charge",
               "Total eve minutes", "Total eve calls", "Total eve charge", "Total night minutes",
               "Total night calls", "Total night charge", "Total intl minutes", "Total intl calls",
               "Total intl charge", "Customer service calls", "Churn"]
    df = spark.createDataFrame(data, columns)
    transformed = transform_data(df)
    assert "features" in transformed.columns
    assert "label" in transformed.columns
    assert transformed.count() == 1
