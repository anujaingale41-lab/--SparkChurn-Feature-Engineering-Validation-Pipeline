from pyspark.sql import DataFrame
from pyspark.sql.functions import when

def enrich_data(df: DataFrame) -> DataFrame:
    """
    Adds business logic flags to the churn dataset for enhanced segmentation.

    Parameters:
        df (DataFrame): Transformed Spark DataFrame with features and label.

    Returns:
        DataFrame: Enriched Spark DataFrame with additional flags.
    """
    # Flag: High-value customer (based on total charges)
    df = df.withColumn("high_value_customer", when(df["Total day charge"] > 40, 1).otherwise(0))

    # Flag: Frequent caller (based on total day calls)
    df = df.withColumn("frequent_caller", when(df["Total day calls"] > 100, 1).otherwise(0))

    # Flag: International user (based on intl minutes)
    df = df.withColumn("uses_international", when(df["Total intl minutes"] > 10, 1).otherwise(0))

    return df
