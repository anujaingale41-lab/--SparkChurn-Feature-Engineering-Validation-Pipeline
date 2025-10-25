import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def get_spark_session(app_name: str = "SparkChurnPipeline") -> SparkSession:
    """
    Initializes and returns a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract_data(file_names: list, folder: str = "telecom_churn") -> DataFrame:
    """
    Loads and combines multiple CSV files into a single Spark DataFrame.

    Parameters:
        file_names (list): List of CSV filenames to load.
        folder (str): Directory where the files are stored.

    Returns:
        DataFrame: Combined Spark DataFrame.
    """
    spark = get_spark_session()
    dataframes = []

    for file_name in file_names:
        file_path = os.path.join(folder, file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv(file_path)

        dataframes.append(df)

    if not dataframes:
        raise ValueError("No dataframes were loaded. Please check file paths.")

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    return combined_df
