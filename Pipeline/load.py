import os
from pyspark.sql import DataFrame

def load_data(df: DataFrame, output_path: str = "data/processed/churn_enriched.parquet") -> None:
    """
    Writes the final enriched DataFrame to Parquet format.

    Parameters:
        df (DataFrame): Final Spark DataFrame to save.
        output_path (str): Destination path for the Parquet file.
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write to Parquet
    df.write.mode("overwrite").parquet(output_path)
