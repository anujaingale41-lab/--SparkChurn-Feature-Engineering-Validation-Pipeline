import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# ---------------------- Logging Setup ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ---------------------- Validation Logic ----------------------
def validate_data(df: DataFrame) -> DataFrame:
    """
    Validates the input DataFrame by enforcing required columns and applying rule-based filters.

    Parameters:
        df (DataFrame): Raw extracted Spark DataFrame.

    Returns:
        DataFrame: Validated and filtered Spark DataFrame.
    """
    required_columns = [
        "State", "Account length", "Area code", "International plan", "Voice mail plan",
        "Number vmail messages", "Total day minutes", "Total day calls", "Total day charge",
        "Total eve minutes", "Total eve calls", "Total eve charge", "Total night minutes",
        "Total night calls", "Total night charge", "Total intl minutes", "Total intl calls",
        "Total intl charge", "Customer service calls", "Churn"
    ]

    # Check for missing columns
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logging.error(f"Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")
    else:
        logging.info("All required columns are present.")

    # Drop rows with nulls
    df = df.dropna()
    logging.info(f"After dropping nulls: {df.count()} rows remain.")

    # Apply rule-based filters
    df = df.filter(
        (col("Account length") >= 0) &
        (col("Total day minutes") >= 0) &
        (col("Total eve minutes") >= 0) &
        (col("Total night minutes") >= 0) &
        (col("Total intl minutes") >= 0)
    )
    logging.info(f"After rule-based filtering: {df.count()} rows remain.")

    return df
