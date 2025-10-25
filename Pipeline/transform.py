from pyspark.sql import DataFrame
from pyspark.ml.feature import StringIndexer, VectorAssembler

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforms the validated churn dataset by encoding categorical features
    and assembling numerical features into a single feature vector.

    Parameters:
        df (DataFrame): Validated Spark DataFrame.

    Returns:
        DataFrame: Transformed Spark DataFrame with 'features' and 'label' columns.
    """
    # Step 1: Encode categorical columns
    categorical_cols = ["International plan", "Voice mail plan", "Churn"]
    for col_name in categorical_cols:
        indexer = StringIndexer(inputCol=col_name, outputCol=col_name + "_index")
        df = indexer.fit(df).transform(df)

    # Step 2: Assemble numerical + indexed features
    feature_cols = [
        "Account length", "Number vmail messages", "Total day minutes", "Total day calls",
        "Total day charge", "Total eve minutes", "Total eve calls", "Total eve charge",
        "Total night minutes", "Total night calls", "Total night charge",
        "Total intl minutes", "Total intl calls", "Total intl charge",
        "Customer service calls", "International plan_index", "Voice mail plan_index"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(df)

    # Step 3: Rename churn label
    df = df.withColumnRenamed("Churn_index", "label")

    return df.select("features", "label")
