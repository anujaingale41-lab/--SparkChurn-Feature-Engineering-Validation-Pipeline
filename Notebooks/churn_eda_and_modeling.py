import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import seaborn as sns

def run_eda(df, output_dir):
    df.groupBy("label").count().show()
    df.groupBy("label").avg("Total day charge").show()
    df.groupBy("high_value_customer", "label").count().show()

    df_pd = df.select("label", "Total day charge", "high_value_customer").toPandas()

    sns.countplot(data=df_pd, x="label")
    plt.title("Churn Distribution")
    plt.savefig(f"{output_dir}/churn_distribution.png")

    sns.boxplot(data=df_pd, x="label", y="Total day charge")
    plt.title("Total Day Charge by Churn")
    plt.savefig(f"{output_dir}/charge_by_churn.png")

def run_model(df):
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(df)
    predictions = model.transform(df)

    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC: {auc:.4f}")
    predictions.select("label", "prediction", "probability").show(5)

def main():
    parser = argparse.ArgumentParser(description="Churn EDA and Modeling")
    parser.add_argument("--input", type=str, default="data/processed/churn_enriched.parquet", help="Path to input Parquet file")
    parser.add_argument("--output", type=str, default="output", help="Directory to save plots")
    parser.add_argument("--eda", action="store_true", help="Run EDA")
    parser.add_argument("--model", action="store_true", help="Run ML model")

    args = parser.parse_args()

    spark = SparkSession.builder.appName("ChurnEDA").getOrCreate()
    df = spark.read.parquet(args.input)

    if args.eda:
        run_eda(df, args.output)
    if args.model:
        run_model(df)

if __name__ == "__main__":
    main()
