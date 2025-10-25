# churn_eda_and_modeling.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import seaborn as sns

# Start Spark
spark = SparkSession.builder.appName("ChurnEDA").getOrCreate()

# Load data
df = spark.read.parquet("data/processed/churn_enriched.parquet")
df.show(5)

# Churn distribution
df.groupBy("label").count().show()

# Convert to Pandas for plotting
df_pd = df.select("label", "Total day charge", "high_value_customer").toPandas()

# Plot churn distribution
sns.countplot(data=df_pd, x="label")
plt.title("Churn Distribution")
plt.savefig("output/churn_distribution.png")

# Train logistic regression
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(df)
predictions = model.transform(df)

# Evaluate
evaluator = BinaryClassificationEvaluator(labelCol="label")
auc = evaluator.evaluate(predictions)
print(f"AUC: {auc:.4f}")
