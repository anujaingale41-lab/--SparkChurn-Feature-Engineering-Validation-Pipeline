# --SparkChurn-Feature-Engineering-Validation-Pipeline
A modular PySpark pipeline that ingests, validates, and transforms customer data for churn analysis. Combines scalable ETL with backend logic—schema enforcement, rule-based filtering, and feature engineering—to deliver clean, analytics-ready outputs for ML or BI workflows.

##  Dataset

This project uses the [Telco Customer Churn dataset](https://www.kaggle.com/datasets/blastchar/telco-customer-churn), provided in CSV format. It includes customer demographics, service usage patterns, and churn labels.

##  Tech Stack

- Python 3.8+
- PySpark 3.5+
- Parquet (for output storage)
- Modular Python scripts
- Optional: Jupyter/Colab for exploration


## Pipeline Overview

###  Extract
- Load TSV data using PySpark
- Infer schema and handle headers

###  Validate
- Enforce required columns
- Apply rule-based filters (e.g., non-negative charges, no nulls)

###  Transform
- Encode categorical fields using `StringIndexer`
- Assemble numerical and indexed features with `VectorAssembler`

###  Enrich
- Add backend-style flags (e.g., high-value customer indicator)

###  Load
- Write clean, enriched data to Parquet format

---

##  How to Run

```bash
pipeline/run_etl.py
```

This command executes the full ETL pipeline: extract → validate → transform → enrich → load.

##  Output

- Cleaned and enriched customer dataset in Parquet format
- Ready for ML modeling, BI dashboards, or retention strategy analysis

## License

This project is for educational and portfolio purposes. Dataset sourced from Kaggle under public license.
