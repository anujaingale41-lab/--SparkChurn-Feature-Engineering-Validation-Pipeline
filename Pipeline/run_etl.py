import logging
from extract import extract_data
from validate import validate_data
from transform import transform_data
from enrich import enrich_data
from load import load_data

# ---------------------- Logging Setup ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def run_etl():
    logging.info("🚀 Starting ETL pipeline...")

    # Step 1: Extract
    logging.info("📥 Extracting data...")
    df_raw = extract_data(["churn-bigml-20.csv", "churn-bigml-80.csv"])

    # Step 2: Validate
    logging.info("🔍 Validating data...")
    df_validated = validate_data(df_raw)

    # Step 3: Transform
    logging.info("🔧 Transforming data...")
    df_transformed = transform_data(df_validated)

    # Step 4: Enrich
    logging.info("✨ Enriching data with business logic...")
    df_enriched = enrich_data(df_validated)  # or df_transformed if you prefer post-transform enrichment

    # Step 5: Load
    logging.info("💾 Saving enriched data to Parquet...")
    load_data(df_enriched)

    logging.info("✅ ETL pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()
