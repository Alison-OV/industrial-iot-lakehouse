import os
import sys
from dotenv import load_dotenv
from config.spark_manager import SparkManager
from src.bronze.raw_ingestion import ingest_landing_to_bronze
from src.silver.telemetry_silver import process_bronze_to_silver
from src.gold.agg_device_health import compute_gold_metrics

# Import the new function for the fct_failures fact table
from src.gold.fct_failures import compute_fct_failures

load_dotenv()

def main():
    print("--- Starting Industrial IoT Lakehouse Pipeline ---")
    
    landing_python = os.getenv("LOCAL_LANDING_PYTHON")
    landing_spark = os.getenv("LOCAL_LANDING_SPARK")
    bronze_path = os.getenv("LOCAL_BRONZE_PATH")
    silver_path = os.getenv("LOCAL_SILVER_PATH")
    gold_health_path = os.getenv("LOCAL_GOLD_HEALTH_PATH")

    # Read the new path
    gold_failures_path = os.getenv("LOCAL_GOLD_FAILURES_PATH")
    
    if not landing_python or not os.path.exists(landing_python):
        print(f" ERROR: No se encontró el dataset en '{landing_python}'.")
        sys.exit(1)
        
    spark = SparkManager.get_session()
        
    try:
        # Complete pipeline execution sequence
        ingest_landing_to_bronze(spark, landing_spark, bronze_path)
        process_bronze_to_silver(spark, bronze_path, silver_path)
        compute_gold_metrics(spark, silver_path, gold_health_path)
        
        # CALL THE NEW FACT TABLE FUNCTION
        compute_fct_failures(spark, silver_path, gold_failures_path)
        
        print(" Pipeline completado con éxito de manera idempotente.")
    except Exception as e:
        print(f" CRITICAL PIPELINE FAILURE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()