import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from src.common.delta_utils import write_delta_table

def sanitize_column_name(name: str) -> str:
    
    # Replace brackets and special characters with empty string or underscores
    clean_name = re.sub(r'[\[\]()]', '', name)
    
    # Replace spaces and invalid characters with underscores
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_name)
    
    # Trim doble underscores and trim leading/trailing underscores
    clean_name = re.sub(r'_+', '_', clean_name).strip('_')
    return clean_name

def ingest_landing_to_bronze(spark: SparkSession, landing_path: str, bronze_path: str) -> None:
    print(f" Reading from Landing: {landing_path}")
    
    # 1. Read the Kaggle CSV file
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(landing_path)
    
    # 2. Clean the column names to ensure they are valid for Delta Lake
    sanitized_cols = [sanitize_column_name(c) for c in raw_df.columns]
    raw_df = raw_df.toDF(*sanitized_cols)
    
    # 3. Audit columns: Add ingestion timestamp, source file name, and row status
    bronze_df = raw_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file_name", input_file_name()) \
        .withColumn("_row_status", lit("ACTIVE"))
    
    print(f" Writing to Bronze Delta Layer: {bronze_path}")
    write_delta_table(bronze_df, bronze_path, mode="overwrite") 