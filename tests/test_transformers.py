from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from src.silver.telemetry_silver import clean_telemetry_data
from src.gold.fct_failures import compute_fct_failures
import os

# Simulated bronze schema for testing purposes, matching the expected input structure of the transformers
BRONZE_MOCK_SCHEMA = StructType([
    StructField("UDI", IntegerType(), True),
    StructField("Product_ID", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Air_temperature_K", DoubleType(), True),
    StructField("Process_temperature_K", DoubleType(), True),
    StructField("Rotational_speed_rpm", IntegerType(), True),
    StructField("Torque_Nm", DoubleType(), True),
    StructField("Tool_wear_min", IntegerType(), True),
    StructField("Machine_failure", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), True)
])


# TEST 1: Basic Deduplication by Time Window (Data Consistency & Deduplication)

def test_clean_telemetry_data_deduplication(spark_session: SparkSession):
    mock_data = [
        (1, "DEV-A", "M", 300.1, 310.2, 1500, 40.0, 5, 0, datetime(2026, 5, 25, 12, 0, 0)),
        (1, "DEV-A", "M", 300.1, 310.2, 1500, 40.0, 5, 0, datetime(2026, 5, 25, 12, 10, 0))
    ]
    df_raw = spark_session.createDataFrame(mock_data, BRONZE_MOCK_SCHEMA)
    
    df_result = clean_telemetry_data(df_raw)
    results = df_result.collect()

    assert len(results) == 1
    assert results[0]["_ingested_at"] == datetime(2026, 5, 25, 12, 10, 0)


# TEST 2: Resilency to Null Values (Data Quality & Null Handling)

def test_clean_telemetry_data_null_handling(spark_session: SparkSession):
    mock_data_with_nulls = [
        (2, "DEV-B", "L", None, 308.5, 1350, None, 12, 0, datetime(2026, 5, 25, 13, 0, 0))
    ]
    df_raw = spark_session.createDataFrame(mock_data_with_nulls, BRONZE_MOCK_SCHEMA)
    
    # Cleaning run
    df_result = clean_telemetry_data(df_raw)
    result_row = df_result.first()
    
    # Null values validations
    assert df_result.count() == 1
    assert result_row["device_id"] == "DEV-B"
    assert "air_temperature_k" in df_result.columns


# TEST 3: Integrity of the Gold Fact Table (Business Rule Enforcement)

def test_compute_fct_failures_filtering(spark_session: SparkSession, tmp_path):

    # Defined temporary paths to simulate Delta storage for Silver and Gold layers, ensuring isolation from actual data
    mock_silver_path = os.path.join(tmp_path, "silver")
    mock_gold_path = os.path.join(tmp_path, "gold_failures")
    
    # Records: One normal and one with Critical Failure (Machine_failure = 1)
    mock_silver_data = [
        (10, "DEV-X", "H", 298.2, 309.1, 1420, 45.2, 80, 0, datetime(2026, 5, 25, 14, 0, 0)),
        (11, "DEV-Y", "H", 302.5, 312.0, 2200, 75.0, 195, 1, datetime(2026, 5, 25, 14, 0, 0))
    ]
    
    # Simulated Silver Dataframe 
    silver_df = spark_session.createDataFrame(mock_silver_data, BRONZE_MOCK_SCHEMA)
    silver_clean_df = clean_telemetry_data(silver_df)
    silver_clean_df.write.format("delta").save(mock_silver_path)
    
    # Execute the business logic of the Gold layer, which should filter out the normal record and only keep the failure record
    compute_fct_failures(spark_session, mock_silver_path, mock_gold_path)
    
    df_gold_result = spark_session.read.format("delta").load(mock_gold_path)
    gold_records = df_gold_result.collect()
    
    # Analitics Verifications:
    # 1. The normal record (DEV-X) should have been discarded; the fact table should only have 1 record
    assert df_gold_result.count() == 1
    # 2. The only surviving record should be the device that failed (DEV-Y)
    assert gold_records[0]["device_id"] == "DEV-Y"
    # 3. Ensure that the failure timestamp column is present in the Gold fact table
    assert "failure_timestamp" in df_gold_result.columns