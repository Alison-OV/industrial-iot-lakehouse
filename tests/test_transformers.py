import pytest
from pyspark.sql import SparkSession
from datetime import datetime
from src.silver.telemetry_silver import clean_telemetry_data

def test_clean_telemetry_data_deduplication(spark_session: SparkSession):
    # Generate mock data with duplicate records for the same device_id and row_id but different timestamps
    schema = (
        "UDI INT, `Product ID` STRING, Type STRING, `Air temperature [K]` DOUBLE, "
        "`Process temperature [K]` DOUBLE, `Rotational speed [rpm]` INT, `Torque [Nm]` DOUBLE, "
        "`Tool wear [min]` INT, `Machine failure` INT, _ingested_at TIMESTAMP"
    )
    
    mock_data = [
        (1, "DEV-A", "M", 300.1, 310.2, 1500, 40.0, 5, 0, datetime(2026, 5, 25, 12, 0, 0)),
        (1, "DEV-A", "M", 300.1, 310.2, 1500, 40.0, 5, 0, datetime(2026, 5, 25, 12, 10, 0)) # Más reciente
    ]
    
    df_raw = spark_session.createDataFrame(mock_data, schema)
    
    # Execute the cleaning function
    df_result = clean_telemetry_data(df_raw)
    
    # Assert that the window removed the old record and preserved the most recent one
    assert df_result.count() == 1
    assert df_result.select("air_temperature_k").first()["air_temperature_k"] == 300.1