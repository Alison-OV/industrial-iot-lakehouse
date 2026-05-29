from datetime import datetime
from pyspark.sql import SparkSession
from src.silver.telemetry_silver import clean_telemetry_data

def test_clean_telemetry_data_deduplication(spark_session: SparkSession):
    schema = (
        "UDI INT, Product_ID STRING, Type STRING, Air_temperature_K DOUBLE, "
        "Process_temperature_K DOUBLE, Rotational_speed_rpm INT, Torque_Nm DOUBLE, "
        "Tool_wear_min INT, Machine_failure INT, _ingested_at TIMESTAMP"
    )

    mock_data = [
        (1, "DEV-A", "M", 300.1, 310.2, 1500, 40.0, 5, 0, datetime(2026, 5, 25, 12, 0, 0)),
        (1, "DEV-A", "M", 300.1, 310.2, 1500, 40.0, 5, 0, datetime(2026, 5, 25, 12, 10, 0)) 
    ]

    df_raw = spark_session.createDataFrame(mock_data, schema)

    df_result = clean_telemetry_data(df_raw)

    results = df_result.collect()


    assert len(results) == 1

    assert results[0]["_ingested_at"] == datetime(2026, 5, 25, 12, 10, 0)
    

    assert "device_id" in df_result.columns
    assert "air_temperature_k" in df_result.columns