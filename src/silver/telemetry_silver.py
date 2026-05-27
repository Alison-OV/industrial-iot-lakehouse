from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from src.common.delta_utils import upsert_delta_table

def clean_telemetry_data(df: DataFrame) -> DataFrame:
    # Read columns of bronze data and cast to appropriate types with clear naming conventions
    clean_df = df.select(
        col("UDI").cast("int").alias("row_id"),
        col("Product_ID").cast("string").alias("device_id"),
        col("Type").cast("string").alias("device_type"),
        col("Air_temperature_K").cast("double").alias("air_temperature_k"),
        col("Process_temperature_K").cast("double").alias("process_temperature_k"),
        col("Rotational_speed_rpm").cast("int").alias("rotational_speed_rpm"),
        col("Torque_Nm").cast("double").alias("torque_nm"),
        col("Tool_wear_min").cast("int").alias("tool_wear_min"),
        col("Machine_failure").cast("int").alias("has_failed"),
        col("_ingested_at")
    )
    
    # Remove duplicates event timestamps
    window_spec = Window.partitionBy("device_id", "row_id").orderBy(col("_ingested_at").desc())
    deduped_df = clean_df.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
        
    return deduped_df

def process_bronze_to_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> None:
    print(f" Processing Bronze to Silver...")
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    silver_df = clean_telemetry_data(bronze_df)
    
    join_condition = "target.device_id = source.device_id AND target.row_id = source.row_id"
    upsert_delta_table(spark, silver_df, silver_path, join_condition)
    print(f" Silver layer updated successfully.")