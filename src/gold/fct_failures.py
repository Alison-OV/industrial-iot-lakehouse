from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.common.delta_utils import write_delta_table
from delta.tables import DeltaTable

def compute_fct_failures(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    print(f" Building Gold Fact Table (fct_failures)...")
    
    # 1. Read clean and enriched data from Silver layer
    silver_df = spark.read.format("delta").load(silver_path)
    
    # 2. Filter and structure only the critical events (where machine failure occurred)
    fct_failures_df = silver_df.filter(col("has_failed") == 1).select(
        col("row_id").alias("fact_key"),
        col("device_id"),
        col("device_type"),
        col("air_temperature_k"),
        col("process_temperature_k"),
        col("rotational_speed_rpm"),
        col("torque_nm"),
        col("tool_wear_min"),
        col("_ingested_at").alias("failure_timestamp")
    )
    
    # 3. Write in Gold Path
    write_delta_table(fct_failures_df, gold_path, mode="overwrite")
    
    # 4. Optomizing the fact table with Z-ORDER on device_id for faster queries on failures by device
    print(f" Optimizing fct_failures table with Z-ORDER by device_id...")
    delta_table = DeltaTable.forPath(spark, gold_path)
    delta_table.optimize().executeZOrderBy("device_id")
    print(f"  Fact table fct_failures optimized successfully.")