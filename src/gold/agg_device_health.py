from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, col
from src.common.delta_utils import write_delta_table
from delta.tables import DeltaTable

def compute_gold_metrics(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    print(f" Building Gold Aggregations...")
    silver_df = spark.read.format("delta").load(silver_path)
    
    # Analytical aggregations of KPIs by industrial device type
    gold_df = silver_df.groupBy("device_type").agg(
        avg("air_temperature_k").alias("avg_air_temp"),
        avg("torque_nm").alias("avg_torque"),
        max("rotational_speed_rpm").alias("max_rpm"),
        min("tool_wear_min").alias("min_tool_wear")
    )
    
    # In Gold overwrite the entire table with the new aggregated metrics
    write_delta_table(gold_df, gold_path, mode="overwrite")
    
    # Optimize the Gold table with Z-ORDER on device_type for faster queries
    print(f" Optimizing Gold table with Z-ORDER...")
    delta_table = DeltaTable.forPath(spark, gold_path)
    delta_table.optimize().executeZOrderBy("device_type")