from pyspark.sql import DataFrame
from delta.tables import DeltaTable

def write_delta_table(df: DataFrame, path: str, mode: str = "append", partition_by: list = None) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

def upsert_delta_table(spark_session, source_df: DataFrame, target_path: str, join_condition: str) -> None:
    if not DeltaTable.isDeltaTable(spark_session, target_path):
        write_delta_table(source_df, target_path, mode="overwrite")
    else:
        target_table = DeltaTable.forPath(spark_session, target_path)
        target_table.alias("target") \
            .merge(source_df.alias("source"), join_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()