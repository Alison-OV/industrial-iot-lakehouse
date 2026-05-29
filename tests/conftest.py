import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """
    Provee una sesión de Spark optimizada y configurada con Delta Lake
    para toda la suite de pruebas unitarias.
    """
    spark = SparkSession.builder \
        .appName("IndustrialIoTLakehouse-Test-Suite") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
        
    yield spark
    
    # Teardown seguro: Detener la JVM al finalizar todos los tests
    spark.stop()