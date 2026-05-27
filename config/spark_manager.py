import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class SparkManager:
    _instance = None

    @classmethod
    def get_session(cls) -> SparkSession:
        if cls._instance is None:
            
            cls._instance = SparkSession.builder \
                .appName("IndustrialIoTLakehouse") \
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
                .config("spark.sql.shuffle.partitions", "4") \
                .getOrCreate()
            
            # config hadoop to obtain credentials from .env file
            ctx = cls._instance._jsc.hadoopConfiguration()
            ctx.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            
            # Read .venv variables to configure S3 access
            ctx.set("fs.s3a.endpoint", os.getenv("STORAGE_ENDPOINT"))
            ctx.set("fs.s3a.access.key", os.getenv("STORAGE_ACCESS_KEY"))
            ctx.set("fs.s3a.secret.key", os.getenv("STORAGE_SECRET_KEY"))
            ctx.set("fs.s3a.path.style.access", "true")
            ctx.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        return cls._instance