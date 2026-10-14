"""Watermark state kept in a small table so every load is restartable and auditable."""
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .spark import table_format
from .storage import exists

SCHEMA = "table_name string, watermark timestamp, business_date string, updated_at timestamp"


def get_watermark(spark: SparkSession, path: str, table_name: str) -> Optional[str]:
    if not exists(spark, path):
        return None
    row = (spark.read.format(table_format()).load(path)
           .filter(F.col("table_name") == table_name)
           .agg(F.max("watermark").alias("wm")).collect()[0])
    return str(row["wm"]) if row["wm"] is not None else None


def set_watermark(spark: SparkSession, path: str, table_name: str, value, business_date: str) -> None:
    (spark.createDataFrame([(table_name, value, business_date)], "table_name string, watermark timestamp, business_date string")
     .withColumn("updated_at", F.current_timestamp())
     .write.format(table_format()).mode("append").save(path))
