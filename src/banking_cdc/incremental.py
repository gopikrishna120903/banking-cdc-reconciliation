"""Incremental CDC loading.

1. read the day's extract and keep only records changed after the last watermark
2. collapse multiple versions of the same transaction to the latest one
3. upsert into the target: insert new transactions, update a transaction only when the
   incoming version is newer (protects against out-of-order / replayed extracts)
"""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .spark import delta_enabled
from .storage import exists, overwrite, read

KEY = "txn_id"


def typed(raw: DataFrame) -> DataFrame:
    return (
        raw.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("business_date", F.to_date("txn_ts"))
    )


def changes_since(df: DataFrame, watermark: Optional[str]) -> DataFrame:
    return df if watermark is None else df.filter(F.col("updated_at") > F.to_timestamp(F.lit(watermark)))


def latest_version(df: DataFrame) -> DataFrame:
    w = Window.partitionBy(KEY).orderBy(F.col("updated_at").desc(), F.col("status").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert_dataframe(target: DataFrame, changes: DataFrame) -> DataFrame:
    """Pure-DataFrame equivalent of the Delta MERGE below (newer version wins)."""
    combined = target.unionByName(changes.select(target.columns))
    return latest_version(combined)


def apply_changes(spark: SparkSession, changes: DataFrame, target_path: str) -> None:
    if not exists(spark, target_path):
        overwrite(changes, target_path)
        return
    if delta_enabled():
        from delta.tables import DeltaTable

        (
            DeltaTable.forPath(spark, target_path).alias("t")
            .merge(changes.alias("s"), f"t.{KEY} = s.{KEY}")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute()
        )
        return
    merged = upsert_dataframe(read(spark, target_path), changes)
    overwrite(merged.localCheckpoint(eager=True), target_path)
