"""Daily CDC load: extract -> reconcile extract -> incremental merge -> reconcile target -> publish.

    python -m banking_cdc.pipeline --data-dir data --business-date 2026-01-01
"""
import argparse
import json
import logging
import os

from pyspark.sql import functions as F

from . import incremental, reconciliation
from .spark import get_spark
from .storage import overwrite, read
from .watermark import get_watermark, set_watermark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("banking_cdc")


def run(data_dir: str, business_date: str) -> dict:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    lake = os.path.join(data_dir, "lake")
    landing = os.path.join(data_dir, "landing")
    target_path = os.path.join(lake, "silver", "transactions")
    wm_path = os.path.join(lake, "_control", "watermarks")

    raw = spark.read.option("header", True).csv(os.path.join(landing, f"transactions_{business_date}.csv"))
    with open(os.path.join(landing, f"control_{business_date}.json")) as f:
        control = json.load(f)
    accounts = spark.read.option("header", True).csv(os.path.join(landing, "accounts.csv"))

    results = reconciliation.reconcile_extract(raw, control)
    reconciliation.assert_passed(results)              # never load a partial or corrupted extract

    watermark = get_watermark(spark, wm_path, "transactions")
    changes = incremental.latest_version(incremental.changes_since(incremental.typed(raw), watermark)).cache()
    n_changes = changes.count()
    log.info("watermark=%s -> %s changed transactions", watermark, n_changes)

    incremental.apply_changes(spark, changes, target_path)
    target = read(spark, target_path)

    results += reconciliation.reconcile_target(target, changes, accounts)
    report = reconciliation.to_dataframe(spark, results, business_date)
    report.write.mode("append").parquet(os.path.join(lake, "_control", "reconciliation"))
    reconciliation.assert_passed(results)              # blocks publish + watermark advance on failure

    new_wm = changes.agg(F.max("updated_at")).collect()[0][0]
    if new_wm is not None:
        set_watermark(spark, wm_path, "transactions", new_wm, business_date)

    summary = (
        target.filter("status = 'POSTED'")
        .groupBy("account_id", "business_date")
        .agg(
            F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credits"),
            F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debits"),
            F.count("*").alias("posted_txns"),
        )
        .withColumn("net_change", F.col("credits") - F.col("debits"))
    )
    overwrite(summary, os.path.join(lake, "gold", "daily_account_activity"))

    metrics = {
        "business_date": business_date,
        "extract_rows": control["record_count"],
        "changes_applied": n_changes,
        "target_rows": target.count(),
        "checks_passed": sum(r.passed for r in results),
        "checks_total": len(results),
    }
    log.info("run metrics %s", json.dumps(metrics))
    return metrics


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="data")
    p.add_argument("--business-date", required=True)
    a = p.parse_args()
    run(a.data_dir, a.business_date)


if __name__ == "__main__":
    main()
