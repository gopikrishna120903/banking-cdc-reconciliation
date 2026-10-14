"""Automated reconciliation run before data is released downstream.

Checks compare the loaded data against the source system's control totals and against
internal consistency rules. Any CRITICAL failure blocks publishing.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    check: str
    severity: str
    passed: bool
    expected: str
    actual: str
    details: str = ""


class ReconciliationError(RuntimeError):
    pass


def _result(check, severity, passed, expected, actual, details="") -> CheckResult:
    return CheckResult(check, severity, bool(passed), str(expected), str(actual), details)


def reconcile_extract(raw_extract: DataFrame, control: dict) -> List[CheckResult]:
    """Source-to-landing: did we receive exactly what the source says it sent?"""
    agg = raw_extract.agg(F.count("*").alias("n"), F.sum(F.col("amount").cast("decimal(18,2)")).alias("amt")).collect()[0]
    amt = agg["amt"] or Decimal("0")
    return [
        _result("extract_record_count", "CRITICAL", agg["n"] == control["record_count"],
                control["record_count"], agg["n"]),
        _result("extract_amount_total", "CRITICAL", amt == Decimal(control["amount_total"]),
                control["amount_total"], amt),
    ]


def reconcile_target(target: DataFrame, changes: DataFrame, accounts: DataFrame) -> List[CheckResult]:
    """Landing-to-target: is the loaded table internally consistent?"""
    results = []

    dupes = target.groupBy("txn_id").count().filter("count > 1").count()
    results.append(_result("no_duplicate_txn_ids", "CRITICAL", dupes == 0, 0, dupes))

    null_keys = target.filter(F.col("txn_id").isNull() | F.col("account_id").isNull()).count()
    results.append(_result("no_null_keys", "CRITICAL", null_keys == 0, 0, null_keys))

    orphans = target.join(accounts, "account_id", "left_anti").count()
    results.append(_result("no_orphan_accounts", "CRITICAL", orphans == 0, 0, orphans))

    # every distinct change in today's increment must be present in the target with the same version
    missing = changes.select("txn_id", "updated_at").join(
        target.select("txn_id", "updated_at"), ["txn_id", "updated_at"], "left_anti"
    ).count()
    results.append(_result("all_changes_applied", "CRITICAL", missing == 0, 0, missing))

    bad_amounts = target.filter((F.col("amount") <= 0) | F.col("amount").isNull()).count()
    results.append(_result("positive_amounts", "WARNING", bad_amounts == 0, 0, bad_amounts))

    as_of = target.agg(F.max("business_date")).collect()[0][0]
    stale_pending = target.filter(
        (F.col("status") == "PENDING") & (F.datediff(F.lit(as_of), F.col("business_date")) > 5)
    ).count() if as_of else 0
    results.append(_result("no_stale_pending", "WARNING", stale_pending == 0, 0, stale_pending,
                           "pending > 5 days usually means a missed settlement update"))
    return results


def to_dataframe(spark: SparkSession, results: List[CheckResult], business_date: str) -> DataFrame:
    rows = [(business_date, r.check, r.severity, "PASS" if r.passed else "FAIL", r.expected, r.actual, r.details)
            for r in results]
    return spark.createDataFrame(
        rows, "business_date string, check string, severity string, status string, expected string, "
              "actual string, details string",
    ).withColumn("checked_at", F.current_timestamp())


def assert_passed(results: List[CheckResult]) -> None:
    failed = [r for r in results if r.severity == "CRITICAL" and not r.passed]
    if failed:
        raise ReconciliationError(
            "critical reconciliation failures: "
            + "; ".join(f"{r.check} expected={r.expected} actual={r.actual}" for r in failed)
        )
