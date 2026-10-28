import pytest

from banking_cdc import incremental, reconciliation
from banking_cdc.reconciliation import ReconciliationError

COLS = "txn_id string, account_id string, txn_type string, amount string, currency string, status string, txn_ts string, updated_at string"
ROWS = [
    ("T1", "A1", "DEBIT", "10.00", "USD", "POSTED", "2026-01-01T10:00:00", "2026-01-01T10:00:00"),
    ("T2", "A2", "CREDIT", "5.50", "USD", "POSTED", "2026-01-01T11:00:00", "2026-01-01T11:00:00"),
]


def test_extract_matches_control_totals(spark):
    raw = spark.createDataFrame(ROWS, COLS)
    results = reconciliation.reconcile_extract(raw, {"record_count": 2, "amount_total": "15.50"})
    reconciliation.assert_passed(results)


def test_truncated_extract_is_blocked(spark):
    raw = spark.createDataFrame(ROWS[:1], COLS)
    results = reconciliation.reconcile_extract(raw, {"record_count": 2, "amount_total": "15.50"})
    with pytest.raises(ReconciliationError, match="extract_record_count"):
        reconciliation.assert_passed(results)


def test_target_checks_catch_duplicates_and_orphans(spark):
    target = incremental.typed(spark.createDataFrame(ROWS + [ROWS[0]] + [
        ("T3", "A_UNKNOWN", "DEBIT", "1.00", "USD", "POSTED", "2026-01-01T12:00:00", "2026-01-01T12:00:00")], COLS))
    accounts = spark.createDataFrame([("A1",), ("A2",)], "account_id string")
    results = {r.check: r for r in reconciliation.reconcile_target(target, target.limit(0), accounts)}
    assert not results["no_duplicate_txn_ids"].passed
    assert not results["no_orphan_accounts"].passed and results["no_orphan_accounts"].actual == "1"
    assert results["no_null_keys"].passed

def test_unknown_transaction_codes_are_blocked(spark):
    target = incremental.typed(spark.createDataFrame(ROWS + [
        ("T3", "A1", "FEE", "2.00", "USD", "POSTED", "2026-01-01T12:00:00", "2026-01-01T12:00:00")], COLS))
    accounts = spark.createDataFrame([("A1",), ("A2",)], "account_id string")
    results = {r.check: r for r in reconciliation.reconcile_target(target, target.limit(0), accounts)}
    assert not results["known_txn_codes"].passed and results["known_txn_codes"].actual == "1"
    with pytest.raises(ReconciliationError, match="known_txn_codes"):
        reconciliation.assert_passed(list(results.values()))