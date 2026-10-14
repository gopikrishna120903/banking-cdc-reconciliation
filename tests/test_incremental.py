from banking_cdc import incremental

COLS = "txn_id string, account_id string, txn_type string, amount string, currency string, status string, txn_ts string, updated_at string"


def _raw(spark, rows):
    return incremental.typed(spark.createDataFrame(rows, COLS))


def test_latest_version_and_watermark_filter(spark):
    df = _raw(spark, [
        ("T1", "A1", "DEBIT", "10.00", "USD", "PENDING", "2026-01-01T10:00:00", "2026-01-01T10:00:00"),
        ("T1", "A1", "DEBIT", "10.00", "USD", "POSTED", "2026-01-01T10:00:00", "2026-01-02T08:00:00"),
        ("T2", "A2", "CREDIT", "5.00", "USD", "POSTED", "2026-01-01T09:00:00", "2026-01-01T09:00:00"),
        ("T2", "A2", "CREDIT", "5.00", "USD", "POSTED", "2026-01-01T09:00:00", "2026-01-01T09:00:00"),
    ])
    latest = {r.txn_id: r.status for r in incremental.latest_version(df).collect()}
    assert latest == {"T1": "POSTED", "T2": "POSTED"}

    newer = incremental.changes_since(df, "2026-01-01 23:00:00").collect()
    assert [r.txn_id for r in newer] == ["T1"]


def test_upsert_newer_version_wins_and_older_is_ignored(spark):
    target = _raw(spark, [("T1", "A1", "DEBIT", "10.00", "USD", "POSTED", "2026-01-01T10:00:00", "2026-01-02T08:00:00")])
    replayed_old = _raw(spark, [
        ("T1", "A1", "DEBIT", "10.00", "USD", "PENDING", "2026-01-01T10:00:00", "2026-01-01T10:00:00"),
        ("T9", "A9", "CREDIT", "1.00", "USD", "POSTED", "2026-01-03T10:00:00", "2026-01-03T10:00:00"),
    ])
    out = {r.txn_id: r.status for r in incremental.upsert_dataframe(target, replayed_old).collect()}
    assert out == {"T1": "POSTED", "T9": "POSTED"}   # late replay did not overwrite the newer status
