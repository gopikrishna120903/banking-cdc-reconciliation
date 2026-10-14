import json

import pytest

from banking_cdc import pipeline
from banking_cdc.generate_data import generate
from banking_cdc.reconciliation import ReconciliationError


def test_three_day_cdc_load(spark, tmp_path):
    generate(str(tmp_path), accounts=50, txns_per_day=300, days=3)
    m = [pipeline.run(str(tmp_path), d) for d in ("2026-01-01", "2026-01-02", "2026-01-03")]
    assert all(x["checks_passed"] == x["checks_total"] for x in m)
    assert m[0]["target_rows"] == 300 and m[2]["target_rows"] == 900   # updates merged, not appended

    target = spark.read.parquet(str(tmp_path / "lake" / "silver" / "transactions"))
    assert target.groupBy("txn_id").count().filter("count > 1").count() == 0
    assert target.filter("status = 'PENDING'").count() < m[0]["changes_applied"]


def test_corrupted_extract_blocks_the_load(spark, tmp_path):
    generate(str(tmp_path), accounts=20, txns_per_day=100, days=1)
    control = tmp_path / "landing" / "control_2026-01-01.json"
    data = json.loads(control.read_text())
    data["record_count"] += 5          # source says it sent more rows than we received
    control.write_text(json.dumps(data))
    with pytest.raises(ReconciliationError):
        pipeline.run(str(tmp_path), "2026-01-01")
    assert not (tmp_path / "lake" / "silver" / "transactions").exists()
