"""Synthetic core-banking daily extracts (no real customer data).

Each business day produces:
  transactions_<day>.csv  - new transactions + status updates to earlier ones (CDC style) + re-sent duplicates
  control_<day>.json      - the source system's control totals (row count, amount sum) for the extract
  accounts.csv            - account master
"""
import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path


def _w(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def generate(out: str, accounts: int = 300, txns_per_day: int = 4000, days: int = 3, seed: int = 11) -> None:
    rng = random.Random(seed)
    base = Path(out) / "landing"
    accts = [{"account_id": f"A{i:07d}", "customer_id": f"C{rng.randint(1, accounts // 2):06d}",
              "account_type": rng.choice(["CHECKING", "SAVINGS", "CREDIT"]), "opened_date": "2024-01-01"}
             for i in range(1, accounts + 1)]
    _w(base / "accounts.csv", accts)

    pending: list = []
    txn_seq = 1
    start = datetime(2026, 1, 1)
    for d in range(days):
        day = start + timedelta(days=d)
        tag = day.strftime("%Y-%m-%d")
        rows = []
        # CDC: about half of the transactions still pending from earlier days settle (or reverse) today
        settle, pending = pending[: len(pending) // 2], pending[len(pending) // 2:]
        for p in settle:
            upd = dict(p)
            upd["status"] = rng.choices(["POSTED", "REVERSED"], [0.95, 0.05])[0]
            upd["updated_at"] = (day + timedelta(hours=rng.randint(1, 20))).isoformat()
            rows.append(upd)
        for _ in range(txns_per_day):
            ts = day + timedelta(seconds=rng.randint(0, 86399))
            status = rng.choices(["POSTED", "PENDING"], [0.85, 0.15])[0]
            row = {
                "txn_id": f"T{txn_seq:010d}", "account_id": rng.choice(accts)["account_id"],
                "txn_type": rng.choice(["DEBIT", "CREDIT"]),
                "amount": f"{Decimal(rng.randint(100, 500000)) / 100:.2f}", "currency": "USD",
                "status": status, "txn_ts": ts.isoformat(), "updated_at": ts.isoformat(),
            }
            txn_seq += 1
            rows.append(row)
            if status == "PENDING":
                pending.append(row)
        rows += [dict(r) for r in rng.sample(rows, k=txns_per_day // 100)]   # re-sent duplicates
        rng.shuffle(rows)
        _w(base / f"transactions_{tag}.csv", rows)
        control = {"business_date": tag, "record_count": len(rows),
                   "amount_total": f"{sum(Decimal(r['amount']) for r in rows):.2f}"}
        (base / f"control_{tag}.json").write_text(json.dumps(control, indent=2))
    print(f"Synthetic banking extracts written to {base}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data")
    p.add_argument("--days", type=int, default=3)
    a = p.parse_args()
    generate(a.out, days=a.days)
