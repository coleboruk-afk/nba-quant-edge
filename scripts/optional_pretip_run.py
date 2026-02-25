#!/usr/bin/env python3
import datetime as dt
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.betting_app import client, model


def main() -> int:
    today = dt.date.today()
    games, _ = client.fetch_official_schedule(today)
    if not games:
        print("No games scheduled today.")
        return 0

    # Optional pre-tip run trigger: execute only when within 30 minutes of first tip-off.
    first_tip = min(
        dt.datetime.fromisoformat(g.tipoff_utc.replace("Z", "+00:00")) for g in games if g.tipoff_utc
    )
    now_utc = dt.datetime.now(dt.timezone.utc)
    delta = (first_tip - now_utc).total_seconds()
    if not (0 <= delta <= 1800):
        print("Not in pre-tip window; skipping.")
        return 0

    report = model.generate_report()
    out = Path("reports/today_latest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
