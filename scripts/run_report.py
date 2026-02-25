#!/usr/bin/env python3
import argparse
import datetime as dt
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.betting_app import model


def main() -> int:
    parser = argparse.ArgumentParser(description="Run NBA_Quant_Edge_Daily report")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (requires --allow-manual-override)")
    parser.add_argument("--allow-manual-override", action="store_true", help="Allow manual date override")
    parser.add_argument("--output", type=str, default="reports/today_latest.json", help="Output JSON path")
    args = parser.parse_args()

    explicit_date = None
    if args.date:
        explicit_date = dt.date.fromisoformat(args.date)

    report = model.generate_report(explicit_date=explicit_date, allow_manual_override=args.allow_manual_override)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2))

    # Per spec: overwrite previous outputs on each run.
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
