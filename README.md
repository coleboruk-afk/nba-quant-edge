# NBA_Quant_Edge_Daily

Recurring daily NBA +EV analytics engine that enforces live-data-only operation.

## Core behavior implemented

- Auto-date detection: runtime uses system date as `TODAY`.
- Manual date override is blocked unless `--allow-manual-override` (CLI) or `allow_manual_override=true` (API).
- Daily reset protocol: runtime state/cache is cleared at the start of every run.
- Fresh pulls each run:
  - Official NBA schedule for TODAY
  - Injury report
  - Projected lineups
  - Betting markets (spread/ML/totals/team totals/player props)
- If live data cannot be validated for TODAY, output is aborted with:
  - `Live data unavailable. Analysis aborted.`
- Monte Carlo simulation floor: `>= 10,000` (default 20,000).
- Edge filter: only bets with `Edge >= 3%`.
- Ranking: returns up to top 10 +EV plays.
- If no qualifying bets:
  - `No positive expected value opportunities today.`

## Setup

```bash
cd '/Users/kristineboruk/Documents/Sports Analytics App'
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `ODDS_API_KEY` in `.env`.

## Run API

```bash
uvicorn src.betting_app:app --reload --port 8080
```

Endpoints:
- `GET /health`
- `GET /report`
- `GET /report?date=2026-02-25&allow_manual_override=true`

## Run CLI

Default (auto-date TODAY):

```bash
python scripts/run_report.py
```

Manual override (explicit only):

```bash
python scripts/run_report.py --date 2026-02-25 --allow-manual-override
```

Output overwrites `reports/today_latest.json` by default every run.

## Recurring scheduler

Run daily at 10:00 AM and 5:00 PM local:

```cron
0 10 * * * cd '/Users/kristineboruk/Documents/Sports Analytics App' && /bin/zsh -lc 'source .venv/bin/activate && python scripts/run_report.py --output reports/today_latest.json'
0 17 * * * cd '/Users/kristineboruk/Documents/Sports Analytics App' && /bin/zsh -lc 'source .venv/bin/activate && python scripts/run_report.py --output reports/today_latest.json'
```

Optional third run for 30 minutes before first tip-off (execute every 10 mins, script self-gates):

```cron
*/10 * * * * cd '/Users/kristineboruk/Documents/Sports Analytics App' && /bin/zsh -lc 'source .venv/bin/activate && python scripts/optional_pretip_run.py'
```

## Notes on strict integrity

- Prior-day projections are not reused.
- OUT/DOUBTFUL/inactive players are excluded.
- Players must appear in at least 2 of last 3 games.
- Unavailable metrics are marked `unverified` instead of fabricated.
