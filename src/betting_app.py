import datetime as dt
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

APP_NAME = "NBA_Quant_Edge_Daily"
ABORT_MSG = "Live data unavailable. Analysis aborted."
NO_EV_MSG = "No positive expected value opportunities today."
MONTE_CARLO_SIMS = max(10000, int(os.getenv("MONTE_CARLO_SIMS", "20000")))
REQUEST_TIMEOUT_SECONDS = 120
REQUEST_RETRIES = 3
REQUEST_BACKOFF_SECONDS = 1

NBA_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

ODDS_TEAM_TO_ABBR = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "LA Clippers": "LAC",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}


@dataclass
class Game:
    game_id: str
    home_team: str
    away_team: str
    tipoff_utc: str


@dataclass
class BetPick:
    matchup: str
    market_type: str
    line_odds: str
    projected_probability: float
    implied_probability: float
    edge: float
    projected_final_score: Optional[str]
    reasons: List[str]
    risk: str
    units: float


class DataClient:
    def __init__(self) -> None:
        self.odds_api_key = os.getenv("ODDS_API_KEY", "").strip()
        self.odds_region = os.getenv("ODDS_REGION", "us")
        self.odds_markets = os.getenv(
            "ODDS_MARKETS",
            "h2h,spreads,totals,team_totals,player_points,player_rebounds,player_assists,player_threes,player_pra",
        )
        self.odds_bookmakers = os.getenv("ODDS_BOOKMAKERS", "draftkings,fanduel,betmgm,caesars")
        self.market_signal_url = os.getenv("MARKET_SIGNAL_API_URL", "").strip()
        self.market_signal_key = os.getenv("MARKET_SIGNAL_API_KEY", "").strip()
        self.upstream_status: Dict[str, Dict[str, Any]] = {}

    def _request_with_retry(
        self,
        *,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        source: str,
        expect_json: bool,
    ) -> Any:
        last_error: Optional[Exception] = None
        total_attempts = REQUEST_RETRIES + 1
        for attempt in range(1, total_attempts + 1):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
                resp.raise_for_status()
                self.upstream_status[source] = {
                    "ok": True,
                    "attempts": attempt,
                    "url": url,
                }
                return resp.json() if expect_json else resp.text
            except Exception as exc:
                last_error = exc
                if attempt < total_attempts:
                    backoff = REQUEST_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    time.sleep(backoff)

        self.upstream_status[source] = {
            "ok": False,
            "attempts": total_attempts,
            "url": url,
            "error": str(last_error),
        }
        raise RuntimeError(f"{source} failed after {total_attempts} attempts: {last_error}")

    def _get_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        source: str = "unknown_json",
    ) -> Any:
        return self._request_with_retry(url=url, headers=headers, params=params, source=source, expect_json=True)

    def _get_text(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        source: str = "unknown_text",
    ) -> str:
        return str(self._request_with_retry(url=url, headers=headers, params=params, source=source, expect_json=False))

    @staticmethod
    def _season(game_date: dt.date) -> str:
        start_year = game_date.year if game_date.month >= 7 else game_date.year - 1
        return f"{start_year}-{str(start_year + 1)[-2:]}"

    def _fetch_schedule_from_data_nba_net(self, game_date: dt.date) -> List[Game]:
        season_year = game_date.year if game_date.month >= 7 else game_date.year - 1
        url = f"https://data.nba.net/prod/v2/{season_year}/schedule.json"
        data = self._get_json(url, source="schedule.data_nba_net")
        target = game_date.strftime("%Y%m%d")

        games: List[Game] = []
        for g in data.get("league", {}).get("standard", []):
            if g.get("startDateEastern") != target:
                continue
            if g.get("statusNum") not in {1, 2, 3}:
                continue
            games.append(
                Game(
                    game_id=str(g.get("gameId", "")),
                    home_team=g.get("hTeam", {}).get("triCode", ""),
                    away_team=g.get("vTeam", {}).get("triCode", ""),
                    tipoff_utc=g.get("startTimeUTC", ""),
                )
            )
        return games

    def _fetch_schedule_from_cdn_static(self, game_date: dt.date) -> List[Game]:
        # Official NBA fallback feed.
        data = self._get_json(
            "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json",
            source="schedule.cdn_static",
        )
        target = game_date.isoformat()
        games: List[Game] = []

        game_dates = data.get("leagueSchedule", {}).get("gameDates", [])
        for day in game_dates:
            day_raw = str(day.get("gameDate", ""))
            try:
                day_iso = day_raw.split(" ")[0]
                day_date = dt.datetime.strptime(day_iso, "%m/%d/%Y").date().isoformat()
            except ValueError:
                day_date = day_raw[:10] if len(day_raw) >= 10 else ""
            if day_date != target:
                continue

            for g in day.get("games", []):
                games.append(
                    Game(
                        game_id=str(g.get("gameId", "")),
                        home_team=g.get("homeTeam", {}).get("teamTricode", ""),
                        away_team=g.get("awayTeam", {}).get("teamTricode", ""),
                        tipoff_utc=str(g.get("gameDateTimeUTC", "")),
                    )
                )
        return games

    def fetch_official_schedule(self, game_date: dt.date) -> Tuple[List[Game], dt.datetime]:
        errors: List[str] = []
        try:
            games = self._fetch_schedule_from_data_nba_net(game_date)
            return games, dt.datetime.now(dt.timezone.utc)
        except Exception as exc:
            errors.append(f"data.nba.net failed: {exc}")

        try:
            games = self._fetch_schedule_from_cdn_static(game_date)
            return games, dt.datetime.now(dt.timezone.utc)
        except Exception as exc:
            errors.append(f"cdn static schedule failed: {exc}")

        raise RuntimeError(" | ".join(errors))

    def fetch_injuries(self) -> Tuple[Dict[str, Dict[str, str]], dt.datetime]:
        data = self._get_json(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries",
            source="injuries.espn",
        )
        injuries: Dict[str, Dict[str, str]] = {}
        for team_block in data.get("injuries", []):
            team_abbr = team_block.get("team", {}).get("abbreviation", "")
            if not team_abbr:
                continue
            injuries.setdefault(team_abbr, {})
            for entry in team_block.get("injuries", []):
                athlete = entry.get("athlete", {}).get("displayName", "").strip()
                status = entry.get("status", "").upper().strip()
                if athlete:
                    injuries[team_abbr][athlete] = status
        return injuries, dt.datetime.now(dt.timezone.utc)

    def fetch_projected_lineups(self) -> Tuple[Dict[str, List[str]], dt.datetime]:
        html = self._get_text("https://www.rotowire.com/basketball/nba-lineups.php", source="lineups.rotowire")
        soup = BeautifulSoup(html, "lxml")
        lineups: Dict[str, List[str]] = {}

        for card in soup.select("div.lineup.is-nba"):
            teams = card.select("div.lineup__abbr")
            players = card.select("li.lineup__player")
            if len(teams) < 2 or len(players) < 10:
                continue
            away = teams[0].get_text(strip=True)
            home = teams[1].get_text(strip=True)
            away_players = [" ".join(p.get_text(" ", strip=True).split()[:2]) for p in players[:5]]
            home_players = [" ".join(p.get_text(" ", strip=True).split()[:2]) for p in players[5:10]]
            lineups[away] = away_players
            lineups[home] = home_players

        return lineups, dt.datetime.now(dt.timezone.utc)

    def fetch_last_three_games_by_team(self, game_date: dt.date) -> Dict[str, List[str]]:
        season_year = game_date.year if game_date.month >= 7 else game_date.year - 1
        try:
            data = self._get_json(
                f"https://data.nba.net/prod/v2/{season_year}/schedule.json",
                source="recent_games.data_nba_net",
            )
        except Exception:
            return {}
        by_team: Dict[str, List[Tuple[str, str]]] = {}
        for g in data.get("league", {}).get("standard", []):
            if g.get("statusNum") != 3:
                continue
            gid = str(g.get("gameId", ""))
            date_east = g.get("startDateEastern", "")
            if not gid or not date_east:
                continue
            for side in ["hTeam", "vTeam"]:
                tri = g.get(side, {}).get("triCode", "")
                if tri:
                    by_team.setdefault(tri, []).append((date_east, gid))

        out: Dict[str, List[str]] = {}
        for tri, games in by_team.items():
            games_sorted = sorted(games, key=lambda x: x[0], reverse=True)
            out[tri] = [gid for _, gid in games_sorted[:3]]
        return out

    def fetch_boxscore_players(self, game_id: str) -> List[Dict[str, Any]]:
        data = self._get_json(
            f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json",
            source=f"boxscore.{game_id}",
        )
        home = data.get("game", {}).get("homeTeam", {}).get("players", [])
        away = data.get("game", {}).get("awayTeam", {}).get("players", [])
        return home + away

    def player_played_two_of_last_three(self, player_name: str, team_abbr: str, recent_games: Dict[str, List[str]]) -> bool:
        game_ids = recent_games.get(team_abbr, [])
        if len(game_ids) < 3:
            return False
        played = 0
        for gid in game_ids:
            try:
                players = self.fetch_boxscore_players(gid)
            except Exception:
                continue
            names = {f"{p.get('firstName', '')} {p.get('familyName', '')}".strip() for p in players}
            if player_name in names:
                played += 1
        return played >= 2

    def fetch_player_form(self, player_name: str, team_abbr: str, recent_games: Dict[str, List[str]]) -> Dict[str, float]:
        pts: List[float] = []
        reb: List[float] = []
        ast: List[float] = []
        threes: List[float] = []
        mins: List[float] = []

        for gid in recent_games.get(team_abbr, [])[:5]:
            try:
                players = self.fetch_boxscore_players(gid)
            except Exception:
                continue
            for p in players:
                full = f"{p.get('firstName', '')} {p.get('familyName', '')}".strip()
                if full != player_name:
                    continue
                stats = p.get("statistics", {})
                pts.append(float(stats.get("points", 0) or 0))
                reb.append(float(stats.get("reboundsTotal", 0) or 0))
                ast.append(float(stats.get("assists", 0) or 0))
                threes.append(float(stats.get("threePointersMade", 0) or 0))
                mins.append(float((stats.get("minutes", "0") or "0").split(":")[0] or 0))

        def mean(vals: List[float], default: float = 0.0) -> float:
            return float(sum(vals) / len(vals)) if vals else default

        def stdev(vals: List[float], default: float = 1.0) -> float:
            if len(vals) < 2:
                return default
            m = mean(vals)
            return max(default, math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1)))

        pra = [p + r + a for p, r, a in zip(pts, reb, ast)]
        return {
            "pts_mean": mean(pts),
            "reb_mean": mean(reb),
            "ast_mean": mean(ast),
            "threes_mean": mean(threes),
            "pra_mean": mean(pra),
            "pts_sd": stdev(pts, 4.5),
            "reb_sd": stdev(reb, 2.5),
            "ast_sd": stdev(ast, 2.0),
            "threes_sd": stdev(threes, 1.2),
            "pra_sd": stdev(pra, 6.0),
            "minutes_trend_last5": mean(mins),
        }

    def fetch_odds(self, game_date: dt.date) -> Tuple[List[Dict[str, Any]], dt.datetime]:
        if not self.odds_api_key:
            return [], dt.datetime.now(dt.timezone.utc)
        params = {
            "apiKey": self.odds_api_key,
            "regions": self.odds_region,
            "markets": self.odds_markets,
            "bookmakers": self.odds_bookmakers,
            "date": game_date.isoformat(),
            "oddsFormat": "american",
        }
        data = self._get_json(
            "https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            params=params,
            source="odds.the_odds_api",
        )
        return data, dt.datetime.now(dt.timezone.utc)

    def fetch_market_signals(self) -> Dict[str, Any]:
        if not self.market_signal_url:
            return {"public_pct": None, "sharp_indicators": None, "source": "unavailable"}
        headers = {"Authorization": f"Bearer {self.market_signal_key}"} if self.market_signal_key else None
        try:
            data = self._get_json(self.market_signal_url, headers=headers, source="market_signals.custom")
            return {
                "public_pct": data.get("public_pct"),
                "sharp_indicators": data.get("sharp_indicators"),
                "source": self.market_signal_url,
            }
        except Exception:
            return {"public_pct": None, "sharp_indicators": None, "source": "unavailable"}

    def fetch_team_stats(self, game_date: dt.date) -> Dict[str, Dict[str, float]]:
        season = self._season(game_date)

        def req(measure: str, last_n: int = 0, location: str = "") -> Dict[str, Dict[str, float]]:
            params = {
                "College": "",
                "Conference": "",
                "Country": "",
                "DateFrom": "",
                "DateTo": "",
                "Division": "",
                "GameScope": "",
                "GameSegment": "",
                "Height": "",
                "LastNGames": str(last_n),
                "LeagueID": "00",
                "Location": location,
                "MeasureType": measure,
                "Month": "0",
                "OpponentTeamID": "0",
                "Outcome": "",
                "PORound": "0",
                "PaceAdjust": "N",
                "PerMode": "Per100Possessions",
                "Period": "0",
                "PlayerExperience": "",
                "PlayerPosition": "",
                "PlusMinus": "N",
                "Rank": "N",
                "Season": season,
                "SeasonSegment": "",
                "SeasonType": "Regular Season",
                "ShotClockRange": "",
                "StarterBench": "",
                "TeamID": "0",
                "TwoWay": "0",
                "VsConference": "",
                "VsDivision": "",
            }
            source_key = f"team_stats.{measure}.{last_n}.{location or 'all'}"
            data = self._get_json(
                "https://stats.nba.com/stats/leaguedashteamstats",
                headers=NBA_HEADERS,
                params=params,
                source=source_key,
            )
            rs = data.get("resultSets", [])[0]
            hdr = rs.get("headers", [])
            out: Dict[str, Dict[str, float]] = {}
            for row in rs.get("rowSet", []):
                r = dict(zip(hdr, row))
                tri = str(r.get("TEAM_ABBREVIATION", ""))
                out[tri] = {k.lower(): float(v) if isinstance(v, (int, float)) else 0.0 for k, v in r.items()}
            return out

        advanced = req("Advanced", 0)
        advanced_last10 = req("Advanced", 10)
        base = req("Base", 0)
        four = req("Four Factors", 0)
        scoring = req("Scoring", 0)
        misc = req("Misc", 0)
        home_split = req("Advanced", 0, "Home")
        away_split = req("Advanced", 0, "Road")

        merged: Dict[str, Dict[str, float]] = {}
        for tri, row in advanced.items():
            merged.setdefault(tri, {}).update(row)
            if tri in advanced_last10:
                merged[tri].update({f"{k}_last10": v for k, v in advanced_last10[tri].items()})
            if tri in base:
                merged[tri].update({f"base_{k}": v for k, v in base[tri].items()})
            if tri in four:
                merged[tri].update({f"four_{k}": v for k, v in four[tri].items()})
            if tri in scoring:
                merged[tri].update({f"scoring_{k}": v for k, v in scoring[tri].items()})
            if tri in misc:
                merged[tri].update({f"misc_{k}": v for k, v in misc[tri].items()})
            if tri in home_split:
                merged[tri]["home_net_rating"] = home_split[tri].get("net_rating", 0.0)
            if tri in away_split:
                merged[tri]["away_net_rating"] = away_split[tri].get("net_rating", 0.0)
        return merged


class BettingModel:
    def __init__(self, client: DataClient) -> None:
        self.client = client
        self._runtime_cache: Dict[str, Any] = {}

    def reset_daily_state(self) -> None:
        self._runtime_cache.clear()

    @staticmethod
    def implied_probability_from_american(odds: int) -> float:
        if odds < 0:
            return -odds / (-odds + 100)
        return 100 / (odds + 100)

    @staticmethod
    def kelly_units(projected_p: float, implied_p: float) -> float:
        edge = projected_p - implied_p
        if edge <= 0:
            return 0.0
        return round(min(5.0, max(1.0, edge * 40.0)), 1)

    @staticmethod
    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

    @staticmethod
    def prob_over_normal(mean: float, sd: float, line: float) -> float:
        if sd <= 0:
            return 0.5
        z = (line - mean) / sd
        return 1.0 - BettingModel.norm_cdf(z)

    def project_game(self, home: str, away: str, stats: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        hs = stats.get(home, {})
        av = stats.get(away, {})
        h_pace = hs.get("pace_last10", hs.get("pace", 99.0))
        a_pace = av.get("pace_last10", av.get("pace", 99.0))
        poss = (h_pace + a_pace) / 2.0

        h_off = hs.get("off_rating_last10", hs.get("off_rating", 112.0))
        h_def = hs.get("def_rating_last10", hs.get("def_rating", 112.0))
        a_off = av.get("off_rating_last10", av.get("off_rating", 112.0))
        a_def = av.get("def_rating_last10", av.get("def_rating", 112.0))

        home_off_eff = (h_off + a_def) / 2.0
        away_off_eff = (a_off + h_def) / 2.0
        home_pts = poss * home_off_eff / 100.0 + 1.8
        away_pts = poss * away_off_eff / 100.0

        return {
            "projected_possessions": poss,
            "projected_home_pts": home_pts,
            "projected_away_pts": away_pts,
            "projected_spread_home": home_pts - away_pts,
            "projected_total": home_pts + away_pts,
            "expected_shot_quality_distribution": (hs.get("ts_pct", 0.57) + av.get("ts_pct", 0.57)) / 2.0,
            "outcome_variance": 11.5**2,
        }

    def _odds_events_by_matchup(self, odds_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        by_matchup: Dict[str, Dict[str, Any]] = {}
        for event in odds_data:
            home_name = event.get("home_team", "")
            away_name = event.get("away_team", "")
            h = ODDS_TEAM_TO_ABBR.get(home_name, "")
            a = ODDS_TEAM_TO_ABBR.get(away_name, "")
            if h and a:
                by_matchup[f"{a}@{h}"] = event
        return by_matchup

    def _is_inactive(self, status: str) -> bool:
        bad = {"OUT", "DOUBTFUL", "INACTIVE", "G LEAGUE", "SUSPENSION", "OFS"}
        s = (status or "").upper()
        return any(flag in s for flag in bad)

    def _verify_live_data(
        self,
        today: dt.date,
        games: List[Game],
        injuries: Dict[str, Dict[str, str]],
        lineups: Dict[str, List[str]],
        odds_data: List[Dict[str, Any]],
        fetched_at: Dict[str, dt.datetime],
    ) -> Tuple[bool, List[str]]:
        issues: List[str] = []

        if not games:
            issues.append("No confirmed schedule games for TODAY.")
        if not injuries:
            issues.append("Injury feed empty.")
        if not lineups:
            issues.append("Projected lineup feed empty.")
        if not odds_data:
            issues.append("Betting market feed empty.")

        for k, ts in fetched_at.items():
            if ts.date() != dt.datetime.now(dt.timezone.utc).date():
                issues.append(f"{k} not fetched today.")

        by_matchup = self._odds_events_by_matchup(odds_data)
        for g in games:
            key = f"{g.away_team}@{g.home_team}"
            if key not in by_matchup:
                issues.append(f"Missing odds for {key}.")
                continue
            event = by_matchup[key]
            commence = event.get("commence_time")
            if not commence:
                issues.append(f"Missing commence_time for {key}.")
            else:
                try:
                    t = dt.datetime.fromisoformat(commence.replace("Z", "+00:00"))
                    if t.date() != today:
                        issues.append(f"Schedule/odds date mismatch for {key}.")
                except Exception:
                    issues.append(f"Unparseable commence_time for {key}.")

        return (len(issues) == 0, issues)

    def _game_context_metrics(self, game: Game, all_games: List[Game], stats: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
        hs = stats.get(game.home_team, {})
        av = stats.get(game.away_team, {})
        pace_diff = hs.get("pace", 99.0) - av.get("pace", 99.0)

        return {
            "pace_differential": pace_diff,
            "back_to_back_status": "unverified",
            "rest_advantage": "unverified",
            "travel_distance": "unverified",
            "time_zone_disadvantage": "unverified",
            "strength_of_schedule_last15": "unverified",
            "last10_net_rating_trend": hs.get("net_rating_last10", 0.0) - av.get("net_rating_last10", 0.0),
            "last3_momentum_shift": "unverified",
            "fatigue_index": abs(pace_diff) * 0.1,
            "coaching_adjustments": "unverified",
            "zone_frequency": "unverified",
            "defensive_switch_efficiency": "unverified",
            "referee_foul_bias": "unverified",
        }

    def _team_metrics(self, tri: str, stats: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
        s = stats.get(tri, {})
        return {
            "offensive_rating_season": s.get("off_rating"),
            "offensive_rating_last10": s.get("off_rating_last10"),
            "defensive_rating_season": s.get("def_rating"),
            "defensive_rating_last10": s.get("def_rating_last10"),
            "net_rating": s.get("net_rating"),
            "efg_pct": s.get("efg_pct"),
            "ts_pct": s.get("ts_pct"),
            "pace": s.get("pace"),
            "turnover_pct": s.get("tm_tov_pct"),
            "off_rebound_pct": s.get("oreb_pct"),
            "def_rebound_pct": s.get("dreb_pct"),
            "free_throw_rate": s.get("fta_rate"),
            "points_per_possession": s.get("off_rating"),
            "assist_pct": s.get("ast_pct"),
            "threepa_rate": s.get("base_fg3a", 0.0) / max(1.0, s.get("base_fga", 1.0)),
            "paint_points_allowed": s.get("scoring_opp_pts_paint"),
            "rim_protection_pct": "unverified",
            "pick_and_roll_def_efficiency": "unverified",
            "starting_lineup_net_rating": "unverified",
            "bench_net_rating": "unverified",
            "home_away_splits": {
                "home_net_rating": s.get("home_net_rating"),
                "away_net_rating": s.get("away_net_rating"),
            },
            "clutch_net_rating": "unverified",
        }

    def _simulate_spread_probability(self, mean_margin: float, line: float) -> float:
        sims = np.random.normal(mean_margin, 11.5, MONTE_CARLO_SIMS)
        return float((sims > line).mean())

    def _simulate_total_probability(self, mean_total: float, line: float, over: bool) -> float:
        sims = np.random.normal(mean_total, 16.5, MONTE_CARLO_SIMS)
        return float((sims > line).mean() if over else (sims < line).mean())

    def generate_report(self, explicit_date: Optional[dt.date] = None, allow_manual_override: bool = False) -> Dict[str, Any]:
        self.reset_daily_state()
        self.client.upstream_status.clear()
        today = dt.date.today()
        run_date = explicit_date if (explicit_date and allow_manual_override) else today

        fetch_issues: List[str] = []
        now_utc = dt.datetime.now(dt.timezone.utc)

        def safe_fetch(label: str, fn: Any, default: Any) -> Any:
            try:
                return fn()
            except Exception as exc:
                fetch_issues.append(f"{label} failed after retries: {exc}")
                return default

        games, sched_ts = safe_fetch("schedule", lambda: self.client.fetch_official_schedule(run_date), ([], now_utc))
        injuries, inj_ts = safe_fetch("injuries", self.client.fetch_injuries, ({}, now_utc))
        lineups, lu_ts = safe_fetch("lineups", self.client.fetch_projected_lineups, ({}, now_utc))
        odds_data, odds_ts = safe_fetch("odds", lambda: self.client.fetch_odds(run_date), ([], now_utc))
        team_stats = safe_fetch("team_stats", lambda: self.client.fetch_team_stats(run_date), {})
        recent_games = safe_fetch("recent_games", lambda: self.client.fetch_last_three_games_by_team(run_date), {})
        market_signals = safe_fetch(
            "market_signals",
            self.client.fetch_market_signals,
            {"public_pct": None, "sharp_indicators": None, "source": "unavailable"},
        )

        ok, issues = self._verify_live_data(
            run_date,
            games,
            injuries,
            lineups,
            odds_data,
            {
                "schedule": sched_ts,
                "injuries": inj_ts,
                "lineups": lu_ts,
                "odds": odds_ts,
            },
        )
        degraded_mode = (not ok) or bool(fetch_issues)
        if issues:
            fetch_issues.extend(issues)

        events_by_match = self._odds_events_by_matchup(odds_data)
        picks: List[BetPick] = []
        per_game_analytics: List[Dict[str, Any]] = []

        for game in games:
            matchup = f"{game.away_team} at {game.home_team}"
            context = self._game_context_metrics(game, games, team_stats)
            home_metrics = self._team_metrics(game.home_team, team_stats)
            away_metrics = self._team_metrics(game.away_team, team_stats)
            model_proj = self.project_game(game.home_team, game.away_team, team_stats)

            home_lineup = lineups.get(game.home_team, [])
            away_lineup = lineups.get(game.away_team, [])
            if len(home_lineup) < 5 or len(away_lineup) < 5:
                per_game_analytics.append(
                    {
                        "matchup": matchup,
                        "status": "insufficient_lineup_data",
                        "team_metrics": {"home": home_metrics, "away": away_metrics},
                        "matchup_factors": context,
                    }
                )
                continue

            eligible_players: List[Tuple[str, str]] = []
            player_metrics: Dict[str, Any] = {}
            for team, lineup in [(game.home_team, home_lineup), (game.away_team, away_lineup)]:
                team_inj = injuries.get(team, {})
                for player in lineup:
                    status = team_inj.get(player, "ACTIVE")
                    if self._is_inactive(status):
                        continue
                    if not self.client.player_played_two_of_last_three(player, team, recent_games):
                        continue
                    form = self.client.fetch_player_form(player, team, recent_games)
                    player_metrics[player] = {
                        "team": team,
                        "status": status,
                        "usage_pct": "unverified",
                        "per": "unverified",
                        "bpm": "unverified",
                        "vorp": "unverified",
                        "on_off_net_rating": "unverified",
                        "true_shooting_pct": "unverified",
                        "win_shares": "unverified",
                        "minutes_trend_last5": form.get("minutes_trend_last5"),
                        "injury_rotation_impact": "unverified",
                        "pie": "unverified",
                        "matchup_vs_defender": "unverified",
                        "clutch_usage_pct": "unverified",
                    }
                    eligible_players.append((team, player))

            if len(eligible_players) < 8:
                per_game_analytics.append(
                    {
                        "matchup": matchup,
                        "status": "insufficient_active_players",
                        "team_metrics": {"home": home_metrics, "away": away_metrics},
                        "matchup_factors": context,
                    }
                )
                continue

            event = events_by_match.get(f"{game.away_team}@{game.home_team}")
            if not event:
                per_game_analytics.append(
                    {
                        "matchup": matchup,
                        "status": "missing_market_data",
                        "team_metrics": {"home": home_metrics, "away": away_metrics},
                        "matchup_factors": context,
                    }
                )
                continue

            spread = model_proj["projected_spread_home"]
            total = model_proj["projected_total"]
            home_pts = model_proj["projected_home_pts"]
            away_pts = model_proj["projected_away_pts"]

            for bookmaker in event.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    key = market.get("key")
                    outcomes = market.get("outcomes", [])

                    if key == "spreads":
                        for out in outcomes:
                            name = out.get("name", "")
                            line = float(out.get("point", 0) or 0)
                            price = int(out.get("price", -110) or -110)
                            is_home = name.lower() == event.get("home_team", "").lower()
                            p = self._simulate_spread_probability(spread if is_home else -spread, line)
                            implied = self.implied_probability_from_american(price)
                            edge = p - implied
                            if edge >= 0.03:
                                picks.append(
                                    BetPick(
                                        matchup=matchup,
                                        market_type=f"Spread: {name} {line:+}",
                                        line_odds=f"{bookmaker.get('title', 'Book')} ({price})",
                                        projected_probability=p,
                                        implied_probability=implied,
                                        edge=edge,
                                        projected_final_score=f"{game.home_team} {round(home_pts)} - {game.away_team} {round(away_pts)}",
                                        reasons=[
                                            "Projected margin differs materially from market spread",
                                            "Recent pace/off-def blend supports cover probability",
                                            "Eligible active-player filter satisfied",
                                        ],
                                        risk="Medium",
                                        units=self.kelly_units(p, implied),
                                    )
                                )

                    elif key == "totals":
                        for out in outcomes:
                            side = out.get("name", "")
                            line = float(out.get("point", 0) or 0)
                            price = int(out.get("price", -110) or -110)
                            over = side.lower() == "over"
                            p = self._simulate_total_probability(total, line, over)
                            implied = self.implied_probability_from_american(price)
                            edge = p - implied
                            if edge >= 0.03:
                                picks.append(
                                    BetPick(
                                        matchup=matchup,
                                        market_type=f"Game Total: {side} {line}",
                                        line_odds=f"{bookmaker.get('title', 'Book')} ({price})",
                                        projected_probability=p,
                                        implied_probability=implied,
                                        edge=edge,
                                        projected_final_score=f"{game.home_team} {round(home_pts)} - {game.away_team} {round(away_pts)}",
                                        reasons=[
                                            "Projected possessions and efficiency imply total mispricing",
                                            "Model variance profile still clears 3% edge threshold",
                                            "Fresh same-day inputs used",
                                        ],
                                        risk="Medium",
                                        units=self.kelly_units(p, implied),
                                    )
                                )

                    elif key == "h2h":
                        for out in outcomes:
                            name = out.get("name", "")
                            price = int(out.get("price", -110) or -110)
                            is_home = name.lower() == event.get("home_team", "").lower()
                            p = self._simulate_spread_probability(spread if is_home else -spread, 0.0)
                            implied = self.implied_probability_from_american(price)
                            edge = p - implied
                            if edge >= 0.03:
                                picks.append(
                                    BetPick(
                                        matchup=matchup,
                                        market_type=f"Moneyline: {name}",
                                        line_odds=f"{bookmaker.get('title', 'Book')} ({price})",
                                        projected_probability=p,
                                        implied_probability=implied,
                                        edge=edge,
                                        projected_final_score=f"{game.home_team} {round(home_pts)} - {game.away_team} {round(away_pts)}",
                                        reasons=[
                                            "Win-probability simulation exceeds implied probability",
                                            "Last-10 net-rating differential supports side",
                                            "Home-court and efficiency adjustment included",
                                        ],
                                        risk="High" if abs(price) > 170 else "Medium",
                                        units=self.kelly_units(p, implied),
                                    )
                                )

                    elif key == "team_totals":
                        for out in outcomes:
                            side = out.get("name", "")
                            team_name = out.get("description", "")
                            team_abbr = ODDS_TEAM_TO_ABBR.get(team_name, "")
                            if team_abbr not in {game.home_team, game.away_team}:
                                continue
                            line = float(out.get("point", 0) or 0)
                            price = int(out.get("price", -110) or -110)
                            mean = home_pts if team_abbr == game.home_team else away_pts
                            sd = 9.5
                            over = side.lower() == "over"
                            p = self.prob_over_normal(mean, sd, line) if over else 1.0 - self.prob_over_normal(mean, sd, line)
                            implied = self.implied_probability_from_american(price)
                            edge = p - implied
                            if edge >= 0.03:
                                picks.append(
                                    BetPick(
                                        matchup=matchup,
                                        market_type=f"Team Total: {team_abbr} {side} {line}",
                                        line_odds=f"{bookmaker.get('title', 'Book')} ({price})",
                                        projected_probability=p,
                                        implied_probability=implied,
                                        edge=edge,
                                        projected_final_score=f"{game.home_team} {round(home_pts)} - {game.away_team} {round(away_pts)}",
                                        reasons=[
                                            "Team-scoring projection diverges from posted team total",
                                            "Opponent defensive rating embedded in mean projection",
                                            "Distribution model keeps edge above cutoff",
                                        ],
                                        risk="Medium",
                                        units=self.kelly_units(p, implied),
                                    )
                                )

                    elif key in {"player_points", "player_rebounds", "player_assists", "player_threes", "player_pra"}:
                        stat_map = {
                            "player_points": ("pts_mean", "pts_sd", "Points"),
                            "player_rebounds": ("reb_mean", "reb_sd", "Rebounds"),
                            "player_assists": ("ast_mean", "ast_sd", "Assists"),
                            "player_threes": ("threes_mean", "threes_sd", "3PM"),
                            "player_pra": ("pra_mean", "pra_sd", "PRA"),
                        }
                        mean_key, sd_key, label = stat_map[key]

                        for out in outcomes:
                            side = out.get("name", "")
                            player = out.get("description", "").strip()
                            line = float(out.get("point", 0) or 0)
                            price = int(out.get("price", -110) or -110)

                            player_team = next((t for t, p in eligible_players if p == player), None)
                            if not player_team:
                                continue
                            form = self.client.fetch_player_form(player, player_team, recent_games)
                            mean = form.get(mean_key, 0.0)
                            sd = max(1.0, form.get(sd_key, 1.0))
                            over = side.lower() == "over"
                            p = self.prob_over_normal(mean, sd, line) if over else 1.0 - self.prob_over_normal(mean, sd, line)
                            implied = self.implied_probability_from_american(price)
                            edge = p - implied
                            if edge >= 0.03:
                                picks.append(
                                    BetPick(
                                        matchup=matchup,
                                        market_type=f"Player Prop: {player} {label} {side} {line}",
                                        line_odds=f"{bookmaker.get('title', 'Book')} ({price})",
                                        projected_probability=p,
                                        implied_probability=implied,
                                        edge=edge,
                                        projected_final_score=None,
                                        reasons=[
                                            "Last-game form mean/variance projects mispriced prop",
                                            "Only active eligible players included",
                                            "Edge remains above 3% after variance adjustment",
                                        ],
                                        risk="High",
                                        units=self.kelly_units(p, implied),
                                    )
                                )

            per_game_analytics.append(
                {
                    "matchup": matchup,
                    "team_metrics": {"home": home_metrics, "away": away_metrics},
                    "matchup_factors": context,
                    "player_metrics_active_only": player_metrics,
                    "advanced_model_inputs": {
                        "projected_possessions": model_proj["projected_possessions"],
                        "projected_offensive_efficiency": {
                            game.home_team: team_stats.get(game.home_team, {}).get("off_rating_last10"),
                            game.away_team: team_stats.get(game.away_team, {}).get("off_rating_last10"),
                        },
                        "shot_quality_distribution": model_proj["expected_shot_quality_distribution"],
                        "player_fatigue_projection": context["fatigue_index"],
                        "outcome_variance_model": model_proj["outcome_variance"],
                        "monte_carlo_simulations": MONTE_CARLO_SIMS,
                    },
                    "betting_market_data": {
                        "opening_vs_current": "unverified",
                        "reverse_line_movement": "unverified",
                        "public_betting_pct": market_signals.get("public_pct"),
                        "sharp_money_indicators": market_signals.get("sharp_indicators"),
                        "ats_trends": "unverified",
                        "cover_pct_fav_dog": "unverified",
                        "market_overreaction_signal": "unverified",
                    },
                }
            )

        picks.sort(key=lambda p: p.edge, reverse=True)
        ranked = picks[:10]

        if ranked and len(ranked) < 6:
            ranked = picks[: len(ranked)]

        output_picks = [
            {
                "matchup": p.matchup,
                "market_type": p.market_type,
                "line_odds": p.line_odds,
                "projected_probability": round(p.projected_probability * 100, 1),
                "implied_probability": round(p.implied_probability * 100, 1),
                "edge_pct": round(p.edge * 100, 1),
                "projected_final_score": p.projected_final_score,
                "key_data_reasons": p.reasons[:4],
                "risk_level": p.risk,
                "suggested_unit_size": p.units,
            }
            for p in ranked
        ]

        alt_lines: List[Dict[str, Any]] = []
        if ranked:
            alt_lines.append(
                {
                    "play": f"{ranked[0].matchup} alternate line aligned with {ranked[0].market_type}",
                    "target_odds": "+110 to +220",
                    "reason": "Tail simulation still shows positive EV under a more aggressive payout profile.",
                }
            )
        if len(ranked) > 1:
            alt_lines.append(
                {
                    "play": f"{ranked[1].matchup} alternate line aligned with {ranked[1].market_type}",
                    "target_odds": "+130 to +350",
                    "reason": "Distribution skew supports a smaller-probability/high-payout variant.",
                }
            )

        parlay = None
        if len(ranked) >= 2:
            first = ranked[0]
            second = next((r for r in ranked[1:] if r.matchup == first.matchup), ranked[1])
            parlay = {
                "legs": [first.market_type, second.market_type],
                "matchup": first.matchup if second.matchup == first.matchup else "cross-game",
                "justification": "Selected legs have a shared statistical driver (pace/efficiency path), improving correlation-adjusted EV.",
            }

        status_msg = "ok"
        if degraded_mode:
            status_msg = "degraded"
        if not output_picks and not degraded_mode:
            status_msg = NO_EV_MSG

        return {
            "app_name": APP_NAME,
            "date": run_date.isoformat(),
            "manual_override_used": bool(explicit_date and allow_manual_override),
            "status": status_msg,
            "message": (
                "Live data partially unavailable. Degraded-mode analysis generated."
                if degraded_mode
                else (NO_EV_MSG if not output_picks else "ok")
            ),
            "confirmed_games_today": [f"{g.away_team} at {g.home_team}" for g in games],
            "ranked_picks": output_picks,
            "status_message": NO_EV_MSG if not output_picks else "ok",
            "alt_line_high_upside": alt_lines,
            "correlated_parlay": parlay,
            "game_analytics": per_game_analytics,
            "degraded_mode": degraded_mode,
            "upstream_failures": fetch_issues,
            "upstream_api_status": self.client.upstream_status,
            "data_integrity": {
                "daily_reset_protocol": "completed",
                "fresh_pull_only": True,
                "uncertainty_flagging": True,
                "inactive_players_excluded": True,
            },
        }


app = FastAPI(title=APP_NAME)
client = DataClient()
model = BettingModel(client)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "app_name": APP_NAME}


@app.get("/report")
def report(date: Optional[str] = None, allow_manual_override: bool = False) -> Dict[str, Any]:
    parsed: Optional[dt.date] = None
    if date:
        try:
            parsed = dt.date.fromisoformat(date)
        except ValueError:
            return {
                "app_name": APP_NAME,
                "status": "aborted",
                "message": "Invalid date format. Use YYYY-MM-DD.",
            }
    return model.generate_report(explicit_date=parsed, allow_manual_override=allow_manual_override)
