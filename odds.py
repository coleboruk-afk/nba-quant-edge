import os
import requests

def get_nba_odds():
    api_key = os.getenv("ODDS_API_KEY")

    if not api_key:
        return {"error": "ODDS_API_KEY not set"}

    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
    
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        return {"error": "Failed to fetch odds"}

    return response.json()
