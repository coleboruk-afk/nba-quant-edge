import requests
import os

ODDS_API_KEY = os.getenv("ODDS_API_KEY")

SPORT = "basketball_nba"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
BOOKMAKERS = "draftkings,fanduel,betmgm,betonlineag,lowvig,betrivers,bovada,mybookieag,betus"

EDGE_THRESHOLD = 1.5  # minimum edge %

def american_to_implied(odds):
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)

def remove_vig(prob_a, prob_b):
    total = prob_a + prob_b
    return prob_a / total, prob_b / total

def calculate_edge(fair_prob, book_prob):
    return (fair_prob - book_prob) * 100

def get_nba_odds():
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "oddsFormat": "american"
    }

    response = requests.get(url, params=params)
    games = response.json()

    ev_bets = []

    for game in games:
        matchup = f"{game['away_team']} at {game['home_team']}"

        for bookmaker in game.get("bookmakers", []):
            book_name = bookmaker["title"]

            for market in bookmaker.get("markets", []):
                outcomes = market.get("outcomes", [])

                if len(outcomes) != 2:
                    continue

                o1, o2 = outcomes

                prob1 = american_to_implied(o1["price"])
                prob2 = american_to_implied(o2["price"])

                fair1, fair2 = remove_vig(prob1, prob2)

                edge1 = calculate_edge(fair1, prob1)
                edge2 = calculate_edge(fair2, prob2)

                if edge1 >= EDGE_THRESHOLD:
                    ev_bets.append({
                        "matchup": matchup,
                        "market": market["key"],
                        "selection": o1["name"],
                        "book": book_name,
                        "odds": o1["price"],
                        "edge_percent": round(edge1, 2)
                    })

                if edge2 >= EDGE_THRESHOLD:
                    ev_bets.append({
                        "matchup": matchup,
                        "market": market["key"],
                        "selection": o2["name"],
                        "book": book_name,
                        "odds": o2["price"],
                        "edge_percent": round(edge2, 2)
                    })

    ev_bets.sort(key=lambda x: x["edge_percent"], reverse=True)

    return {
        "status": "live",
        "positive_ev_bets": ev_bets[:10]
    }
