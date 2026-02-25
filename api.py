from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from odds import get_nba_odds
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {

"status": "NBA Quant Edge API is live"}

@app.get("/picks")
def get_picks():
    model_data = {}

    file_path = "reports/today_latest.json"

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            model_data = json.load(f)

    odds_data = get_nba_odds()

    return {
        "model": model_data,
        "odds": odds_data
    }
