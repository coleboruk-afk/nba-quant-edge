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
  @app.get("/picks")
def get_picks():
    return get_nba_odds()
