from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os

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
    return {"status": "NBA Quant Edge API is live"}

@app.get("/picks")
def get_picks():
    file_path = "reports/today_latest.json"

    if not os.path.exists(file_path):
        return {"error": "No picks generated yet"}

    with open(file_path, "r") as f:
        data = json.load(f)

    return data
