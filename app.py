# Force-refresh deployment 2025-08-24-v5-NORMALIZATION
import sys
import re
from flask import Flask, render_template, request, jsonify
from time import time
import threading
import os
import csv
import glob
import pandas as pd
from datetime import datetime, timedelta
import pytz

# --- Local Imports ---
# Make sure scraper.py and scraper_pokemon.py are in the same directory
import scraper 
import scraper_pokemon
import data_loader

# Initialize local Pokemon TCG data so search has data
try:
    data_loader.load_data()
except Exception as e:
    print(f"[startup] Warning: failed to load TCG data: {e}")

app = Flask(__name__, template_folder="templates")

# --- Helper Functions ---
def parse_date_from_filename(name):
    """Parses a date from a filename string with multiple possible formats."""
    formats_to_try = ["%d %m %Y", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"]
    ...

@app.route("/")
def home():
    ...
    return render_template("index.html", mode="list", items=page_items, page=page, total=len(all_items), has_more=end < len(all_items), base_url=request.url_root)

@app.route("/item")
def item_detail():
    ...
    return render_template("index.html", mode="detail", item=item_details, base_url=request.url_root)

@app.route("/tcg-tracker")
def tcg_tracker_page():
    """Renders the TCG Tracker page."""
    # This route now correctly serves the tcg_tracker.html template
    # and passes the necessary base_url for API calls.
    return render_template("tcg_tracker.html", base_url=request.url_root)

@app.route("/wallpapers")
def wallpapers():
    ...
    return render_template("wallpapers.html", wallpapers=wallpapers, base_url=request.url_root)

@app.route("/marketplace")
def marketplace():
    ...
    return render_template("marketplace.html", items=page_items, page=page, has_more=end < len(all_items), total=len(all_items))

@app.route("/top100")
def top100():
    ...
    return render_template("top100.html", cards=cards)

# --- API Routes ---
@app.route("/api/market-status")
def api_market_status():
    """Analyzes price history to provide a market overview."""
    history_dir = "Greek_Prices_History"
    # Define categories and associated keywords for market analysis
    categories = { "Booster Packs": ["Booster Pack"], "Booster Box": ...
    ...
    return jsonify({"updated": updated, "categories": out})

@app.route("/api/items")
def api_items():
    ...
    return jsonify({"items": page_items, "page": page, "has_more": end < len(all_items), "total": len(all_items)})

@app.route("/api/suggest")
def api_suggest():
    """Provides search suggestions as the user types."""
    q = (request.args.get("q") or "").strip()
    if not q: return jsonify({"items": []})
    return jsonify({"items": scraper.suggest_titles(q, limit=10)})

# --- TCG TRACKER API ROUTES ---
@app.route("/api/tcg/suggest")
def api_tcg_suggest():
    """Provides card suggestions for the TCG tracker."""
    q = (request.args.get("q") or "").strip()
    if not q: return jsonify({"items": []})
    return jsonify({"items": scraper_pokemon.search_pokemon_tcg(q, page_size=12)})

@app.route("/api/tcg/card")
def api_tcg_card():
    """Fetches detailed information for a specific card."""
    card_id = (request.args.get("id") or "").strip()
    if not card_id: return jsonify({"error": "Missing id"}), 400
    data = scraper_pokemon.get_card_details(card_id)
    if not data: return jsonify({"error": "Not found"}), 404
    return jsonify(data)

@app.route("/api/tcg/related")
def api_tcg_related():
    """Fetches cards related to a given card."""
    set_id = (request.args.get("setId") or "").strip()
    rarity = (request.args.get("rarity") or "").strip()
    card_id = (request.args.get("cardId") or "").strip()
    try:
        count = int(request.args.get("count", 8))
    except ValueError:
        count = 8
    items = scraper_pokemon.get_related_cards(set_id, rarity, card_id, count=count)
    return jsonify({"items": items})

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)
