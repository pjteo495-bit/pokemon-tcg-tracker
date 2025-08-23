import sys
from flask import Flask, render_template, request, jsonify
from time import time
import threading
import os
import csv
import glob
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__, template_folder="templates")

# --- Load local datasets ---
try:
    from data_loader import load_data
    load_data()
    print("[init] Local card & price data loaded.")
except Exception as e:
    print("[init] WARNING: could not load local datasets:", e)

# --- App Routes ---
@app.route("/")
def index():
    # --- CHANGE: Added current date to be passed to the template ---
    current_date = datetime.now().strftime("%d %B %Y")
    return render_template("index.html", base_url=request.url_root, current_date=current_date)

@app.route("/tcg-tracker")
def tcg_tracker_page():
    return render_template("tcg_tracker.html")

@app.route("/top100")
def top_100_page():
    trending_dir = "Top 100 trending" # Use the relative path
    latest_file = None
    latest_time = 0
    if os.path.isdir(trending_dir):
        for item in os.listdir(trending_dir):
            item_path = os.path.join(trending_dir, item)
            if os.path.isdir(item_path):
                try:
                    csv_path = os.path.join(item_path, 'pokemon_wizard_prices.csv')
                    if os.path.exists(csv_path):
                        mtime = os.path.getmtime(csv_path)
                        if mtime > latest_time:
                            latest_time = mtime
                            latest_file = csv_path
                except Exception:
                    continue
    cards = []
    if latest_file:
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cards.append(row)
        except Exception as e:
            print(f"Error reading CSV file {latest_file}: {e}")
    return render_template("top100.html", cards=cards)

# --------------- Store APIs ---------------

@app.route("/api/market-status")
def api_market_status():
    history_dir = r"C:\Users\pjteo\Desktop\POKEGR_TCG_TRACKER\Greek_Prices_History"
    categories = {
        "Booster Packs": ["Booster Pack"],
        "Booster Box": ["Booster Box"],
        "Elite Trainer Box": ["Elite Trainer Box", "ETB"],
        "Binders": ["Binder"],
        "Collections": ["Collection"],
        "Tins": ["Tin"],
        "Blisters": ["Blister"],
        "Sleeves": ["Sleeves"],
        "Booster Bundles": ["Booster Bundle"],
        "Decks": ["Deck"]
    }
    cutoff_date = datetime.now() - timedelta(days=30)
    all_data = []
    files = glob.glob(os.path.join(history_dir, "*.xlsx")) + glob.glob(os.path.join(history_dir, "*.csv"))

    for file_path in files:
        try:
            file_date = datetime.strptime(os.path.splitext(os.path.basename(file_path))[0], "%d %m %Y")
            if file_date >= cutoff_date:
                df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
                df.columns = [str(c).lower().strip() for c in df.columns]
                df['date'] = file_date
                all_data.append(df)
        except Exception as e:
            print(f"Skipping history file {file_path}: {e}")
            continue
    
    if not all_data:
        return jsonify({"error": "No recent history data found"}), 404

    full_history = pd.concat(all_data, ignore_index=True)
    title_col = next((c for c in ['item_title', 'title', 'name'] if c in full_history.columns), None)
    price_col = next((c for c in ['price', 'current_price'] if c in full_history.columns), None)

    if not title_col or not price_col:
        return jsonify({"error": "Could not find title/price columns in history files"}), 500

    full_history[price_col] = pd.to_numeric(full_history[price_col].astype(str).str.replace('[€,]', '', regex=True), errors='coerce')
    full_history.dropna(subset=[price_col], inplace=True)

    market_status = []
    for category_name, keywords in categories.items():
        cat_df = full_history[full_history[title_col].str.contains('|'.join(keywords), case=False, na=False)]
        if cat_df.empty:
            continue

        changes = []
        for item, group in cat_df.groupby(title_col):
            if len(group) > 1:
                group = group.sort_values('date')
                start_price = group.iloc[0][price_col]
                end_price = group.iloc[-1][price_col]
                if start_price > 0:
                    percent_change = ((end_price - start_price) / start_price) * 100
                    changes.append(percent_change)
        
        status = 'yellow'
        explanation = '(Prices Stable)'
        if changes:
            avg_change = sum(changes) / len(changes)
            if avg_change > 2.5:
                status = 'green'
                explanation = '(Prices Rising)'
            elif avg_change < -2.5:
                status = 'red'
                explanation = '(Prices Lowering)'
        
        market_status.append({"category": category_name, "status": status, "explanation": explanation})

    return jsonify(market_status)

@app.route("/api/home")
def api_home():
    sort = request.args.get("sort", "bestsellers")
    page = max(1, int(request.args.get("page", 1)))
    page_size = 24
    from scraper import search_products_all
    all_items = search_products_all("", sort=sort)
    start = (page - 1) * page_size
    end = start + page_size
    paginated_items = all_items[start:end]
    has_more = end < len(all_items)
    return jsonify({"items": paginated_items, "has_more": has_more, "total": len(all_items)})

@app.route("/api/search")
def api_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"items": [], "has_more": False, "total": 0})
    page = max(1, int(request.args.get("page", 1)))
    sort = request.args.get("sort", "bestsellers")
    page_size = 24
    from scraper import search_products_all
    all_items = search_products_all(q, sort=sort)
    start = (page - 1) * page_size
    end = start + page_size
    return jsonify({
        "items": all_items[start:end],
        "has_more": end < len(all_items),
        "total": len(all_items)
    })

@app.route("/api/price-history")
def api_price_history():
    item_title = request.args.get("title", "").strip()
    if not item_title:
        return jsonify({"error": "Missing item title"}), 400
    history_dir = r"C:\Users\pjteo\Desktop\POKEGR_TCG_TRACKER\Greek_Prices_History"
    price_history = []
    if not os.path.isdir(history_dir):
        return jsonify({"error": "History directory not found"}), 500
    files = glob.glob(os.path.join(history_dir, "*.xlsx")) + glob.glob(os.path.join(history_dir, "*.csv"))
    for file_path in files:
        try:
            filename = os.path.basename(file_path)
            date_str = os.path.splitext(filename)[0]
            file_date = datetime.strptime(date_str, "%d %m %Y").strftime("%Y-%m-%d")
            df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
            df.columns = [str(c).lower().strip() for c in df.columns]
            title_col = next((c for c in ['item_title', 'title', 'name'] if c in df.columns), None)
            price_col = next((c for c in ['price', 'current_price'] if c in df.columns), None)
            if not title_col or not price_col:
                continue
            item_row = df[df[title_col].str.strip() == item_title]
            if not item_row.empty:
                price_str = str(item_row.iloc[0][price_col])
                price_val = float(price_str.replace('€', '').replace(',', '.').strip())
                price_history.append({"date": file_date, "price": price_val})
        except Exception as e:
            print(f"Could not process file {file_path}: {e}")
            continue
    price_history.sort(key=lambda x: x['date'])
    return jsonify(price_history)

@app.route("/api/search_skroutz")
def api_search_skroutz():
    q = (request.args.get("q") or "").strip()
    sort = request.args.get("sort", "bestsellers")
    from scraper import search_products_skroutz
    skroutz_items = search_products_skroutz(q, sort=sort)
    return jsonify({
        "items": skroutz_items,
        "has_more": False,
        "total": len(skroutz_items)
    })

@app.route("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"items": []})
    from scraper import suggest_titles
    return jsonify({"items": suggest_titles(q, limit=10)})

@app.route("/api/tcg/card")
def api_tcg_card():
    from scraper_pokemon import get_card_details
    card_id = (request.args.get("id") or "").strip()
    if not card_id:
        return jsonify({"error": "Missing id"}), 400
    data = get_card_details(card_id)
    if not data:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data)

@app.route("/api/tcg/related")
def api_tcg_related():
    from scraper_pokemon import get_related_cards
    set_id  = (request.args.get("setId") or "").strip()
    rarity  = (request.args.get("rarity") or "").strip()
    card_id = (request.args.get("cardId") or "").strip()
    items = get_related_cards(set_id, rarity, card_id, count=8)
    return jsonify({"items": items})

@app.route("/api/tcg/suggest")
def api_tcg_suggest():
    try:
        from scraper_pokemon import search_pokemon_tcg
        q = (request.args.get("q") or "").strip()
        if not q: return jsonify({"items": []})
        return jsonify({"items": search_pokemon_tcg(q, page_size=12)})
    except ImportError:
        return jsonify({"items": []})

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
