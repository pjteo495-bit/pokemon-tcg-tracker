# Force-refresh deployment 2025-08-27-v8-COMPARE
import sys, re, os, csv, glob, random, threading
from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import pandas as pd
import pytz

# --- Local Imports ---
try:
    import scraper
    import scraper_pokemon
    import data_loader
    # --- Load Data on Startup ---
    data_loader.load_data()
except ImportError:
    print("Warning: Local modules (scraper, scraper_pokemon, data_loader) not found. Some features may not work.")
    # Create dummy modules/functions if they don't exist to prevent crashes
    class Dummy:
        def __getattr__(self, _):
            return lambda *args, **kwargs: None
    scraper = Dummy()
    scraper_pokemon = Dummy()
    data_loader = Dummy()


app = Flask(__name__, template_folder="templates")

# ---- Config ----
# Allow overriding the FX rate via env; defaults to a conservative 0.92 EUR/USD.
USD_TO_EUR = float(os.environ.get("USD_TO_EUR", "0.86"))

# ---------- Helpers ----------
def parse_date_from_filename(name):
    for fmt in ("%d %m %Y", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(name, fmt)
        except ValueError:
            pass
    return None

def normalize_title_for_history(title):
    if not isinstance(title, str):
        return ""
    return re.sub(r'[^a-z0-9]', '', title.lower())

# === Sealed Products (HD images + EUR) ===
def _upgrade_image(url: str, level: int = 1) -> str:
    if not url:
        return url
    try:
        if "pricecharting.com" in url:
            return url.replace("/60.jpg", "/1600.jpg")

        if "i.ebayimg.com" in url:
            size = 1200 if level >= 2 else 800
            url = re.sub(r"/s-l\d+(\.\w+)(\?.*)?$", fr"/s-l{size}\1", url)

        if "tcgplayer" in url:
            box = 1000 if level >= 2 else 700
            url = re.sub(r"/fit-in/\d+x\d+/", fr"/fit-in/{box}x{box}/", url)
            url = re.sub(r"([?&])(w|width)=\d+", fr"\1\2={box}", url)
            url = re.sub(r"([?&])(q|quality)=\d+", r"\g<1>\2=90", url)
            url = url.replace("/thumbnail/", "/main/")

        url = re.sub(r"(_thumb)(\.\w+)$", r"\2", url)
    except Exception:
        pass
    return url

def _parse_price_to_float(s: str):
    if s is None:
        return None
    m = re.search(r"[\d,.]+", str(s))
    if not m:
        return None
    val = m.group(0)
    try:
        return float(val.replace(",", ""))
    except ValueError:
        try:
            return float(val.replace(".", "").replace(",", "."))
        except ValueError:
            return None

def _normalize_sealed_row(row: dict) -> dict:
    norm = {re.sub(r"\s+", "_", (k or "").strip().lower()): (v or "").strip()
            for k, v in row.items()}

    set_name = norm.get("set_name") or norm.get("set") or norm.get("series")
    item_title = norm.get("item_title") or norm.get("title") or norm.get("item") or norm.get("product_title")
    raw_price = norm.get("raw_price") or norm.get("price") or norm.get("current_price")
    image_url = norm.get("image_url") or norm.get("image") or norm.get("img_url") or norm.get("img")

    usd = _parse_price_to_float(raw_price) or 0.0
    # If the file already contains EUR, take it; else convert USD->EUR.
    eur = _parse_price_to_float(norm.get("price_eur") or "") or round(usd * USD_TO_EUR, 2)

    img_hd = _upgrade_image(image_url, level=1) or image_url
    img_xhd = _upgrade_image(image_url, level=2) or img_hd

    return {
        "set_name": set_name,
        "item_title": item_title,
        "raw_price": raw_price,
        "price_usd": usd,
        "price_eur": eur,
        "image_url": image_url,
        "image_url_hd": img_hd,
        "image_url_xhd": img_xhd,
    }

# ---------- Routes ----------
@app.route("/")
def index():
    tz = pytz.timezone('Europe/Athens')
    current_date = datetime.now(tz).strftime("%d %B %Y")
    return render_template("index.html", base_url=request.url_root, current_date=current_date)

@app.route("/item")
def item_page():
    item_details = {
        "title": request.args.get("title", "Item"),
        "price": request.args.get("price", ""),
        "image_url": request.args.get("image_url", ""),
        "url": request.args.get("url", ""),
        "source": request.args.get("source", "N/A")
    }
    return render_template("index.html", mode="detail", item=item_details, base_url=request.url_root)

@app.route("/tcg-tracker")
def tcg_tracker_page():
    return render_template("tcg_tracker.html", base_url=request.url_root)

@app.route("/wallpapers")
def wallpapers_page():
    return render_template("wallpapers.html")

@app.route("/top100")
def top_100_page():
    trending_dir = "Top 100 trending"
    latest_file, latest_time = None, 0
    if os.path.isdir(trending_dir):
        for item in os.listdir(trending_dir):
            item_path = os.path.join(trending_dir, item)
            if os.path.isdir(item_path):
                try:
                    csv_path = os.path.join(item_path, 'pokemon_wizard_prices.csv')
                    if os.path.exists(csv_path):
                        mtime = os.path.getmtime(csv_path)
                        if mtime > latest_time:
                            latest_time, latest_file = mtime, csv_path
                except Exception:
                    pass
    cards = []
    if latest_file:
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                cards = list(csv.DictReader(f))
        except Exception as e:
            print(f"Error reading CSV file {latest_file}: {e}")
    return render_template("top100.html", cards=cards)

@app.route("/global-prices")
def sealed_products_page():
    return render_template("sealed_products.html")

@app.route("/api/sealed-products")
def api_sealed_products():
    candidates = [
        os.path.join(app.root_path, "sealed_item_prices", "tcg_sealed_prices.csv"),
        os.path.join(app.root_path, "Sealed_Item_prices", "tcg_sealed_prices.csv"),
        os.path.join(app.root_path, "tcg_sealed_prices.csv"),
    ]
    csv_path = next((p for p in candidates if os.path.exists(p)), None)
    if not csv_path:
        print("Data file not found. Looked for:", candidates, "cwd=", os.getcwd())
        return jsonify({"error": "Data file not found."}), 404

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            out = [_normalize_sealed_row(row) for row in reader]
        return jsonify(out)
    except Exception as e:
        print(f"Error reading sealed products CSV at {csv_path}: {e}")
        return jsonify({"error": "Failed to read product data."}), 500

# ---- Market / history / other existing routes (unchanged) ----
@app.route("/api/market-status")
def api_market_status():
    history_dir = "Greek_Prices_History"
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
    all_data, files = [], glob.glob(os.path.join(history_dir, "*.xlsx")) + glob.glob(os.path.join(history_dir, "*.csv"))
    for file_path in files:
        try:
            file_datetime = parse_date_from_filename(os.path.splitext(os.path.basename(file_path))[0])
            if not file_datetime or file_datetime < cutoff_date:
                continue
            df = pd.read_csv(file_path, encoding='utf-8-sig') if file_path.endswith('.csv') else pd.read_excel(file_path)
            df.columns = [str(c).lower().strip() for c in df.columns]
            df['date'] = file_datetime
            all_data.append(df)
        except Exception as e:
            print(f"Skipping history file {file_path}: {e}")
    if not all_data:
        return jsonify({"error": "No recent history data found"}), 404
    full_history = pd.concat(all_data, ignore_index=True)
    title_col = next((c for c in ['item_title', 'title', 'name'] if c in full_history.columns), None)
    price_col = next((c for c in ['price', 'current_price'] if c in full_history.columns), None)
    if not title_col or not price_col:
        return jsonify({"error": "Could not find title/price columns"}), 500
    full_history[price_col] = pd.to_numeric(
        full_history[price_col].astype(str).str.replace('[€,]', '', regex=True),
        errors='coerce'
    )
    full_history.dropna(subset=[price_col], inplace=True)
    market_status = []
    for category_name, keywords in categories.items():
        cat_df = full_history[full_history[title_col].str.contains('|'.join(keywords), case=False, na=False)]
        if cat_df.empty:
            continue
        changes = []
        for _, group in cat_df.groupby(title_col):
            if len(group) > 1:
                group = group.sort_values('date')
                start_price = group.iloc[0][price_col]
                end_price = group.iloc[-1][price_col]
                if start_price > 0:
                    changes.append(((end_price - start_price) / start_price) * 100)
        status, explanation = 'yellow', '(Prices Stable)'
        if changes:
            avg_change = sum(changes) / len(changes)
            if avg_change > 2.5:  status, explanation = 'green', '(Prices Rising)'
            elif avg_change < -2.5: status, explanation = 'red', '(Prices Lowering)'
        market_status.append({"category": category_name, "status": status, "explanation": explanation})
    return jsonify(market_status)

@app.route("/api/price-history")
def api_price_history():
    item_title = request.args.get("title", "").strip()
    if not item_title:
        return jsonify({"error": "Missing item title"}), 400
    normalized_search_title = normalize_title_for_history(item_title)
    history_dir = "Greek_Prices_History"
    price_history = []
    if not os.path.isdir(history_dir):
        return jsonify({"error": "History directory not found"}), 500
    files = glob.glob(os.path.join(history_dir, "*.xlsx")) + glob.glob(os.path.join(history_dir, "*.csv"))
    for file_path in files:
        try:
            filename = os.path.basename(file_path)
            date_str = os.path.splitext(filename)[0]
            file_datetime = parse_date_from_filename(date_str)
            if not file_datetime:
                continue
            file_date = file_datetime.strftime("%Y-%m-%d")
            df = pd.read_csv(file_path, encoding='utf-8-sig') if file_path.lower().endswith('.csv') else pd.read_excel(file_path)
            df.columns = [str(c).lower().strip() for c in df.columns]
            title_col = next((c for c in ['item_title', 'title', 'name'] if c in df.columns), None)
            price_col = next((c for c in ['price', 'current_price'] if c in df.columns), None)
            if not title_col or not price_col:
                continue
            df['normalized_title'] = df[title_col].apply(normalize_title_for_history)
            item_row = df[df['normalized_title'] == normalized_search_title]
            if not item_row.empty:
                price_str = str(item_row.iloc[0][price_col])
                price_val = float(price_str.replace('€', '').replace(',', '.').strip())
                price_history.append({"date": file_date, "price": price_val})
        except Exception as e:
            print(f"Could not process file {file_path}: {e}")
    price_history.sort(key=lambda x: x['date'])
    return jsonify(price_history)

@app.route("/api/related-products")
def api_related_products():
    title = request.args.get("title", "").strip()
    original_url = request.args.get("url", "").strip()
    if not title:
        return jsonify({"items": []})
    items = scraper.get_related_products(title, original_url, limit=8)
    return jsonify({"items": items})

@app.route("/api/home")
def api_home():
    sort = request.args.get("sort", "bestsellers")
    page = max(1, int(request.args.get("page", 1)))
    page_size = 24
    all_items = scraper.search_products_all("", sort=sort)
    start, end = (page - 1) * page_size, (page - 1) * page_size + page_size
    return jsonify({"items": all_items[start:end], "has_more": end < len(all_items), "total": len(all_items)})

@app.route("/api/search")
def api_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"items": [], "has_more": False, "total": 0})
    sort = request.args.get("sort", "bestsellers")
    page = max(1, int(request.args.get("page", 1)))
    page_size = 24
    all_items = scraper.search_products_all(q, sort=sort)
    start, end = (page - 1) * page_size, (page - 1) * page_size + page_size
    return jsonify({"items": all_items[start:end], "has_more": end < len(all_items), "total": len(all_items)})

@app.route("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"items": []})
    return jsonify({"items": scraper.suggest_titles(q, limit=10)})

@app.route("/api/tcg/suggest")
def api_tcg_suggest():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"items": []})
    return jsonify({"items": scraper_pokemon.search_pokemon_tcg(q, page_size=12)})

@app.route("/api/tcg/card")
def api_tcg_card():
    card_id = (request.args.get("id") or "").strip()
    if not card_id:
        return jsonify({"error": "Missing id"}), 400
    data = scraper_pokemon.get_card_details(card_id)
    if not data:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data)

@app.route("/api/tcg/related")
def api_tcg_related():
    set_id = (request.args.get("setId") or "").strip()
    rarity = (request.args.get("rarity") or "").strip()
    card_id = (request.args.get("cardId") or "").strip()
    try:
        count = int(request.args.get("count", 8))
    except ValueError:
        count = 8
    items = scraper_pokemon.get_related_cards(set_id, rarity, card_id, count=count)
    return jsonify({"items": items})

@app.route("/api/tcg/random-trending")
def api_tcg_random_trending():
    trending_dir = "Top 100 trending"
    latest_file, latest_time = None, 0
    if os.path.isdir(trending_dir):
        for item in os.listdir(trending_dir):
            item_path = os.path.join(trending_dir, item)
            if os.path.isdir(item_path):
                try:
                    csv_path = os.path.join(item_path, 'pokemon_wizard_prices.csv')
                    if os.path.exists(csv_path):
                        mtime = os.path.getmtime(csv_path)
                        if mtime > latest_time:
                            latest_time, latest_file = mtime, csv_path
                except Exception:
                    pass
    cards = []
    if latest_file:
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                cards = list(csv.DictReader(f))
        except Exception as e:
            print(f"Error reading CSV file {latest_file}: {e}")
    return jsonify({"cards": random.sample(cards, 10) if len(cards) > 10 else cards})

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)
