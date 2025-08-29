# Force-refresh deployment 2025-08-28-v10-GLOBAL-RELATED
import sys, re, os, csv, glob, random, threading, unicodedata
from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import pandas as pd
import pytz

# --- Local Imports ---
try:
    import scraper
    import scraper_pokemon
    import data_loader
    data_loader.load_data()
except ImportError:
    print("Warning: Local modules (scraper, scraper_pokemon, data_loader) not found.")
    class Dummy:
        def __getattr__(self, _): return lambda *a, **k: None
    scraper = Dummy(); scraper_pokemon = Dummy(); data_loader = Dummy()

app = Flask(__name__, template_folder="templates")

# ---- Config ----
USD_TO_EUR = float(os.environ.get("USD_TO_EUR", "0.86"))

# ---------- Generic helpers ----------
def parse_date_from_filename(name):
    for fmt in ("%d %m %Y", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try: return datetime.strptime(name, fmt)
        except ValueError: pass
    return None

def normalize_title_for_history(title):
    if not isinstance(title, str): return ""
    return re.sub(r'[^a-z0-9]', '', title.lower())

def _upgrade_image(url: str, level: int = 1) -> str:
    if not url: return url
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
    if s is None: return None
    m = re.search(r"[\d,.]+", str(s))
    if not m: return None
    val = m.group(0)
    try: return float(val.replace(",", ""))
    except ValueError:
        try: return float(val.replace(".", "").replace(",", "."))
        except ValueError: return None

def _normalize_sealed_row(row: dict) -> dict:
    norm = {re.sub(r"\s+", "_", (k or "").strip().lower()): (v or "").strip()
            for k, v in row.items()}
    set_name   = norm.get("set_name") or norm.get("set") or norm.get("series")
    item_title = norm.get("item_title") or norm.get("title") or norm.get("item") or norm.get("product_title")
    raw_price  = norm.get("raw_price") or norm.get("price") or norm.get("current_price")
    image_url  = norm.get("image_url") or norm.get("image") or norm.get("img_url") or norm.get("img")

    usd = _parse_price_to_float(raw_price) or 0.0
    eur = _parse_price_to_float(norm.get("price_eur") or "") or round(usd * USD_TO_EUR, 2)

    img_hd  = _upgrade_image(image_url, level=1) or image_url
    img_xhd = _upgrade_image(image_url, level=2) or img_hd

    return {
        "set_name": set_name, "item_title": item_title,
        "raw_price": raw_price, "price_usd": usd, "price_eur": eur,
        "image_url": image_url, "image_url_hd": img_hd, "image_url_xhd": img_xhd,
    }

# ---------- Text helpers used by /api/global-related ----------
GENERIC = {
    "pokemon","pokémon","tcg","sealed","official","english","card","cards","and",
    "scarlet","violet","sv","series","set","base","tcg"
}
TYPE_SYNONYMS = {
    "booster pack":   {"booster pack","pack","sealed booster pack"},
    "booster box":    {"booster box","display","box","case"},
    "booster bundle": {"booster bundle","bundle"},
    "elite trainer box": {"elite trainer box","etb"},
    "binder":         {"binder","binder collection"},
    "collection":     {"collection","special collection"},
    "blister":        {"blister","checklane"},
    "deck":           {"deck","starter deck","theme deck","battle deck"},
    "tin":            {"tin"},
    "sleeves":        {"sleeves"},
}
TYPE_LOOKUP = {syn: canon for canon, syns in TYPE_SYNONYMS.items() for syn in syns}
ALLOWED_TYPES = set(TYPE_SYNONYMS.keys())

def _ascii_fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"\s+", " ", s.lower()).strip()

def _tokens(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _ascii_fold(s))

# SKU-like tokens (mix letters+digits) that should NOT define the set identity
_SKUISH = re.compile(r"^(?:sv\d+|s?v?\d+|pok\d+|pkm\d+|tcg\d+|sku\d+|upc\d+|ean\d+|[a-z]*\d{3,}[a-z0-9]*)$")

def _canonical_type(text: str) -> str | None:
    t = _ascii_fold(text)
    for phrase in sorted(TYPE_LOOKUP.keys(), key=len, reverse=True):
        if phrase in t: return TYPE_LOOKUP[phrase]
    for tok in _tokens(t):
        if tok in TYPE_LOOKUP: return TYPE_LOOKUP[tok]
    if "booster" in t and "box" not in t and "bundle" not in t:
        return "booster pack"
    return None

def _keywords(text: str, drop_types=True) -> set[str]:
    words = set(_tokens(text))
    type_words = set(TYPE_LOOKUP.keys()) | {
        "booster","trainer","box","pack","bundle","display","case",
        "tin","deck","sleeves","binder","collection","blister","etb"
    } if drop_types else set()
    kept = set()
    for w in words:
        if w in GENERIC or w in type_words: continue
        if _SKUISH.match(w) and not w.isdigit():  # drop “pok103193”, “sv9”, etc.
            continue
        kept.add(w)
    return kept

def _signature_tokens(title: str) -> list[str]:
    """
    Return the most distinctive set tokens from the page title:
    - Prefer alphabetic tokens (>=4 chars)
    - If none exist, allow a single 3-digit token (e.g., '151')
    """
    kws = _keywords(title)
    alpha = [w for w in kws if w.isalpha() and len(w) >= 4]
    if alpha:
        alpha.sort(key=lambda w: (-len(w), w))
        return alpha[:3]
    nums = [w for w in kws if w.isdigit() and len(w) == 3]
    nums.sort()
    return nums[:1]

def _is_non_english(text: str) -> bool:
    t = _ascii_fold(text)
    return any(k in t for k in ["japanese","korean","chinese","kr","jp","cn","jpn","zh"])

def _build_ebay_query(set_name: str, canon_type: str) -> str:
    parts = ["pokemon"]
    if set_name:  parts.append(set_name)
    if canon_type: parts.append(canon_type)
    q = " ".join(parts) + " -japanese -korean -chinese -jp -kr -cn"
    return re.sub(r"\s+", " ", q).strip()

# ---------- Pages ----------
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
                        if mtime > latest_time: latest_time, latest_file = mtime, csv_path
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
        return jsonify({"error": "Data file not found."}), 404
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            out = [_normalize_sealed_row(row) for row in reader]
        return jsonify(out)
    except Exception as e:
        print(f"Error reading sealed products CSV at {csv_path}: {e}")
        return jsonify({"error": "Failed to read product data."}), 500

# ------------------------------------------------------------------
# Global Related: 8 English sealed items for the set in the title
# ------------------------------------------------------------------
@app.route("/api/global-related")
def api_global_related():
    title = (request.args.get("title") or "").strip()
    if not title:
        return jsonify({"items": []})

    sig_list = _signature_tokens(title)         # e.g., ["journey", "together"]
    sig = set(sig_list)
    if not sig:
        return jsonify({"items": []})

    # locate CSV
    candidates = [
        os.path.join(app.root_path, "sealed_item_prices", "tcg_sealed_prices.csv"),
        os.path.join(app.root_path, "Sealed_Item_prices", "tcg_sealed_prices.csv"),
        os.path.join(app.root_path, "tcg_sealed_prices.csv"),
    ]
    csv_path = next((p for p in candidates if os.path.exists(p)), None)
    if not csv_path:
        return jsonify({"items": []})

    type_rank = {
        "booster pack": 6, "booster box": 6, "booster bundle": 5,
        "elite trainer box": 5, "blister": 4, "collection": 4,
        "tin": 3, "binder": 2, "deck": 2, "sleeves": 1,
    }

    results_and = []   # rows that match ALL signature tokens
    results_or  = []   # rows that match at least ONE signature token

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                r = _normalize_sealed_row(row)
                set_name   = r.get("set_name", "") or ""
                item_title = r.get("item_title", "") or ""
                joined     = f"{item_title} {set_name}"

                # English only
                if _is_non_english(joined):
                    continue

                # Only sealed product types we care about
                row_type = _canonical_type(joined) or ""
                if row_type and row_type not in ALLOWED_TYPES:
                    continue

                row_kws = _keywords(joined)
                overlap = sig & row_kws
                if not overlap:
                    continue

                # scoring
                score = 10.0 * (1 if sig.issubset(row_kws) else 0) \
                        + 3.0 * len(overlap) \
                        + (type_rank.get(row_type, 0) / 10.0)

                display_title = (set_name or "").strip()
                if item_title and _ascii_fold(item_title) not in _ascii_fold(set_name):
                    display_title = (display_title + " — " + item_title).strip(" —")

                ebay_q   = _build_ebay_query(set_name or " ".join(sig_list), row_type)
                ebay_url = "https://www.ebay.com/sch/i.html?_nkw=" + re.sub(r"\s+", "+", ebay_q)

                pack = {
                    "title": display_title or (item_title or set_name or "Item"),
                    "price": f"€{(r.get('price_eur') or 0):.2f}",
                    "image_url": r.get("image_url_hd") or r.get("image_url"),
                    "url": ebay_url
                }

                if sig.issubset(row_kws):
                    results_and.append((score, pack))
                else:
                    results_or.append((score, pack))
    except Exception as e:
        print(f"Error in /api/global-related: {e}")
        return jsonify({"error": "Failed to process related items."}), 500

    # Prefer AND matches; if fewer than 8, fill with OR matches.
    results_and.sort(key=lambda t: t[0], reverse=True)
    results_or.sort(key=lambda t: t[0], reverse=True)
    merged = [p for _, p in results_and[:8]]
    if len(merged) < 8:
        needed = 8 - len(merged)
        merged += [p for _, p in results_or[:needed]]

    return jsonify({"items": merged})

# ---------- Market / history / other routes (unchanged) ----------
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
    all_data = []
    files = glob.glob(os.path.join(history_dir, "*.xlsx")) + glob.glob(os.path.join(history_dir, "*.csv"))
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
        if cat_df.empty: continue
        changes = []
        for _, group in cat_df.groupby(title_col):
            if len(group) > 1:
                group = group.sort_values('date')
                start_price = group.iloc[0][price_col]
                end_price   = group.iloc[-1][price_col]
                if start_price > 0:
                    changes.append(((end_price - start_price) / start_price) * 100)
        status, explanation = 'yellow', '(Prices Stable)'
        if changes:
            avg_change = sum(changes) / len(changes)
            if   avg_change >  2.5: status, explanation = 'green', '(Prices Rising)'
            elif avg_change < -2.5: status, explanation = 'red',   '(Prices Lowering)'
        market_status.append({"category": category_name, "status": status, "explanation": explanation})
    return jsonify(market_status)

@app.route("/api/price-history")
def api_price_history():
    item_title = request.args.get("title", "").strip()
    if not item_title: return jsonify({"error": "Missing item title"}), 400
    normalized_search_title = normalize_title_for_history(item_title)
    history_dir = "Greek_Prices_History"
    price_history = []
    if not os.path.isdir(history_dir): return jsonify({"error": "History directory not found"}), 500
    files = glob.glob(os.path.join(history_dir, "*.xlsx")) + glob.glob(os.path.join(history_dir, "*.csv"))
    for file_path in files:
        try:
            filename = os.path.basename(file_path)
            date_str = os.path.splitext(filename)[0]
            file_datetime = parse_date_from_filename(date_str)
            if not file_datetime: continue
            file_date = file_datetime.strftime("%Y-%m-%d")
            df = pd.read_csv(file_path, encoding='utf-8-sig') if file_path.lower().endswith('.csv') else pd.read_excel(file_path)
            df.columns = [str(c).lower().strip() for c in df.columns]
            title_col = next((c for c in ['item_title', 'title', 'name'] if c in df.columns), None)
            price_col = next((c for c in ['price', 'current_price'] if c in df.columns), None)
            if not title_col or not price_col: continue
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
    if not title: return jsonify({"items": []})
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
    if not q: return jsonify({"items": [], "has_more": False, "total": 0})
    sort = request.args.get("sort", "bestsellers")
    page = max(1, int(request.args.get("page", 1)))
    page_size = 24
    all_items = scraper.search_products_all(q, sort=sort)
    start, end = (page - 1) * page_size, (page - 1) * page_size + page_size
    return jsonify({"items": all_items[start:end], "has_more": end < len(all_items), "total": len(all_items)})

@app.route("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip()
    if not q: return jsonify({"items": []})
    return jsonify({"items": scraper.suggest_titles(q, limit=10)})

@app.route("/api/tcg/suggest")
def api_tcg_suggest():
    q = (request.args.get("q") or "").strip()
    if not q: return jsonify({"items": []})
    return jsonify({"items": scraper_pokemon.search_pokemon_tcg(q, page_size=12)})

@app.route("/api/tcg/card")
def api_tcg_card():
    card_id = (request.args.get("id") or "").strip()
    if not card_id: return jsonify({"error": "Missing id"}), 400
    data = scraper_pokemon.get_card_details(card_id)
    if not data: return jsonify({"error": "Not found"}), 404
    return jsonify(data)

@app.route("/api/tcg/related")
def api_tcg_related():
    set_id = (request.args.get("setId") or "").strip()
    rarity = (request.args.get("rarity") or "").strip()
    card_id = (request.args.get("cardId") or "").strip()
    try: count = int(request.args.get("count", 8))
    except ValueError: count = 8
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
                        if mtime > latest_time: latest_time, latest_file = mtime, csv_path
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
