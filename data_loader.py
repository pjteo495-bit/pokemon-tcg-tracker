import json
import os
import re
from unicodedata import normalize

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

# --- Configuration ---
DATA_PATH = os.path.join('pokemon-tcg-data-master', 'cards', 'en')
SETS_PATH = os.path.join('pokemon-tcg-data-master', 'sets', 'en')
PRICES_DIR = os.path.join('prices')

# --- In-Memory Data Store ---
_card_data = []
_card_dict = {}
_set_dict = {}
_price_map = {}                 # (name_norm, set_norm, num_norm) -> prices
_price_index_by_setnum = {}     # (set_norm, num_norm) -> [(name_norm, prices)]

# ---------- Normalization ----------
_alnum = re.compile(r'[^a-z0-9]+')

def _ascii_lower(s: str) -> str:
    return normalize('NFKD', str(s)).encode('ASCII', 'ignore').decode('utf-8').lower().strip()

def _tokenize(s: str) -> str:
    s = _ascii_lower(s)
    s = _alnum.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _normalize_set(text: str) -> str:
    """
    Make set names comparable across CSV and local data.
    """
    s = _tokenize(text)
    # --- FIX START: Handle specific edge cases and RETURN immediately ---
    if 'pokemon go' in s:
        return 'go'
    if 'wizards black star promos' in s:
        return 'wizards black star promos'
    if 'black star' in s and 'promo' in s:
        return 'black star promo'
    # --- FIX END ---
    
    s = re.sub(r'\b(1st|first|edition|shadowless)\b', '', s)
    # Be less aggressive with removing 'pokemon'
    if 'pokemon' in s.split() and len(s.split()) > 1:
        s = re.sub(r'\bpokemon\b', '', s)

    s = re.sub(r'\b(tcg|the|trading|card|game|series)\b', '', s)
    series_patterns = [
        r'diamond\s*(?:&|and)?\s*pearl', r'black\s*(?:&|and)?\s*white',
        r'sun\s*(?:&|and)?\s*moon', r'sword\s*(?:&|and)?\s*shield',
        r'scarlet\s*(?:&|and)?\s*violet', r'heartgold\s*(?:&|and)?\s*soulsilver',
    ]
    for pat in series_patterns:
        s = re.sub(r'\b' + pat + r'\b', '', s)
    s = re.sub(r'\b(?:dp|bw|xy|sm|swsh|sv|hgss)\b', '', s)
    s = re.sub(r'\b(wizards|wotc)\b', '', s)
    s = ' '.join([word for word in s.split() if word != 'set'])
    s = re.sub(r'\bpromos\b', 'promo', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _normalize_number(num) -> str:
    """Normalize card number by taking the part before any slash and lowercasing."""
    s = str(num or '').strip().lower()
    s = s.split('/')[0]
    s = s.lstrip('#')
    return s

def _digits_only(num_norm: str) -> str:
    """Extract digits only (e.g., 'bw83' -> '83')."""
    return re.sub(r'[^0-9]', '', num_norm or '')

def _name_norm(name: str) -> str:
    """Tokenize the name without stripping bracketed content."""
    return _tokenize(name)

def _key(name, set_name, number):
    return (_name_norm(name), _normalize_set(set_name), _normalize_number(number))

# ---------- Public API ----------
def load_data():
    global _card_data, _card_dict, _set_dict
    if _card_data:
        return

    # Load sets
    print(f"Loading sets from: {SETS_PATH}")
    if os.path.isdir(SETS_PATH):
        for filename in os.listdir(SETS_PATH):
            if filename.endswith(".json"):
                with open(os.path.join(SETS_PATH, filename), 'r', encoding='utf-8') as f:
                    try:
                        s_data = json.load(f)
                        if isinstance(s_data, list):
                            for s_item in s_data:
                                _set_dict[s_item['id']] = s_item
                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode JSON from set file {filename}")

    # Load cards
    print(f"Loading cards from: {DATA_PATH}")
    if os.path.isdir(DATA_PATH):
        for filename in os.listdir(DATA_PATH):
            if not filename.endswith('.json'):
                continue
            set_id = os.path.splitext(filename)[0]
            set_for_this_file = _set_dict.get(set_id)
            if not set_for_this_file:
                continue
            with open(os.path.join(DATA_PATH, filename), 'r', encoding='utf-8') as f:
                try:
                    cards_in_file = json.load(f)
                    for card in cards_in_file:
                        card['set'] = set_for_this_file
                        card['_normalized_name'] = _name_norm(card.get('name'))
                        card['_normalized_set'] = _normalize_set(card['set'].get('name', ''))
                        card['_normalized_number'] = _normalize_number(card.get('number'))
                        card['_normalized_number_digits'] = _digits_only(card['_normalized_number'])
                        _card_data.append(card)
                        _card_dict[card['id']] = card
                except json.JSONDecodeError:
                    print(f"Warning: Could not decode JSON from card file {filename}")

    if not _card_data:
        print("Warning: No card data was loaded.")
    else:
        print(f"Successfully loaded {len(_card_data)} cards and {len(_set_dict)} sets into memory.")

    _load_price_data()

def search_local_cards(query, limit=12):
    """
    Scores and ranks cards based on query matching name, set, and number to find the best result.
    """
    if not query:
        return []

    search_num_digits = "".join(re.findall(r'\d+', query))
    query_text = _tokenize(re.sub(r'\d+', ' ', query))
    search_tokens = set(t for t in query_text.split() if t)

    if not search_tokens and not search_num_digits:
        return []

    results_with_scores = []
    for card in _card_data:
        score = 0.0
        card_name_tokens = set(card['_normalized_name'].split())
        card_set_tokens = set(card['_normalized_set'].split())
        card_num_digits = card['_normalized_number_digits']
        
        card_all_text_tokens = card_name_tokens.union(card_set_tokens)

        text_match_count = 0
        if search_tokens:
            for s_token in search_tokens:
                for c_token in card_all_text_tokens:
                    if s_token in c_token:
                        text_match_count += 1
                        break
        
        if search_tokens and text_match_count == 0:
            continue

        score += 50 * text_match_count

        name_match_score = len(search_tokens.intersection(card_name_tokens))
        score += 30 * name_match_score

        set_match_score = len(search_tokens.intersection(card_set_tokens))
        score += 20 * set_match_score
        
        if search_num_digits and card_num_digits == search_num_digits:
            score += 50

        unmatched_tokens = len(card_name_tokens - search_tokens)
        score -= 5 * unmatched_tokens

        if score > 0:
            rarity = (card.get('rarity') or '').lower()
            tie_breaker = 0
            if 'rare' in rarity: tie_breaker = 1
            if 'holo' in rarity: tie_breaker = 2
            if 'ultra' in rarity: tie_breaker = 3
            results_with_scores.append((score, tie_breaker, card))

    results_with_scores.sort(key=lambda x: (x[0], x[1]), reverse=True)
    
    return [card for score, tie_breaker, card in results_with_scores[:limit]]

def get_local_card_by_id(card_id):
    return _card_dict.get(card_id)

def get_local_related_cards(set_id, rarity, current_card_id, count=5):
    if not all([set_id, rarity, current_card_id]):
        return []
    related = []
    for card in _card_data:
        if (card.get('set') and
            card['set'].get('id') == set_id and
            card.get('rarity') == rarity and
            card.get('id') != current_card_id):
            related.append(card)
    if len(related) > count:
        import random
        return random.sample(related, count)
    return related

# ---------- Price overrides ----------
def get_price_override(name, set_name, number):
    global _price_map
    name_norm, set_norm, num_norm = _key(name, set_name, number)
    return _price_map.get((name_norm, set_norm, num_norm))

def refresh_price_data():
    _load_price_data()

# ---------- Internal (price loading) ----------
def _find_latest_price_file():
    if not os.path.isdir(PRICES_DIR):
        return None
    candidates = []
    for fn in os.listdir(PRICES_DIR):
        lower = fn.lower()
        if lower.endswith(('.xlsx', '.xls', '.csv')):
            path = os.path.join(PRICES_DIR, fn)
            try:
                mtime = os.path.getmtime(path)
            except Exception:
                mtime = 0
            candidates.append((mtime, path))
    if not candidates:
        return None
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]

def _parse_price(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    s = s.replace(",", "").replace("€", "").replace("$", "").replace("£", "")
    try:
        return float(s)
    except Exception:
        return None

def _detect_currency_from_rows(rows):
    joined = " ".join([(" ".join(f"{k}:{v}" for k, v in r.items())) for r in rows[:5]]).lower()
    if "€" in joined or " eur" in joined or "euro" in joined:
        return "EUR"
    if "$" in joined or " usd" in joined or "dollar" in joined:
        return "USD"
    if "£" in joined or " gbp" in joined or "pound" in joined:
        return "GBP"
    return "EUR"

def _load_price_data():
    global _price_map
    _price_map = {}

    path = _find_latest_price_file()
    if not path:
        print(f"No price file found in {PRICES_DIR}. Skipping overrides.")
        return

    print(f"Loading price overrides from: {path}")

    rows = []
    try:
        if pd is None:
            raise RuntimeError("pandas not available")
        if path.lower().endswith('.csv'):
            df = pd.read_csv(path)
        else:
            df = pd.read_excel(path, engine='openpyxl')
        rows = df.to_dict(orient='records')
        currency = _detect_currency_from_rows(rows)
    except Exception as e:
        print(f"WARNING: Failed to read via pandas ({e}). Trying basic CSV reader.")
        currency = "EUR"
        try:
            import csv
            if path.lower().endswith('.csv'):
                with open(path, encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    currency = _detect_currency_from_rows(rows)
            else:
                print("Cannot read Excel without pandas/openpyxl.")
                rows = []
        except Exception as e2:
            print(f"WARNING: Fallback CSV read failed: {e2}")
            rows = []

    def _col(d, *cands):
        for c in cands:
            for k in list(d.keys()):
                if str(k).strip().lower() == str(c).strip().lower():
                    return k
        return None

    loaded = 0
    for row in rows:
        name_k   = _col(row, 'name', 'card name', 'card', 'title', 'card_title')
        set_k    = _col(row, 'set', 'set name', 'game')
        num_k    = _col(row, 'number', 'no', '#')
        raw_k    = _col(row, 'raw price', 'raw', 'price', 'unguided_price')
        psa9_k   = _col(row, 'psa 9 price', 'psa9 price', 'psa9', 'psa9_price')
        psa10_k  = _col(row, 'psa 10 price', 'psa10 price', 'psa10', 'psa10_price')

        card_name  = (row.get(name_k) or "").strip() if name_k else ""
        set_name   = (row.get(set_k) or "").strip() if set_k else ""
        number_raw = str(row.get(num_k) or "").strip() if num_k else ""

        if set_name.lower().startswith("pokemon "):
            set_name = set_name.split(" ", 1)[1].strip()

        if not number_raw and "#" in card_name:
            parts = card_name.rsplit("#", 1)
            if len(parts) == 2:
                card_name = parts[0].strip()
                number_raw = parts[1].strip()

        raw_price  = _parse_price(row.get(raw_k))  if raw_k  else None
        psa9_price = _parse_price(row.get(psa9_k)) if psa9_k else None
        psa10_price= _parse_price(row.get(psa10_k))if psa10_k else None

        if not card_name or not set_name or not number_raw:
            continue

        name_norm = _name_norm(card_name)
        set_norm  = _normalize_set(set_name)
        num_norm  = _normalize_number(number_raw)

        price_obj = {
            "market": raw_price,
            "psa9": psa9_price,
            "psa10": psa10_price,
            "currency": currency or "EUR",
            "source": "excel"
        }
        
        price_key = (name_norm, set_norm, num_norm)
        _price_map[price_key] = price_obj
        loaded += 1

    print(f"Loaded {loaded} price override rows.")
