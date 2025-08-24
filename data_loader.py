import json
import os
import re
from unicodedata import normalize

try:
    import pandas as pd
except Exception:
    pd = None

# --- Configuration ---
DATA_PATH = os.path.join('pokemon-tcg-data-master', 'cards', 'en')
SETS_PATH = os.path.join('pokemon-tcg-data-master', 'sets', 'en')
PRICES_DIR = os.path.join('prices')

# --- In-Memory Data Store ---
_card_data = []
_card_dict = {}
_set_dict = {}
_price_map = {}

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
    Revised normalization for set names to be more robust.
    """
    s = _tokenize(text)
    
    # Handle specific, known sets first to avoid over-stripping
    if 'pokemon go' in s:
        return 'go'
    if 'wizards black star promos' in s:
        return 'wizards black star promo'
    if 'black star' in s and 'promo' in s:
        return 'black star promo'
        
    # General cleanup for regular sets
    common_words = r'\b(pokemon|tcg|the|trading|card|game|series|set|edition)\b'
    s = re.sub(common_words, '', s)
    
    # Remove series names, but only if it's not the whole name
    series_patterns = [
        r'diamond\s*(?:&|and)?\s*pearl', r'black\s*(?:&|and)?\s*white',
        r'sun\s*(?:&|and)?\s*moon', r'sword\s*(?:&|and)?\s*shield',
        r'scarlet\s*(?:&|and)?\s*violet', r'heartgold\s*(?:&|and)?\s*soulsilver',
    ]
    
    temp_s = s
    for pat in series_patterns:
        temp_s = re.sub(r'\b' + pat + r'\b', '', temp_s).strip()
    
    # If removing the series name left something, use that. Otherwise, keep the original.
    if temp_s:
        s = temp_s

    return re.sub(r'\s+', ' ', s).strip()

def _normalize_number(num) -> str:
    s = str(num or '').strip().lower()
    return s.split('/')[0].lstrip('#')

def _digits_only(num_norm: str) -> str:
    return re.sub(r'[^0-9]', '', num_norm or '')

def _name_norm(name: str) -> str:
    return _tokenize(name)

def _key(name, set_name, number):
    return (_name_norm(name), _normalize_set(set_name), _normalize_number(number))

# ---------- Public API ----------
def load_data():
    global _card_data, _card_dict, _set_dict
    if _card_data:
        return

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

    print(f"Loading cards from: {DATA_PATH}")
    if os.path.isdir(DATA_PATH):
        for filename in os.listdir(DATA_PATH):
            if not filename.endswith('.json'): continue
            set_id = os.path.splitext(filename)[0]
            set_for_this_file = _set_dict.get(set_id)
            if not set_for_this_file: continue
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

    print(f"Successfully loaded {len(_card_data)} cards and {len(_set_dict)} sets.")
    _load_price_data()

def search_local_cards(query, limit=12):
    if not query: return []
    search_num_digits = "".join(re.findall(r'\d+', query))
    query_text = _tokenize(re.sub(r'\d+', ' ', query))
    search_tokens = set(t for t in query_text.split() if t)
    if not search_tokens and not search_num_digits: return []

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
        if search_tokens and text_match_count == 0: continue
        score += 50 * text_match_count
        score += 30 * len(search_tokens.intersection(card_name_tokens))
        score += 20 * len(search_tokens.intersection(card_set_tokens))
        if search_num_digits and card_num_digits == search_num_digits: score += 50
        score -= 5 * len(card_name_tokens - search_tokens)
        if score > 0:
            rarity = (card.get('rarity') or '').lower()
            tie_breaker = 1 if 'rare' in rarity else 0
            if 'holo' in rarity: tie_breaker = 2
            if 'ultra' in rarity: tie_breaker = 3
            results_with_scores.append((score, tie_breaker, card))
    results_with_scores.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [card for score, tie_breaker, card in results_with_scores[:limit]]

def get_local_card_by_id(card_id):
    return _card_dict.get(card_id)

def get_local_related_cards(set_id, rarity, current_card_id, count=5):
    if not all([set_id, rarity, current_card_id]): return []
    related = [
        card for card in _card_data
        if card.get('set') and card['set'].get('id') == set_id
        and card.get('rarity') == rarity and card.get('id') != current_card_id
    ]
    if len(related) > count:
        import random
        return random.sample(related, count)
    return related

def get_price_override(name, set_name, number):
    global _price_map
    price_key = _key(name, set_name, number)
    return _price_map.get(price_key)

def _find_latest_price_file():
    if not os.path.isdir(PRICES_DIR): return None
    candidates = []
    for fn in os.listdir(PRICES_DIR):
        if fn.lower().endswith(('.xlsx', '.xls', '.csv')):
            path = os.path.join(PRICES_DIR, fn)
            try:
                candidates.append((os.path.getmtime(path), path))
            except Exception:
                continue
    if not candidates: return None
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]

def _parse_price(val):
    if val is None: return None
    s = str(val).strip().replace(",", "").replace("€", "").replace("$", "").replace("£", "")
    if s.lower() in {"", "nan", "none"}: return None
    try:
        return float(s)
    except Exception:
        return None

def _load_price_data():
    global _price_map
    _price_map = {}
    path = _find_latest_price_file()
    if not path:
        print(f"No price file found in {PRICES_DIR}. Skipping overrides.")
        return

    print(f"Loading price overrides from: {path}")
    try:
        df = pd.read_csv(path) if path.lower().endswith('.csv') else pd.read_excel(path)
        rows = df.to_dict(orient='records')
    except Exception as e:
        print(f"WARNING: Failed to read price file with pandas: {e}")
        return

    def _col(d, *cands):
        for c in cands:
            for k in d.keys():
                if str(k).strip().lower() == str(c).strip().lower():
                    return k
        return None

    for row in rows:
        name_k, set_k, num_k = _col(row, 'name', 'card name', 'title'), _col(row, 'set', 'set name'), _col(row, 'number', '#')
        raw_k, psa9_k, psa10_k = _col(row, 'raw price', 'price'), _col(row, 'psa 9 price', 'psa9'), _col(row, 'psa 10 price', 'psa10')
        
        card_name = (row.get(name_k) or "").strip()
        set_name = (row.get(set_k) or "").strip()
        number_raw = str(row.get(num_k) or "").strip()

        if not all([card_name, set_name, number_raw]): continue

        price_key = _key(card_name, set_name, number_raw)
        
        price_obj = {
            "market": _parse_price(row.get(raw_k)),
            "psa9": _parse_price(row.get(psa9_k)),
            "psa10": _parse_price(row.get(psa10_k)),
            "currency": "EUR", "source": "excel"
        }
        
        _price_map[price_key] = price_obj

    print(f"Loaded {len(_price_map)} price override rows.")
