# data_loader.py
# Unified, deduped loader with robust normalization and Gold Star handling

import json
import os
import re
from unicodedata import normalize
from difflib import SequenceMatcher

# Decode helpers (your file already used these)
import html as _html
import urllib.parse as _urlparse

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

# --- Paths (same as before) ---------------------------------------------------
DATA_PATH  = os.path.join('pokemon-tcg-data-master', 'cards', 'en')
SETS_PATH  = os.path.join('pokemon-tcg-data-master', 'sets', 'en')
PRICES_DIR = os.path.join('prices')

# --- In-memory stores ---------------------------------------------------------
_card_data = []
_card_dict = {}
_set_dict  = {}

# Price lookup globals
_price_map = {}                  # (name_norm, set_norm, num_norm) -> {market, psa9, psa10, ...}
_price_index = set()             # set of (name_norm, num_norm) that exist in CSV (any set)
_by_name_num = {}                # (name_norm, num_norm) -> list[(set_norm, val)]
_price_index_by_setnum = {}      # (set_norm, num_norm) -> [(name_norm, prices)]  (kept if you use it elsewhere)

# --- Normalization ------------------------------------------------------------
_alnum = re.compile(r'[^a-z0-9]+')

# Words/markers that should *not* be part of the name key.
# (Added "gold star" family here)
_VARIANT_WORDS = {
    'reverse', 'rev', 'reverse holo', 'rev holo', 'reverse-holo',
    'holo', 'holofoil', 'foil', 'rainbow foil', 'galaxy', 'cosmos',
    'non-holo', 'non holo', 'unlimited', 'first edition', '1st edition', '1st',
    'shadowless', 'staff', 'prerelease', 'pre-release', 'promo',
    'jumbo', 'oversize', 'shattered glass', 'cracked ice', 'e-reader', 'mini',
    'gold star', 'gold-star', 'goldstar'
}

_GOLD_STAR_RE = re.compile(r'\bgold\s*-\s*star\b|\bgold\s*star\b|\bgoldstar\b', re.IGNORECASE)

def _unescape_decode(s: str) -> str:
    """Decode HTML entities and percent-encoding early (e.g., %27, &amp;)."""
    try:
        s = _html.unescape(str(s))
        s = _urlparse.unquote(s)
        return s
    except Exception:
        return str(s)

def _ascii_lower(s: str) -> str:
    s = _unescape_decode(s)
    return normalize('NFKD', str(s)).encode('ASCII', 'ignore').decode('utf-8').lower().strip()

def _tokenize(s: str) -> str:
    s = _ascii_lower(s)
    s = _alnum.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _strip_variant_tags(text: str) -> str:
    """
    Remove bracketed/suffixed variant descriptors from the card title when safe.
    Handles things like "[Reverse Holo]" and "[Gold Star]".
    """
    if not text:
        return text
    s = str(text)

    def _repl(m):
        inside = m.group(1)
        low = inside.lower()
        if any(w in low for w in _VARIANT_WORDS):
            return ''
        return m.group(0)

    # remove [ ... ] chunks that are variant-y
    s = re.sub(r'\[(.*?)\]', _repl, s)

    # also drop loose variant words that appear as prefix/suffix
    for w in list(_VARIANT_WORDS):
        s = re.sub(r'(?:^|\s|[-–—])' + re.escape(w) + r'(?:$|\b)', ' ', s, flags=re.IGNORECASE)

    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _normalize_set(text: str) -> str:
    """
    Make set names comparable across CSV and local data.
    Canonicalizes common variants (e.g., 'Expedition Base Set' -> 'expedition').
    """
    s = _tokenize(_unescape_decode(text))

    # Canonicalize known aliases
    s = re.sub(r'\bexpedition base(?: set)?\b', 'expedition', s, flags=re.IGNORECASE)
    s = re.sub(r'\bpokemon expedition\b', 'expedition', s, flags=re.IGNORECASE)

    # Remove filler words often present in CSVs
    s = re.sub(r'\b(1st|first|edition|shadowless)\b', '', s)
    s = re.sub(r'\b(pokemon|tcg|the|trading|card|game|series)\b', '', s)

    # Series patterns
    series_patterns = [
        r'diamond\s*(?:&|and)?\s*pearl', r'black\s*(?:&|and)?\s*white',
        r'sun\s*(?:&|and)?\s*moon', r'sword\s*(?:&|and)?\s*shield',
        r'scarlet\s*(?:&|and)?\s*violet', r'heartgold\s*(?:&|and)?\s*soulsilver',
    ]
    for pat in series_patterns:
        s = re.sub(r'\b' + pat + r'\b', '', s)

    s = re.sub(r'\b(?:dp|bw|xy|sm|swsh|sv|hgss)\b', '', s)
    s = re.sub(r'\b(wizards|wotc)\b', '', s)
    s = ' '.join(word for word in s.split() if word != 'set')
    s = re.sub(r'\bpromos\b', 'promo', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _normalize_number(num) -> str:
    """Normalize card number by taking the part before slash and removing '#'. """
    s = str(num or '').strip().lower()
    s = s.split('/')[0]
    s = s.lstrip('#')
    return s

def _digits_only(num_norm: str) -> str:
    """Extract digits only (e.g., 'H6' -> '6')."""
    return re.sub(r'[^0-9]', '', num_norm or '')

def _name_norm(name: str) -> str:
    """Tokenize the name with variant descriptors stripped out."""
    name = _strip_variant_tags(name or '')
    return _tokenize(name)

def _name_norm_raw(name: str) -> str:
    """Tokenize the raw name (without variant stripping)."""
    return _tokenize(name or '')

def _key(name, set_name, number):
    return (_name_norm(name), _normalize_set(set_name), _normalize_number(number))

# --- Similarity for fuzzy set match ------------------------------------------
def _token_set(s: str):
    return set((s or '').split())

def _set_similarity(a: str, b: str) -> float:
    """Blend Jaccard over tokens with SequenceMatcher for robust fuzzy matching."""
    ta, tb = _token_set(a), _token_set(b)
    jac = (len(ta & tb) / max(1, len(ta | tb))) if (ta and tb) else 0.0
    seq = SequenceMatcher(None, a, b).ratio()
    return 0.6 * jac + 0.4 * seq

# --- Load local sets & cards --------------------------------------------------
def load_data():
    """Load local JSON for sets and cards, then price data."""
    global _card_data, _card_dict, _set_dict
    if _card_data:
        return

    # Sets
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

    # Cards
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
                        card['_normalized_name']   = _name_norm(card.get('name'))
                        card['_normalized_set']    = _normalize_set(card['set'].get('name', ''))
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

    _load_price_data()  # load/refresh prices after cards
    # (Structure mirrors your original file.) :contentReference[oaicite:3]{index=3}

# --- Local search helpers (unchanged logic, tidied) ---------------------------
def search_local_cards(query, limit=12):
    """Score and rank local cards by query."""
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
        name_tokens = set(card['_normalized_name'].split())
        set_tokens  = set(card['_normalized_set'].split())
        num_digits  = card['_normalized_number_digits']

        # partial text matches (name + set)
        all_tokens = name_tokens | set_tokens
        text_match_count = 0
        if search_tokens:
            for s_token in search_tokens:
                for c_token in all_tokens:
                    if s_token in c_token:
                        text_match_count += 1
                        break
        if search_tokens and text_match_count == 0:
            continue

        score += 50 * text_match_count
        score += 30 * len(search_tokens & name_tokens)
        score += 20 * len(search_tokens & set_tokens)

        if search_num_digits and num_digits == search_num_digits:
            score += 50

        unmatched_tokens = len(name_tokens - search_tokens)
        score -= 5 * unmatched_tokens

        if score > 0:
            rarity = (card.get('rarity') or '').lower()
            tie = 0
            if 'rare' in rarity:  tie = 1
            if 'holo' in rarity:  tie = 2
            if 'ultra' in rarity: tie = 3
            results_with_scores.append((score, tie, card))

    results_with_scores.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [c for _, __, c in results_with_scores[:limit]]

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

# --- Price overrides: public API ---------------------------------------------
def get_price_override(name, set_name, number):
    """
    Return the price dict for (name, set, number) using:
      1) exact match on normalized name/set/number
      2) fallbacks: raw-name & digits-only number
      3) fuzzy set match limited to rows sharing the same (name, number)
    """
    global _price_map, _price_index
    name_norm = _name_norm(name)
    set_norm  = _normalize_set(set_name)
    num_norm  = _normalize_number(number)

    # Exact + standard fallbacks
    for nm in (name_norm, _name_norm_raw(name)):
        for nn in (num_norm, _digits_only(num_norm)):
            if not nn:
                continue
            v = _price_map.get((nm, set_norm, nn))
            if v:
                return v

    # Fuzzy set, but only among rows with same (name, number)
    nm_candidates = {name_norm, _name_norm_raw(name)}
    nn_candidates = {num_norm, _digits_only(num_norm)}
    nn_candidates = {x for x in nn_candidates if x}

    best = None
    best_score = 0.0
    for nm_k in nm_candidates:
        for nn_k in nn_candidates:
            for (set_k, val) in _by_name_num.get((nm_k, nn_k), []):
                score = _set_similarity(set_norm, set_k)
                if score > best_score:
                    best_score = score
                    best = val

    if best and best_score >= 0.72:
        return best

    return None  # not found

def get_price_override_ex(name, set_name, number):
    """
    Same as get_price_override but returns (value, reason)
    reason ∈ {'found', 'unmatched_set', 'absent_in_csv'}
    """
    global _price_map, _price_index
    val = get_price_override(name, set_name, number)
    if val is not None:
        return val, 'found'

    nm_vars = {_name_norm(name), _name_norm_raw(name)}
    nn_raw  = _normalize_number(number)
    nn_vars = {nn_raw, _digits_only(nn_raw)}
    nn_vars = {x for x in nn_vars if x}

    exists_any = any(((nm, nn) in _price_index) for nm in nm_vars for nn in nn_vars)
    if exists_any:
        return None, 'unmatched_set'
    else:
        return None, 'absent_in_csv'

def refresh_price_data():
    _load_price_data()

# --- Price loading internals --------------------------------------------------
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

def _row_is_variant(original_name: str) -> bool:
    low = (original_name or '').lower()
    if '[' in low and ']' in low:
        return True
    return any(w in low for w in _VARIANT_WORDS)

def _insert_price_key(price_map, key, price_obj, score):
    """
    Insert/replace a price entry for a key using a score:
      - Prefer non-variant rows
      - Prefer rows with more price fields filled
    """
    if key not in price_map or score > price_map[key].get('_score', -1):
        new_obj = dict(price_obj)
        new_obj['_score'] = score
        price_map[key] = new_obj

def _load_price_data():
    """Read the newest CSV/XLSX from PRICES_DIR and build lookup indexes."""
    global _price_map, _price_index, _by_name_num
    _price_map = {}
    _price_index = set()
    _by_name_num = {}

    path = _find_latest_price_file()
    if not path:
        print(f"No price file found in {PRICES_DIR}. Skipping overrides.")
        return

    print(f"Loading price overrides from: {path}")

    # Read rows
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

    # column resolver
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
        set_name   = (row.get(set_k) or "").strip()  if set_k  else ""
        number_raw = str(row.get(num_k) or "").strip() if num_k else ""

        # Decode encodings early (e.g., Champion%27S Path)
        card_name = _unescape_decode(card_name)
        set_name  = _unescape_decode(set_name)

        if set_name.lower().startswith("pokemon "):
            set_name = set_name.split(" ", 1)[1].strip()

        # If number missing, pull "#..." from name
        if not number_raw and "#" in card_name:
            parts = card_name.rsplit("#", 1)
            if len(parts) == 2:
                card_name  = parts[0].strip()
                number_raw = parts[1].strip()

        raw_price   = _parse_price(row.get(raw_k))  if raw_k  else None
        psa9_price  = _parse_price(row.get(psa9_k)) if psa9_k else None
        psa10_price = _parse_price(row.get(psa10_k))if psa10_k else None

        if not card_name or not set_name or not number_raw:
            continue

        name_norm_base = _name_norm(card_name)     # variant-stripped
        name_norm_raw  = _name_norm_raw(card_name) # raw-tokenized
        set_norm       = _normalize_set(set_name)
        num_norm       = _normalize_number(number_raw)

        price_obj = {
            "market": raw_price,
            "psa9": psa9_price,
            "psa10": psa10_price,
            "currency": currency or "EUR",
            "source": "excel"
        }

        # Prefer non-variant rows + richer rows
        is_variant = _row_is_variant(card_name)
        richness   = (1 if raw_price is not None else 0) + (2 if psa9_price is not None else 0) + (3 if psa10_price is not None else 0)
        score      = richness + (2 if not is_variant else 0)

        # Index with both base and raw names
        for nm in {name_norm_base, name_norm_raw}:
            _price_index.add((nm, num_norm))
            _insert_price_key(_price_map, (nm, set_norm, num_norm), price_obj, score)
            _by_name_num.setdefault((nm, num_norm), []).append((set_norm, price_obj))

            # digits-only fallback (e.g., H6 -> 6)
            num_digits = _digits_only(num_norm)
            if num_digits and num_digits != num_norm:
                _insert_price_key(_price_map, (nm, set_norm, num_digits), price_obj, score - 0.10)

            # --- Gold Star fallback: also index a version with 'gold star' removed ---
            nm_no_gold = _GOLD_STAR_RE.sub(' ', nm).strip()
            if nm_no_gold != nm:
                nm_no_gold = re.sub(r'\s+', ' ', nm_no_gold)
                _price_index.add((nm_no_gold, num_norm))
                _insert_price_key(_price_map, (nm_no_gold, set_norm, num_norm), price_obj, score - 0.05)

                if num_digits and num_digits != num_norm:
                    _insert_price_key(_price_map, (nm_no_gold, set_norm, num_digits), price_obj, score - 0.15)

        loaded += 1

    print(f"Loaded {loaded} price override rows (with fallback keys).")
