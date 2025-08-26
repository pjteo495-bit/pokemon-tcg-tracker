# scraper.py — Excel/CSV–backed product source
# De-duplicates ONLY when (normalized title, canonical URL, price text) are identical.

import os, csv, threading, re
from urllib.parse import urlparse, unquote

try:
    import pandas as pd  # optional; works without it
except ImportError:
    pd = None

# ---------- Config ----------
EXCEL_FILE = os.environ.get("GREEK_PRICES_FILE", "").strip()
EXCEL_DIR = os.environ.get(
    "GREEK_PRICES_DIR",
    "greek prices" # Use the relative path
).strip()

# Deduplication is ON
DEDUP = True

# ---------- State ----------
_ITEMS = []
_LAST_PATH = None
_LAST_MTIME = 0.0
_LOCK = threading.Lock()

# ---------- Helpers ----------
def _find_latest_file(dirname: str):
    """Finds the most recently modified Excel or CSV file in a directory."""
    if not dirname or not os.path.isdir(dirname):
        return None
    best, best_m = None, -1
    for fn in os.listdir(dirname):
        lo = fn.lower()
        if lo.endswith((".xlsx", ".xls", ".csv")):
            p = os.path.join(dirname, fn)
            try:
                m = os.path.getmtime(p)
            except Exception:
                m = 0
            if m > best_m:
                best, best_m = p, m
    return best

def _chosen_path():
    """Determines which data file to load."""
    return EXCEL_FILE if (EXCEL_FILE and os.path.isfile(EXCEL_FILE)) else _find_latest_file(EXCEL_DIR)

def _price_text_to_float(val):
    """Converts a formatted price string (e.g., '€1.234,56') to a float."""
    if val is None:
        return None
    s = str(val).replace(".", "").replace(",", ".")
    digits = "".join(ch for ch in s if ch.isdigit() or ch == ".")
    if not digits:
        return None
    try:
        return float(digits)
    except (ValueError, TypeError):
        return None

def _format_price_text(v):
    """Formats a float into a standard EU-style price string."""
    if v is None:
        return ""
    try:
        f = float(v)
        # EU-style comma decimal
        return f"€{f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(v)

def _pick(row, *cands):
    """Finds the first matching key in a dictionary from a list of candidates."""
    for c in cands:
        for k in row.keys():
            if str(k).strip().lower() == str(c).strip().lower():
                return k
    return None

def _infer_source(url, fallback="bestprice"):
    """Determines the product source (e.g., 'skroutz') from its URL."""
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if "skroutz" in host:
        return "skroutz"
    if "bestprice" in host:
        return "bestprice"
    return fallback

_TITLE_RE = re.compile(r"\s+", re.UNICODE)
def _norm_title(t: str) -> str:
    """Normalizes a title for consistent matching."""
    s = (t or "")
    s = s.replace("™", "").replace("®", "")
    s = _TITLE_RE.sub(" ", s).strip().casefold()
    return s

def _canon_url(u: str) -> str:
    """Creates a canonical URL (host + path) to help with deduplication."""
    if not u:
        return ""
    try:
        p = urlparse(u.strip())
    except Exception:
        return u.strip()
    host = p.netloc.lower()
    if host.startswith("www."): host = host[4:]
    path = unquote(p.path or "").rstrip("/")
    return f"{host}{path}"

def _normalize_row(row, default_source="bestprice"):
    """Converts a raw row from a file into a standardized dictionary."""
    title_k = _pick(row, "title", "product", "name", "item_title")
    price_k = _pick(row, "price", "current_price", "amount", "lowest_price")
    img_k   = _pick(row, "image_url", "image", "img", "thumbnail")
    url_k   = _pick(row, "url", "link", "permalink", "product_url")
    src_k   = _pick(row, "source", "site", "platform", "store", "website")

    title = (row.get(title_k) or "").strip() if title_k else ""
    url   = (row.get(url_k) or "").strip() if url_k else ""
    image = (row.get(img_k) or "").strip() if img_k else ""
    price_raw = row.get(price_k)

    price_float = _price_text_to_float(price_raw)
    price_text = _format_price_text(price_float) if price_float is not None else (str(price_raw or "").strip())

    src = (row.get(src_k) or "").strip().lower() if src_k else ""
    if not src:
        src = _infer_source(url, default_source)
    if src.startswith("best"): src = "bestprice"
    if src.startswith("skr"):  src = "skroutz"

    if not title or not url:
        return None

    return {
        "title": title,
        "image_url": image,
        "price": price_text,
        "price_float": price_float,
        "url": url,
        "source": src,
        "_k_title": _norm_title(title),
        "_k_url": _canon_url(url),
    }

def _read_rows(path: str):
    """Reads rows from an Excel or CSV file."""
    ext = os.path.splitext(path)[1].lower()
    rows = []
    if pd and ext in (".xlsx", ".xls"):
        try:
            rows = pd.read_excel(path, engine="openpyxl").to_dict(orient="records")
        except Exception:
            rows = []
    elif ext == ".csv":
        try:
            with open(path, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception:
            rows = []
    else:
        if pd:
            try:
                rows = pd.read_csv(path).to_dict(orient="records")
            except Exception:
                rows = []
    return rows

def _load_items_from_file(path: str):
    """Loads, normalizes, and optionally de-duplicates items from a file."""
    rows = _read_rows(path)
    items = []
    dropped_no_title = dropped_no_url = 0

    for r in rows:
        if not ((r.get("item_title") or r.get("title") or r.get("product") or r.get("name"))):
            dropped_no_title += 1
            continue
        if not ((r.get("product_url") or r.get("url") or r.get("link") or r.get("permalink"))):
            dropped_no_url += 1
            continue
        it = _normalize_row(r)
        if it:
            items.append(it)

    if DEDUP:
        seen = set()
        deduped = []
        for it in items:
            key = (it["_k_title"], it["_k_url"], it["price"])
            if key in seen:
                continue
            seen.add(key)
            it.pop("_k_title", None)
            it.pop("_k_url", None)
            deduped.append(it)
        print(f"[excel] DEBUG loaded {len(deduped)} items from: {path}")
        return deduped

    for it in items:
        it.pop("_k_title", None)
        it.pop("_k_url", None)
    print(f"[excel] DEBUG loaded {len(items)} items (no dedup) from: {path}")
    return items

def _ensure_loaded():
    """Ensures the item data is loaded into memory, reloading if the file has changed."""
    global _ITEMS, _LAST_PATH, _LAST_MTIME
    with _LOCK:
        path = _chosen_path()
        if not path or not os.path.isfile(path):
            print("[excel] No data file found. Set GREEK_PRICES_FILE or GREEK_PRICES_DIR.")
            return
        try:
            mtime = os.path.getmtime(path)
        except Exception:
            mtime = 0.0
        if path != _LAST_PATH or mtime > _LAST_MTIME:
            _ITEMS = _load_items_from_file(path)
            _LAST_PATH, _LAST_MTIME = path, mtime
            print(f"[excel] Loaded {len(_ITEMS)} items from: {path}")

def _filter_sort(items, search_term="", sort="bestsellers"):
    """Filters items by a search term and applies sorting."""
    q = (search_term or "").casefold().strip()
    toks = [t for t in q.split() if t]

    def matches(it):
        if not toks:
            return True
        title = (it["title"] or "").casefold()
        return all(t in title for t in toks)

    out = [it for it in items if matches(it)]

    if sort == "price_asc":
        out.sort(key=lambda i: (i.get("price_float") is None, i.get("price_float") or 1e12))
    elif sort == "price_desc":
        out.sort(key=lambda i: (i.get("price_float") is None, -(i.get("price_float") or 0.0)))
    elif sort == "alpha":
        out.sort(key=lambda i: (i.get("title") or "").casefold())
    return out

# ---------- Public API used by app.py ----------
def search_products_all(q, sort="bestsellers"):
    """Searches all loaded products."""
    _ensure_loaded()
    return _filter_sort(list(_ITEMS), search_term=q, sort=sort)

def suggest_titles(q, limit=10):
    """Provides title suggestions for search."""
    _ensure_loaded()
    items = _filter_sort(list(_ITEMS), search_term=q, sort="alpha")
    return items[:limit]

def get_related_products(title, original_url, limit=6):
    """Finds items with titles similar to the given one."""
    _ensure_loaded()
    
    q = (title or "").casefold().strip()
    # Use significant words (more than 3 chars) for matching
    toks = {t for t in q.split() if len(t) > 3}
    
    if not toks:
        return []

    canon_original_url = _canon_url(original_url)

    def score_item(it):
        if not it or not it.get("title"): return 0
        # Exclude the exact same item by comparing canonical URLs
        if _canon_url(it.get("url", "")) == canon_original_url: return 0
            
        item_title = (it["title"] or "").casefold()
        # Score is the number of matching keywords
        score = sum(1 for t in toks if t in item_title)
        return score

    # Score all items in the inventory
    scored_items = [(score_item(it), it) for it in _ITEMS]
    
    # Sort by score in descending order
    scored_items.sort(key=lambda x: x[0], reverse=True)
    
    # Filter for items that have a score greater than 0
    related = [item for score, item in scored_items if score > 0]
    
    return related[:limit]
