# scraper.py — Excel/CSV–backed product source
# De-duplicates ONLY when (normalized title, canonical URL, price text) are identical.

import os, csv, threading, re
from urllib.parse import urlparse, unquote

try:
    import pandas as pd  # optional; works without it
except Exception:
    pd = None

# ---------- Config ----------
EXCEL_FILE = os.environ.get("GREEK_PRICES_FILE", "").strip()
EXCEL_DIR = os.environ.get(
    "GREEK_PRICES_DIR",
    r"C:\Users\pjteo\Desktop\POKEGR_TCG_TRACKER\greek prices"
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
    return EXCEL_FILE if (EXCEL_FILE and os.path.isfile(EXCEL_FILE)) else _find_latest_file(EXCEL_DIR)

def _price_text_to_float(val):
    if val is None:
        return None
    s = str(val).replace(".", "").replace(",", ".")
    digits = "".join(ch for ch in s if ch.isdigit() or ch == ".")
    if not digits:
        return None
    try:
        return float(digits)
    except Exception:
        return None

def _format_price_text(v):
    if v is None:
        return ""
    try:
        f = float(v)
        # EU-style comma decimal
        return f"€{f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _pick(row, *cands):
    for c in cands:
        for k in row.keys():
            if str(k).strip().lower() == str(c).strip().lower():
                return k
    return None

def _infer_source(url, fallback="bestprice"):
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
    s = (t or "")
    s = s.replace("™", "").replace("®", "")
    s = _TITLE_RE.sub(" ", s).strip().casefold()
    return s

def _canon_url(u: str) -> str:
    """host + decoded path, lowercase, no query/fragment — kills tracking params."""
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
            # --- FIX: Deduplication key now includes title, url, AND price ---
            key = (it["_k_title"], it["_k_url"], it["price"])
            if key in seen:
                continue
            seen.add(key)
            it.pop("_k_title", None)
            it.pop("_k_url", None)
            deduped.append(it)
        bp = sum(1 for i in deduped if i["source"] == "bestprice")
        sk = sum(1 for i in deduped if i["source"] == "skroutz")
        print(f"[excel] DEBUG rows_read={len(rows)} normalized={len(items)} deduped={len(deduped)} "
              f"dropped_title={dropped_no_title} dropped_url={dropped_no_url} (bestprice={bp}, skroutz={sk}) from: {path}")
        return deduped

    # no dedup
    for it in items:
        it.pop("_k_title", None)
        it.pop("_k_url", None)
    bp = sum(1 for i in items if i["source"] == "bestprice")
    sk = sum(1 for i in items if i["source"] == "skroutz")
    print(f"[excel] DEBUG rows_read={len(rows)} normalized={len(items)} dropped_title={dropped_no_title} dropped_url={dropped_no_url} "
          f"(bestprice={bp}, skroutz={sk}) from: {path}")
    return items

def _ensure_loaded():
    global _ITEMS, _LAST_PATH, _LAST_MTIME
    with _LOCK:
        path = _chosen_path()
        if not path or not os.path.isfile(path):
            print("[excel] No file found. Set GREEK_PRICES_FILE or GREEK_PRICES_DIR.")
            return
        try:
            mtime = os.path.getmtime(path)
        except Exception:
            mtime = 0.0
        if path != _LAST_PATH or mtime > _LAST_MTIME:
            items = _load_items_from_file(path)
            _ITEMS = items
            _LAST_PATH, _LAST_MTIME = path, mtime
            bp = sum(1 for i in items if i["source"] == "bestprice")
            sk = sum(1 for i in items if i["source"] == "skroutz")
            print(f"[excel] Loaded {len(items)} items (bestprice={bp}, skroutz={sk}) from: {path}")

def _filter_sort(items, search_term="", sort="bestsellers"):
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
def get_bestprice_bestsellers(sort="bestsellers"):
    _ensure_loaded()
    base = [it for it in _ITEMS if it["source"] == "bestprice"]
    return _filter_sort(base, sort=sort)

def search_products_bestprice(q, sort="bestsellers"):
    _ensure_loaded()
    base = [it for it in _ITEMS if it["source"] == "bestprice"]
    return _filter_sort(base, search_term=q, sort=sort)

def search_products_skroutz(q, sort="bestsellers"):
    _ensure_loaded()
    base = [it for it in _ITEMS if it["source"] == "skroutz"]
    return _filter_sort(base, search_term=q, sort=sort)

def search_products_all(q, sort="bestsellers"):
    _ensure_loaded()
    return _filter_sort(list(_ITEMS), search_term=q, sort=sort)

def suggest_titles(q, limit=10):
    _ensure_loaded()
    items = _filter_sort(list(_ITEMS), search_term=q, sort="alpha")
    return items[:limit]
