import time
import requests
import threading
from requests.exceptions import ReadTimeout, RequestException

from data_loader import (
    search_local_cards, get_local_card_by_id, get_local_related_cards,
    get_price_override
)
from fun_facts import get_greek_fun_fact

POKEMON_TCG_API_KEY = "ef66b505-18ad-457e-b4f2-b8ec126dbb7d"
BASE_URL = "https://api.pokemontcg.io/v2"

HEADERS = {
    "X-Api-Key": POKEMON_TCG_API_KEY,
    "Accept": "application/json",
    "User-Agent": "POKEGR-TCG-TRACKER/3.3-fast"
}

# ----- tiny API cache with short timeouts -----
API_CACHE = {}
API_CACHE_LOCK = threading.Lock()
API_TTL = 600  # 10 minutes

def _fetch_json(url, params=None, timeout=2.0, retries=0, is_single_item=False):
    cache_key = f"{url}?{str(params)}"
    with API_CACHE_LOCK:
        if cache_key in API_CACHE:
            ts, data = API_CACHE[cache_key]
            if time.time() - ts < API_TTL:
                return data
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            with API_CACHE_LOCK:
                API_CACHE[cache_key] = (time.time(), data)
            return data
        except (ReadTimeout, RequestException):
            if attempt < retries:
                time.sleep(0.15)
                continue
            return None if is_single_item else {"data": []}

def _fallback_search_links(name, set_name, number):
    q = requests.utils.quote(f"{name} {set_name} {number}")
    tcg = f"https://www.tcgplayer.com/search/pokemon/product?q={q}"
    cmk = f"https://www.cardmarket.com/en/Pokemon/Products/Search?searchString={q}"
    eb  = f"https://www.ebay.com/sch/i.html?_nkw={q}+Pokemon+TCG"
    return tcg, cmk, eb

def search_pokemon_tcg(query, page_size=12):
    if not query or not query.strip():
        return []
    local_results = search_local_cards(query, limit=page_size)
    cards = []
    for c in local_results:
        set_obj = c.get("set") or {}
        images_obj = c.get("images") or {}
        cards.append({
            "id": c.get("id"),
            "name": c.get("name"),
            "smallImage": images_obj.get("small"),
            "set": set_obj.get("name"),
            "number": c.get("number"),
            "rarity": c.get("rarity"),
            "prices": {"market": None},
        })
    return cards

def get_card_details(card_id):
    if not card_id:
        return None
    card = get_local_card_by_id(card_id)
    if not card:
        return None

    set_obj = card.get("set") or {}
    images_obj = card.get("images") or {}
    set_images_obj = set_obj.get("images") or {}

    tcgPlayerUrl, cardmarketUrl, ebayUrl = _fallback_search_links(card.get("name"), set_obj.get("name"), card.get("number"))

    # 1) Use price override (fast & offline)
    override = get_price_override(card.get("name"), set_obj.get("name"), card.get("number"))
    if override:
        prices = {
            "market": override.get("market"),
            "psa9": override.get("psa9"),
            "psa10": override.get("psa10"),
            "currency": override.get("currency") or "EUR",
            "source": "excel"
        }
        api = _fetch_json(f"{BASE_URL}/cards/{card_id}", is_single_item=True, timeout=1.8, retries=0)
        updatedAt = None
        if api and "data" in api:
            d = api["data"]
            cardmarketUrl = (d.get("cardmarket") or {}).get("url") or cardmarketUrl
            tcgPlayerUrl  = (d.get("tcgplayer")  or {}).get("url") or tcgPlayerUrl
            updatedAt     = (d.get("cardmarket") or {}).get("updatedAt")
        return {
            "id": card.get("id"), "name": card.get("name"), "imageUrl": images_obj.get("large"),
            "ebayUrl": ebayUrl, "set": set_obj.get("name"), "setId": set_obj.get("id"),
            "setIcon": set_images_obj.get("symbol"), "number": card.get("number"), "rarity": card.get("rarity"),
            "artist": card.get("artist"), "flavorText": card.get("flavorText"),
            "pullRate": _get_pull_rate(card.get("rarity")), "cardmarketUrl": cardmarketUrl,
            "tcgPlayerUrl": tcgPlayerUrl, "updatedAt": updatedAt, "prices": prices or {},
            "attacks": card.get("attacks"), "abilities": card.get("abilities"), "hp": card.get("hp"),
            "types": card.get("types"), "funFact": get_greek_fun_fact(card.get("name", ""))
        }

    # 2) No override: quick API call (short timeout) for prices + links
    api = _fetch_json(f"{BASE_URL}/cards/{card_id}", is_single_item=True, timeout=2.0, retries=0)
    prices = {}
    updatedAt = None
    if api and "data" in api:
        d = api["data"]
        tcg = (d.get("tcgplayer")  or {}).get("prices") or {}
        cmk = (d.get("cardmarket") or {}).get("prices") or {}
        def first(*vals):
            for v in vals:
                try:
                    if v is None: continue
                    return float(v)
                except (ValueError, TypeError):
                    continue
            return None
        market = None
        for k in ("holofoil","reverseHolofoil","normal"):
            market = market or first(*( (tcg.get(k) or {}).get(x) for x in ("market","directLow","mid") ))
        market = market or first(cmk.get("averageSellPrice"), cmk.get("trendPrice"))
        prices = {"market": market, "currency": "USD"}
        cardmarketUrl = (d.get("cardmarket") or {}).get("url") or cardmarketUrl
        tcgPlayerUrl  = (d.get("tcgplayer")  or {}).get("url")  or tcgPlayerUrl
        updatedAt     = (d.get("cardmarket") or {}).get("updatedAt")

    return {
        "id": card.get("id"), "name": card.get("name"), "imageUrl": images_obj.get("large"),
        "ebayUrl": ebayUrl, "set": set_obj.get("name"), "setId": set_obj.get("id"),
        "setIcon": set_images_obj.get("symbol"), "number": card.get("number"), "rarity": card.get("rarity"),
        "artist": card.get("artist"), "flavorText": card.get("flavorText"),
        "pullRate": _get_pull_rate(card.get("rarity")), "cardmarketUrl": cardmarketUrl,
        "tcgPlayerUrl": tcgPlayerUrl, "updatedAt": updatedAt, "prices": prices or {},
        "attacks": card.get("attacks"), "abilities": card.get("abilities"), "hp": card.get("hp"),
        "types": card.get("types"), "funFact": get_greek_fun_fact(card.get("name", ""))
    }

def _get_pull_rate(rarity_str):
    rarity = (rarity_str or '').lower()
    rates = {
        'special illustration': (0.04, 0.09), 'hyper': (0.08, 0.15), 'secret': (0.08, 0.15),
        'illustration rare': (0.5, 1.0), 'ultra': (0.4, 0.9), 'double rare': (1.5, 2.5),
        'holo': (4.0, 6.0)
    }
    for key, (low, high) in rates.items():
        if key in rarity:
            import random as _rnd
            rate = _rnd.uniform(low, high)
            return f"{rate:.3f}%" if rate < 0.1 else f"{rate:.2f}%"
    return None

def get_local_related_cards_safe(set_id, rarity, current_card_id, count=5):
    return get_local_related_cards(set_id, rarity, current_card_id, count=count)

def get_related_cards(set_id, rarity, current_card_id, count=5):
    raw_cards = get_local_related_cards_safe(set_id, rarity, current_card_id, count=count)
    formatted_cards = []
    for c in raw_cards:
        images_obj = c.get("images") or {}
        formatted_cards.append({
            "id": c.get("id"), "name": c.get("name"), "smallImage": images_obj.get("small"),
        })
    return formatted_cards
