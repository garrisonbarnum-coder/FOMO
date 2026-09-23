#!/usr/bin/env python3
"""
FOMO Opportunity Engine - 24/7 market + security monitor.

Scans established, emerging, and newly-created tokens. Security is a hard gate:
BUY and NEW 99 alerts are suppressed unless the token has a verified security
result and passes the configured security/liquidity rules.

Security provider: GoPlus Token Security API. GitHub Actions supplies the
GoPlus App Key + App Secret and this monitor generates a short-lived access token.
The credentials and access token are never written to the public snapshot.
"""
import hashlib
import json
import math
import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

BASE = "https://api.dexscreener.com"
PROFILE_URLS = [
    f"{BASE}/token-profiles/latest/v1",
    f"{BASE}/token-boosts/latest/v1",
    f"{BASE}/token-boosts/top/v1",
    f"{BASE}/community-takeovers/latest/v1",
]
OUT = "data/latest.json"
STATE = "data/state.json"
TIMEOUT = 15
NTFY_BASE = "https://ntfy.sh"
NTFY_MAIN_TOPIC = os.getenv("FOMO_NTFY_TOPIC", "").strip()
GOPLUS_APP_KEY = os.getenv("GOPLUS_APP_KEY", "").strip()
GOPLUS_APP_SECRET = os.getenv("GOPLUS_APP_SECRET", "").strip()
GOPLUS_ACCESS_TOKEN = ""
SECURITY_CACHE_MINUTES = 30
NEW_99_MAX_AGE_MINUTES = 360
MAX_DISCOVERY = 100
MAX_RESULTS = 50
SECURITY_TARGETS = 15
URGENT_SECURITY_SLOTS = 6
STALE_SECURITY_SLOTS = 4
ROTATING_SECURITY_SLOTS = 5
MIN_LIQUIDITY_BUY = 25000
MIN_LIQUIDITY_NEW = 50000

# Search terms broaden the universe beyond brand-new token feeds.
SEARCH_TERMS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "SUI", "TON",
    "PEPE", "SHIB", "BONK", "WIF", "TRUMP", "AI", "DEFI", "RWA", "MEME", "DEPIN",
]

# Common GoPlus EVM chain IDs. Unknown EVM chains remain SECURITY UNKNOWN rather
# than being guessed.
EVM_CHAIN_IDS = {
    "ethereum": "1", "bsc": "56", "polygon": "137", "arbitrum": "42161",
    "avalanche": "43114", "optimism": "10", "base": "8453", "fantom": "250",
    "linea": "59144", "scroll": "534352", "zksync": "324", "mantle": "5000",
    "blast": "81457", "mode": "34443", "gnosis": "100", "celo": "42220",
    "cronos": "25", "moonbeam": "1284", "moonriver": "1285", "aurora": "1313161554",
}


def get_json(url, headers=None, retries=3):
    last = None
    for attempt in range(retries):
        try:
            h = {"User-Agent": "FOMO-Coin-Tracker/4.0", "Accept": "application/json"}
            if headers:
                h.update(headers)
            req = Request(url, headers=h)
            with urlopen(req, timeout=TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last = exc
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f"request failed: {url} ({last})")


def num(value, default=0.0):
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def pct(value):
    return round(num(value), 2)


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, path)


def age_minutes(created_at):
    if not created_at:
        return None
    return round(max(0, time.time() - num(created_at) / 1000) / 60, 1)


def choose_pair(pairs, chain_id=None):
    candidates = [p for p in (pairs or []) if not chain_id or p.get("chainId") == chain_id]
    if not candidates:
        candidates = pairs or []
    if not candidates:
        return None
    return max(candidates, key=lambda p: (num((p.get("liquidity") or {}).get("usd")), num((p.get("volume") or {}).get("h24"))))


def market_score(pair, previous):
    """Opportunity score before security gating. 0-99, not a prediction."""
    change1 = num((pair.get("priceChange") or {}).get("h1"))
    change24 = num((pair.get("priceChange") or {}).get("h24"))
    volume = num((pair.get("volume") or {}).get("h24"))
    volume1 = num((pair.get("volume") or {}).get("h1"))
    liquidity = num((pair.get("liquidity") or {}).get("usd"))
    tx = pair.get("txns") or {}
    h1 = tx.get("h1") or {}
    h24 = tx.get("h24") or {}
    buys1, sells1 = num(h1.get("buys")), num(h1.get("sells"))
    buys24, sells24 = num(h24.get("buys")), num(h24.get("sells"))
    total1 = buys1 + sells1
    total24 = buys24 + sells24
    buy_ratio = buys1 / total1 if total1 else 0
    age = age_minutes(pair.get("pairCreatedAt"))

    score = 25
    reasons = []
    if change1 > 3: score += 8; reasons.append("1h momentum")
    if change1 > 10: score += 7; reasons.append("strong 1h momentum")
    if change24 > 10: score += 7; reasons.append("24h momentum")
    if change24 > 30: score += 6; reasons.append("strong 24h momentum")
    if liquidity >= 25000: score += 8
    if liquidity >= 100000: score += 5
    if liquidity >= 1000000: score += 5
    if volume >= 100000: score += 6
    if volume >= 1000000: score += 5
    if volume1 > 0 and volume > 0 and volume1 / volume > 0.12: score += 5; reasons.append("volume accelerating")
    if total1 >= 20 and buy_ratio > 0.55: score += 7; reasons.append("buyers leading")
    if total1 >= 50 and buy_ratio > 0.65: score += 6; reasons.append("strong buy pressure")
    if total1 >= 20 and sells1 > buys1: score -= 8; reasons.append("sell pressure")
    if total24 >= 100: score += 3
    if age is not None and age < 60: score += 4; reasons.append("new token")
    elif age is not None and age < 1440: score += 2; reasons.append("emerging token")

    key = pair.get("pairAddress") or ""
    old = previous.get(key) if isinstance(previous, dict) else None
    if old:
        old_liq = num(old.get("liquidityUsd"))
        if old_liq > 0:
            drop = (liquidity - old_liq) / old_liq
            if drop <= -0.30: score -= 20; reasons.append("liquidity falling")
            elif drop >= 0.20: score += 5; reasons.append("liquidity rising")

    return max(0, min(99, int(round(score)))), reasons[:6]


def security_result_unknown(reason="Security provider unavailable"):
    return {"status": "UNKNOWN", "verified": False, "blocked": False, "risk": None, "reasons": [reason], "source": "GoPlus"}


def get_goplus_access_token():
    global GOPLUS_ACCESS_TOKEN
    if GOPLUS_ACCESS_TOKEN:
        return GOPLUS_ACCESS_TOKEN
    if not GOPLUS_APP_KEY or not GOPLUS_APP_SECRET:
        return ""
    ts = int(time.time())
    signature = hashlib.sha1(f"{GOPLUS_APP_KEY}{ts}{GOPLUS_APP_SECRET}".encode("utf-8")).hexdigest()
    body = json.dumps({"app_key": GOPLUS_APP_KEY, "time": ts, "sign": signature}).encode("utf-8")
    try:
        req = Request("https://api.gopluslabs.io/api/v1/token", data=body, method="POST", headers={"Content-Type":"application/json", "Accept":"application/json", "User-Agent":"FOMO-Coin-Tracker/5.0"})
        with urlopen(req, timeout=TIMEOUT) as r:
            raw = json.loads(r.read().decode("utf-8"))
        result = raw.get("result") if isinstance(raw, dict) else None
        token = None
        if isinstance(result, dict):
            token = result.get("access_token") or result.get("token")
        token = token or (raw.get("access_token") if isinstance(raw, dict) else None) or (raw.get("token") if isinstance(raw, dict) else None)
        if not token:
            print("GoPlus token response did not contain an access token")
            return ""
        GOPLUS_ACCESS_TOKEN = str(token)
        return GOPLUS_ACCESS_TOKEN
    except Exception as exc:
        print("GoPlus access-token request failed:", exc)
        return ""


def goplus_security(chain, address, cached):
    token = get_goplus_access_token()
    if not token:
        return security_result_unknown("GoPlus credentials/token unavailable")
    if cached and (time.time() - num(cached.get("checkedAt"))) < SECURITY_CACHE_MINUTES * 60:
        return cached.get("result") or security_result_unknown("Cached result missing")

    headers = {"Authorization": f"Bearer {token}"}
    try:
        if str(chain).lower() == "solana":
            url = "https://api.gopluslabs.io/api/v1/solana/token_security?" + urlencode({"contract_addresses": address})
        else:
            chain_id = EVM_CHAIN_IDS.get(str(chain).lower())
            if not chain_id:
                return security_result_unknown(f"Unsupported security chain: {chain}")
            url = f"https://api.gopluslabs.io/api/v1/token_security/{chain_id}?" + urlencode({"contract_addresses": address})
        raw = get_json(url, headers=headers)
        result = raw.get("result") if isinstance(raw, dict) else None
        if not isinstance(result, dict):
            return security_result_unknown("Security API returned no token result")
        item = result.get(address) or result.get(address.lower()) or next(iter(result.values()), None)
        if not isinstance(item, dict):
            return security_result_unknown("Security API returned no matching token")

        reasons = []
        hard_block = []
        caution = []
        def flag(name, label, block=False):
            val = str(item.get(name, ""))
            if val == "1":
                (hard_block if block else caution).append(label)

        flag("is_honeypot", "honeypot detected", True)
        flag("cannot_buy", "cannot buy", True)
        flag("cannot_sell_all", "cannot sell all tokens", True)
        flag("is_blacklisted", "blacklist capability", False)
        flag("transfer_pausable", "transfers can be paused", True)
        flag("owner_change_balance", "owner can change balances", True)
        flag("hidden_owner", "hidden owner", True)
        flag("selfdestruct", "self-destruct capability", True)
        flag("is_mintable", "mintable", True)
        flag("personal_slippage_modifiable", "per-address tax can change", True)
        flag("slippage_modifiable", "trading tax can change", True)
        flag("can_take_back_ownership", "ownership can be reclaimed", True)
        flag("is_proxy", "proxy contract", False)
        flag("is_open_source", "closed source", True if str(item.get("is_open_source")) == "0" else False)
        flag("is_airdrop_scam", "airdrop scam", True)

        sell_tax = item.get("sell_tax", "")
        buy_tax = item.get("buy_tax", "")
        try:
            st = float(sell_tax) if sell_tax != "" else None
            if st is not None and st >= 0.20: hard_block.append(f"sell tax {st*100:.0f}%")
            elif st is not None and st >= 0.08: caution.append(f"sell tax {st*100:.0f}%")
        except (TypeError, ValueError): pass
        try:
            bt = float(buy_tax) if buy_tax != "" else None
            if bt is not None and bt >= 0.20: hard_block.append(f"buy tax {bt*100:.0f}%")
            elif bt is not None and bt >= 0.08: caution.append(f"buy tax {bt*100:.0f}%")
        except (TypeError, ValueError): pass

        # Holder concentration: flag a very concentrated top holder as caution.
        holders = item.get("holders") or []
        top_percent = max([num(h.get("percent")) for h in holders if isinstance(h, dict)] or [0])
        if top_percent >= 0.30: caution.append(f"top holder {top_percent*100:.0f}%")
        elif top_percent >= 0.15: caution.append(f"top holder {top_percent*100:.0f}%")

        risk = 0
        risk += min(70, len(hard_block) * 30)
        risk += min(25, len(caution) * 8)
        if item.get("trust_list") == "1": risk = max(0, risk - 30)
        if item.get("is_in_cex", {}).get("listed") == "1": risk = max(0, risk - 20)
        risk = min(100, risk)
        if hard_block:
            status = "BLOCKED"
        elif caution:
            status = "CAUTION"
        else:
            status = "VERIFIED"
        reasons = hard_block + caution
        if not reasons:
            reasons = ["No configured hard security blockers detected"]
        return {
            "status": status,
            "verified": True,
            "blocked": bool(hard_block),
            "risk": risk,
            "reasons": reasons[:8],
            "source": "GoPlus",
            "raw": {
                "is_open_source": item.get("is_open_source"),
                "is_honeypot": item.get("is_honeypot"),
                "is_mintable": item.get("is_mintable"),
                "sell_tax": item.get("sell_tax"),
                "buy_tax": item.get("buy_tax"),
                "is_blacklisted": item.get("is_blacklisted"),
                "slippage_modifiable": item.get("slippage_modifiable"),
                "holder_count": item.get("holder_count"),
                "top_holder_percent": top_percent,
            },
        }
    except Exception as exc:
        return security_result_unknown("Security check failed: " + str(exc)[:120])


def trade_signal(item):
    if item.get("dataStatus") != "OK":
        return {"signal": "WAIT", "points": 0, "reason": "Market data unavailable"}
    if item.get("securityStatus") != "VERIFIED":
        return {"signal": "WAIT", "points": item.get("opportunityScore", 0), "reason": "Security gate not passed"}
    if item.get("securityBlocked"):
        return {"signal": "EXIT / AVOID", "points": 0, "reason": "Security gate blocked token"}
    liq = num(item.get("liquidityUsd"))
    if liq < MIN_LIQUIDITY_BUY:
        return {"signal": "WAIT", "points": item.get("opportunityScore", 0), "reason": "Liquidity below BUY threshold"}
    score = int(item.get("opportunityScore", 0))
    if score >= 75:
        return {"signal": "BUY SETUP", "points": score, "reason": "Security verified • momentum/liquidity conditions aligned"}
    if score <= 30:
        return {"signal": "EXIT / AVOID", "points": score, "reason": "Momentum conditions weakened"}
    return {"signal": "WAIT", "points": score, "reason": "Mixed opportunity conditions"}


def topic_for(alert_topics, key, signal_type):
    item = alert_topics.get(key)
    if not isinstance(item, dict): item = {}
    topic = item.get(signal_type)
    if not topic:
        topic = f"fomo-{signal_type}-{secrets.token_urlsafe(18).replace('-', '').replace('_', '')}"
        item[signal_type] = topic
        alert_topics[key] = item
    return topic


def publish_ntfy(topic, title, message, priority="4", tags="chart_with_upwards_trend"):
    if not topic: return False
    try:
        req = Request(f"{NTFY_BASE}/{topic}", data=message.encode(), method="POST", headers={"Title": title, "Priority": str(priority), "Tags": tags, "User-Agent": "FOMO-Coin-Tracker/4.0"})
        with urlopen(req, timeout=10) as r: return 200 <= r.status < 300
    except Exception as exc:
        print("ntfy publish failed:", exc); return False


def discover_profiles():
    found = {}
    for url in PROFILE_URLS:
        try:
            data = get_json(url)
            if isinstance(data, list):
                for p in data:
                    if p.get("chainId") and p.get("tokenAddress"):
                        found[f"{p['chainId']}:{p['tokenAddress']}"] = p
        except Exception as exc: print("discovery failed", url, exc)
    # Search common large/active symbols to include established and emerging coins.
    for term in SEARCH_TERMS:
        try:
            data = get_json(f"{BASE}/latest/dex/search?q={quote(term)}")
            for pair in (data.get("pairs") or [])[:8]:
                base = pair.get("baseToken") or {}
                if pair.get("chainId") and base.get("address"):
                    found[f"{pair['chainId']}:{base['address']}"] = {"chainId": pair["chainId"], "tokenAddress": base["address"], "url": pair.get("url")}
        except Exception as exc: print("search failed", term, exc)
    return list(found.values())[:MAX_DISCOVERY]


def inspect_profile(profile, previous):
    chain, token = profile.get("chainId"), profile.get("tokenAddress")
    try:
        data = get_json(f"{BASE}/tokens/v1/{quote(str(chain))}/{quote(str(token))}")
        pair = choose_pair(data if isinstance(data, list) else data.get("pairs"), chain)
        if not pair: return None
        base = pair.get("baseToken") or {}
        opportunity, reasons = market_score(pair, previous)
        return {
            "chainId": chain, "tokenAddress": token,
            "name": base.get("name") or "Unknown", "symbol": base.get("symbol") or "UNKNOWN",
            "priceUsd": num(pair.get("priceUsd")), "liquidityUsd": round(num((pair.get("liquidity") or {}).get("usd")), 2),
            "volume24h": round(num((pair.get("volume") or {}).get("h24")), 2), "marketCap": round(num(pair.get("marketCap") or pair.get("fdv")), 2),
            "priceChange5m": pct((pair.get("priceChange") or {}).get("m5")), "priceChange1h": pct((pair.get("priceChange") or {}).get("h1")), "priceChange24h": pct((pair.get("priceChange") or {}).get("h24")),
            "buys1h": int(num((pair.get("txns") or {}).get("h1", {}).get("buys"))), "sells1h": int(num((pair.get("txns") or {}).get("h1", {}).get("sells"))),
            "buys24h": int(num((pair.get("txns") or {}).get("h24", {}).get("buys"))), "sells24h": int(num((pair.get("txns") or {}).get("h24", {}).get("sells"))),
            "pairAgeMinutes": age_minutes(pair.get("pairCreatedAt")), "pairAddress": pair.get("pairAddress"), "dexId": pair.get("dexId"), "dexUrl": pair.get("url"),
            "opportunityScore": opportunity, "opportunityReasons": reasons, "fomoScore": opportunity,
            "dataStatus": "OK", "marketCapSource": "DEX Screener",
        }
    except Exception as exc:
        print("inspect failed", chain, token, exc)
        return None


def main():
    os.makedirs("data", exist_ok=True)
    state = load_json(STATE, {})
    previous = state.get("pairs", {}) if isinstance(state, dict) else {}
    security_cache = state.get("securityCache", {}) if isinstance(state, dict) else {}
    alert_topics = state.get("alertTopics", {}) if isinstance(state, dict) else {}
    known_tokens = state.get("knownTokens", {}) if isinstance(state, dict) else {}
    new99_alerted = state.get("new99Alerted", {}) if isinstance(state, dict) else {}
    security_cursor = int(num(state.get("securityCursor", 0))) if isinstance(state, dict) else 0
    bootstrap = not bool(known_tokens)

    profiles = discover_profiles()
    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(inspect_profile, p, previous) for p in profiles]
        for f in as_completed(futures):
            item = f.result()
            if item: results.append(item)
    results.sort(key=lambda x: (x.get("opportunityScore", 0), x.get("volume24h", 0)), reverse=True)
    results = results[:MAX_RESULTS]

    # Rotate security coverage instead of rescanning the same top 15 forever.
    # A few slots are reserved for urgent/new opportunities, a few for stale
    # results, and the rest are a true rotating slice of the entire universe.
    def token_key(item):
        return f"{item.get('chainId')}:{item.get('tokenAddress')}"

    now_ts = time.time()
    urgent = []
    stale = []
    for item in results:
        key = token_key(item)
        cached = security_cache.get(key) if isinstance(security_cache, dict) else None
        cached_result = cached.get("result") if isinstance(cached, dict) else None
        cached_age = now_ts - num(cached.get("checkedAt")) if isinstance(cached, dict) else float("inf")
        age = item.get("pairAgeMinutes")
        is_new_high = isinstance(age, (int, float)) and age <= NEW_99_MAX_AGE_MINUTES and item.get("opportunityScore", 0) >= 80
        if is_new_high:
            urgent.append(item)
        elif not cached_result or cached_age >= SECURITY_CACHE_MINUTES * 60:
            stale.append(item)

    targets = []
    seen_target_keys = set()

    for item in sorted(urgent, key=lambda x: x.get("opportunityScore", 0), reverse=True)[:URGENT_SECURITY_SLOTS]:
        key = token_key(item)
        if key not in seen_target_keys:
            targets.append(item); seen_target_keys.add(key)

    for item in sorted(stale, key=lambda x: x.get("opportunityScore", 0), reverse=True)[:STALE_SECURITY_SLOTS]:
        key = token_key(item)
        if key not in seen_target_keys:
            targets.append(item); seen_target_keys.add(key)

    # Always reserve slots for the rotating universe, so established coins
    # cannot be starved forever by a stream of new/high-scoring tokens.
    rotation_slots = max(0, SECURITY_TARGETS - len(targets))
    n = len(results)
    if n and rotation_slots:
        for offset in range(n):
            item = results[(security_cursor + offset) % n]
            key = token_key(item)
            if key in seen_target_keys:
                continue
            targets.append(item); seen_target_keys.add(key)
            if len(targets) >= SECURITY_TARGETS or len(targets) >= SECURITY_TARGETS - 0:
                break
        security_cursor = (security_cursor + ROTATING_SECURITY_SLOTS) % max(1, n)
    else:
        security_cursor = (security_cursor + ROTATING_SECURITY_SLOTS) % max(1, n) if n else 0

    targets = targets[:SECURITY_TARGETS]

    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = {}
        for item in targets:
            key = f"{item['chainId']}:{item['tokenAddress']}"
            jobs[pool.submit(goplus_security, item["chainId"], item["tokenAddress"], security_cache.get(key))] = (item, key)
        for f in as_completed(jobs):
            item, key = jobs[f]
            sec = f.result()
            item["securityStatus"] = sec.get("status", "UNKNOWN")
            item["securityVerified"] = bool(sec.get("verified"))
            item["securityBlocked"] = bool(sec.get("blocked"))
            item["securityRisk"] = sec.get("risk")
            item["securityReasons"] = sec.get("reasons", [])
            item["securitySource"] = sec.get("source", "GoPlus")
            item["securityScanned"] = True
            security_cache[key] = {"checkedAt": time.time(), "result": sec}

    for item in results:
        if "securityStatus" not in item:
            item.update({"securityStatus":"UNKNOWN","securityVerified":False,"securityBlocked":False,"securityRisk":None,"securityReasons":["Not selected for security scan yet"],"securitySource":"GoPlus","securityScanned":False})
        # Market/liquidity hard gates are separate from contract security.
        if item["liquidityUsd"] < MIN_LIQUIDITY_BUY:
            item["securityReasons"] = list(item.get("securityReasons", [])) + [f"liquidity below ${MIN_LIQUIDITY_BUY:,} BUY threshold"]
        if item["securityStatus"] == "VERIFIED" and not item["securityBlocked"] and item["liquidityUsd"] >= MIN_LIQUIDITY_BUY:
            item["securityGate"] = "PASSED"
        elif item["securityBlocked"] or item["liquidityUsd"] < MIN_LIQUIDITY_BUY:
            item["securityGate"] = "BLOCKED"
        else:
            item["securityGate"] = "UNKNOWN"
        trade = trade_signal(item)
        item["tradeSignal"], item["tradePoints"], item["tradeReason"] = trade["signal"], trade["points"], trade["reason"]

    now = datetime.now(timezone.utc).isoformat()
    alerts = []
    next_state = {"updatedAt": now, "pairs": {}, "alertTopics": alert_topics, "knownTokens": known_tokens, "securityCache": security_cache}

    for item in results:
        key = item.get("pairAddress") or f"{item['chainId']}:{item['tokenAddress']}"
        topic_for(alert_topics, key, "buy"); topic_for(alert_topics, key, "sell")
        old = previous.get(key, {}) if isinstance(previous, dict) else {}
        next_state["pairs"][key] = {"liquidityUsd":item["liquidityUsd"],"priceUsd":item["priceUsd"],"opportunityScore":item["opportunityScore"],"riskScore":item.get("securityRisk"),"tradeSignal":item["tradeSignal"],"securityStatus":item["securityStatus"],"securityGate":item["securityGate"]}

        token_id = f"{item['chainId']}:{item['tokenAddress']}"
        if token_id not in known_tokens:
            known_tokens[token_id] = {"firstSeenAt":now,"name":item["name"],"symbol":item["symbol"]}

        # A NEW 99 is allowed to wait for a security scan. UNKNOWN never counts
        # as safe, and we deliberately do not mark it alerted until a real gate
        # decision exists. This prevents the rotation system from losing a 99.
        age = item.get("pairAgeMinutes")
        is_new = isinstance(age,(int,float)) and age <= NEW_99_MAX_AGE_MINUTES
        if not bootstrap and is_new and item.get("opportunityScore", 0) >= 95 and token_id not in new99_alerted:
            if item["securityGate"] == "PASSED":
                alert = {"type":"NEW_FOMO_99","status":"SECURITY_PASSED","symbol":item["symbol"],"name":item["name"],"chainId":item["chainId"],"tokenAddress":item["tokenAddress"],"score":item["opportunityScore"]}
                alerts.append(alert)
                new99_alerted[token_id] = {"status":"SECURITY_PASSED","at":now}
                publish_ntfy(NTFY_MAIN_TOPIC, f"🔥 NEW 99 • SECURITY VERIFIED • {item['symbol']}", f"{item['name']} ({item['symbol']})\n🔥 Opportunity: {item['opportunityScore']}/99\nSecurity: VERIFIED\nLiquidity: ${item['liquidityUsd']:,.0f}\n24h: {item['priceChange24h']}%\nAge: {age} min\n\nSecurity gate passed; still verify before trading.", "5", "fire,white_check_mark,chart_with_upwards_trend")
            elif item["securityGate"] == "BLOCKED":
                alert = {"type":"NEW_FOMO_99_BLOCKED","status":"BLOCKED","symbol":item["symbol"],"name":item["name"],"chainId":item["chainId"],"tokenAddress":item["tokenAddress"],"score":item["opportunityScore"],"reasons":item.get("securityReasons",[])[:5]}
                alerts.append(alert)
                new99_alerted[token_id] = {"status":"BLOCKED","at":now}
                publish_ntfy(NTFY_MAIN_TOPIC, f"🚨 99 BLOCKED • {item['symbol']}", f"{item['name']} ({item['symbol']})\nOpportunity: {item['opportunityScore']}/99\nSecurity gate: BLOCKED\nReasons: {'; '.join(item.get('securityReasons',[])[:4])}", "5", "warning,no_entry_sign")

        old_signal = old.get("tradeSignal")
        if item["tradeSignal"] == "BUY SETUP" and old_signal != "BUY SETUP":
            if item["securityGate"] == "PASSED":
                topic = topic_for(alert_topics,key,"buy")
                alerts.append({"type":"BUY_SIGNAL","symbol":item["symbol"],"topic":topic,"score":item["opportunityScore"]})
                publish_ntfy(topic, f"🟢 BUY SETUP • {item['symbol']}", f"{item['name']} ({item['symbol']})\nOpportunity: {item['opportunityScore']}/99\nSecurity: VERIFIED\nLiquidity: ${item['liquidityUsd']:,.0f}\n24h: {item['priceChange24h']}%\n{item['tradeReason']}\n\nRule-based signal, not a guarantee.", "4", "chart_with_upwards_trend,green_circle")
        if item["tradeSignal"] == "EXIT / AVOID" and old_signal != "EXIT / AVOID":
            topic = topic_for(alert_topics,key,"sell")
            alerts.append({"type":"SELL_SIGNAL","symbol":item["symbol"],"topic":topic,"score":item["opportunityScore"]})
            publish_ntfy(topic, f"🔴 EXIT / AVOID • {item['symbol']}", f"{item['name']} ({item['symbol']})\nOpportunity: {item['opportunityScore']}/99\n{item['tradeReason']}", "5", "warning,red_circle")

    if len(known_tokens) > 5000: known_tokens = dict(list(known_tokens.items())[-5000:])
    next_state["knownTokens"] = known_tokens
    next_state["new99Alerted"] = new99_alerted
    next_state["alertTopics"] = alert_topics
    next_state["securityCursor"] = security_cursor
    topic_view = {k:{"buy":v.get("buy"),"sell":v.get("sell")} for k,v in alert_topics.items() if isinstance(v,dict)}

    security_counts = {"VERIFIED":0,"CAUTION":0,"BLOCKED":0,"UNKNOWN":0,"scanned":0,"unscanned":0,"total":len(results)}
    for item in results:
        status = item.get("securityStatus", "UNKNOWN")
        security_counts[status] = security_counts.get(status, 0) + 1
        if item.get("securityScanned"):
            security_counts["scanned"] += 1
        else:
            security_counts["unscanned"] += 1

    snapshot = {
        "version": 5, "generatedAt": now, "source":"DEX Screener + GoPlus",
        "monitor": {"status":"online","tokenCount":len(results),"interval":"5 minutes","securityProvider":"GoPlus","securityConfigured":bool(GOPLUS_APP_KEY and GOPLUS_APP_SECRET),"securityCoverage":security_counts,"securityRotationCursor":security_cursor,"securityRotationBatch":SECURITY_TARGETS,"warning":"Opportunity scores are rule-based market indicators. Security verification is not a guarantee and BUY alerts are suppressed unless the security gate passes. UNKNOWN security never passes the BUY gate.","universe":"Established + emerging + new-token discovery feeds; not literally every token on every exchange."},
        "alerts": alerts, "alertTopics": topic_view, "tokens": results,
    }
    save_json(OUT,snapshot); save_json(STATE,next_state)
    print(f"Wrote {OUT}: {len(results)} opportunities, {len(alerts)} alerts, security={'ON' if (GOPLUS_APP_KEY and GOPLUS_APP_SECRET) else 'OFF'}")

if __name__ == "__main__": main()
