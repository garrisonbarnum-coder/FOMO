#!/usr/bin/env python3
"""
FOMO Coin Tracker - background monitor.

Uses public DEX Screener data, calculates a conservative risk score, compares
liquidity/price with the previous run, and writes data/latest.json.

This is a warning system, not a guarantee that a token is safe or that a
token is a rug pull. Missing data lowers confidence rather than proving safety.
"""
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE = "https://api.dexscreener.com"
PROFILE_URL = f"{BASE}/token-profiles/latest/v1"
OUT = "data/latest.json"
STATE = "data/state.json"
MAX_PROFILES = 30
TIMEOUT = 15

NTFY_URL = "https://ntfy.sh"
NTFY_TOPIC = os.getenv("FOMO_NTFY_TOPIC", "").strip()
TEST_NOTIFICATION = os.getenv("FOMO_TEST_NOTIFICATION", "").strip().lower() in {"1", "true", "yes", "on"}

def send_push(title, message, priority=5, tags="warning"):
    if not NTFY_TOPIC:
        return False

    try:
        payload = json.dumps({
            "topic": NTFY_TOPIC,
            "title": title,
            "message": message,
            "priority": priority,
            "tags": tags,
        }).encode("utf-8")

        req = Request(
            NTFY_URL,
            data=payload,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "FOMO-Coin-Tracker/1.0",
            },
            method="POST",
        )

        with urlopen(req, timeout=TIMEOUT) as response:
            return 200 <= response.status < 300

    except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
        print(f"Push notification failed: {exc}")
        return False
def get_json(url, retries=3):
    last = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "FOMO-Coin-Tracker/1.0"})
            with urlopen(req, timeout=TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last = exc
            time.sleep(1.5 * (attempt + 1))
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
    # DEX Screener uses milliseconds since epoch.
    age = max(0, time.time() - num(created_at) / 1000)
    return round(age / 60, 1)


def choose_pair(pairs, chain_id):
    candidates = [p for p in (pairs or []) if p.get("chainId") == chain_id]
    if not candidates:
        candidates = pairs or []
    if not candidates:
        return None
    # Prefer liquidity, then recent volume.
    return max(
        candidates,
        key=lambda p: (
            num((p.get("liquidity") or {}).get("usd")),
            num((p.get("volume") or {}).get("h24")),
        ),
    )


def risk_for(pair, previous):
    if not pair:
        return 0, ["No DEX pair/data returned"], "UNKNOWN"

    liq = num((pair.get("liquidity") or {}).get("usd"))
    pc = pair.get("priceChange") or {}
    vol = pair.get("volume") or {}
    tx = pair.get("txns") or {}
    h1 = tx.get("h1") or {}
    buys = num(h1.get("buys"))
    sells = num(h1.get("sells"))
    total = buys + sells
    sell_ratio = sells / total if total else 0

    score = 0
    reasons = []

    if liq < 5000:
        score += 45; reasons.append("very low liquidity")
    elif liq < 15000:
        score += 30; reasons.append("low liquidity")
    elif liq < 50000:
        score += 15; reasons.append("modest liquidity")

    p1 = pct(pc.get("h1"))
    p5 = pct(pc.get("m5"))
    if p1 <= -50:
        score += 30; reasons.append("severe 1h price drop")
    elif p1 <= -25:
        score += 20; reasons.append("large 1h price drop")
    elif p1 <= -10:
        score += 10; reasons.append("negative 1h price move")
    if p5 <= -20:
        score += 20; reasons.append("sharp 5m price drop")
    elif p5 <= -10:
        score += 10; reasons.append("negative 5m price move")

    if total >= 5:
        if sell_ratio >= 0.75:
            score += 20; reasons.append("heavy sell pressure")
        elif sell_ratio >= 0.60:
            score += 10; reasons.append("elevated sell pressure")

    h24vol = num(vol.get("h24"))
    if liq > 0 and h24vol / liq > 8:
        score += 10; reasons.append("extreme volume/liquidity ratio")
    elif liq > 0 and h24vol / liq > 4:
        score += 5; reasons.append("high volume/liquidity ratio")

    age = age_minutes(pair.get("pairCreatedAt"))
    if age is not None and age < 60:
        score += 20; reasons.append("pair is less than 1 hour old")
    elif age is not None and age < 360:
        score += 10; reasons.append("pair is less than 6 hours old")

    key = pair.get("pairAddress") or pair.get("url") or ""
    old = previous.get(key) if isinstance(previous, dict) else None
    if old:
        old_liq = num(old.get("liquidityUsd"))
        if old_liq > 0:
            drop = (liq - old_liq) / old_liq
            if drop <= -0.60:
                score += 45; reasons.append("liquidity dropped more than 60%")
            elif drop <= -0.40:
                score += 30; reasons.append("liquidity dropped more than 40%")
            elif drop <= -0.20:
                score += 15; reasons.append("liquidity dropped more than 20%")

    score = min(100, max(0, round(score)))
    if score >= 75:
        level = "CRITICAL"
    elif score >= 50:
        level = "HIGH"
    elif score >= 25:
        level = "MEDIUM"
    else:
        level = "LOW"
    return score, reasons[:6], level


def inspect_profile(profile, previous):
    chain = profile.get("chainId")
    token = profile.get("tokenAddress")
    if not chain or not token:
        return None
    try:
        data = get_json(f"{BASE}/latest/dex/tokens/{token}")
        pair = choose_pair(data.get("pairs"), chain)
        risk, reasons, level = risk_for(pair, previous)
        if not pair:
            return {
                "chainId": chain, "tokenAddress": token,
                "name": "Unknown", "symbol": "UNKNOWN",
                "riskScore": None, "riskLevel": "UNKNOWN",
                "riskReasons": reasons, "dataStatus": "NO_PAIR",
                "dexUrl": profile.get("url"),
            }

        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}
        liq = num((pair.get("liquidity") or {}).get("usd"))
        h1tx = (pair.get("txns") or {}).get("h1") or {}
        buys, sells = int(num(h1tx.get("buys"))), int(num(h1tx.get("sells")))
        total = buys + sells
        return {
            "chainId": chain,
            "tokenAddress": token,
            "name": base.get("name") or profile.get("description") or "Unknown",
            "symbol": base.get("symbol") or "UNKNOWN",
            "priceUsd": num(pair.get("priceUsd")),
            "liquidityUsd": round(liq, 2),
            "volume24h": round(num((pair.get("volume") or {}).get("h24")), 2),
            "marketCap": round(num(pair.get("marketCap") or pair.get("fdv")), 2),
            "priceChange5m": pct((pair.get("priceChange") or {}).get("m5")),
            "priceChange1h": pct((pair.get("priceChange") or {}).get("h1")),
            "priceChange6h": pct((pair.get("priceChange") or {}).get("h6")),
            "priceChange24h": pct((pair.get("priceChange") or {}).get("h24")),
            "buys1h": buys,
            "sells1h": sells,
            "sellPressure1h": round(sells / total, 3) if total else 0,
            "pairAgeMinutes": age_minutes(pair.get("pairCreatedAt")),
            "pairAddress": pair.get("pairAddress"),
            "dexId": pair.get("dexId"),
            "dexUrl": pair.get("url"),
            "riskScore": risk,
            "riskLevel": level,
            "riskReasons": reasons,
            "dataStatus": "OK",
        }
    except Exception as exc:
        return {
            "chainId": chain, "tokenAddress": token,
            "name": "Unknown", "symbol": "UNKNOWN",
            "riskScore": None, "riskLevel": "ERROR",
            "riskReasons": ["monitor request failed"],
            "dataStatus": "ERROR",
            "error": str(exc)[:180],
            "dexUrl": profile.get("url"),
        }


def main():
    os.makedirs("data", exist_ok=True)
    state = load_json(STATE, {})
    previous = state.get("pairs", {}) if isinstance(state, dict) else {}

    profiles = get_json(PROFILE_URL)
    if not isinstance(profiles, list):
        raise RuntimeError("Unexpected token profile response")

    # Deduplicate by chain + token and cap work per run.
    unique = {}
    for p in profiles:
        key = f"{p.get('chainId')}:{p.get('tokenAddress')}"
        if p.get("chainId") and p.get("tokenAddress"):
            unique[key] = p
    profiles = list(unique.values())[:MAX_PROFILES]

    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(inspect_profile, p, previous) for p in profiles]
        for f in as_completed(futures):
            item = f.result()
            if item:
                results.append(item)

    results.sort(
    key=lambda x: (
        x.get("riskScore") if isinstance(x.get("riskScore"), (int, float)) else -1,
        x.get("liquidityUsd", 0)
    ),
    reverse=True
    )

    now = datetime.now(timezone.utc).isoformat()
    alerts = []
    next_state = {"updatedAt": now, "pairs": {}}

    for item in results:
        key = item.get("pairAddress") or f"{item.get('chainId')}:{item.get('tokenAddress')}"
        next_state["pairs"][key] = {
            "liquidityUsd": item.get("liquidityUsd", 0),
            "priceUsd": item.get("priceUsd", 0),
            "riskScore": item.get("riskScore"),
        }
        old = previous.get(key, {})
        current_score = item.get("riskScore")
        old_score = old.get("riskScore")
        if (
            item.get("dataStatus") == "OK"
            and isinstance(current_score, (int, float))
            and current_score >= 75
            and (
                not isinstance(old_score, (int, float))
                or old_score < 75
            )
        ):
            alerts.append({
                "type": "RUG_RISK",
                "chainId": item.get("chainId"),
                "symbol": item.get("symbol"),
                "tokenAddress": item.get("tokenAddress"),
                "riskScore": item.get("riskScore"),
                "reasons": item.get("riskReasons", []),
            })

    snapshot = {
        "version": 1,
        "generatedAt": now,
        "source": "DEX Screener",
        "monitor": {
            "status": "online",
            "tokenCount": len(results),
            "interval": "5 minutes",
            "warning": "Risk scores are heuristics. They do not prove that a token is or is not a rug pull. Missing/error data is reported separately and is not treated as proof of a rug.",
        },
        "alerts": alerts,
        "tokens": results,
    }

        save_json(OUT, snapshot)
    save_json(STATE, next_state)

    if TEST_NOTIFICATION:
        send_push(
            "🔥 FOMO Test Alert",
            "Jarvis 24/7 push notifications are working. You can close the website.",
            priority=4,
            tags="white_check_mark,rocket",
        )

    for alert in alerts:
        reasons = ", ".join(alert.get("reasons", [])[:4]) or "High-risk signals detected"

        send_push(
            f"🚨 FOMO RUG RISK • {alert.get('symbol', 'UNKNOWN')}",
            f"Risk score: {alert.get('riskScore', '?')}/100\n{reasons}",
            priority=5,
            tags="rotating_light,warning",
        )

    print(
        f"Wrote {OUT}: {len(results)} tokens, {len(alerts)} new alerts; "
        f"push={'on' if NTFY_TOPIC else 'off'}"
    )


if __name__ == "__main__":
    main()
