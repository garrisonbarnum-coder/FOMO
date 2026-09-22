#!/usr/bin/env python3
"""
FOMO Coin Tracker - 24/7 background monitor with per-coin BUY/SELL alerts.

Runs from GitHub Actions, reads public DEX Screener data, calculates the same
rule-based trade signal used by the dashboard, persists signal state, and
publishes a notification to a unique ntfy topic for each coin/signal type.
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
from urllib.request import Request, urlopen

BASE = "https://api.dexscreener.com"
PROFILE_URL = f"{BASE}/token-profiles/latest/v1"
OUT = "data/latest.json"
STATE = "data/state.json"
MAX_PROFILES = 30
TIMEOUT = 15
NTFY_BASE = "https://ntfy.sh"
NTFY_MAIN_TOPIC = os.getenv("FOMO_NTFY_TOPIC", "").strip()


def get_json(url, retries=3):
    last = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "FOMO-Coin-Tracker/2.0"})
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
    age = max(0, time.time() - num(created_at) / 1000)
    return round(age / 60, 1)


def choose_pair(pairs, chain_id):
    candidates = [p for p in (pairs or []) if p.get("chainId") == chain_id]
    if not candidates:
        candidates = pairs or []
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda p: (
            num((p.get("liquidity") or {}).get("usd")),
            num((p.get("volume") or {}).get("h24")),
        ),
    )


def risk_for(pair, previous):
    if not pair:
        return 100, ["No DEX pair/data returned"], "UNKNOWN"

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


def trade_signal(pair):
    """Keep this aligned with the dashboard's getTradeSignal()."""
    change = num((pair.get("priceChange") or {}).get("h24"))
    volume = num((pair.get("volume") or {}).get("h24"))
    liquidity = num((pair.get("liquidity") or {}).get("usd"), None)
    tx = pair.get("txns") or {}
    h24 = tx.get("h24") or {}
    buys = int(num(h24.get("buys")))
    sells = int(num(h24.get("sells")))
    total = buys + sells

    if not total:
        return {"signal": "WAIT", "points": 0, "reason": "Not enough trading activity"}

    buy_ratio = buys / total
    points = 0
    reasons = []

    if change > 5:
        points += 15; reasons.append("positive momentum")
    if change > 20:
        points += 10; reasons.append("strong price movement")
    if volume > 10000:
        points += 10
    if volume > 100000:
        points += 10

    if liquidity is not None:
        if liquidity > 5000:
            points += 10
        if liquidity > 25000:
            points += 10
        if liquidity > 100000:
            points += 10
    else:
        reasons.append("liquidity data unavailable")

    if buy_ratio > 0.55:
        points += 15; reasons.append("buyers leading")
    if buy_ratio > 0.65:
        points += 10

    if sells > buys and total > 20:
        points -= 15; reasons.append("sellers leading")

    if liquidity is not None and liquidity < 2000:
        points -= 20; reasons.append("thin liquidity")

    points = max(0, min(100, points))
    if points >= 65:
        signal = "BUY SETUP"
    elif points <= 25:
        signal = "EXIT / AVOID"
    else:
        signal = "WAIT"

    return {
        "signal": signal,
        "points": points,
        "reason": " • ".join(reasons[:3]) or "Mixed market conditions",
    }


def topic_for(alert_topics, key, signal_type):
    item = alert_topics.get(key)
    if not isinstance(item, dict):
        item = {}
    topic = item.get(signal_type)
    if not topic:
        topic = f"fomo-{signal_type}-{secrets.token_urlsafe(18).replace('-', '').replace('_', '')}"
        item[signal_type] = topic
        alert_topics[key] = item
    return topic


def publish_ntfy(topic, title, message, priority="4", tags="chart_with_upwards_trend"):
    if not topic:
        return False
    try:
        req = Request(
            f"{NTFY_BASE}/{topic}",
            data=message.encode("utf-8"),
            method="POST",
            headers={
                "Title": title,
                "Priority": str(priority),
                "Tags": tags,
                "User-Agent": "FOMO-Coin-Tracker/2.0",
            },
        )
        with urlopen(req, timeout=10) as response:
            return 200 <= response.status < 300
    except Exception as exc:
        print(f"ntfy publish failed for {topic}: {exc}")
        return False


def publish_main_alert(alert):
    if not NTFY_MAIN_TOPIC:
        return
    title = "🚨 FOMO RUG RISK"
    message = (
        f"{alert.get('symbol', 'Token')}\n"
        f"Risk score: {alert.get('riskScore')}\n"
        f"{'; '.join(alert.get('reasons', [])[:3])}"
    )
    publish_ntfy(NTFY_MAIN_TOPIC, title, message, priority="5", tags="warning,skull")


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
        liq = num((pair.get("liquidity") or {}).get("usd"))
        tx = pair.get("txns") or {}
        h1tx = tx.get("h1") or {}
        h24tx = tx.get("h24") or {}
        buys1h, sells1h = int(num(h1tx.get("buys"))), int(num(h1tx.get("sells")))
        buys24h, sells24h = int(num(h24tx.get("buys"))), int(num(h24tx.get("sells")))
        total1h = buys1h + sells1h
        trade = trade_signal(pair)
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
            "buys1h": buys1h,
            "sells1h": sells1h,
            "buys24h": buys24h,
            "sells24h": sells24h,
            "sellPressure1h": round(sells1h / total1h, 3) if total1h else 0,
            "pairAgeMinutes": age_minutes(pair.get("pairCreatedAt")),
            "pairAddress": pair.get("pairAddress"),
            "dexId": pair.get("dexId"),
            "dexUrl": pair.get("url"),
            "riskScore": risk,
            "riskLevel": level,
            "riskReasons": reasons,
            "tradeSignal": trade["signal"],
            "tradePoints": trade["points"],
            "tradeReason": trade["reason"],
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
    alert_topics = state.get("alertTopics", {}) if isinstance(state, dict) else {}
    if not isinstance(alert_topics, dict):
        alert_topics = {}

    profiles = get_json(PROFILE_URL)
    if not isinstance(profiles, list):
        raise RuntimeError("Unexpected token profile response")

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

    results.sort(key=lambda x: (x.get("riskScore", 100) if isinstance(x.get("riskScore"), (int, float)) else 101, x.get("liquidityUsd", 0)), reverse=True)

    now = datetime.now(timezone.utc).isoformat()
    alerts = []
    next_state = {"updatedAt": now, "pairs": {}, "alertTopics": alert_topics}

    # Create stable per-coin BUY and SELL ntfy topics immediately, so the
    # dashboard can let the user subscribe before the first signal transition.
    for item in results:
        if item.get("dataStatus") != "OK":
            continue
        key = item.get("pairAddress") or f"{item.get('chainId')}:{item.get('tokenAddress')}"
        topic_for(alert_topics, key, "buy")
        topic_for(alert_topics, key, "sell")

    next_state["alertTopics"] = alert_topics

    for item in results:
        key = item.get("pairAddress") or f"{item.get('chainId')}:{item.get('tokenAddress')}"
        old = previous.get(key, {}) if isinstance(previous, dict) else {}
        current_signal = item.get("tradeSignal") if item.get("dataStatus") == "OK" else None
        old_signal = old.get("tradeSignal")

        next_state["pairs"][key] = {
            "liquidityUsd": item.get("liquidityUsd", 0),
            "priceUsd": item.get("priceUsd", 0),
            "riskScore": item.get("riskScore"),
            "tradeSignal": current_signal,
        }

        # Seed first observation; only notify on a later transition into a signal.
        if current_signal and old_signal and current_signal != old_signal:
            if current_signal == "BUY SETUP":
                topic = topic_for(alert_topics, key, "buy")
                alerts.append({
                    "type": "BUY_SIGNAL",
                    "chainId": item.get("chainId"),
                    "symbol": item.get("symbol"),
                    "tokenAddress": item.get("tokenAddress"),
                    "signal": current_signal,
                    "points": item.get("tradePoints"),
                    "reason": item.get("tradeReason"),
                    "topic": topic,
                })
                publish_ntfy(
                    topic,
                    f"🟢 {item.get('symbol', 'Token')} BUY SIGNAL",
                    f"{item.get('name', 'Token')} ({item.get('symbol', 'Token')})\n"
                    f"BUY SETUP • score {item.get('tradePoints')}/100\n"
                    f"Price: ${item.get('priceUsd')}\n"
                    f"24h: {item.get('priceChange24h')}%\n"
                    f"Liquidity: ${item.get('liquidityUsd'):,.0f}\n"
                    f"{item.get('tradeReason', '')}\n\n"
                    f"Rule-based market signal; not a guarantee.",
                    priority="4",
                    tags="chart_with_upwards_trend,green_circle",
                )

            elif current_signal == "EXIT / AVOID":
                topic = topic_for(alert_topics, key, "sell")
                alerts.append({
                    "type": "SELL_SIGNAL",
                    "chainId": item.get("chainId"),
                    "symbol": item.get("symbol"),
                    "tokenAddress": item.get("tokenAddress"),
                    "signal": current_signal,
                    "points": item.get("tradePoints"),
                    "reason": item.get("tradeReason"),
                    "topic": topic,
                })
                publish_ntfy(
                    topic,
                    f"🔴 {item.get('symbol', 'Token')} SELL / EXIT",
                    f"{item.get('name', 'Token')} ({item.get('symbol', 'Token')})\n"
                    f"EXIT / AVOID • score {item.get('tradePoints')}/100\n"
                    f"Price: ${item.get('priceUsd')}\n"
                    f"24h: {item.get('priceChange24h')}%\n"
                    f"Liquidity: ${item.get('liquidityUsd'):,.0f}\n"
                    f"{item.get('tradeReason', '')}\n\n"
                    f"Rule-based market signal; not a guarantee.",
                    priority="5",
                    tags="warning,red_circle",
                )

        current_risk = item.get("riskScore")
        old_risk = old.get("riskScore")
        if (
            item.get("dataStatus") == "OK"
            and isinstance(current_risk, (int, float))
            and current_risk >= 75
            and (not isinstance(old_risk, (int, float)) or old_risk < 75)
        ):
            alert = {
                "type": "RUG_RISK",
                "chainId": item.get("chainId"),
                "symbol": item.get("symbol"),
                "tokenAddress": item.get("tokenAddress"),
                "riskScore": current_risk,
                "reasons": item.get("riskReasons", []),
            }
            alerts.append(alert)
            publish_main_alert(alert)

    # Persist the per-coin topics in the public monitor snapshot so the UI can
    # open the correct ntfy Android deep link without exposing the main ntfy topic.
    topic_view = {}
    for key, value in alert_topics.items():
        if isinstance(value, dict) and (value.get("buy") or value.get("sell")):
            topic_view[key] = {
                "buy": value.get("buy"),
                "sell": value.get("sell"),
            }

    snapshot = {
        "version": 2,
        "generatedAt": now,
        "source": "DEX Screener",
        "monitor": {
            "status": "online",
            "tokenCount": len(results),
            "interval": "5 minutes",
            "warning": "Trade signals are rule-based market indicators. They do not guarantee that a token will rise or fall.",
        },
        "alerts": alerts,
        "alertTopics": topic_view,
        "tokens": results,
    }

    save_json(OUT, snapshot)
    save_json(STATE, next_state)
    print(f"Wrote {OUT}: {len(results)} tokens, {len(alerts)} alerts")


if __name__ == "__main__":
    main()
