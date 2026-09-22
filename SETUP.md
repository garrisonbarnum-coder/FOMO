# FOMO 24/7 Monitor — setup

## Files

Upload these files to the root of the `garrisonbarnum-coder/FOMO` repository:

- `monitor.py`
- `.github/workflows/fomo-monitor.yml`
- `fomo-background.js`

The monitor will create/update:

- `data/latest.json`
- `data/state.json`

## GitHub setup

1. Open the FOMO repository on GitHub.
2. Use **Add file → Upload files**.
3. Upload `monitor.py` and `fomo-background.js`.
4. Create the `.github/workflows` folders if needed and upload `fomo-monitor.yml` there.
5. Commit the changes to `main`.
6. Open **Actions → FOMO 24/7 Monitor**.
7. Run it once with **Run workflow**.
8. After the first successful run, the scheduled job runs about every 5 minutes.

GitHub Actions schedules are not guaranteed to execute at the exact minute, so this is best described as near-continuous background monitoring rather than a guaranteed 24/7 real-time feed.

## Frontend

Add this immediately before `</body>` in `index.html`:

```html
<script src="./fomo-background.js"></script>
```

Your existing live DEX Screener code can continue running. The bridge exposes the background snapshot as:

```js
window.FOMO_BACKGROUND
```

and emits:

```js
window.addEventListener("fomo:background-data", (event) => {
  const snapshot = event.detail;
  // Use snapshot.tokens and snapshot.alerts here.
});
```

## What the monitor checks

- Liquidity size
- Liquidity drops between runs
- 5-minute and 1-hour price drops
- 1-hour buy/sell pressure
- Volume-to-liquidity ratio
- Pair age
- Missing pair/data
- New high-risk transitions

A high score means **higher observed risk**, not proof of a rug pull.

This version intentionally does not claim that a token is safe. Contract-level checks such as mint/freeze authority, ownership privileges, LP lock/burn status, holder concentration, honeypot simulation, and verified-source analysis require chain-specific data and should be added as separate checks rather than guessed from DEX data.
