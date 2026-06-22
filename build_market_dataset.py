#!/usr/bin/env python3
"""Distill the raw treasury download into the compact dataset the server serves.

Input  : market_index.db   (the big ~200MB Solscan/RPC download — treasury_txns with
                             full tx_json/deltas_json/programs_json; built by
                             build_treasury_index.py on the laptop)
Output : market.db          (a small, indexed `market_txns` table — only the columns the
                             Market Watch page needs, each transaction priced in USD at the
                             KINS/USD close of its own day)

Why a separate file: the raw download is huge and full of bytes the website never reads
(raw transaction JSON, per-account delta maps, program lists). The server only needs:
who/when/how much/what kind/how much in USD. We compute that once here and ship the small
result. The big file stays on the laptop; never commit it, never put it on the server.

These are reconstructions of real sales, so each txn is priced with the KINS/USD at the
*minute of the trade*, not a daily average — KINS pumped ~300x over the captured month and
moves several % intraday during spikes. build_price_series() pages GeckoTerminal 1-minute
candles backward across the range (paced to dodge 429s) with an hourly baseline underneath
for any older span the minute feed can't reach; price_at() snaps each txn to its nearest
candle. See those functions for the resolution/coverage detail.

Run:
    python3 build_market_dataset.py                 # market_index.db -> market.db
    python3 build_market_dataset.py --src X --out Y
Then ship it to the server's data volume (out of band — the DB is gitignored and the
hosted kintara.db lives on a volume that deploys never touch):
    scp market.db root@<host>:/opt/kintara-data/market.db
"""
import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import time

# kind (on-chain transaction shape, set by build_treasury_index.py) -> user-facing category
#   marketplace : player buys from player; treasury takes a ~5% fee
#   sink        : player pays; ~50% burned + ~50% to treasury, no seller (casino / wheel /
#                 spinner wagers + any other KINS burn-sink — the game has all of these)
#   payout      : treasury pays a player (casino winnings / refunds / gifts)
#   other       : rare mixed/edge transactions
KIND_CATEGORY = {
    "marketplace_trade": "marketplace",
    "treasury_income": "sink",
    "treasury_income_with_receivers": "sink",
    "treasury_payout": "payout",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS market_txns(
    sig          TEXT PRIMARY KEY,
    ts           INTEGER,   -- unix ms
    date         TEXT,      -- YYYY-MM-DD (UTC) — aggregation/join key
    category     TEXT,      -- marketplace | sink | payout | other
    buyer        TEXT,      -- payer (wallet)
    seller       TEXT,      -- counterparty / receiver (wallet); NULL for pure sinks
    gross_kins   REAL,      -- total KINS the payer moved (the transaction's size)
    to_player    REAL,      -- KINS that reached the other player (seller net)
    to_treasury  REAL,      -- KINS that reached the treasury (fee / take)
    burned_kins  REAL,      -- KINS burned (sinks)
    kins_usd     REAL,      -- KINS/USD at the trade's minute (nearest candle, 1m where reachable)
    usd_value    REAL       -- gross_kins * kins_usd  (economic size in USD at the time of the trade)
);
CREATE INDEX IF NOT EXISTS idx_mt_ts     ON market_txns(ts);
CREATE INDEX IF NOT EXISTS idx_mt_date   ON market_txns(date);
CREATE INDEX IF NOT EXISTS idx_mt_cat    ON market_txns(category);
CREATE INDEX IF NOT EXISTS idx_mt_buyer  ON market_txns(buyer);
CREATE INDEX IF NOT EXISTS idx_mt_seller ON market_txns(seller);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


import bisect

# These are reconstructions of real sales, so each transaction is priced with the KINS/USD
# *at the time of the trade*, not a daily average. GeckoTerminal gives 1-minute candles but
# only ~18h per 1000-candle page (so the full month needs ~45 paged requests) and rate-limits
# hard; 1-hour candles cover the whole month in a single request. We therefore page 1m
# backward across the range (paced to dodge 429s) and lay an hourly baseline underneath so
# every minute the 1m feed can't reach still has a price within ±30min, then price each txn by
# its NEAREST candle.
GECKO_PACE_S = 2.6        # spacing between GeckoTerminal requests (≈ <30/min)
MINUTE_PAGE_BUDGET = 80   # safety cap on 1m pages (≈55 days of minute coverage)


def _fetch_retry(kt, tf, agg, before=None, tries=5):
    """fetch_kins_ohlcv with backoff on 429/transient errors. Returns [] if it keeps failing."""
    for i in range(tries):
        try:
            return kt.fetch_kins_ohlcv(tf, agg, 1000, before=before)
        except Exception as e:
            wait = GECKO_PACE_S * (i + 2)
            print(f"   …{tf}/{agg} retry {i+1} after error ({str(e)[:60]}); sleep {wait:.0f}s")
            time.sleep(wait)
    return []


def build_price_series(ts_min_s, ts_max_s):
    """Sorted [(ts_seconds, close_usd)] covering [ts_min_s, ts_max_s] at the finest feasible
    resolution: 1-minute paged back to ts_min (or the page budget), hourly underneath for any
    older span the minute feed didn't reach. Returns (series, coverage_note)."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import kintara_tracker as kt
    pts = {}  # ts_seconds -> close  (1m overwrites hourly at the same ts)

    # hourly baseline (one request usually covers the whole month)
    print("== pricing: hourly baseline …")
    before = None
    for _ in range(6):
        rows = _fetch_retry(kt, "hour", 1, before=before)
        if not rows:
            break
        for ts, px in rows:
            pts[ts] = px
        oldest = min(r[0] for r in rows)
        if oldest <= ts_min_s:
            break
        before = oldest
        time.sleep(GECKO_PACE_S)

    # 1-minute overlay, paged backward until we cover ts_min (or hit the budget)
    print("== pricing: 1-minute overlay (paged, paced) …")
    before, pages, minute_floor = None, 0, ts_max_s
    while pages < MINUTE_PAGE_BUDGET:
        rows = _fetch_retry(kt, "minute", 1, before=before)
        if not rows:
            break
        for ts, px in rows:
            pts[ts] = px
        oldest = min(r[0] for r in rows)
        minute_floor = min(minute_floor, oldest)
        pages += 1
        print(f"   1m page {pages}: back to {dt.datetime.utcfromtimestamp(oldest)}", end="\r")
        if oldest <= ts_min_s:
            break
        before = oldest
        time.sleep(GECKO_PACE_S)
    print()

    series = sorted(pts.items())
    note = (f"1m back to {dt.datetime.utcfromtimestamp(minute_floor):%Y-%m-%d %H:%M}, "
            f"hourly before; {len(series)} candles")
    return series, note


def price_at(series, ts_ms):
    """KINS/USD at a transaction time: the close of the candle NEAREST (by absolute time) to
    the txn. series is sorted (ts_seconds, close). Minute-covered txns land on their own
    minute; older txns snap to the nearest hourly candle (±30min)."""
    if not series or not ts_ms:
        return None
    t = ts_ms / 1000.0
    i = bisect.bisect_left(series, (t,))
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(series):
            if best is None or abs(series[j][0] - t) < abs(best[0] - t):
                best = series[j]
    return best[1] if best else None


def distill(src_path, out_path):
    if not os.path.exists(src_path):
        sys.exit(f"source not found: {src_path}")
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row

    rng = src.execute("SELECT MIN(ts) a, MAX(ts) b FROM treasury_txns").fetchone()
    ts_min_s, ts_max_s = (rng["a"] or 0) / 1000.0, (rng["b"] or 0) / 1000.0
    print(f"== txn range {dt.datetime.utcfromtimestamp(ts_min_s)} → {dt.datetime.utcfromtimestamp(ts_max_s)}")
    series, price_note = build_price_series(ts_min_s, ts_max_s)
    print(f"   price series: {price_note}")

    if os.path.exists(out_path):
        os.remove(out_path)
    out = sqlite3.connect(out_path)
    out.row_factory = sqlite3.Row
    out.executescript(SCHEMA)

    total = src.execute("SELECT COUNT(*) c FROM treasury_txns").fetchone()["c"]
    print(f"== distilling {total:,} treasury_txns -> {out_path}")
    n = 0
    batch = []
    for r in src.execute("SELECT * FROM treasury_txns"):
        cat = KIND_CATEGORY.get(r["kind"], "other")
        ts = r["ts"] or 0
        date = dt.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else None
        gross = r["gross_kins"] or 0.0
        to_player = r["seller_net_kins"] or 0.0
        to_treasury = r["fee_kins"]
        if to_treasury is None:
            to_treasury = abs(r["treasury_delta_kins"] or 0.0)
        # burned = whatever the payer moved that didn't reach the player or the treasury
        burned = 0.0
        if cat == "sink":
            burned = max(0.0, gross - to_treasury - to_player)
        kins_usd = price_at(series, ts)        # KINS/USD nearest the trade's actual minute
        usd_value = (gross * kins_usd) if (kins_usd is not None) else None
        batch.append((r["sig"], ts, date, cat, r["buyer"], r["seller"],
                      gross, to_player, to_treasury, burned, kins_usd, usd_value))
        n += 1
        if len(batch) >= 5000:
            out.executemany("INSERT OR REPLACE INTO market_txns VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            batch.clear()
            print(f"   {n:,}/{total:,}", end="\r")
    if batch:
        out.executemany("INSERT OR REPLACE INTO market_txns VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
    out.commit()
    print(f"   {n:,}/{total:,}  done")

    # carry forward the treasury identity from the source meta
    smeta = {row["k"]: row["v"] for row in src.execute("SELECT k, v FROM meta")} \
        if src.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meta'").fetchone() else {}
    rng = out.execute("SELECT MIN(ts) a, MAX(ts) b FROM market_txns").fetchone()
    meta = {
        "generated_at": str(int(time.time())),
        "source": os.path.basename(src_path),
        "rows": str(n),
        "ts_min": str(rng[0] or 0),
        "ts_max": str(rng[1] or 0),
        "kins_mint": smeta.get("kins_mint", ""),
        "treasury_owner": smeta.get("treasury_owner", ""),
        "price_resolution": price_note,   # how finely each txn was priced (1m where reachable)
    }
    out.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", list(meta.items()))
    out.commit()
    out.execute("VACUUM")

    print("\n== category summary (gross KINS / USD value) ==")
    for row in out.execute(
        """SELECT category, COUNT(*) c, ROUND(SUM(gross_kins),1) g,
                  ROUND(SUM(to_treasury),1) t, ROUND(SUM(burned_kins),1) b,
                  ROUND(SUM(usd_value),0) usd
           FROM market_txns GROUP BY category ORDER BY g DESC"""):
        usd = row['usd'] if row['usd'] is not None else 0
        print(f"  {row['category']:12} n={row['c']:>6,}  grossKINS={row['g']:>16,.1f}  "
              f"treasury={row['t']:>13,.1f}  burned={row['b']:>13,.1f}  USD=${usd:>12,.0f}")
    sz = os.path.getsize(out_path) / 1e6
    print(f"\n== wrote {out_path}  ({sz:.1f} MB)")
    src.close()
    out.close()


def main():
    ap = argparse.ArgumentParser(description="Distill treasury download -> compact market.db")
    ap.add_argument("--src", default="market_index.db", help="raw download (default market_index.db)")
    ap.add_argument("--out", default="market.db", help="compact output (default market.db)")
    distill(*vars(ap.parse_args()).values())


if __name__ == "__main__":
    main()
