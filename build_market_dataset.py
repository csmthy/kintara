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

KINS pumped ~300x over the captured month, so a transaction's USD value depends heavily on
*which day* it happened — we price each txn at that day's KINS/USD close (GeckoTerminal
daily candles via kintara_tracker.fetch_kins_ohlcv), with nearest-prior carry-forward for
any gap day. Pricing everything at today's rate would overstate early volume by orders of
magnitude.

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
    kins_usd     REAL,      -- KINS/USD close used for this day
    usd_value    REAL       -- gross_kins * kins_usd  (economic size in USD at the time)
);
CREATE INDEX IF NOT EXISTS idx_mt_ts     ON market_txns(ts);
CREATE INDEX IF NOT EXISTS idx_mt_date   ON market_txns(date);
CREATE INDEX IF NOT EXISTS idx_mt_cat    ON market_txns(category);
CREATE INDEX IF NOT EXISTS idx_mt_buyer  ON market_txns(buyer);
CREATE INDEX IF NOT EXISTS idx_mt_seller ON market_txns(seller);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


def kins_usd_by_date():
    """{ 'YYYY-MM-DD' (UTC): KINS close USD } from GeckoTerminal daily candles.
    Reuses kintara_tracker.fetch_kins_ohlcv so the source matches the live site."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import kintara_tracker as kt
    rows = kt.fetch_kins_ohlcv("day", 1, 1000)
    out = {}
    for ts, px in rows:
        out[dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")] = px
    return out


def price_for(date, pmap, sorted_dates):
    """KINS/USD for a date, nearest-prior carry-forward (then nearest-next) if missing."""
    if date in pmap:
        return pmap[date]
    prior = [d for d in sorted_dates if d <= date]
    if prior:
        return pmap[prior[-1]]
    return pmap[sorted_dates[0]] if sorted_dates else None


def distill(src_path, out_path):
    if not os.path.exists(src_path):
        sys.exit(f"source not found: {src_path}")
    print(f"== pricing: fetching KINS daily closes …")
    pmap = kins_usd_by_date()
    sorted_dates = sorted(pmap)
    print(f"   {len(pmap)} daily prices  ({sorted_dates[0]} … {sorted_dates[-1]})")

    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row
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
        kins_usd = price_for(date, pmap, sorted_dates) if date else None
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
        "price_dates": f"{sorted_dates[0]}..{sorted_dates[-1]}" if sorted_dates else "",
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
