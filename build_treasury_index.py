#!/usr/bin/env python3
"""
Build the Kintara treasury KINS ledger into a local SQLite DB.

This is the base on-chain dataset for KinScan's player/account economics. It indexes
every KINS balance-changing transaction seen on the treasury's KINS token account(s),
classifies the shape of the flow, and also writes a clean `market_txns` subset for
transactions that match the marketplace fee pattern.

Examples:
  RPC=https://your-helius-or-quicknode-url python build_treasury_index.py
  python build_treasury_index.py --probe
  python build_treasury_index.py --summary
  python build_treasury_index.py --wallet <PLAYER_WALLET>
  python build_treasury_index.py --kinds

Env:
  RPC / SOLANA_RPC     Solana JSON-RPC URL. Use an archival Helius/QuickNode URL
                       for full history; public RPC prunes and rate-limits.
  MARKET_INDEX_DB      output DB path (default market_index.db)
  KINS_MINT            override the auto-resolved KINS mint
  KINS_TREASURY        treasury owner wallet (default identified marketplace fee wallet)
  HELIUS_HISTORY       auto/1/0. auto uses getTransactionsForAddress on Helius URLs
                       and falls back to normal RPC if unavailable.
  HELIUS_LIMIT         getTransactionsForAddress page size (default 1000)
  BATCH                getTransaction calls per JSON-RPC batch POST (default 1;
                       set >1 only if your RPC allows batch POSTs; oversized or
                       rejected batches are split automatically)
  TX_WORKERS           parallel single-transaction workers for fallback mode (default 1)
  TX_CHUNK             signatures processed before each DB commit (default 50)
  RPC_RPS              max JSON-RPC requests/second across all workers (default 5;
                       auto-reduces on 429)
  RPC_RPS_MAX          highest adaptive request rate (default 8)
  RPC_RPS_MIN          lowest auto-reduced request rate (default 0.5)
  RPC_RPS_STEP         additive increase after clean chunks (default 0.5)
  RPC_RPS_UP_EVERY     clean chunks before one increase (default 4)
  RPC_429_BACKOFF      shared cooldown seconds after a 429 (default 20)
  SLEEP                seconds between pages/chunks (default 0.02)
  MAX_PAGES            optional safety cap for this run; unset = all possible
  STORE_TX_JSON        set 1 to store compact parsed transactions for later research
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation

import requests

RPC = os.environ.get("RPC") or os.environ.get("SOLANA_RPC") or "https://api.mainnet-beta.solana.com"
DB_PATH = os.environ.get("MARKET_INDEX_DB", "market_index.db")
WSOL = "So11111111111111111111111111111111111111112"
GECKO_POOL = "F42tZnKPavq1VUcrL6ymhc6YqVpt84fWwgzbNTv2wb3W"
TREASURY = os.environ.get("KINS_TREASURY", "4zW4zuZb9rXpvw3cTYyGoQ2iHTtG9E17YpdeNUbwuQVt").strip()
HELIUS_HISTORY = os.environ.get("HELIUS_HISTORY", "auto").strip().lower()
HELIUS_LIMIT = int(os.environ.get("HELIUS_LIMIT", "1000"))
BATCH = int(os.environ.get("BATCH", "1"))
TX_WORKERS = int(os.environ.get("TX_WORKERS", "1"))
TX_CHUNK = int(os.environ.get("TX_CHUNK", "50"))
RPC_RPS = float(os.environ.get("RPC_RPS", "5"))
RPC_RPS_MAX = float(os.environ.get("RPC_RPS_MAX", "8"))
RPC_RPS_MIN = float(os.environ.get("RPC_RPS_MIN", "0.5"))
RPC_RPS_STEP = float(os.environ.get("RPC_RPS_STEP", "0.5"))
RPC_RPS_UP_EVERY = int(os.environ.get("RPC_RPS_UP_EVERY", "4"))
RPC_429_BACKOFF = float(os.environ.get("RPC_429_BACKOFF", "20"))
SLEEP = float(os.environ.get("SLEEP", "0.02"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "0") or "0")
STORE_TX_JSON = os.environ.get("STORE_TX_JSON", "0") == "1"
UA = {"User-Agent": "kinscan-treasury-indexer/1.2"}
EPS = Decimal("0.000000001")


class PayloadTooLarge(RuntimeError):
    pass


class BatchRejected(RuntimeError):
    pass


class HeliusHistoryUnavailable(RuntimeError):
    pass


_rpc_pace_lock = threading.Lock()
_rpc_next_at = [0.0]
_rpc_rps_current = [RPC_RPS]
_rpc_cooldown_until = [0.0]
_rpc_429_count = [0]
_rpc_clean_chunks = [0]


def masked_rpc_url():
    return re.sub(r"api-key=[^&\s]+", "api-key=***", RPC)


def pace_rpc():
    if _rpc_rps_current[0] <= 0:
        return
    with _rpc_pace_lock:
        now = time.monotonic()
        gap = 1.0 / max(_rpc_rps_current[0], RPC_RPS_MIN)
        wait = max(_rpc_next_at[0], _rpc_cooldown_until[0]) - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _rpc_next_at[0] = max(_rpc_next_at[0], now) + gap


def note_429():
    with _rpc_pace_lock:
        old = _rpc_rps_current[0]
        if old > 0:
            _rpc_rps_current[0] = max(RPC_RPS_MIN, old * 0.75)
        _rpc_cooldown_until[0] = max(_rpc_cooldown_until[0], time.monotonic() + RPC_429_BACKOFF)
        _rpc_429_count[0] += 1
        _rpc_clean_chunks[0] = 0
        print(
            f"    429 rate limited; cooling {RPC_429_BACKOFF:g}s, RPC_RPS {old:g}->{_rpc_rps_current[0]:g}",
            file=sys.stderr,
        )


def note_clean_chunk():
    if RPC_RPS_STEP <= 0 or RPC_RPS_UP_EVERY <= 0:
        return
    with _rpc_pace_lock:
        if _rpc_rps_current[0] >= RPC_RPS_MAX:
            return
        _rpc_clean_chunks[0] += 1
        if _rpc_clean_chunks[0] < RPC_RPS_UP_EVERY:
            return
        old = _rpc_rps_current[0]
        _rpc_rps_current[0] = min(RPC_RPS_MAX, old + RPC_RPS_STEP)
        _rpc_clean_chunks[0] = 0
        print(f"    clean chunks; RPC_RPS {old:g}->{_rpc_rps_current[0]:g}", file=sys.stderr)


def rpc(method, params, retries=7):
    delay = 0.5
    for attempt in range(retries):
        try:
            pace_rpc()
            r = requests.post(
                RPC,
                headers=UA,
                timeout=40,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            )
            if r.status_code == 429:
                note_429()
                raise RuntimeError("429 rate limited")
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(str(j["error"])[:180])
            return j.get("result")
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            print(f"    retry {attempt + 1}/{retries} in {wait:.1f}s: {e}", file=sys.stderr)
            time.sleep(wait)


def rpc_batch(calls, retries=7):
    body = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]
    delay = 0.5
    for attempt in range(retries):
        try:
            pace_rpc()
            r = requests.post(RPC, headers=UA, timeout=90, json=body)
            if r.status_code == 413:
                raise PayloadTooLarge(f"413 payload too large for {len(calls)} call(s)")
            if r.status_code == 403:
                raise BatchRejected(f"403 batch rejected for {len(calls)} call(s)")
            if r.status_code == 429:
                note_429()
                raise RuntimeError("429 rate limited")
            r.raise_for_status()
            raw = r.json()
            if isinstance(raw, dict):
                if "error" in raw:
                    raise RuntimeError(str(raw["error"])[:180])
                raw = [raw]
            out = [None] * len(calls)
            for item in raw:
                if not isinstance(item, dict) or not isinstance(item.get("id"), int):
                    continue
                if "result" in item:
                    out[item["id"]] = item["result"]
            return out
        except PayloadTooLarge:
            raise
        except BatchRejected:
            raise
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            print(f"    batch retry {attempt + 1}/{retries} in {wait:.1f}s: {e}", file=sys.stderr)
            time.sleep(wait)


def get_transactions(calls):
    """Fetch getTransaction calls. Single calls use normal JSON-RPC objects; batches
    are optional and split on provider-specific 413/403 responses."""
    if not calls:
        return []
    if len(calls) == 1:
        method, params = calls[0]
        return [rpc(method, params)]
    try:
        return rpc_batch(calls)
    except (PayloadTooLarge, BatchRejected) as e:
        mid = max(1, len(calls) // 2)
        print(f"    {e}; splitting batch {len(calls)} -> {mid}+{len(calls)-mid}", file=sys.stderr)
        return get_transactions(calls[:mid]) + get_transactions(calls[mid:])


def use_helius_history():
    if HELIUS_HISTORY in ("1", "true", "yes", "on"):
        return True
    if HELIUS_HISTORY in ("0", "false", "no", "off"):
        return False
    return "helius" in RPC.lower()


def helius_history_page(address, pagination_token=None):
    opts = {
        "transactionDetails": "full",
        "sortOrder": "desc",
        "limit": max(1, min(1000, HELIUS_LIMIT)),
        "commitment": "finalized",
        "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
        "filters": {"status": "succeeded"},
    }
    if pagination_token:
        opts["paginationToken"] = pagination_token
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransactionsForAddress",
        "params": [address, opts],
    }
    try:
        r = requests.post(RPC, headers=UA, timeout=120, json=body)
        if r.status_code in (400, 401, 403, 404):
            msg = r.text[:240].replace("\n", " ")
            raise HeliusHistoryUnavailable(f"getTransactionsForAddress unavailable ({r.status_code}): {msg}")
        if r.status_code == 429:
            note_429()
            raise RuntimeError("429 rate limited")
        r.raise_for_status()
        j = r.json()
    except HeliusHistoryUnavailable:
        raise
    except Exception as e:
        raise RuntimeError(f"getTransactionsForAddress error: {e}") from e
    if "error" in j:
        err = str(j["error"])[:240]
        if "not found" in err.lower() or "forbidden" in err.lower() or "unauthorized" in err.lower():
            raise HeliusHistoryUnavailable(f"getTransactionsForAddress unavailable: {err}")
        raise RuntimeError(f"getTransactionsForAddress error: {err}")
    result = j.get("result") or {}
    return result.get("data") or [], result.get("paginationToken")


def resolve_mint():
    if os.environ.get("KINS_MINT"):
        return os.environ["KINS_MINT"].strip()
    r = requests.get(
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{GECKO_POOL}",
        headers=UA,
        timeout=30,
    )
    r.raise_for_status()
    rel = ((r.json() or {}).get("data") or {}).get("relationships") or {}
    for key in ("base_token", "quote_token"):
        tid = (((rel.get(key) or {}).get("data") or {}).get("id") or "")
        if tid.startswith("solana_"):
            addr = tid[len("solana_"):]
            if addr and addr != WSOL:
                return addr
    raise SystemExit("could not resolve KINS mint; set KINS_MINT env")


def connect(path=DB_PATH):
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute(
        """CREATE TABLE IF NOT EXISTS treasury_txns(
            sig TEXT PRIMARY KEY,
            slot INTEGER,
            ts INTEGER,
            kind TEXT,
            confidence REAL,
            primary_wallet TEXT,
            buyer TEXT,
            seller TEXT,
            gross_kins REAL,
            seller_net_kins REAL,
            fee_kins REAL,
            fee_bps REAL,
            treasury_delta_kins REAL,
            payer_count INTEGER,
            receiver_count INTEGER,
            counterparty_count INTEGER,
            treasury_owner TEXT,
            treasury_token_account TEXT,
            programs_json TEXT,
            instruction_types_json TEXT,
            deltas_json TEXT,
            tx_json TEXT,
            parsed_at INTEGER
        )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS market_txns(
            sig TEXT PRIMARY KEY,
            slot INTEGER,
            ts INTEGER,
            buyer TEXT,
            seller TEXT,
            gross_kins REAL,
            seller_net_kins REAL,
            fee_kins REAL,
            fee_bps REAL,
            treasury_owner TEXT,
            treasury_token_account TEXT,
            deltas_json TEXT,
            parsed_at INTEGER
        )"""
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_tt_kind_ts ON treasury_txns(kind, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tt_primary_ts ON treasury_txns(primary_wallet, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tt_buyer_ts ON treasury_txns(buyer, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tt_seller_ts ON treasury_txns(seller, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tt_ts ON treasury_txns(ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tt_treasury_acct ON treasury_txns(treasury_token_account)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_mt_buyer_ts ON market_txns(buyer, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_mt_seller_ts ON market_txns(seller, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_mt_ts ON market_txns(ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_mt_treasury_acct ON market_txns(treasury_token_account)")
    con.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
    con.commit()
    return con


def meta_get(con, key, default=None):
    row = con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return row["v"] if row else default


def meta_set(con, key, value):
    con.execute(
        "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, str(value)),
    )


def ui_amount(balance):
    amt = balance.get("uiTokenAmount") or {}
    raw = amt.get("uiAmountString")
    if raw is None:
        raw = amt.get("uiAmount")
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def kins_owner_deltas(tx, mint):
    meta = (tx or {}).get("meta") or {}
    pre, post = {}, {}
    for b in meta.get("preTokenBalances") or []:
        if b.get("mint") == mint and b.get("owner"):
            pre[b["owner"]] = pre.get(b["owner"], Decimal("0")) + ui_amount(b)
    for b in meta.get("postTokenBalances") or []:
        if b.get("mint") == mint and b.get("owner"):
            post[b["owner"]] = post.get(b["owner"], Decimal("0")) + ui_amount(b)
    return {o: post.get(o, Decimal("0")) - pre.get(o, Decimal("0")) for o in set(pre) | set(post)}


def compact_json(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def walk_instructions(tx):
    msg = (((tx or {}).get("transaction") or {}).get("message") or {})
    meta = (tx or {}).get("meta") or {}
    ins = list(msg.get("instructions") or [])
    for inner in meta.get("innerInstructions") or []:
        ins.extend(inner.get("instructions") or [])
    return ins


def tx_programs(tx):
    programs, types = set(), set()
    for ins in walk_instructions(tx):
        pid = ins.get("programId") or ins.get("program")
        if pid:
            programs.add(str(pid))
        parsed = ins.get("parsed")
        if isinstance(parsed, dict):
            typ = parsed.get("type")
            if typ:
                types.add(str(typ))
        elif ins.get("type"):
            types.add(str(ins["type"]))
    return sorted(programs), sorted(types)


def classify_flow(deltas, treasury_owner):
    treasury_delta = deltas.get(treasury_owner, Decimal("0"))
    others = {o: d for o, d in deltas.items() if o != treasury_owner and abs(d) > EPS}
    payers = sorted([(o, d) for o, d in others.items() if d < -EPS], key=lambda x: x[1])
    receivers = sorted([(o, d) for o, d in others.items() if d > EPS], key=lambda x: x[1], reverse=True)
    buyer = seller = primary = None
    gross = seller_net = fee = fee_bps = None
    confidence = 0.6

    if treasury_delta > EPS and payers and receivers:
        buyer, buyer_delta = payers[0]
        seller, seller_delta = receivers[0]
        gross_d = -buyer_delta
        if gross_d > EPS:
            fee_d = treasury_delta
            bps = (fee_d / gross_d) * Decimal("10000")
            if Decimal("10") <= bps <= Decimal("2500") and fee_d < gross_d:
                return {
                    "kind": "marketplace_trade",
                    "confidence": 0.95 if Decimal("450") <= bps <= Decimal("550") else 0.8,
                    "primary_wallet": buyer,
                    "buyer": buyer,
                    "seller": seller,
                    "gross_kins": gross_d,
                    "seller_net_kins": seller_delta,
                    "fee_kins": fee_d,
                    "fee_bps": bps,
                    "treasury_delta_kins": treasury_delta,
                    "payer_count": len(payers),
                    "receiver_count": len(receivers),
                    "counterparty_count": len(others),
                }
        return {
            "kind": "treasury_income_with_receivers",
            "confidence": 0.45,
            "primary_wallet": payers[0][0],
            "buyer": payers[0][0],
            "seller": receivers[0][0],
            "gross_kins": -payers[0][1],
            "seller_net_kins": receivers[0][1],
            "fee_kins": treasury_delta,
            "fee_bps": None,
            "treasury_delta_kins": treasury_delta,
            "payer_count": len(payers),
            "receiver_count": len(receivers),
            "counterparty_count": len(others),
        }

    if treasury_delta > EPS and payers:
        primary = payers[0][0]
        gross = -payers[0][1]
        # This bucket should contain direct KINS sinks: spin wheel, memberships,
        # paid game actions, or treasury sales that do not pay another player.
        return {
            "kind": "treasury_income",
            "confidence": confidence,
            "primary_wallet": primary,
            "buyer": primary,
            "seller": None,
            "gross_kins": gross,
            "seller_net_kins": None,
            "fee_kins": treasury_delta,
            "fee_bps": None,
            "treasury_delta_kins": treasury_delta,
            "payer_count": len(payers),
            "receiver_count": len(receivers),
            "counterparty_count": len(others),
        }

    if treasury_delta < -EPS and receivers:
        primary = receivers[0][0]
        return {
            "kind": "treasury_payout",
            "confidence": confidence,
            "primary_wallet": primary,
            "buyer": None,
            "seller": primary,
            "gross_kins": abs(treasury_delta),
            "seller_net_kins": receivers[0][1],
            "fee_kins": None,
            "fee_bps": None,
            "treasury_delta_kins": treasury_delta,
            "payer_count": len(payers),
            "receiver_count": len(receivers),
            "counterparty_count": len(others),
        }

    if abs(treasury_delta) <= EPS and payers and receivers:
        primary = payers[0][0]
        return {
            "kind": "non_treasury_transfer_seen_by_account",
            "confidence": 0.25,
            "primary_wallet": primary,
            "buyer": primary,
            "seller": receivers[0][0],
            "gross_kins": -payers[0][1],
            "seller_net_kins": receivers[0][1],
            "fee_kins": None,
            "fee_bps": None,
            "treasury_delta_kins": treasury_delta,
            "payer_count": len(payers),
            "receiver_count": len(receivers),
            "counterparty_count": len(others),
        }

    return {
        "kind": "unknown_kins_treasury",
        "confidence": 0.1,
        "primary_wallet": (payers[0][0] if payers else receivers[0][0] if receivers else None),
        "buyer": (payers[0][0] if payers else None),
        "seller": (receivers[0][0] if receivers else None),
        "gross_kins": (-payers[0][1] if payers else abs(treasury_delta) if abs(treasury_delta) > EPS else None),
        "seller_net_kins": (receivers[0][1] if receivers else None),
        "fee_kins": (treasury_delta if treasury_delta > EPS else None),
        "fee_bps": None,
        "treasury_delta_kins": treasury_delta,
        "payer_count": len(payers),
        "receiver_count": len(receivers),
        "counterparty_count": len(others),
    }


def parse_treasury_tx(sig, tx, mint, treasury_owner, treasury_token_account):
    meta = (tx or {}).get("meta") or {}
    if not tx or meta.get("err"):
        return None
    deltas = {o: d for o, d in kins_owner_deltas(tx, mint).items() if abs(d) > EPS}
    if treasury_owner not in deltas:
        return None
    flow = classify_flow(deltas, treasury_owner)
    programs, instruction_types = tx_programs(tx)
    row = {
        "sig": sig,
        "slot": tx.get("slot"),
        "ts": (tx.get("blockTime") or 0) * 1000,
        "treasury_owner": treasury_owner,
        "treasury_token_account": treasury_token_account,
        "programs_json": compact_json(programs),
        "instruction_types_json": compact_json(instruction_types),
        "deltas_json": compact_json({k: float(v) for k, v in sorted(deltas.items())}),
        "tx_json": compact_json(tx) if STORE_TX_JSON else None,
        "parsed_at": int(time.time() * 1000),
    }
    for key, value in flow.items():
        row[key] = float(value) if isinstance(value, Decimal) else value
    return row


def market_row(row):
    if not row or row["kind"] != "marketplace_trade":
        return None
    return {
        "sig": row["sig"],
        "slot": row["slot"],
        "ts": row["ts"],
        "buyer": row["buyer"],
        "seller": row["seller"],
        "gross_kins": row["gross_kins"],
        "seller_net_kins": row["seller_net_kins"],
        "fee_kins": row["fee_kins"],
        "fee_bps": row["fee_bps"],
        "treasury_owner": row["treasury_owner"],
        "treasury_token_account": row["treasury_token_account"],
        "deltas_json": row["deltas_json"],
        "parsed_at": row["parsed_at"],
    }


def tx_signature(tx):
    if not tx:
        return None
    if tx.get("signature"):
        return tx["signature"]
    trx = tx.get("transaction") or {}
    sigs = trx.get("signatures")
    if sigs:
        return sigs[0]
    nested = trx.get("transaction") if isinstance(trx, dict) else None
    if isinstance(nested, dict):
        sigs = nested.get("signatures")
        if sigs:
            return sigs[0]
    return None


def treasury_token_accounts(treasury_owner, mint):
    accs = rpc("getTokenAccountsByOwner", [treasury_owner, {"mint": mint}, {"encoding": "jsonParsed"}])
    return [a["pubkey"] for a in ((accs or {}).get("value") or [])]


def sig_page(account, before=None, until=None, limit=1000):
    opts = {"limit": limit}
    if before:
        opts["before"] = before
    if until:
        opts["until"] = until
    return rpc("getSignaturesForAddress", [account, opts]) or []


def insert_treasury_rows(con, rows):
    if not rows:
        return 0
    before = con.total_changes
    con.executemany(
        """INSERT OR IGNORE INTO treasury_txns(
            sig,slot,ts,kind,confidence,primary_wallet,buyer,seller,gross_kins,
            seller_net_kins,fee_kins,fee_bps,treasury_delta_kins,payer_count,
            receiver_count,counterparty_count,treasury_owner,treasury_token_account,
            programs_json,instruction_types_json,deltas_json,tx_json,parsed_at
        ) VALUES(
            :sig,:slot,:ts,:kind,:confidence,:primary_wallet,:buyer,:seller,:gross_kins,
            :seller_net_kins,:fee_kins,:fee_bps,:treasury_delta_kins,:payer_count,
            :receiver_count,:counterparty_count,:treasury_owner,:treasury_token_account,
            :programs_json,:instruction_types_json,:deltas_json,:tx_json,:parsed_at
        )""",
        rows,
    )
    return con.total_changes - before


def insert_market_rows(con, rows):
    rows = [r for r in rows if r]
    if not rows:
        return 0
    before = con.total_changes
    con.executemany(
        """INSERT OR IGNORE INTO market_txns(
            sig,slot,ts,buyer,seller,gross_kins,seller_net_kins,fee_kins,fee_bps,
            treasury_owner,treasury_token_account,deltas_json,parsed_at
        ) VALUES(
            :sig,:slot,:ts,:buyer,:seller,:gross_kins,:seller_net_kins,:fee_kins,:fee_bps,
            :treasury_owner,:treasury_token_account,:deltas_json,:parsed_at
        )""",
        rows,
    )
    return con.total_changes - before


def process_full_txns(con, txns, mint, treasury_owner, treasury_token_account, stop_at_sig=None):
    ledger_rows = []
    market_rows = []
    hit_stop = False
    for tx in txns:
        sig = tx_signature(tx)
        if not sig:
            continue
        if stop_at_sig and sig == stop_at_sig:
            hit_stop = True
            break
        row = parse_treasury_tx(sig, tx, mint, treasury_owner, treasury_token_account)
        if row:
            ledger_rows.append(row)
            market_rows.append(market_row(row))
    ledger = insert_treasury_rows(con, ledger_rows)
    market = insert_market_rows(con, market_rows)
    con.commit()
    return len(ledger_rows), ledger, market, hit_stop


def fetch_and_parse_sig(sig_row, mint, treasury_owner, treasury_token_account):
    sig = sig_row["signature"]
    tx = rpc(
        "getTransaction",
        [sig, {"maxSupportedTransactionVersion": 0, "encoding": "jsonParsed"}],
    )
    row = parse_treasury_tx(sig, tx, mint, treasury_owner, treasury_token_account)
    return row


def process_sigs(con, sigs, mint, treasury_owner, treasury_token_account):
    added_ledger = 0
    added_market = 0
    parsed = 0
    chunk_size = max(1, TX_CHUNK if BATCH <= 1 else BATCH)
    for i in range(0, len(sigs), chunk_size):
        chunk = [s for s in sigs[i : i + chunk_size] if not s.get("err")]
        chunk_429_start = _rpc_429_count[0]
        ledger_rows = []
        market_rows = []
        if BATCH <= 1:
            if TX_WORKERS > 1 and len(chunk) > 1:
                with ThreadPoolExecutor(max_workers=max(1, TX_WORKERS)) as pool:
                    futs = [
                        pool.submit(fetch_and_parse_sig, s, mint, treasury_owner, treasury_token_account)
                        for s in chunk
                    ]
                    for fut in as_completed(futs):
                        row = fut.result()
                        if row:
                            ledger_rows.append(row)
                            market_rows.append(market_row(row))
            else:
                for s in chunk:
                    row = fetch_and_parse_sig(s, mint, treasury_owner, treasury_token_account)
                    if row:
                        ledger_rows.append(row)
                        market_rows.append(market_row(row))
        else:
            calls = [
                (
                    "getTransaction",
                    [
                        s["signature"],
                        {"maxSupportedTransactionVersion": 0, "encoding": "jsonParsed"},
                    ],
                )
                for s in chunk
            ]
            results = get_transactions(calls)
            for s, tx in zip(chunk, results):
                row = parse_treasury_tx(s["signature"], tx, mint, treasury_owner, treasury_token_account)
                if row:
                    ledger_rows.append(row)
                    market_rows.append(market_row(row))
        parsed += len(ledger_rows)
        added_ledger += insert_treasury_rows(con, ledger_rows)
        added_market += insert_market_rows(con, market_rows)
        con.commit()
        if _rpc_429_count[0] == chunk_429_start:
            note_clean_chunk()
        done = min(i + len(chunk), len(sigs))
        print(
            f"  processed {done}/{len(sigs)} sigs, +{added_ledger} ledger, +{added_market} market"
            f" · rps {_rpc_rps_current[0]:.2f} · 429s {_rpc_429_count[0]}",
            end="\r",
            flush=True,
        )
        if SLEEP:
            time.sleep(SLEEP)
    print()
    return parsed, added_ledger, added_market


def pull_forward_helius(con, account, mint):
    newest = meta_get(con, f"{account}:gta_newest_sig")
    if not newest:
        return 0, 0
    total_ledger = total_market = 0
    cursor = None
    pages = 0
    while True:
        data, cursor = helius_history_page(account, cursor)
        pages += 1
        if not data:
            break
        parsed, ledger, market, hit_stop = process_full_txns(con, data, mint, TREASURY, account, stop_at_sig=newest)
        total_ledger += ledger
        total_market += market
        first_sig = tx_signature(data[0]) if data else None
        if first_sig and first_sig != newest:
            meta_set(con, f"{account}:gta_newest_sig", first_sig)
        print(f"forward-gTFA {account[:6]}... page {pages}: {len(data)} txns, +{ledger} ledger, +{market} market")
        con.commit()
        if hit_stop or not cursor or (MAX_PAGES and pages >= MAX_PAGES):
            break
        if SLEEP:
            time.sleep(SLEEP)
    return total_ledger, total_market


def backfill_helius(con, account, mint):
    if meta_get(con, f"{account}:gta_done") == "1":
        return 0, 0
    cursor = meta_get(con, f"{account}:gta_cursor")
    total_ledger = total_market = 0
    pages = 0
    while True:
        data, next_cursor = helius_history_page(account, cursor)
        pages += 1
        if not data:
            meta_set(con, f"{account}:gta_done", "1")
            con.commit()
            break
        if not meta_get(con, f"{account}:gta_newest_sig"):
            sig = tx_signature(data[0])
            if sig:
                meta_set(con, f"{account}:gta_newest_sig", sig)
        parsed, ledger, market, _hit_stop = process_full_txns(con, data, mint, TREASURY, account)
        total_ledger += ledger
        total_market += market
        print(f"backfill-gTFA {account[:6]}... page {pages}: {len(data)} txns, +{ledger} ledger, +{market} market")
        cursor = next_cursor
        if cursor:
            meta_set(con, f"{account}:gta_cursor", cursor)
        else:
            meta_set(con, f"{account}:gta_done", "1")
        con.commit()
        if not cursor or (MAX_PAGES and pages >= MAX_PAGES):
            break
        if SLEEP:
            time.sleep(SLEEP)
    return total_ledger, total_market


def pull_forward(con, account, mint):
    newest = meta_get(con, f"{account}:newest_sig")
    if not newest:
        return 0, 0
    total_ledger = total_market = 0
    before = None
    pages = 0
    while True:
        batch = sig_page(account, before=before, until=newest)
        pages += 1
        if not batch:
            break
        print(f"forward {account[:6]}...: {len(batch)} signatures")
        _, ledger, market = process_sigs(con, batch, mint, TREASURY, account)
        total_ledger += ledger
        total_market += market
        meta_set(con, f"{account}:newest_sig", batch[0]["signature"])
        con.commit()
        if len(batch) < 1000 or (MAX_PAGES and pages >= MAX_PAGES):
            break
        before = batch[-1]["signature"]
    return total_ledger, total_market


def backfill(con, account, mint):
    if meta_get(con, f"{account}:done") == "1":
        return 0, 0
    before = meta_get(con, f"{account}:oldest_sig")
    total_ledger = total_market = 0
    pages = 0
    while True:
        batch = sig_page(account, before=before)
        pages += 1
        if not batch:
            meta_set(con, f"{account}:done", "1")
            con.commit()
            break
        if not meta_get(con, f"{account}:newest_sig"):
            meta_set(con, f"{account}:newest_sig", batch[0]["signature"])
        print(f"backfill {account[:6]}...: {before or 'head'} -> {batch[-1]['signature'][:8]}... ({len(batch)})")
        _, ledger, market = process_sigs(con, batch, mint, TREASURY, account)
        total_ledger += ledger
        total_market += market
        before = batch[-1]["signature"]
        meta_set(con, f"{account}:oldest_sig", before)
        if len(batch) < 1000:
            meta_set(con, f"{account}:done", "1")
        con.commit()
        if len(batch) < 1000 or (MAX_PAGES and pages >= MAX_PAGES):
            break
        if SLEEP:
            time.sleep(SLEEP)
    return total_ledger, total_market


def print_summary(con):
    ledger = con.execute(
        """SELECT COUNT(*) c, MIN(ts) min_ts, MAX(ts) max_ts,
                  SUM(CASE WHEN treasury_delta_kins > 0 THEN treasury_delta_kins ELSE 0 END) income,
                  SUM(CASE WHEN treasury_delta_kins < 0 THEN -treasury_delta_kins ELSE 0 END) outflow
           FROM treasury_txns"""
    ).fetchone()
    market = con.execute(
        "SELECT COUNT(*) c, SUM(gross_kins) gross, SUM(fee_kins) fee FROM market_txns"
    ).fetchone()
    print(f"ledger txns:     {ledger['c'] or 0:,}")
    print(f"market trades:   {market['c'] or 0:,}")
    print(f"treasury in:     {(ledger['income'] or 0):,.2f} KINS")
    print(f"treasury out:    {(ledger['outflow'] or 0):,.2f} KINS")
    print(f"market gross:    {(market['gross'] or 0):,.2f} KINS")
    print(f"market fees:     {(market['fee'] or 0):,.2f} KINS")
    if ledger["min_ts"]:
        lo = time.strftime("%Y-%m-%d", time.gmtime(ledger["min_ts"] / 1000))
        hi = time.strftime("%Y-%m-%d", time.gmtime(ledger["max_ts"] / 1000))
        print(f"span:            {lo} -> {hi}")


def print_kinds(con):
    rows = con.execute(
        """SELECT kind, COUNT(*) c, SUM(treasury_delta_kins) treasury_net,
                  SUM(gross_kins) gross, AVG(fee_bps) avg_fee_bps
           FROM treasury_txns
           GROUP BY kind
           ORDER BY c DESC"""
    ).fetchall()
    if not rows:
        print("no treasury rows indexed yet")
        return
    for r in rows:
        avg = "" if r["avg_fee_bps"] is None else f" avg fee {r['avg_fee_bps']:.1f} bps"
        print(f"{r['kind']:<34} {r['c']:>8,}  net {r['treasury_net'] or 0:>14,.2f}  gross {r['gross'] or 0:>14,.2f}{avg}")


def print_wallet(con, wallet):
    row = con.execute(
        """SELECT
             COUNT(CASE WHEN buyer=? THEN 1 END) buys,
             COUNT(CASE WHEN seller=? THEN 1 END) sells,
             COUNT(CASE WHEN primary_wallet=? AND kind!='marketplace_trade' THEN 1 END) other,
             COALESCE(SUM(CASE WHEN buyer=? THEN gross_kins END),0) spent,
             COALESCE(SUM(CASE WHEN seller=? THEN seller_net_kins END),0) earned,
             COALESCE(SUM(CASE WHEN primary_wallet=? AND kind!='marketplace_trade' THEN gross_kins END),0) other_gross,
             MIN(CASE WHEN buyer=? OR seller=? OR primary_wallet=? THEN ts END) first_ts,
             MAX(CASE WHEN buyer=? OR seller=? OR primary_wallet=? THEN ts END) last_ts
           FROM treasury_txns""",
        (wallet, wallet, wallet, wallet, wallet, wallet, wallet, wallet, wallet, wallet, wallet, wallet),
    ).fetchone()
    print(wallet)
    print(f"  market buys:       {row['buys']:,}")
    print(f"  market sells:      {row['sells']:,}")
    print(f"  other treasury:    {row['other']:,}")
    print(f"  market spent:      {row['spent']:,.2f} KINS")
    print(f"  market earned:     {row['earned']:,.2f} KINS")
    print(f"  other gross:       {row['other_gross']:,.2f} KINS")
    print(f"  market net:        {(row['earned'] - row['spent']):,.2f} KINS")
    if row["first_ts"]:
        lo = time.strftime("%Y-%m-%d", time.gmtime(row["first_ts"] / 1000))
        hi = time.strftime("%Y-%m-%d", time.gmtime(row["last_ts"] / 1000))
        print(f"  span:              {lo} -> {hi}")
    recent = con.execute(
        """SELECT ts,kind,buyer,seller,primary_wallet,gross_kins,seller_net_kins,treasury_delta_kins,sig
           FROM treasury_txns
           WHERE buyer=? OR seller=? OR primary_wallet=?
           ORDER BY ts DESC
           LIMIT 12""",
        (wallet, wallet, wallet),
    ).fetchall()
    for r in recent:
        day = time.strftime("%Y-%m-%d %H:%M", time.gmtime((r["ts"] or 0) / 1000))
        if r["kind"] == "marketplace_trade":
            side = "bought" if r["buyer"] == wallet else "sold"
            amt = r["gross_kins"] if side == "bought" else r["seller_net_kins"]
        else:
            side = r["kind"]
            amt = r["gross_kins"] or abs(r["treasury_delta_kins"] or 0)
        print(f"  {day}  {side:<34} {amt:,.2f} KINS  {r['sig'][:10]}...")


def index():
    mint = resolve_mint()
    con = connect()
    start_ledger = con.execute("SELECT COUNT(*) FROM treasury_txns").fetchone()[0]
    start_market = con.execute("SELECT COUNT(*) FROM market_txns").fetchone()[0]
    meta_set(con, "kins_mint", mint)
    meta_set(con, "treasury_owner", TREASURY)
    con.commit()

    print(f"KINS mint:  {mint}")
    print(f"treasury:   {TREASURY}")
    print(f"RPC:        {masked_rpc_url()}")
    print(f"output:     {DB_PATH}")
    accounts = treasury_token_accounts(TREASURY, mint)
    if not accounts:
        raise SystemExit("treasury has no KINS token accounts")
    print("token accts:")
    for account in accounts:
        print(f"  {account}")
    print(f"\nalready indexed: {start_ledger:,} ledger txns, {start_market:,} market trades\n")

    gta = use_helius_history()
    if gta:
        print("history:    Helius getTransactionsForAddress")
    else:
        print("history:    getSignaturesForAddress + getTransaction")
    print(
        f"fallback:   TX_WORKERS={TX_WORKERS} TX_CHUNK={TX_CHUNK} RPC_RPS={RPC_RPS:g} "
        f"RPC_RPS_MIN={RPC_RPS_MIN:g} RPC_RPS_MAX={RPC_RPS_MAX:g} RPC_RPS_STEP={RPC_RPS_STEP:g} "
        f"RPC_RPS_UP_EVERY={RPC_RPS_UP_EVERY} RPC_429_BACKOFF={RPC_429_BACKOFF:g} BATCH={BATCH}"
    )

    for account in accounts:
        if gta:
            try:
                pull_forward_helius(con, account, mint)
                backfill_helius(con, account, mint)
                print(f"final-gTFA {account[:6]}... catch-up")
                pull_forward_helius(con, account, mint)
                continue
            except HeliusHistoryUnavailable as e:
                print(f"gTFA unavailable, falling back to normal RPC: {e}", file=sys.stderr)
                gta = False
        pull_forward(con, account, mint)
        backfill(con, account, mint)
        print(f"final {account[:6]}... catch-up")
        pull_forward(con, account, mint)

    total_ledger = con.execute("SELECT COUNT(*) FROM treasury_txns").fetchone()[0]
    total_market = con.execute("SELECT COUNT(*) FROM market_txns").fetchone()[0]
    print()
    print_summary(con)
    print(f"added this run:  {total_ledger - start_ledger:,} ledger, {total_market - start_market:,} market")
    con.close()


def probe():
    mint = resolve_mint()
    accounts = treasury_token_accounts(TREASURY, mint)
    print(f"KINS mint:  {mint}")
    print(f"treasury:   {TREASURY}")
    print(f"RPC:        {masked_rpc_url()}")
    if not accounts:
        print("token accts: none")
        return
    print("token accts:")
    for account in accounts:
        page = sig_page(account, limit=1)
        latest = page[0] if page else None
        suffix = f" latest {latest['signature'][:10]}... slot {latest.get('slot')}" if latest else ""
        print(f"  {account}{suffix}")


def main():
    ap = argparse.ArgumentParser(description="Build/query the local Kintara treasury KINS ledger.")
    ap.add_argument("--probe", action="store_true", help="resolve mint + treasury token accounts only")
    ap.add_argument("--summary", action="store_true", help="print DB summary and exit")
    ap.add_argument("--kinds", action="store_true", help="print indexed treasury classification counts")
    ap.add_argument("--wallet", help="query one wallet from the local ledger and exit")
    args = ap.parse_args()
    if args.probe:
        probe()
        return
    con = connect()
    if args.summary:
        print_summary(con)
        return
    if args.kinds:
        print_kinds(con)
        return
    if args.wallet:
        print_wallet(con, args.wallet.strip())
        return
    con.close()
    index()


if __name__ == "__main__":
    main()
