#!/usr/bin/env python3
"""
Check unclaimed pump.fun creator rewards for one or more wallets.

Only PUBLIC wallet addresses are needed (no private keys) — rewards are SOL
held in on-chain "creator vault" PDAs derived from the creator's pubkey.

  pip install solders requests
  python check_pump_rewards.py wallets.txt          # a text file, one address per line
  python check_pump_rewards.py <WALLET_PUBKEY> ...   # or addresses directly
  # or pipe:  cat wallets.txt | python check_pump_rewards.py

Text file format: one wallet address per line. Blank lines and lines starting
with '#' are ignored, so you can add comments.

Lookups are batched (getMultipleAccounts, 100 addrs/call) with retry/backoff,
so hundreds of wallets take only a handful of requests. Tune with env vars:
  RPC=https://your-helius-or-quicknode-url   # better endpoint, avoids rate limits
  BATCH=100                                  # addresses per request
  SLEEP=0.25                                 # seconds between requests
"""
import os, sys, time, base64, struct, requests
from solders.pubkey import Pubkey

RPC = os.environ.get("RPC", "https://api.mainnet-beta.solana.com")
BATCH = int(os.environ.get("BATCH", "100"))
SLEEP = float(os.environ.get("SLEEP", "0.25"))
PUMP   = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")  # bonding curve
PSWAP  = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")   # PumpSwap AMM
WSOL   = Pubkey.from_string("So11111111111111111111111111111111111111112")
ATA_PROG = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
TOKEN_PROG = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
LAMPORTS = 1_000_000_000

def rpc(method, params, retries=6):
    """POST with exponential backoff on rate limits / transient errors."""
    delay = 0.5
    for attempt in range(retries):
        try:
            r = requests.post(RPC, json={"jsonrpc":"2.0","id":1,"method":method,
                                         "params":params}, timeout=30)
            if r.status_code == 429:
                raise RuntimeError("429 rate limited")
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(j["error"])
            return j["result"]
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            print(f"    (retry {attempt+1}/{retries} after {wait:.1f}s: {e})", file=sys.stderr)
            time.sleep(wait)

def get_multiple_accounts(pubkeys):
    """Return list of account dicts (or None) for the given pubkeys, batched."""
    out = []
    n = len(pubkeys)
    for i in range(0, n, BATCH):
        chunk = pubkeys[i:i+BATCH]
        res = rpc("getMultipleAccounts",
                  [[str(p) for p in chunk], {"encoding": "base64"}])
        out.extend(res["value"])
        done = min(i+BATCH, n)
        print(f"  ...fetched {done}/{n}", file=sys.stderr)
        if done < n:
            time.sleep(SLEEP)
    return out

def lamports_of(acc):
    return acc["lamports"] if acc else 0

def token_amount_of(acc):
    """Parse SPL token account balance (u64 at byte offset 64) from base64 data."""
    if not acc:
        return 0
    raw = base64.b64decode(acc["data"][0])
    if len(raw) < 72:
        return 0
    return struct.unpack_from("<Q", raw, 64)[0]

def collect_addresses(args):
    """Each arg is either a wallet address or a path to a text file of addresses
    (one per line; blank lines and '#' comments ignored)."""
    out = []
    for a in args:
        if os.path.isfile(a):
            with open(a) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        out.append(line)
        else:
            out.append(a)
    return out

def main(wallets):
    # Parse + dedupe addresses, derive both vault addresses for each.
    valid = []
    seen = set()
    for w in wallets:
        try:
            creator = Pubkey.from_string(w)
        except Exception:
            print(f"! skip invalid address: {w}"); continue
        if w in seen:
            continue
        seen.add(w)
        bc_vault, _ = Pubkey.find_program_address([b"creator-vault", bytes(creator)], PUMP)
        ps_auth, _  = Pubkey.find_program_address([b"creator_vault", bytes(creator)], PSWAP)
        ps_ata, _   = Pubkey.find_program_address(
            [bytes(ps_auth), bytes(TOKEN_PROG), bytes(WSOL)], ATA_PROG)
        valid.append((w, bc_vault, ps_ata))

    n = len(valid)
    if not n:
        print("No valid addresses."); return
    print(f"Checking {n} wallet(s) via {RPC} ...\n", file=sys.stderr)

    # One batched fetch for all bonding-curve vaults, one for all pumpswap ATAs.
    bc_accs = get_multiple_accounts([v[1] for v in valid])
    ps_accs = get_multiple_accounts([v[2] for v in valid])

    grand = 0.0
    funded = []
    for (w, bc_vault, ps_ata), bc_acc, ps_acc in zip(valid, bc_accs, ps_accs):
        bc = lamports_of(bc_acc) / LAMPORTS
        ps = token_amount_of(ps_acc) / LAMPORTS
        total = bc + ps
        grand += total
        flag = "  <-- HAS FUNDS" if total > 0.001 else ""
        print(f"{w}{flag}")
        print(f"    bonding-curve vault {bc_vault}: {bc:.6f} SOL")
        print(f"    pumpswap vault      {ps_ata}: {ps:.6f} SOL")
        if total > 0.001:
            funded.append((w, total))

    print(f"\n=== total across {n} wallet(s): {grand:.6f} SOL ===")
    if funded:
        print(f"\n{len(funded)} wallet(s) with funds:")
        for w, t in sorted(funded, key=lambda x: -x[1]):
            print(f"  {t:.6f} SOL  {w}")
    else:
        print("No wallets with claimable balances found.")

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args and not sys.stdin.isatty():
        args = [l.strip() for l in sys.stdin if l.strip()]
    if not args:
        print(__doc__); sys.exit(1)
    main(collect_addresses(args))
