# Player Page — Implementation Brief

> Hand-off doc for the implementer. This describes a new **Player Page** for KinScan: a
> single profile view that aggregates *everything we can know about a Kintara player* from
> public + on-chain sources. Read `README.md` first (it's the source of truth for the app's
> architecture, DB schema, endpoints, and the kintara.gg data we already pull).

## The idea (and why it's the moat)

A visitor types in a **player name** and (optionally) their **Solana wallet**, we **verify**
the link, and we render an extremely fleshed-out profile: marketplace economics, on-chain
activity, in-game public profile, and everything our own DB already knows about them.

This is a huge moat because it fuses three datasets nobody else combines:
1. **Our market-memory DB** — every listing, every detected sale, property ownership, live-world
   presence, seller behavior (we already collect all of this).
2. **Kintara's public profile/API** — whatever the game exposes publicly per player.
3. **On-chain (Solana)** — the part the game UI hides: real KINS/USD spent and earned,
   wheel-spin activity through the treasury, Kintara Club membership, balances/holdings.

It's **uninvasive by design**: every field comes from public marketplace data, public profiles,
or the public blockchain. No private keys, no scraping behind auth.

## Verification flow (design decision — resolve first)

Goal: confidently link a **wallet address ↔ in-game player name** so we show the right person's
data and can badge it "verified."

**Step 1 — check if it's already public.** First investigate whether kintara.gg's public
profile/API exposes a player's wallet (or a stable user id we can map to a wallet). If it does,
we can **auto-link with zero friction** — the user just types a name, we resolve the wallet, done.
*This is the preferred path; confirm it before building a challenge flow.*

**Step 2 — fallback challenge (only if not public):** lightweight, read-only proof of wallet
control:
- **Sign-a-message** (best UX): user signs a nonce with their wallet (Phantom/Solflare); we verify
  the signature server-side. Proves wallet control, costs nothing, moves no funds.
- **Memo micro-tx** (no wallet-connect needed): user sends a tiny self-transfer with a code we give
  them; we look for it on-chain.
- Linking the wallet to the *name* still needs one cross-check: confirm that wallet's on-chain
  marketplace activity matches the named player's marketplace activity in our DB (e.g. a sale we
  logged for that seller name corresponds to a KINS transfer from/to that wallet). Use this as the
  name↔wallet bridge.

**Important:** because all displayed data is public, the page can also work in an **unverified,
public-only mode** (type a name → show everything keyed on the name; type a wallet → show on-chain
stuff). Verification just unlocks the *combined* view + a "verified" badge and prevents mislinking.
Recommend shipping public-mode first, layer verification on.

## Data sources & the stats to show

### A. From our own DB (already collected — see README schema)
Attributable by `seller_id`/`seller_name` (listings, sales_events), `ownerName/ownerId`
(property), and player name (live world):
- **Marketplace earned (sell side):** total + per-item units sold, gross paid, in gold/USD/$KINS,
  from `sales_events` (seller_name). Time series, best sales, avg sale price.
- **Active listings & inventory value:** current live listings, total ask value, favorite items
  /categories, from `listings`.
- **Seller behavior:** undercut frequency, avg premium/discount vs floor, fast-seller vs
  sits-forever, supply concentration they control (the seller-intelligence metrics).
- **Property:** mansions/houses/trailers owned, lock state, map position, from the property feed.
- **Live world:** last seen online (server, area, coords), level, equipped item, badge, HP, outfit
  avatar — from the spectate roster.

> Note: the kintara marketplace API does **not** expose buyers, so "**spent** in marketplace" is
> NOT in our DB — that's exactly what the on-chain layer is for.

### B. From kintara's public profile pulls
Display all public profile fields the game exposes (reverse-engineer the public profile
endpoint, same way we did `/api/servers`, `/api/property-signs/status`, etc.):
- Display name, level/XP, badges, cosmetics/outfit, achievements, join date, guild, **Kintara Club
  membership** (if it's shown publicly on the profile — confirm). Record what's available.

### C. On-chain (Solana) — the hidden economics
Build on the existing **`check_pump_rewards.py`** pattern (public-address-only, Solana RPC via
`solders`/JSON-RPC, batched with backoff, configurable `RPC` endpoint — point it at Helius/
QuickNode for headroom). **Solscan's API** is a good complement for parsed/labeled token transfers
and transaction history (less decoding than raw RPC). Use whichever is cleaner per data point.
- **KINS spent & earned in the marketplace:** parse the wallet's KINS (SPL token) transfers
  to/from the marketplace program / treasury / counterparties. Net + gross, in KINS and USD (price
  it with our existing `kins_daily_usd()` / live KINS price). This is the headline stat.
- **Wheel spins via the treasury:** count (and outcomes, if decodable) of the wallet's transactions
  to the wheel/treasury program — spins/day, total spent on spins, notable pulls.
- **Kintara Club membership:** if membership is an on-chain payment / NFT / subscription, detect it
  on-chain; otherwise read it from the public profile (B).
- **Holdings/context:** KINS balance, SOL balance, relevant NFTs — quick wallet snapshot.

> Needs reverse-engineering: the **program/treasury/marketplace addresses** for KINS, the wheel,
> and club membership. Identify these from a known transaction (e.g. do one wheel spin / one
> marketplace buy and inspect the tx on Solscan to capture the program + token accounts), then
> hard-code them as constants like the existing `PUMP`/`PSWAP` addresses in `check_pump_rewards.py`.

## Implementation notes (fit the existing app)

- **Single file:** everything lives in `kintara_tracker.py` (Flask + sqlite3 + the embedded
  `INDEX_HTML`). Add a **Player** tab + a `GET /api/player?name=&wallet=` endpoint that assembles
  the three sources into one payload; render it like the other tabs.
- **Caching & politeness:** on-chain RPC and profile pulls are slow/rate-limited — cache per
  wallet/name (e.g. a `player_cache` table or in-memory TTL), and respect the existing pacing
  philosophy. On-chain history is mostly immutable → archive it (like `sales_daily`) and only fetch
  recent txns on refresh. Don't hammer the RPC; batch and back off (the script already does).
- **Reuse what exists:** seller cross-reference + property + live-world are already wired in places
  (e.g. the Property/Live World tabs cross-reference marketplace listings) — factor those into the
  player payload. KINS→USD via `kins_daily_usd()`/`current_kins_usd()`.
- **Keep docs current:** update `README.md` (new tab, endpoint, any new tables/constants, on-chain
  data sources) in the same change.

## Suggested build order
1. **Public profile pull** — reverse-engineer kintara's public player profile endpoint; new
   `/api/player` returns DB stats + public profile for a given name (public mode, no wallet yet).
2. **Player tab UI** — render the DB + profile sections (sell-side economics, listings, property,
   live-world, seller behavior). Already a strong page from data we own.
3. **On-chain layer** — identify the KINS / treasury / wheel program addresses; add wallet
   spent/earned + wheel spins + balances (Solscan/RPC), priced in USD/$KINS; cache/archive.
4. **Verification** — auto-link via public wallet if available; else sign-a-message + the
   DB↔chain activity cross-check; add a "verified" badge.
5. **Kintara Club** — from profile or on-chain, whichever exposes it.

## Open questions to resolve while building
- Does kintara's public profile/API expose a player's **wallet** (or a mappable user id)? (Decides
  the whole verification flow.)
- Is **Kintara Club** membership public on the profile, or on-chain only?
- What are the exact **program/treasury addresses** for KINS transfers, the wheel, and club
  membership? (Capture from a sample transaction on Solscan.)
- Solscan API vs raw Solana RPC per data point — pick by which gives clean, labeled transfers with
  the least decoding.

## Guardrails
- **Public/on-chain data only.** No private keys; wallet proof (if used) is read-only signing.
- Label every field's provenance (DB / public profile / on-chain) and show data freshness.
- It should feel like a research/intelligence profile, not surveillance — only what's already public.
