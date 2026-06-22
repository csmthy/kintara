# KinScan

KinScan is a self-hosted tracker + arbitrage scanner + price-history desk for the
**Kintara** MMO marketplace (kintara.gg). One Python file, one SQLite DB, a
single-page web dashboard. Runs locally or as a hosted site.

> **For the AI assistant / future maintainer:** This README is the source of truth
> for "what is this project and where is everything." Keep it current. **Whenever you
> make a change that alters behavior, data sources, the DB schema, API routes, the
> tabs/UI, caching, or run instructions, update the relevant section here in the same
> change.** Not every tiny tweak needs an entry, but anything a fresh chat would need
> to know to avoid re-discovering it does. The "Changelog / state" section at the
> bottom is the quickest place to note what recently changed. If you reverse-engineer
> a new endpoint, asset path, or data quirk, record it here so it never has to be
> re-derived.

---

## Run

```bash
pip install flask requests websockets   # websockets is optional — only the Live World tab needs it
python kintara_tracker.py            # dashboard at http://127.0.0.1:8765
```

CLI flags: `--interval <s>` (listing poll seconds, default **90**), `--port <n>`
(default 8765), `--host <addr>` (bind address; `0.0.0.0` when hosted), `--gold-item
<itemType>` (override the gold item), `--no-browser`.

**Env-tunable cadence + politeness** (defaults are 24/7-friendly; flags override env):
`KINTARA_DB` (DB path — point at a volume when hosted),
`KINTARA_MARKET_DB` (compact on-chain market dataset the Market Watch home page reads, read-only;
defaults to `./market.db` locally, `/opt/kintara-data/market.db` when hosted — see "Market Watch" +
"On-chain market dataset"), `PORT`, `KINTARA_HOST`,
`POLL_INTERVAL` (90 — FULL-book poll, pages the whole book for removal detection),
`FIRSTPAGE_INTERVAL` (3 — fast page-1 capture poll; 1 request, records newest listings
before they can be created+sold inside a full-poll window — see "Two-tier polling"),
`KINTARA_MIN_GAP` (0.5 — global min seconds between **any** two
kintara.gg requests, a shared pacer across all loops ⇒ ≈ ≤2 req/s total),
`KINTARA_BACKOFF` (45 — pause after a 429/403), `STATS_STALE_HOT`/`STATS_STALE_COLD`
(120/900 — per-item stats refresh cadence). All kintara.gg requests go through
`pace_kintara()` so it can run continuously without bursting the marketplace.
Historical-pipeline cadence (all DB-local, no kintara request): `SNAPSHOT_INTERVAL` (300 — order-book
snapshot tick), `SNAPSHOT_RETENTION_DAYS` (14 — prune raw snapshots after roll-up), `MERCHANT_SNAP_INTERVAL`
(300 — merchant campaign snapshot tick).

**Hosting:** see `DEPLOY.md`. It's a stateful always-on process (serves the dashboard
*and* runs the DB-building loops), so serverless (Vercel/etc.) won't work — use an
always-on container/VM with a persistent volume (Fly.io / Railway / Oracle free VM).
Ships a `Dockerfile`, `requirements.txt`, `.dockerignore`. Run as the single
`python kintara_tracker.py` process — **not** gunicorn (the loops are threads in `main()`;
forking workers would multiply the pollers).

**Publishing updates:** from the Mac repo, run `bash deploy/publish.sh "what changed"`.
It stages/commits local changes, pushes `main`, SSHes to the DigitalOcean Droplet, and
runs `/opt/kintara/deploy/deploy.sh` there. If there are no local changes, the same
script can be run without a message to re-run the server deploy. Override the target
with `KINSCAN_DEPLOY_HOST=root@<ip>` if the Droplet changes.

Artifacts created in the working dir (or `KINTARA_DB`'s dir): `kintara.db` (SQLite, WAL
mode) and `icons_cache/` (downloaded item art). Both are safe to delete; they rebuild.
`world_map.jpg` ships alongside the script (the isometric world map, served at
`/worldmap.jpg`); keep it for the Property Map / Live World backdrops.

**On-chain treasury ledger (optional/offline):** `build_treasury_index.py` downloads the
Kintara treasury's KINS token-account history into `market_index.db` as a local ledger of
KINS flows involving the treasury. It stores the full treasury ledger first, then classifies
marketplace trades as a clean subset. Run with an archival RPC for full depth:
`RPC=https://<helius-or-quicknode> python build_treasury_index.py`. Useful checks:
`python build_treasury_index.py --probe`, `--summary`, `--kinds`, and `--wallet <address>`.
Helius may reject JSON-RPC batch POSTs with HTTP 413/403; the script defaults to
single-transaction calls (`BATCH=1`) and only batches if you explicitly set `BATCH>1`.
Oversized/rejected batches are split automatically. Helius docs currently say
`getTransactionsForAddress` requires Developer plan or higher; if unavailable, the script
falls back to standard RPC pagination. Fallback mode defaults to the stable single-worker path
(`TX_WORKERS=1`, `TX_CHUNK=50`, `RPC_RPS=5`), and 429s trigger a shared cooldown plus automatic
RPS reduction down to `RPC_RPS_MIN=0.5`. Clean chunks gently raise the speed up to
`RPC_RPS_MAX=8` (`RPC_RPS_STEP=0.5` every `RPC_RPS_UP_EVERY=4` chunks), so it can discover the key's
usable ceiling. If a key is still hot, run `RPC_RPS=3 TX_WORKERS=1`.

Everything lives in **`kintara_tracker.py`** (~2100 lines): Python backend (Flask +
sqlite3 + requests) up top, the entire frontend as one embedded `INDEX_HTML` string
(vanilla JS, canvas charts, no build step) at the bottom.

---

## What it does (core concepts)

Two currencies trade on the Kintara marketplace:
- **gold** — in-game currency (`currency:"gold"`, `priceGold` set).
- **KINS / token** — priced in USD (`currency:"token"`, `priceUsd` set, `priceGold`
  is a placeholder `1`). KINS is also a real Solana token.

**Everything is priced PER SINGLE ITEM** = listing price ÷ stack size (`quantity`).
Almost every listing is a stack (e.g. 5000 wood for 2 gold = 0.0004 gold/wood). This
is the central correctness rule — never compare raw listing prices.

**Gold rate** = our own measured price = the **average per-gold USD ask of the 3
cheapest** active `token` listings of the configured gold item (`our_gold_price()`;
averaging the few cheapest smooths out a lone lowball vs. a raw `MIN`). Currently
~$3–4 per gold. The gold item's `itemType` is literally `"gold"` (auto-detected and
persisted in `settings`). If there are no live gold listings to price from (e.g. the
poller hasn't run), `gold_rate_usd()` falls back to kintaragold.xyz's spot price.

**Arbitrage** (primary tab): for each item, compare cheapest per-item gold ask
(× gold rate, in USD) vs cheapest per-item KINS ask. Direction toggle
(gold→KINS / KINS→gold), and a min-profit/profitable-only/sold-today filter (no sell
fee). Reserved listings (active `reserved_until`) are excluded — you can't buy them.
Profit is shown **per gold spent** for items under 1 gold each (1-gold minimum spend)
and **per item** otherwise. The **kins / gold** column = how many $KINS it costs to buy
enough of the item (at its cheapest USD/KINS listing) to assemble **1 gold's worth**
(`per_gold × kins_unit / kins_price`) — the KINS price of a "manufactured" gold; green
when it's below the live market KINS/gold (`gold_rate / kins_price`), i.e. cheaper than
buying gold outright. Most meaningful for the materials this tab is used to flip.

**KINS / gold** = `gold_usd / kins_usd` — the "real" exchange rate (how many KINS one
gold is worth), the metric the user cares most about.

**CMP** = **C**osmetics / **M**ounts / **P**ets — the collectible, non-commodity item classes
(as opposed to materials/tools/food). The mispricing mode targets CMP. Empirically (re-pricing
every CMP sale into USD, KINS, and gold via the gold/KINS history at sale time), **collectible CMP
prices are stickiest in gold and KINS (tied, CoV ~0.52) and least sticky in USD (CoV ~0.82)** —
so a historical *USD* average is the worst "fair value" anchor and is the source of the stale-price
errors at low liquidity. Carrying a stale price forward is most reliable in **gold** (best balance
of short-horizon predictiveness + long-horizon stickiness, and it's the unit the game/shop
denominates in — the nominal-gold listing lag is the exploitable edge); KINS is what matters
economically but, being the most volatile denominator, is the *worst* unit to carry a stale price
forward in. **Farmable/grindable mounts** (wolf/dragon/whale — high volume, cheap) are the exception:
they're USD/utility-anchored commodities, not collectibles, and should be bucketed with the
commodity (gold↔KINS) logic, not the CMP mispricing scan. **Total world supply per item IS available**
and is now shown in the Index ("In world" column) — see `world_item_supply()`. It's served by `GET
/api/world-item-index?category=<cat>&sort=<dir>`, which powers kintara.gg's own `/#index` page
(rendered by `site/js/components/itemIndex.js` → `createItemIndex`). Response: `{ok, playerCount,
generatedAt, rows:[{id (=our item_type), label, icon, count, category}]}` where **`count` = total units
of that item across every player inventory, bank, and bag** ("Totals across N players"). The
authoritative origin (`kintara.gg`) requires a **logged-in wallet session** (`{"ok":false,"error":
"unauthorized"}` without cookies), BUT the public **`fanout.kintara.gg` mirror serves it without auth**
— so we fetch it from there (it sometimes returns `fanout_unavailable` transiently; we cache last-good).
The sibling `world-marketplace-index` (per-item sales + `tokenPriceUsd`) is also public on the fanout.
(`game.js` itself has no edition/supply concept — the totals are computed server-side.) **Earlier note
(now resolved):** this was thought login-only because the fanout was temporarily down for these two
endpoints when first probed.

---

## Data sources (all reverse-engineered)

| What | Endpoint | Notes |
|---|---|---|
| Active listings | `GET kintara.gg/api/marketplace/listings?sort=latest&currency=all&category=all&limit=40&offset=0&q=` | Returns `{ok, listings[], total, limit, offset, hasMore}`. Each listing: `id, sellerId, sellerName, itemType, quantity, priceGold, currency, priceUsd, createdAt, reservedBy, reservedUntilMs, itemDurability`. **No public sales-history here; no category field** (the `category` param is ignored — server returns everything). We build history ourselves. |
| Daily completed sales | `GET kintara.gg/api/marketplace/stats?[currency=token&]itemType=<x>` | `{ok, currency, avg30d, samples:[{date, avgUnitPrice, sales}]}`. **Daily only** (ignores interval params), ~30 days, **sparse** (only days with sales). `avgUnitPrice` is per single item; gold prices are rounded to 2 decimals (sub-cent gold collapses to 0). Omit `currency` = gold; `currency=token` = USD. |
| Live KINS price (USD) | `GET kintara.gg/api/token/blimp-stats` | `{priceUsd, ...}` — kintara's own KINS/USD, matches their index page. |
| Item art | `kintara.gg/assets/hud/<category>/<name>.(png|svg)` | Real in-game icons. Mapping is per-item (`ICON_OVERRIDES` + `icon_asset()`); cosmetics = `cosmetics/<itemType>.png`, keys = bronze/silver/gold. **Pets/furniture**: the exact per-item paths aren't in the override map, so `icon_candidates()` probes likely schemes (`pets/<name>.png`, `furniture/<name>.png`, …) and the `/icon` route caches the first that returns 200, falling back to the generic paw for pets. Cached under a `__art` namespace so the old generic-paw files don't shadow real art. **The probed paths are unverified guesses** — confirm a real one in-browser and add it to `ICON_OVERRIDES` if a pet/furniture stays blank. |
| Item display names | (ripped from `kintara.gg/game.js` label catalog) | Baked into `ITEM_LABELS` dict (133 entries). `item_label()` resolves itemType→name with a prettify fallback. e.g. `cosmetic_dog_mask`→"Jotchua", `wild_sword`→"Training Sword". |
| **Gold USD price (ours)** | our own `listings` DB | Authoritative while the tracker runs. `gold_price_loop` snapshots one row into `gold_price` every ~3 min = `our_gold_price()` (avg per-gold USD of the 3 cheapest live token gold listings). Drives both the gold chart and the arbitrage gold rate. |
| **Gold USD price history (fallback)** | (ripped from `kintaragold.xyz` HTML) | The page embeds `"history":[{t,price}]` + `"spotPriceUsd"` in its RSC payload (escaped, can straddle chunk boundaries — we regex the `t`/`price` pairs). Independent gold-USD series (~10-min, ~25 days), NOT derived from KINS. `fetch_kintara_gold_history()`, cached ~3 min. Used **only to backfill the stretch before our own `gold_price` data begins** (see `gold_series_for_chart()`) and as the gold-rate fallback when no live gold listings exist. |
| **KINS/USD price history** | `GET api.geckoterminal.com/api/v2/networks/solana/pools/<POOL>/ohlcv/<tf>?aggregate=&limit=&currency=usd&token=base` | Pool `F42tZnKPavq1VUcrL6ymhc6YqVpt84fWwgzbNTv2wb3W` (KINS/SOL on pumpswap). `currency=usd` already converts SOL→USD (no separate SOL feed needed). Valid aggregates: minute 1/5/15, hour 1/4/12, day 1. **Rate-limits if hammered** — we cache (see below). |
| **Treasury KINS ledger** | Solana JSON-RPC on the treasury's KINS token account | `build_treasury_index.py` resolves the KINS mint from the GeckoTerminal pool, finds the treasury wallet's KINS token account, pages Helius `getTransactionsForAddress` when available (full transactions, cursor pagination), otherwise falls back to `getSignaturesForAddress` + parallel `getTransaction`, decodes each transaction's KINS owner deltas, and writes every KINS balance-changing treasury row to `treasury_txns`. It classifies flow shapes (`marketplace_trade`, `treasury_income`, `treasury_payout`, etc.) and also writes proven 5% marketplace-fee trades to `market_txns`. Live sample: marketplace fee is 500 bps and recent marketplace rows use program `L2TExMFKdjpN9kozasaurPirfHy9P8sbXoAN1qA3S95`. Default treasury owner: `4zW4zuZb9rXpvw3cTYyGoQ2iHTtG9E17YpdeNUbwuQVt`; current token account: `FawpB6tqFaZybcQjUzHaSXFASmRRzxuFzTEsbGzxHFq4`. Use archival `SOLANA_RPC`/`RPC`; public RPC is okay for probes but not complete history. Helius 413/403 batch errors are avoided by default (`BATCH=1`) and handled by adaptive splitting when batching is enabled. Fallback throughput is governed by `TX_WORKERS`, `TX_CHUNK`, and `RPC_RPS`. |
| **Server list** | `GET kintara.gg/api/servers` | `{ok, servers:[{id, name, populationLabel, full, queueLength, minLevel}]}`. Live population + queue per game server. Drives the top status bar. `fetch_servers()`, cached ~30s (last-good on failure). |
| **Traveling-merchant state** | `GET kintara.gg/api/world/merchant-campaign` | kintara.gg's **own** public endpoint (no auth; the game client reads it the same way via `KINTARA_READ_FANOUT_ORIGIN`, also reachable at `ktra-server-b.onrender.com`). Returns `{ok, mode, wood, stone, coal, cooked_fish_meat, metal, goals:{...}, complete, goldTradeEnabled, goldStock, goldStockFull}`. **No overall %** — we compute it as the mean of the five per-resource (capped) percentages. `fetch_merchant()`, cached ~60s (last-good on failure). |
| **Merchant gold-mint recipe** | (from `kintara.gg/game.js` `MERCHANT_TRADE_COST`) | `MERCHANT_RECIPE` = resources consumed per 1 gold minted: 2500 wood + 1500 stone + 700 coal + 30 cooked_fish_meat. This is now separate from the **donation campaign** resources (`MERCHANT_CAMPAIGN_RESOURCES`: wood, stone, coal, cooked fish, **metal**); the cost calculator follows the current gold-trade recipe, while the left progress tracker follows the live campaign goals. |
| **World item supply** | `GET fanout.kintara.gg/api/world-item-index?category=all&sort=desc` | Total units of each item across **all** player inventories/banks/bags — powers kintara.gg's `/#index`. `{ok, playerCount, generatedAt, rows:[{id (=item_type), label, icon, count, category}]}`. The authoritative origin needs a logged-in session; the **public read-fanout mirror serves it without auth** (occasionally `fanout_unavailable` transiently → we keep last-good). `world_item_supply()`, cached ~10min (`WORLD_INDEX_CACHE_SEC`). Drives the Index "In world" column. |
| **Property ownership** | `GET kintara.gg/api/property-signs/status` | Public. `{ok, mansions:{1..3}, houses:{1..5}, trailers:{1..8}}` each → `ownerName, ownerId, sold, locked`. `fetch_property_status()`, cached ~30s, last-good on failure. Drives the Property Map. |
| **Property map coordinates** | (from `kintara.gg/game.js`: `MANSIONS`, `REGULAR_HOUSES`+`REGULAR_HOUSE_SLOT_TO_ID`, `TRAILERS`) | Each property's world-grid footprint `(col0,col1,row0,row1)`, baked into `PROPERTY_PLOTS`, so the map matches the in-game estate row. |
| **Live world roster + positions** | `wss://kintara.gg/ws/spectate/sN` (per **server**, N = 1–12) | **Public spectator WebSocket, plain JSON, no auth.** Streams `{t:"snap", region, onlineTotal, players:[{id,name,x,z,ry,avg(level),eq(held item),bdg(badge),php(hp%),mov,outfit{hat,top,pants,shoe,skinTone,*C colors,aura,...}}]}`. **`sN` is the server number** (the same `s${shardId}` the game opens for queue/presence after you pick a server). All **12 servers** are separate worlds (zero player overlap). Note the read-fanout mirror (`ktra-server-b.onrender.com`) only carries servers 1–4 — kintara.gg itself serves all 12, so we use it. `onlineTotal` is the **global** count (identical across all 4). The spectator is only sent players in the **realm** it's subscribed to (set via `{t:"spec_reg",region}`), and within the big `world` realm only those near the hub camera. So `SpectateHub` **round-robins every realm** (`SPECTATE_REGIONS`: world/pond/beach/eldergrove/frostmere/arena/wild/mine/spider/…, lingering longer on `world`), accumulating a per-world roster tagged by realm — ~75–80 named players per world after a ~20s sweep, vs ~25 from the hub alone. Each player carries its `realm`. One socket per world, opened lazily on the first `/api/live` hit, closed after ~75s idle. Needs the `websockets` package. |
| **World map image** | `kintara.gg` exports it client-side as `kintara-full-map-mainland.png` (no server copy); the isometric overview is shipped in-repo as `world_map.jpg`, served at `/worldmap.jpg` | Used as the Property Map backdrop and the per-player location view. `world_map.jpg` is a static asset next to the script. |
| **Per-realm map art** | generated into `MapImages/` by **`render_maps.py`** | True top-down 2D renders rebuilt from the game's own world-gen (grid dims + seeded RNG + hardcoded prop/building coords in `constants.js`/`game.js`), **not** screenshots. `render_maps.py` (`pip install pillow && python render_maps.py`) writes: `The Shores (Beach).png` → `/shores.png`, `The Pond.png` → `/pond.png`, `The Arena.png` → `/arena.png`. Each is wired into Live World via `REALM_MAPS` (frontend): `{img, toMap(x,z)→(u,v)}`, the inverse of how the PNG was drawn, so player dots land exactly. **Beach** (`beach`): 40×40, off `-19.5`, wavy shoreline from xorshift seed `0x9e3779b1`, props at exact tiles; `shoresToMap()` (`u=(19.5-z)/39, v=(x+19.5)/39`). **Pond** (`pond`): 40×40; central lake from `pondRand` seed `0x50dcee` (seed block + 70 flood-grow steps) — exact central shape; dock (cols 15-19×18-19) + NE tower (32-38×1-6) carved; trees/rocks are representative scatter (the NE resource-exclude bounds are game.js-only, so the lake edge nearest NE is approximate); `u=(x+19.5)/39, v=(z+19.5)/39`. **Arena** (`arena`): 20×20 sand + central boxing ring; `u=(x+9.5)/19, v=(z+9.5)/19`. **The Mainland** (`world`, 62×62, `GRID_COLS`) → `/maps/mainland.png` (top-down overworld: central cobblestone plaza + fountain + market/bank/alchemist, the **exact** property estate from `PROPERTY_PLOTS`, dirt paths, edge portal-gates, scattered trees) — replaced the old eyeballed isometric `worldmap.jpg` crop (`worldToMap()`/`MAP_ZOOM` removed); now plotted exactly via `centerMap(62)` like every realm, with all overworld players shown. **All remaining realms are now rendered too**, served under `/maps/<slug>.png` and wired via `centerMap(N)` (the generic centred-square transform `u=(x+(N-1)/2)/(N-1)`, `v` likewise; grid sizes are exact from `constants.js`): **Whisperwood** (`eldergrove`, 62×62) → `/maps/whisperwood.png` (forest + dirt path); **Frostmere** (40×40) → `/maps/frostmere.png` (snow, snow-capped pines, frozen ponds); **The Wilderness** (`wild`, 50×50 = `round(62×0.8)`) → `/maps/wild.png`; **Deep Wilderness** (`wild_ext`, 25×25 = `max(14,round(50×0.5))`) → `/maps/wild-deep.png`; **Wilderness East** (`wild_exp`, 25×25) → `/maps/wild-east.png`; **The Mine** (20×20) → `/maps/mine.png` (cavern + ore-flecked boulders + torches); **Spider Lair** (`spider`, 20×20 **approx** — no public grid const, dungeon size assumed) → `/maps/spider.png` (webs + egg sacs); **The Shack** (5×5) → `/maps/shack.png`. For these, the **grid + coordinate transform are exact** (so player dots land correctly) but the trees/rocks/features are a **seeded representative scatter** (same as Pond/Arena), since the game ships no public prop-coordinate dump for them. The **property/estate** map is drawn client-side as a tilted-2.5D SVG (see Property Map tab) — no PNG. |

The Gold-price tab reconstructs intraday gold: `gold_usd(t)` = our `gold_price` series
(kintaragold backfilling only the times before ours begins) interpolated onto the KINS
time grid; `kins_per_gold(t) = gold_usd / kins_usd`. Ranges
4H/1D = true 3-min (1-min candles paginated + bucketed), 3D = 15-min, 7D/14D = 1h, ALL = 4h.

---

## Persistence & caching (minimize API calls)

Past data never changes, so we **archive it and only re-fetch recent/live data.**

- **`sales_daily`** table = the archive: one row per `(item_type, currency, date)` →
  `sales, avg_price`. Past dates are immutable; only the current (partial) day updates.
  `/api/sales-history` and `/api/sales-summary` read from here (live `stats` fetch only
  on a cold miss, then stored).
- **`stats_loop`** (background thread) is the only thing that calls `/stats`. It
  refreshes one item+currency every ~0.4s from a **wide watchlist**: captured listings,
  archived sales tables, cached item_stats, the baked in-game label catalog, and critical
  new-drop items (currently Venomweaver loot). This is intentional: a rare item can list
  and sell between listing polls, leaving no active-listing row, but `/stats` still knows
  the sale happened. High-liquidity/urgent pairs are prioritized, but quiet known items are
  still re-checked on the cold cadence, writing to both `sales_daily` (archive) and
  `item_stats` (last-day summary).
- **Two-tier listing poll (capture vs. removal):** listings are polled by *two* loops.
  **`poll_loop`** (FULL, `POLL_INTERVAL`=90s) pages the **whole book** via
  `fetch_all_active()` and calls `reconcile(..., complete=True)` — this is the only fetch
  that sees every listing, so it's the only one that marks vanished listings removed. But
  a full sweep is ~143 requests at PAGE=40 (~5700 listings) ≈ a minute-plus at ≤2 req/s, so
  a listing **created *and* sold inside one sweep** was never captured — the sale could only
  be recovered later as a count tick with no detail (a synthetic row). **`firstpage_loop`**
  (`FIRSTPAGE_INTERVAL`=3s) closes that blind spot: it fetches **only page 1**
  (`fetch_all_active(max_pages=1)`, newest-first = 1 request) and calls
  `reconcile(..., complete=False, record_poll=False)` — **capture-only**: it upserts the
  newest listings (so every creation is recorded almost immediately) but never marks
  removals (it can't see the rest of the book) and writes no `polls` row (avoids 17k/day of
  bloat). Net: we now log essentially every listing the moment it appears, and removal
  detection still lives with the full poll. `fetch_all_active(max_pages=N)` returns
  `complete=False` when it hits the page cap with book remaining, so a capped fetch never
  makes `reconcile` mistake unseen pages for removed listings.
- **Gold price (ours)**: `gold_price_loop` writes one `gold_price` row every ~3 min from
  the local `listings` DB (no external call). This is the live gold series.
- **Servers / merchant / property**: `/api/servers` cached ~30s, `/api/merchant` ~60s,
  `/api/property` ~30s, all serving last-good on upstream failure.
- **Live world**: `SpectateHub` holds at most one WebSocket per requested shard, opened on
  the first `/api/live` hit and auto-closed ~75s after the Live World tab stops polling. The
  roster prunes players not seen in a snapshot for ~8s. Be gentle — it's the game's own
  read-fanout host.
- **Gold chart**: `/api/gold-history` is cached per range ~3 min server-side (serves last
  good payload if gecko rate-limits); the kintaragold rip (fallback/backfill only) is
  cached ~3 min; KINS spot price ~2 min. So the gold chart hits external APIs at most
  ~once / 3 min per range.
- **Item icons** download once to `icons_cache/`, then serve from disk.

---

## Database schema (SQLite, `kintara.db`)

- **`listings`** — every listing ever seen. Key cols: `id` (PK = game listing id),
  `seller_id/name`, `item_type`, `category`, `quantity`, `price_gold`, `currency`,
  `price_usd`, `unit_price` (headline total), `per_unit` (price ÷ quantity — per item),
  `reserved_by`, `reserved_until` (epoch ms), `item_durability`, `created_at`,
  `first_seen`, `last_seen`, `active` (1/0), `removed_at`. `reconcile()` upserts and marks
  vanished listings `active=0, removed_at=now` (only on a complete non-empty fetch, so a
  network blip can't wipe the market). `removed` ≠ guaranteed sale (sold or delisted).
- **`item_stats`** — last-day summary per `(item_type, currency)`: `day`, `day_sales`,
  `day_avg`, `avg30d`, `updated_at`. Drives the arbitrage "sold today" column.
- **`sales_daily`** — the daily archive (see above).
- **`sales_events`** — **ACTUAL completed sales**, one row per sale, now with the real stack size,
  seller, total paid and time-on-market. Detection (`_archive_samples` → `_log_sales`): kintara's
  `/stats` completed-sale **count** is the authoritative trigger (cancel-and-relist doesn't move it),
  and it gives a marginal per-unit price. `_log_sales` consumes that sale-count delta by **matching it to
  the listing(s) we watched vanish** (recently `removed_at`, `sold_claimed IS NULL`, ranked by how
  close `per_unit` is to the marginal sold price, so the genuinely-sold listing wins over a
  coincident cancellation) to recover `qty` (stack size), `total`, `seller_id/name`, `listing_ms`
  (time on market = `removed_at - created_at`) and `listing_id`. The matched listing is flagged
  `listings.sold_claimed=1` so it's never double-counted. The first time we ever see an item-day we
  only attribute **price-matching** recent removals and emit **no** synthetic rows (so pre-tracking
  history is never replayed as "now"); confirmed-but-unmatched sales (we missed the listing) get
  synthetic rows (`qty`/`seller` unknown) so we stop re-detecting. `units` is the completed-sale count
  for that row, while `qty` is the real stack size when matched. Items with a fresh unclaimed removal are
  **prioritised per item+currency** in the stats queue (`_next_stats_pair`), but only until stats have
  been checked after that removal; urgent selection is aged fairly so constant wood churn cannot starve
  stone/coal/fish/metal. Indexed on `ts` and `(item_type, ts)`; legacy count-semantics rows were dropped
  on migration and the feed fills forward.
- **`gold_price`** — our own measured gold-USD series: `ts` (epoch ms, PK), `usd` (USD per
  1 gold), `listings` (how many listings the avg used, ≤3). One row per ~3 min from
  `gold_price_loop`.
- **`orderbook_snapshots`** — **Substrate A**, the historical order book. One row per
  `(ts, item_type, currency)` per snapshot tick (~5 min, `snapshot_loop`): `floor`/`floor2`/
  `floor3` (cheapest three per-unit asks = undercut depth), `floor_usd` (floor converted to
  USD at tick time), `listed_qty`, `listings`, `sellers`, `depth1/5/10/25` (units within
  1/5/10/25% of floor), `reserved_qty`. Computed from our **own DB** (no kintara request).
  This preserves the market's *shape over time*, which `reconcile()` otherwise overwrites.
  Highest-write table → writes are batched one transaction per tick, cadence is coarser than
  the poll loop, and raw rows are **pruned after `SNAPSHOT_RETENTION_DAYS` (14)** once rolled
  into `item_daily_metrics`. Indexed on `(item_type, ts)` and `ts`.
- **`item_daily_metrics`** — **Substrate B**, the durable daily roll-up (`rollup_loop`,
  hourly, re-rolls today+yesterday). One row per `(item_type, day)`: `floor_usd_open/close/
  min/max`, `floor_gold_close`, `listed_qty_avg`, `sellers_avg`, `volume_units`/`volume_usd`/
  `sales_count` (from `sales_events`), `volatility` (stdev/mean of intraday floor),
  `undercut_count` (downward floor moves). This is the cache the scorecard / floor-history
  read from — **never aggregate raw snapshots per request**. Storage stays bounded because raw
  snapshots are pruned once their day is rolled up here.
- **`merchant_events`** — **merchant RESTOCK events** (`ts` PK, `kind`, `gold_stock`, `note`). Detected in
  `merchant_snapshot_loop` by comparing each snapshot to the previous: the campaign filling (complete/
  overall % crossing 100) or the gold stock jumping back up (refill ≥40% of full), deduped to one per
  ~30 min. A market-wide shock, overlaid as gold markers on the time charts via `/api/merchant-events`.
- **`merchant_snapshots`** — traveling-merchant campaign history (`merchant_snapshot_loop`,
  ~5 min, reuses `fetch_merchant()`'s 60s cache → no extra kintara load): `ts` (PK), `mode`,
  `overall_pct`, `gold_stock`, `gold_trade`, `complete`, `resources` (JSON
  `{key:{current,goal,pct}}`), `mint_usd` (cheapest $/gold to mint at tick time, walks the
  live order book), `gold_rate`. Drives the Merchant tab's forecast.
- **`polls`** — one row per listing poll (ts, active, removed, ok).
- **`settings`** — key/value (notably `gold_item`).

Schema migrations are handled inline in `init_db()` (ALTER + backfill for older DBs).

Separate optional DB:
- **`market_index.db` / `treasury_txns`** — offline Solana treasury ledger built by
  `build_treasury_index.py`: one row per decoded KINS transaction where the treasury owner balance changes.
  Key columns: `sig` PK, `slot`, `ts`, `kind`, `confidence`, `primary_wallet`, `buyer`, `seller`,
  `gross_kins`, `seller_net_kins`, `fee_kins`, `fee_bps`, `treasury_delta_kins`, payer/receiver counts,
  treasury owner/token account, program IDs, instruction types, and raw owner deltas JSON. Current coarse
  `kind` values are shape-based (`marketplace_trade`, `treasury_income`, `treasury_payout`,
  `treasury_income_with_receivers`, `non_treasury_transfer_seen_by_account`, `unknown_kins_treasury`);
  spin wheel / Kintara Club / KINS purchase buckets should be refined from program IDs, instruction
  shapes, and repeated amount patterns after a larger archival backfill.
- **`market_index.db` / `market_txns`** — compatibility/fast subset: only the high-confidence
  marketplace trades (`buyer` pays KINS, `seller` receives KINS, treasury receives a positive fee;
  recent rows decode at 500 bps). Cursors live in `meta` per treasury token account (`newest_sig`,
  `oldest_sig`, `done`). This DB is not yet required by `kintara_tracker.py`; it is the intended
  replacement for slow per-wallet marketplace RPC scans after validation.

### On-chain market dataset (the Market Watch source)

The big `market_index.db` (~200MB: raw tx JSON, per-account deltas, program lists) stays on the
laptop — it is **never** committed and **never** put on the server. `build_market_dataset.py`
distills it into a small, indexed **`market.db`** that the website actually serves:

- One lean table **`market_txns`**: `sig` PK, `ts` (ms), `date` (UTC), **`category`**, `buyer`,
  `seller`, `gross_kins`, `to_player`, `to_treasury`, `burned_kins`, `kins_usd`, `usd_value`, plus
  indexes on ts/date/category/buyer/seller and a `meta` snapshot.
- **Category** maps the downloader's shape-based `kind` → user-facing buckets: `marketplace_trade`
  → **marketplace** (player↔player, ~5% treasury fee); `treasury_income`(`_with_receivers`) →
  **sink** (player pays, ~50% burned + ~50% to treasury, no seller — casino/wheel/spinner wagers +
  other KINS burn-sinks); `treasury_payout` → **payout** (treasury → player); else **other**.
- **USD valuation (per-minute, not daily):** these are reconstructions of real sales, so each txn is
  priced with the **KINS/USD at the trade's actual minute**, not a daily average. `build_price_series()`
  pages GeckoTerminal **1-minute** candles backward across the whole range (paced ~2.6s/req to dodge
  429s; ~45 pages for a month) and lays an **hourly** baseline underneath for any older span the minute
  feed can't reach (1m only returns ~18h per 1000-candle page; 1h covers the month in one request).
  `price_at()` then snaps each txn to its **nearest candle** (its own minute where reachable, ±30min
  hourly before). The resolution used is recorded in `meta.price_resolution`. This precision matters
  enormously — KINS pumped ~300× over the captured month, and moves several % intraday during spikes.
- Run `python3 build_market_dataset.py` (`--src`/`--out` to override; takes ~2 min for the paced
  1-minute price download). Captured month 2026-05-22 → 06-22: 95,716 txns → marketplace **118.9M
  KINS / ~$528k** (minute-priced), sinks **5.97M KINS burned**, totals **~$582k volume / 4,089
  buyers**. Ship it out of band (DBs are gitignored; the hosted data volume is never touched by git
  deploys): `scp market.db root@<host>:/opt/kintara-data/market.db`.

---

## API routes

- `GET /` — the dashboard (single HTML page).
- `GET /favicon.ico`, `/favicon.png`, `/apple-touch-icon.png`, `/site.webmanifest` — site
  identity assets for the hosted KinScan brand. The favicon/apple icon use the real
  Kintara gold HUD icon through the same disk cache as `/icon/gold`.
- `GET /api/status` — poller state, tracking-since, row count.
- `GET /api/market-watch` — whole-market on-chain stats for the **Market Watch** home page,
  aggregated live from the compact `market.db` (~95k treasury-derived txns, each priced in USD at
  the KINS/USD of the trade's own minute). **Trading volume = the `marketplace` category ONLY;** the
  **spin wheel** (the `sink` category — the only txns that burn ~50% of the stake) is gambling, not
  trading, so it's reported separately and excluded from every market total. Returns `{ok,
  generated_at, ts_min, ts_max, kins_price, treasury_owner,
  market:{txns, volume_kins, volume_usd, fees_kins, unique_buyers/sellers/traders},
  spinwheel:{spins, wagered_kins, wagered_usd, burned_kins, treasury_kins, unique_spinners},
  payout:{n, kins, usd}, daily:[{date, market_txns, market_kins, market_usd, spins, spin_kins,
  spin_usd}], top_trades:[…marketplace only]}`. Returns `503 {ok:false}` if `market.db` isn't present
  (page shows a "dataset not loaded" placeholder). Reads are read-only and fast (~ms), so no caching.
- `GET /api/market-caps` — every item ranked by **market cap** = in-world supply × per-unit USD floor
  (`item_floors()`'s `usd_equiv` = lesser of USD / gold→USD). `{ok, items:[{item_type, label, category,
  supply, floor_usd, market_cap}] (desc), total_market_cap, players, kins_price}`. Items missing a live
  floor or a world-supply number are omitted (can't be valued). Drives the Market Watch market-cap
  leaderboard.
- `GET /api/items` — distinct item types, categories, **labels** map, gold_item.
- `GET/POST /api/settings` — get/set `gold_item`.
- `GET /api/arbitrage?direction=&min_qty=&gold_item=` — the arbitrage table. Returns `gold_rate`,
  `kins_price` (for the kins/gold column), and per-row `per_gold`/`kins_unit`/`gold_unit_usd`/`margin`/
  `profit`/etc. `min_qty` = min-stack filter (prefers listings ≥ N items; falls back to normal cheapest
  if none that big, so single-unit items like mounts are never hidden). (`fee` is accepted but unused —
  the sell-fee control was removed.)
- `GET /api/mispricing?gold_item=` — the **Collectables scan** (3rd arbitrage mode, CMP markets).
  For each CMP item, compares the **cheapest current buyable listing** (per-item, whichever currency
  is cheaper, gold/token both converted to gold at the live rate) against a **gold-anchored, recency-
  & volume-weighted fair value**: every recent sale (≤21 days, both currencies) is re-expressed in
  **gold** at the gold price on its own day (`gold_daily_usd()` = our `gold_price` series + kintaragold
  backfill), weighted by `units_sold × 0.5^(age/7d)`, then carried to today. This fixes the low-liquidity
  staleness problem (an item that last sold weeks ago is re-valued at today's rate, not compared in raw
  old USD). Only the **most recent ~50%** of each item's sale records are used (records sorted by date,
  keep `recs[len//2:]`; sparse items with <4 records keep all) so **launch-day outlier prices don't skew
  the value**. Returns per row `{item_type, category, buy_gold, buy_ccy, fair_gold, spread_gold, margin,
  vol_window (cumulative units across the kept days), trade_days:[{date,sales}] (kept days, newest first),
  last_sale, last_age, conf}` (sorted by `margin` desc), plus `gold_rate`, `kins_price`. `conf`
  (high/med/low) = sale recency + kept-window volume. **Farmable
  commodities** (`FARMABLE_CMP` = wolf/dragon/whale + a high-volume/low-price heuristic; tested on the
  full window) are excluded —
  they're USD/utility-anchored, not collectibles. The frontend converts `*_gold` to the chosen display
  unit (KINS/Gold/Both). `compute_mispricing()`.
- `GET /api/kins-price` — `{usd}` live KINS/USD (`current_kins_usd()`, cached ~2 min). Drives the
  header price pill (polled ~30s).
- `POST /api/refresh-stats` `{items:[...], currency}` — force-refresh sales cache for the
  shown items (used by the Arbitrage tab's auto-refresh + "Refresh shown").
- `GET /api/current` / `GET /api/removed` — live listings / removed-listings (filters: `q`,
  `currency`, `item_type`, `category`, `sort`, `limit`). `q` matches itemType, seller, OR
  in-game label. (`/api/removed` is retained for internal/debug use; the **Sales feed** tab no longer
  uses it — see below.)
- `GET /api/sales-feed?limit=&item_type=&currency=&category=&q=` — **actual completed sales** from
  `sales_events`, newest first (cancellations excluded). Each row now carries the **real stack `qty`,
  `total` paid, per-unit `price`, `seller`, and `listing_ms`** (time the listing sat before it sold) —
  recovered by matching each sale to the listing that vanished. Fully matched rows are ordered ahead of
  unmatched synthetic rows so the UI prefers the normal `qty x total paid + seller + time-listed` display.
  Drives the **Sales feed** tab and the per-item **Recent sales** panel in the Index expand.
- `GET /icon/<item_type>` — real item art, lazily downloaded + disk-cached; 404 → UI
  falls back to a category emoji.
- `GET /api/sales-history?item_type=&currency=gold|token|kins|goldstd` — daily price series from the
  archive. `gold`/`token` return that single currency's daily series. `currency=kins` returns **one blended
  $KINS series combining BOTH gold and token (USD) sales**: each day, token sales convert as
  `avg_usd / kins_usd(date)` and gold sales as `avg_gold × gold_usd(date) / kins_usd(date)`, then the
  two are averaged **weighted by units sold** (so a day priced in either or both currencies yields a
  single all-market $KINS price; equivalently a units-weighted USD blend ÷ `kins_usd`). `currency=goldstd`
  (**"Gold Standard"**) is the same units-weighted blend valued in **gold** instead: gold sales as-is,
  token sales as `avg_usd / gold_usd(date)` (the gold that USD would have bought that day) — i.e. the USD
  blend ÷ `gold_usd`. Both use `kins_daily_usd()` (GeckoTerminal daily close, ~5 min) and `gold_daily_usd()`
  (our `gold_price` series + kintaragold backfill), nearest-prior carry-forward. `kins` adds a `vs_token`
  block (item blended-USD %, KINS USD %, net % in $KINS) to separate item alpha from token beta; `goldstd`
  has no `vs_token`.
- `GET /api/item-listings?item_type=` — up to the **5 cheapest buyable (non-reserved) live listings per
  currency** (gold + token/$KINS) for one item, each with per-unit price, stack size, seller; plus the
  current `gold_rate` and `kins_price` for cross-conversion. Drives the Index "Cheapest live listings" panel.
  Each listing also carries **price-memory `badges`** (`cheapest-ever`/`cheapest-7d` vs the
  `orderbook_snapshots` floor history, `below-sale-avg`/`above-fair`/`likely-overpriced` vs
  `recent_fair_usd()`), and the response includes a `memory` block (`floor_ever/7d/1d_usd`, `fair_usd`).
- `GET /api/scorecard?item_type=` — the **Item Scorecard** payload (the stock-page header in the
  Index expand). Current floor in **gold/USD/KINS**, `change` (24h/7d/30d % vs `floor_usd_close` from
  the rollup), `velocity` (units/day, 7d), `listed_supply`, `sellers`, `liquidity` (0–100 exit score =
  `liquidity_score()`: velocity+depth+sellers+recency), `volatility`, `time_to_sell` (median/p25/p75
  minutes from listing lifecycle, `removed_at−created_at` for listings created after tracking began,
  plus a `sold_ratio` = sale-event units ÷ removed units calibration), `fair_usd`+`verdict`
  (cheap/fair/expensive vs gold-anchored `recent_fair_usd()`) + `confidence`, `last_sale`. Reads the
  durable metrics + a couple of cheap indexed live queries — never aggregates raw snapshots.
- `GET /api/floor-history?item_type=&range=24H|3D|7D|30D|ALL` — floor-price history for the floor chart.
  Per point `{t, usd, gold, kins}`: **`gold` = the ACTUAL cheapest gold-currency ask** (not a conversion);
  **`usd` = the cheapest RAW USD listing only** (a real typed price — NOT gold converted to USD, so no weird
  decimals; `null` when the item isn't listed in USD); `kins` = the cheapest acquisition cost (whichever of
  USD or gold→USD is cheaper) ÷ KINS price **at that tick's time**
  (intraday, via `kins_intraday_ms()` + `interp_gold()` — NOT a single daily close, otherwise the $KINS
  line would just be a scaled copy of the USD line within a day; falls back to the daily close for old
  pre-snapshot points or if GeckoTerminal is unreachable). Recent points from `orderbook_snapshots`, older
  from `item_daily_metrics`. The frontend graphs cheap gold floors as **items-per-gold**.
- `GET /api/item-meta?item_type=` — Index info-panel metadata for one cosmetic/mount/pet: sourcing
  channel + **availability cadence** (weekly 7d vs daily 24h, from the game's shop-payload shapes),
  **ride speed** (mounts), special-feature flavor (`ITEM_DESC`), a **cheapest-ever-traded** source/floor
  proxy (exact rotating shop gold prices are server-side/auth-gated, not public), plus the derived
  availability window + supply status from `sales_daily` (`item_index_meta()`).
- `GET /api/sales-summary?window=1|7|30` — per-item totals over the window: sales,
  sales-weighted avg gold/USD, `$KINS`, `ref_day`. Each item also carries **`world_supply`** (total
  units across all players, from `world_item_supply()` — the Index "In world" column) and
  **`market_cap`** (= `world_supply` × the per-unit USD floor `usd_equiv`); the payload adds
  `world_players` + `world_generated`. Also returns the **current floor** per item
  (`floor_gold` = actual cheapest gold ask, `floor_usd` = cheapest USD-equivalent, `floor_kins`) via
  `item_floors()` — drives the Index tab's floor columns. `item_floors()` applies the **bulk-material
  rule**: for `material` items it ignores listings smaller than `MIN_BULK_QTY` (1000), so a "100 wood for
  a pittance" dump doesn't set the floor. The item set is the union of `listings`, `sales_daily`, and
  `sales_events`, so an item discovered only through official sales stats can still appear in the Index.
- `GET /api/merchant-history` — each campaign resource's donation **% over time** (and the overall %),
  from `merchant_snapshots`. Drives the click-to-expand chart on each Merchant resource bar.
- `GET /api/player?name=&wallet=` — one aggregated **player profile** (the Player tab). From our DB
  (keyed on `seller_name`, case-insensitive): **marketplace earned** (sell side — sales count, units,
  gross in gold/USD/$KINS, avg sale), **top items sold**, **recent sales**, **active listings / inventory
  market value + category mix**, and **first seen**. Active-listing value is market-anchored
  (`active_market_unit_usd()` prefers the cheapest buyable listing from another seller, then recent fair
  value, then raw floor/ask as a last resort) because public listings can contain joke asks like 12k wood
  for 112k gold. Plus **property owned** (live `fetch_property_status()` match
  on owner name). Echoes `wallet`; the on-chain KINS stats load separately from `/api/wallet-onchain` so
  the DB profile renders instantly. All data is public/on-chain — uninvasive by design.
  > Marketplace treasury/fee wallet (identified on-chain — it skims ~5% off every trade, present in 137/140
  > of a known wallet's KINS txns): **`4zW4zuZb9rXpvw3cTYyGoQ2iHTtG9E17YpdeNUbwuQVt`** (`KINS_TREASURY_DEFAULT`,
  > env-overridable). A tx where it's a KINS participant = a marketplace buy/sell.
- `GET /api/wallet-onchain?wallet=` — **all-time on-chain KINS stats** for a wallet (the Player page's
  on-chain panel). Reads Solana via plain JSON-RPC (`_sol_rpc`, no `solders`): finds the wallet's KINS
  token account(s), pages their signatures, decodes each tx's KINS delta (`_kins_delta` from
  pre/postTokenBalances). Returns **earned/spent KINS + USD** (priced at each tx's day), **net**, tx count,
  first/last activity, recent transfers, and a **marketplace earned/spent split** when `KINS_TREASURY` is
  set. The **KINS mint auto-resolves** from the GeckoTerminal pool (`kins_mint()`) — no config. **Captures
  the FULL history, not a fixed cap:** the scan backfills incrementally (a forward pass for new txns + a
  backward chunk of `ONCHAIN_MAX_SIGS` older sigs per call) and persists progress + cursors in
  `wallet_onchain`, so over a few loads (the Player page auto-repolls until `_done`) it reaches the wallet's
  first KINS tx. The only depth limit is the RPC node's history — the default public node prunes old
  `getTransaction`s and rate-limits, so **set `SOLANA_RPC` to an archival Helius/QuickNode URL to get
  everything**. Cached ~10 min once `_done`. Wheel spins + Kintara Club still need their program addresses
  (`PLAYER_PAGE_PLAN.md`).
- `GET /api/sales-audit?days=` — self-check of the sales feed vs the **hard in-game `/stats` count**:
  `in_game_total`, `logged_total`, `missing_total`, `item_days_behind`, and the `gaps` we're behind on.
  Drives the Sales-feed coverage line; the backfill loop keeps `missing_total` ~0.
- `GET /api/merchant-events` — **merchant RESTOCK timestamps** (campaign-fill / gold-stock-refill), last
  ~90 days, from `merchant_events`. The frontend overlays these as **gold markers** on the floor, sales and
  gold-price charts (a restock is a market-wide shock worth anchoring for research).
- `GET /api/gold-history?range=4H|1D|3D|7D|14D|ALL` — gold/USD + KINS/gold series (our
  `gold_price` series spliced over the kintaragold fallback).
- `GET /api/liquidity?item_type=&gold_item=` — buy-side liquidity depth for one item, in **USD
  per 1000 units**. Buckets all active, buyable listings into **$0.10 (per-1000) price tranches**
  (token/KINS listings use USD directly; gold listings convert at the current gold rate, excluded
  if no rate). Returns `markers[]` (`price` = tranche upper bound, `cum_units`, `tranche_units`,
  `listings`), `total_units`, `best_per_1000`, `gold_units`/`token_units`/`excluded_gold_units`.
  The axis caps near the market (~3.5× the cheapest price, or the median, whichever is larger;
  ≤50 markers) so overpriced tail stacks don't squash the actionable range. Drives the materials
  liquidity chart in the Sales-History item expand.
- `GET /api/servers` — normalized server list + rollup (`open/full/queued/queue_total`) for
  the top status bar. Also includes per-server **`boss`** (players in the new Venomweaver boss area right now,
  `null` until measured) plus top-level **`boss_region`** (the resolved spectate key) and **`boss_total`**,
  from `BossCensus`.
- `GET /api/live?shard=1|2|3|4` — live world roster for a shard (from the spectate
  WebSocket): `online_total`, `players[]` (id, name, x, z, level, held item, badge, hp,
  outfit), `connected`, `err`. Opens the socket on first hit, keeps it warm while polled.
- `GET /api/live-search?q=<name>` — find a player by name **across all 12 servers**. Opens (and keeps
  warm) a spectate socket on every shard and returns current name matches as `results[]`
  (`{shard, id, name, realm, level}`, exact-name matches first), plus `connected` = how many shard
  sockets are open and `ready` = how many shards have a populated roster yet. Rosters fill over ~20s
  after a socket opens, so the client polls this quickly (~750ms) until `ready == 12` or a match is
  found; the extra sockets idle-close ~75s after the search stops. **Be gentle** — this fans out to
  all 12 read-fanout sockets, so it's an on-demand action, not polled.
- `GET /api/property` — every mansion/house/trailer: owner, lock state, real map
  coordinates, plus a marketplace cross-reference (the owner's live listing count + total
  ask USD) and how many properties that owner holds.
- `GET /api/merchant` — traveling-merchant tracker **and** cost calculator in one payload:
  `state` (five donation resources: wood, stone, coal, cooked fish, **metal** current/goal/**pct**,
  overall %, mode, gold stock) and `calc` (`gold_rate` + the current `MERCHANT_TRADE_COST`
  gold-trade recipe with per-ingredient **order-book ladder** — cheapest-first `[unit_usd, qty]`
  levels across both currencies, gold converted at the rate). The client walks the ladder so larger
  mints cost more as cheap listings run out (**liquidity-aware**), reports avg & marginal $/gold,
  and caps the mint to the listed liquidity. Also returns `forecast` (from `merchant_snapshots` via
  `merchant_forecast()`): **completion ETA** (`eta_hours`/`eta_iso`, when the donation phase finishes =
  gold-trade unlocks, from the recent overall-% donation velocity), the **bottleneck** resource
  (finishes last at current donation rate → its demand is about to spike), per-resource
  `velocity_per_hr`/`eta_hours`/`pressure` (frac of goal/hr), the **break-even gold price**
  (`break_even_gold_usd` = current mint cost; `profitable` vs the live `current_gold_usd`), and
  `profit_history` (mint profit $/gold over the campaign). `null` until the snapshot loop has logged
  some history.

---

## Frontend tabs (all in `INDEX_HTML`)

Game-styled aesthetic site-wide (navy gradient, gold **Cinzel** headings, **Fredoka**
body, gold pill tabs, rounded panels). Public branding is **KinScan** with a Kintara
gold-icon brand mark, browser title/description metadata, favicon/apple icon, and web
manifest for a more polished hosted-site shell.

**Market Watch is the first tab and the default landing tab** (`TAB="market"`) — the splash/home
page (see below). The Index (per-item scorecards) is the flagship intelligence product and sits
second. The tabs below are numbered by topic, not bar order.

0. **Market Watch** (home / splash) — `loadMarket()` → `/api/market-watch`. Whole-market on-chain
   overview built from the distilled treasury dataset. **Trading volume is marketplace-only** —
   the paid **spin wheel** (the 50%-burn sink) is gambling, not trading, so it's pulled out into its
   own infographic and excluded from the headline numbers. Sections: a glowing hero with **trading
   volume** (animated count-up) over marketplace `$KINS` + trade count; stat cards (trading volume,
   marketplace trades + avg trade size, treasury fees, unique traders); an interactive **daily
   trading-volume SVG chart** (marketplace only, hover tooltip per day); a **🎡 Spin wheel
   infographic** (`mwSpinwheel()`: spins, $KINS/USD wagered, unique spinners, and a split bar showing
   ~50% burned / ~50% to treasury); a **biggest-trades** table (Solscan links); and a **🏆 market-cap
   leaderboard** at the bottom (`mwLeaderboard()` ← `/api/market-caps`) — every valued item as a
   horizontal bar (icon + name, gold bar ∝ market cap, $ value on the right, top-3 glow). Game-styled,
   all in `.mw-*` CSS. Fetches `/api/market-watch` + `/api/market-caps` in parallel. Refreshes gently
   (60s); historical so it rarely changes. Placeholder if `market.db` is absent; quiet auto-retry on a
   transient feed blip.

1. **Arbitrage** (landing) — per-item table: `items/$` (green; shows `$X.XX` per item for
   items >$1 each), `per gold` (gold), **kins/gold** ($KINS to assemble 1 gold's worth, green when
   below the live market KINS/gold), margin, profit, `sold KINS/gold · <day>`. The rate line shows
   `1 gold = $X = N $KINS` as the kins/gold benchmark.
   Controls: direction, item filter, profitable-only, sold-today-only, **min stack**,
   category chips, "Refresh shown". Hovering the `items/$` cell shows a card with the exact
   cheapest listing (stack/price/seller). **Hovering the `sold` cell** (far-right column) shows a
   second card (compact, styled like the deal card): the last **3 days**, each listing that day's
   **units sold + avg sale price** in the currency you'd sell into (`showSold()`, fed by
   `/api/sales-history`, cached per item+currency). Auto-refreshes the visible rows ~every 7s.
   - **Mode toggle (3-way `.seg`):** *gold → KINS* / *KINS → gold* (the two cross-currency arbitrage
     directions, for commodities — the arrow implies buy/sell) / **Collectables** (a third mode, below).
     No sell-fee input anymore. The "Refresh shown" button is just a **↻** icon.
   - **Collectables mode** (`loadMispricing()`, `/api/mispricing`) — built for **CMP** (cosmetics/
     mounts/pets). Columns: item (with a **confidence dot** — green/amber/red for fresh+liquid →
     stale/thin) · **price** (cheapest live listing) · **fair value** (gold-anchored, recency- & volume-
     weighted, carried to today, using only the recent ~50% of sales — see Data-sources note on CMP;
     rendered **gold-tinted** to set it apart) · spread · margin · profit · **volume** (cumulative units
     across the recent trading days; hover a row's value for the per-day split) · **▸** (far-right
     dropdown arrow). Sorted by **largest margin** (price error). A **KINS / Gold / Both** toggle is purely
     a display-unit choice (the comparison is one gold-anchored number); **Both** shows $KINS over gold and
     a `via g/$K` tag for which currency the cheapest listing is in. Category chips default to CMP via a
     separate `state.mpCatOff` set; farmable commodities are excluded server-side. The **▸** expands an
     inline dropdown reusing the Index tab's chart + item-info + cheapest-listings panels
     (`mpRenderExpand()`); auto-refresh freezes while a dropdown is open. Because the flicker-free morph
     reuses DOM nodes, the render **clears stale per-cell mouse handlers** (`td.onmouse*`) so an arbitrage
     deal/sold hover card can't bleed into the Collectables table.
2. **Live listings** — current active listings (item **icon** + name · seller · qty · price · listed; the
   per-item column was removed). **Sales feed** — **actual completed sales** (`/api/sales-feed`): item
   **icon** + name · **qty sold**
   (real stack size) · **total paid** · **seller** · **time listed** (how long it sat before selling) ·
   when. Each sale is matched to the listing that vanished, so "13,251 stone for 1g" reads correctly
   instead of the old misleading "5 units". Cancellations excluded.
3. **Index** (was "Sales history") — game "index" layout: category **sidebar**, **Today / 7d / 30d**
   window selector, a **sort dropdown** (Most/Least sold · **Most in world / Rarest in world** by total
   world supply · Cheapest/Most expensive by the $KINS floor · Newest/Oldest added by first-sale date —
   `first_sale` from `/api/sales-summary`; **Market cap** by `market_cap`; cheapest/newest are sort-only),
   columns ITEM · SALES · **IN WORLD** · **FLOOR GOLD · FLOOR USD · FLOOR $KINS · MKT CAP**. **IN WORLD**
   = total units of that item across every player (kintara.gg world index, `world_supply`; note shows
   "across N players"). **MKT CAP** = in-world supply × USD floor (lesser of USD / gold→USD), gold-tinted.
   Floors are the live cheapest price per item, from `item_floors()`. (On phones Sales + both gold/USD
   floor columns are hidden to keep In-world + $KINS + Mkt-cap legible.) Cheap commodities show **items-per-gold** (e.g. `24k/g`) instead of a tiny gold fraction, and
   **material/food/potion** show **USD/$KINS per 1,000**. Click a row → expands to a full **Item
   Scorecard** (stock-page) view:
   - **Scorecard header** (`/api/scorecard`, `loadScorecard`/`scorecardHTML`): the floor in
     **gold/USD/KINS** with a **cheap/fair/expensive verdict** pill, **24h/7d/30d %** change, and a
     stat strip — **liquidity** (0–100 exit score, deep/ok/thin), **sells/day**, **time to sell**
     (median, hover for sample size + sold-vs-cancelled ratio), **listed supply**, **sellers**, **volatility**.
   - **Floor price chart** (`/api/floor-history`, `loadFloorHistory`/`floorChartHTML`): the cheapest
     listing over time — like the Gold-price chart but per item — with a **gold / USD / $KINS** unit
     toggle, **24H/3D/7D/30D/ALL** ranges, and a **crosshair hover card** (`attachFloorHover`) showing
     the point's date and floor in all units (incl. gold/ea + gold/1k). **Gold** is the ACTUAL cheapest
     gold-currency ask; when it's under 1g/item the chart graphs **items-per-gold** (line rises as it gets
     cheaper). **$KINS** is whichever is cheaper at the time (token-USD vs gold→USD). For
     **material/potion/food** the scorecard floor and USD/$KINS axes quote **per 1,000 units** — a single
     unit is a fraction of a cent and nobody trades one (the Solana fee dwarfs it).
   - Then the existing **line chart** with a 4-way currency toggle
   **USD ($KINS) ⇆ vs $KINS ⇆ Gold ⇆ Gold Standard**, hover card, + a cumulative stats panel.
   **Gold Standard** (`currency=goldstd`) values *every* sale in gold — gold sales as-is, USD/KINS sales
   converted to the gold they'd have bought that day — blended units-weighted into one all-sales gold
   series (vs the plain **Gold** option, which is gold-denominated sales only). Same chart/stat code
   (gold-formatted), used in both the Index expand and the Collectables dropdown.
   - **vs $KINS** shows the item's **full market price in $KINS per day**, blending **both gold and
     token (USD) sales** into one units-weighted series (`/api/sales-history?currency=kins`; gold sales
     converted via `kins_per_gold`, USD sales via the KINS price), so you can see whether it's real
     **alpha** (line rises) or just **token beta** (flat/falling while KINS pumps). A headline banner
     reports the item's (blended-USD) return vs KINS's own USD return and the net % in $KINS terms
     (green = outpacing the token, red = lagging). Built for treating CMP as investments.
   - **For `cosmetic`/`mount`/`pet` items**, an **item-info panel** (`/api/item-meta`) shows how it's
     **sourced** + the **availability cadence** (code-confirmed from the game's shop-payload shapes):
     mounts = **alchemist weekly drop** (one mount, 7-day window); cosmetics = **cosmetic shop daily slot
     (24h) OR weekly bundle (7d)**, or the **$5 paid spin**; pets = **pet shop weekly** (3/week, 7d);
     Red Aura = **daily free spin** (~1 in 22); wolf = **world-tamed** (always available). Exact shop gold
     prices rotate server-side (not public), so instead of a fake fixed price it shows **cheapest ever
     traded** (USD) as a source/floor proxy. Plus **ride speed** for mounts (+10%…+50%, from
     `*_MOUNT_SPEED_FACTOR`), special-feature flavor, the **first→last sale window** (days traded), and a
     derived **supply status** (flooding / tapering / dried-up-resale) from the sales archive.
   - **Every** item expand also shows a **Cheapest live listings** panel (`/api/item-listings`): up to the
     **5 cheapest buyable (non-reserved) listings per currency** — **gold** and **$KINS** side by side. Shows
     the **actual listing price** (the stack total, e.g. stone *1g ×13,251*, not the 0g per-unit), **sorted by
     cheapest per-unit**, showing **what you pay (the listing total) and how many you get** — not the
     tiny per-unit number — with stack size, seller, cross-converted value (gold→USD, USD→$KINS), and a
     **per-1,000** figure for bulk commodities. Each listing is stamped with **price-memory badges**
     (`cheapest ever`/`7d low` vs the floor history, `below sale avg`/`above fair`/`overpriced` vs recent
     fair value) so Live Listings beats the in-game market. For **material** items only listings ≥1000 units
  are shown (tiny dumps filtered). Each row shows **price · quantity · $KINS value** (no per-unit/per-1k
  clutter). Many items have <5; that's fine.
   - **Every** item expand also shows a **Recent sales** panel (`loadRecentSales()`, `/api/sales-feed`):
     the item's latest **actual completed sales** — **qty × total paid**, who sold it, **how long it sat**
     before selling, relative time — newest first, cancellations excluded. Empty until real sales are
     observed (it logs going forward).
   - **For `material` items**, the expand still shows the buy-side **liquidity depth chart**
     (`/api/liquidity`): cumulative units available by USD price per 1000, in $0.10 tranches.
4. **Gold price** — kintaragold-style chart, now driven by our own `gold_price` series.
   Toggle **Gold (USD) ⇆ KINS/gold**, ranges 4H/1D/3D/7D/14D/ALL, % change pill, hover
   card, auto log-scale for extreme ranges.
5. **Merchant** — traveling-merchant desk. Left: progress tracker (overall % + five donation-resource
   current/goal bars **with a % next to each item**: wood, stone, coal, cooked fish, **metal**; mode
   badge donation/gold-trade; gold stock). **Each resource bar is clickable** → expands an inline chart of
   that resource's donation **% over time** (`/api/merchant-history`, `drawMerchResChart`, with a gold-chart
   style crosshair hover via the reusable `svgHover()`). Right: **cost
   calculator** — the current gold-trade recipe
   from `MERCHANT_TRADE_COST` priced **liquidity-aware** (walks the live order book, so each additional
   gold costs more as cheap listings are consumed), with a "mint N gold" input, avg & marginal $/gold,
   craft cost vs gold value, profit/margin, and a cap when the mint exceeds listed liquidity.
   Auto-refreshes ~30s (skips while the mint field is focused). Below the tracker + calculator, a
   full-width **Merchant Forecast** desk (`forecastHTML`, `/api/merchant` `forecast` block): **completion
   ETA** (when the donation phase finishes / gold-trade unlocks), a **bottleneck** callout, a per-resource
   **demand-pressure** list (donation rate/hr + time-to-goal + a pressure bar), a **break-even gold price /
   live gold price / mint-profit-per-gold** economics row, and a **mint-profit-over-the-campaign** sparkline.
   Empty until `merchant_snapshots` has logged some history.

6. **Live World** — roster of who's online, per server, **grouped by area** (Overworld, Fishing
   Pond, The Shores, Eldergrove, Frostmere, dungeons…). Selector across **all 12 servers** (labeled
   with their in-game names from `/api/servers`) + global online count; ~75–80 named players per
   server (the sweep fills over ~20s). A **player search box** (`#lwsearch`, `state.liveSearch`)
   filters the roster on the current server by name as you type (tagging each match's area). Hitting
   **Search all servers** (button or Enter) runs an **all-server lookup** (`searchAllServers()` →
   `/api/live-search`): it opens a socket on every shard and polls until the rosters fill (~up to 18s,
   showing "swept N/12 servers"), then **auto-switches to the server the player is on**, selects them,
   loads their card, and scrolls them into view — or reports they're not on any server. Click a
   player to **expand a dropdown** (sales-history style): if they're on the overworld it shows a
   zoomed crop of the world map centred on them with a single pulsing **"you-are-here" dot**; if
   they're in any **instanced realm** (The Shores, Pond, Arena, Whisperwood, Frostmere, the Wilderness
   zones, Mine, Spider Lair, Shack — every realm now has a `REALM_MAPS` entry) it shows that realm's
   dedicated top-down map (`/shores.png` + `/maps/<slug>.png`, from `MapImages/`) with **every** character
   then in that realm plotted at their exact `(x,z)` — drawn as their generated avatar icon with a white
   glow (the selected player larger/brighter). Plus their full card: SVG avatar,
   level, held item, badge, HP, area, exact coords, and a marketplace + property cross-reference.
   Auto-refreshes ~2.2s; the map only renders for the expanded player (no laggy always-on radar).
   The overworld dot is isometric-projected and **approximately** placed (eyeballed calibration
   in `worldToMap()`); exact x/z is shown.
7. **Property Map** — a **tilted 2.5D estate**, drawn client-side as SVG (`renderProperty()`):
   pitched-down, axis-aligned (a rectangular box, **not** an iso diamond — same slant as in-game,
   rotated so the pond entrance is toward the bottom). Every mansion/house/trailer is an extruded,
   gabled-roof building at its real grid footprint (`PROPERTY_PLOTS`), drawn back-to-front so nearer
   ones overlap; **hover glows white**, locked = red outline, selected = gold glow. Click a building
   for an owner card (name + id, owned/for-sale,
   locked/open, how many properties they hold, their live marketplace listing count + value,
   and a "view their listings" jump). Polled ~30s.
8. **Player** — a per-player profile (`/api/player`, `loadPlayer`/`renderPlayer`). Type a player **name**
   (+ optional **wallet**) → header (name, seller id, first seen), stat cards (**marketplace earned** in
   USD/$KINS, items sold, avg sale, gold earned, **active listings + ask value**; a *spent (buy side)* card
   marked pending on-chain), then panels for **top items sold**, **recent sales**, **active listings**, and
   **property owned**, plus a **character card** (`/api/player-live`) — their rendered avatar with
   cosmetics, level, held item, badge, HP and area, swept from the spectate streams; shows **live** when
   online, else their **last-seen** look (from the `player_seen` cache) tagged with when/where. An
   **on-chain panel** is a clear pending state (KINS spent/earned, wheel spins, Kintara Club, wallet
   verification) until the Solana program addresses are wired — see `PLAYER_PAGE_PLAN.md`. All
   public/on-chain data — uninvasive.

The bubble also shows a **🕷 boss count per server** (players in the new level-20 Venomweaver boss area
right now) and a **🕷 N fighting** total in the header — from `BossCensus` (see below).

Site-wide, in the header: a compact **server status icon** (shows server count + total queue)
that expands into a floating bubble listing every server's population + queue. Polled ~30s,
closes on outside click. Next to it, a **live `$KINS` price pill** (`#kpx`, `/api/kins-price`,
polled ~30s, value-flashes on change). Also in the header: a **⌘K command palette** (search items /
sellers / tabs → jump straight to filtered Arbitrage) and a **live "updated Xs ago"** freshness
indicator (green light + relative time — the old "N active · rows tracked · since …" line was removed;
the `#status` slot now only surfaces a poller error if one occurs).

### Premium QoL layer (frontend polish, all in `INDEX_HTML`)
A site-wide quality-of-life pass that sits under every tab:
- **Flicker-free re-renders:** `#view` and the listing `#ltable` have their `innerHTML` setter
  replaced (`defineMorph()`) with a small **DOM-morph** engine (`morphChildren`/`morphNode`) that
  updates text/attributes **in place** instead of nuking and rebuilding the DOM. Polling tabs no
  longer flash or lose scroll/expanded-row state. Wrapped in try/catch → falls back to native
  `innerHTML` on any error. Skips morphing into `<canvas>` (chart code owns it) and live
  form fields (`input`/`select`/`textarea`).
- **Value flash (#8):** when a morphed number changes, the enclosing `.flashable` cell pulses
  green/red (up/down). Applied to the arbitrage price/spread/margin/profit cells, the gold-rate,
  and the Live World online count. Opt-in via the `.flashable` class so time columns don't flash.
- **Design tokens (#4):** `:root` spacing scale (`--s1..--s6`), two radii (`--r1/--r2`), three
  elevations (`--sh1/2/3`). **Tabular numerals** site-wide (`body{font-variant-numeric:tabular-nums}`).
- **Color discipline (#3):** table `th` recede to muted (gold reserved for currency/brand;
  green/red lead the data).
- **Command palette (#7):** `⌘K`/`Ctrl-K` (or the header Search button) → fuzzy search over items
  (`state.items`/labels) + sellers + tabs, arrow/enter nav, `esc` to close.
- **Helpers:** `abbr()` (1.2k/3.4M with full value on hover, #13), `relAbs()` (relative time +
  absolute on hover, #17), `freshness()` (aging/stale badge for listings >1h old, #20),
  `sparkline()` (inline SVG; used for the 24h gold-rate trend in the arb header, #14), a single
  floating `data-tip` **tooltip** engine (#16), `skel()` skeleton loaders (#9), `fadeView()` tab
  cross-fade + row-expand animation (#10), and a 1s **"updated Xs ago"** ticker (#12).
- **Reduced motion (#11):** `@media (prefers-reduced-motion: reduce)` neutralizes animations;
  the JS also reads `RM` to skip value-flashes.

---

## Known caveats (don't "fix" these — they're data limitations)

- `/stats` rounds **gold** prices to 2 decimals, so AVG GOLD / the gold sales chart read
  `—`/flat for cheap materials (wood/coal). USD columns are the meaningful ones.
- **Dates are UTC** (`YYYY-MM-DD` strings). Chart date labels must format with
  `timeZone:'UTC'` or they shift a day in western timezones.
- `removed`/`active=0` ≠ guaranteed sale (that's why the **Sales feed** is now driven by `sales_events`,
  detected from kintara's own completed-sale counter, not by listing removals).
- The actual-sales feed is **per-sale** (one row per completed sale, matched to the vanished listing for
  the real stack qty/seller/duration). Detection is **multi-signal**: an instant path (run every listing
  poll, ~90s) logs reservations-that-completed and collectibles-that-vanished-without-relisting right
  away; a count-based `/stats` reconciler catches everything else (including the first sale of a day) and
  tops up to the authoritative count, deduped against the instant path. `/stats`-only sales still lag the
  cold-poll cadence (~≤7 min for quiet items). **Recent `/stats` rows are never startup-baselined away**
  (`SALES_RECENT_BASE_DAYS`, default 3), because losing a recent rare sale is worse than showing an
  estimated row. When we can't match a confirmed sale to a captured listing it shows as a `qty —`/`~est`
  row (price known, stack unknown). Gold sale prices inherit the `/stats` 2-dp rounding (coarse for cheap
  goods); USD/$KINS are exact. **Rare over-count:** if a collectible was truly delisted (not sold) and
  never relisted, the instant path counts it once — acceptably rare vs the cost of missing real sales.
- Profit is an upper bound (no buy orders to price against; you'd undercut to sell).
- KINS pumped ~60× since launch, so KINS/gold over ALL legitimately spans ~45,000→~340
  (auto log-scale handles it — not a bug).
- Item display names + icons + the gold-USD series are **ripped from kintara.gg / 
  kintaragold.xyz**; if those sites change structure, the regexes/asset paths may need
  updating (each returns a clear error / falls back rather than crashing).
- HTTP 403/429 from kintara → raise `--interval` or tweak the User-Agent in
  `fetch_all_active()`. **Timeouts on deep listing pages are common and harmless:**
  `fetch_all_active()` retries each page up to 3× and, if one still fails, returns the pages it already
  got with `complete=False` (never raises); `reconcile()` then keeps last-good data instead of marking
  anything gone. The poller tracks `last_success`/`fail_streak`, and the header only shows a quiet
  "reconnecting to kintara…" note once it's **persistently** failing (≥3 misses AND no successful update
  in >4 min) — transient blips self-heal silently and are not surfaced.
- **Live World** shows players in the spectator's *area of interest* (near the world hub),
  not all `onlineTotal` players — the game only streams nearby avatars to a spectator. The
  count is global; the radar/roster is the visible crowd. Switching shards opens a fresh
  socket. If `websockets` isn't installed, the tab shows a one-line install hint. The
  WebSocket connector supports both old `websockets.connect(extra_headers=...)` and newer
  `websockets.connect(additional_headers=...)` APIs.
- **Merchant data** comes from kintara.gg's own `/api/world/merchant-campaign` (public, no
  auth). If the game rebalances the gold-trade recipe, update `MERCHANT_RECIPE` (its values
  live in `game.js`'s `MERCHANT_TRADE_COST`). If the donation campaign resources change, update
  `MERCHANT_CAMPAIGN_RESOURCES` and this README; the live endpoint supplies the current goals.
  The merchant cost calculator's liquidity cap reflects only **listed** market depth — real fills
  may differ (whole-stack buys, slippage).

---

## Changelog / current state

Keep a short running note here of meaningful changes (newest first), so a fresh chat
sees the latest state at a glance.

- **Merchant donation-drive phone alert (personal, opt-in).** New `merchant_watch_loop` polls the
  traveling-merchant `mode` every ~30s (`MERCHANT_WATCH_INTERVAL`) and pushes a one-time phone
  notification the moment it flips into **`donation`** — the drive reopening — never on `gold_trade`
  (actual gold selling). Rotation: `gold_trade → resting → donation → gold_trade`. Push goes via
  **ntfy.sh** (`send_ntfy()`, set `NOTIFY_NTFY_TOPIC` to enable — dormant/no-op otherwise). Last mode
  is persisted (`merchant_last_mode` setting) so restarts/deploys neither false-fire nor re-fire; the
  secret topic lives in `/opt/kintara-data/kinscan.env` (off git, via an optional systemd
  `EnvironmentFile`). See DEPLOY.md.
- **All charts now render in the browser's local timezone.** Previously mixed: the gold chart was
  local while the floor-history, sales, merchant-forecast and merchant-resource charts forced
  `timeZone:'UTC'` (with " UTC" labels), and the Market Watch daily chart had an off-by-one (parsed the
  UTC date string but read local getters). Now all **intraday** time displays drop the UTC override →
  local; all **daily** series (sales chart, "last 7 days sold" card, Market Watch daily volume) parse
  their `YYYY-MM-DD` bucket as **local midnight** so the calendar date is shown consistently with no
  shift. (Daily buckets are still UTC calendar days in the source data — only the label rendering is
  unified to local.)
- **Market cap: Index column + Market Watch leaderboard.** Market cap = **in-world supply × USD floor**
  (the lesser of the USD floor and the gold floor converted to USD = `item_floors()`'s `usd_equiv`).
  Added a **MKT CAP** column + "Market cap" sort to the Index (`market_cap` on `/api/sales-summary`),
  and a new **`GET /api/market-caps`** (every valued item ranked desc + `total_market_cap`) powering a
  **🏆 market-cap leaderboard** at the bottom of Market Watch — a horizontal bar chart, every item with
  icon + name, gold bar ∝ cap, $ value on the right (`mwLeaderboard()`, `.mw-lb` CSS). Capture sample:
  121 valued items, total ~$835k; top = gold $98k, marble gate $79k, mansion key #1 $68k.
- **Index: "In world" total-supply column.** Each item now shows its **total world supply** (units
  across every player's inventory/bank/bag) — the number from kintara.gg's own `/#index`. Source:
  `GET /api/world-item-index` on the **public `fanout.kintara.gg` mirror** (no auth needed after all —
  the fanout was just transiently down when first probed; the authoritative origin is login-gated).
  New `world_item_supply()` (cached ~10min, last-good), surfaced via `world_supply` + `world_players`
  on `/api/sales-summary`. Added an **IN WORLD** column to the Index table and two sorts (**Most in
  world**, **Rarest in world**); the note shows "across N players" (14,476 at capture). Captured
  sample: wood 62.5M, stone 33.0M, coal 21.7M units in the world.
- **Market Watch: spin wheel split out of trading volume.** The paid spin wheel is the `sink`
  category (the only txns that burn ~50% of the stake — confirmed nothing else does), so it's
  gambling, not trading. `/api/market-watch` now returns separate `market` (marketplace-only trading)
  and `spinwheel` blocks; the headline **trading volume is marketplace-only** ($527.7k, was $581.6k
  when the wheel was lumped in). Added a **🎡 Spin wheel infographic** (`mwSpinwheel()`: spins,
  wagered $KINS/USD, unique spinners, 50%-burn/50%-treasury split bar) replacing the old category
  bars; the daily chart and biggest-trades are marketplace-only. Captured month: 6,633 spins, $32.8k
  wagered, 5.97M $KINS burned, 775 spinners.
- **Market Watch home page + on-chain market dataset.** New flagship **splash/home tab** (now the
  default landing tab, `TAB="market"`) showing whole-market on-chain stats: animated total USD volume,
  stat cards (volume / transactions / marketplace / treasury revenue / $KINS burned / unique wallets),
  an interactive daily-volume SVG chart, a category breakdown, and biggest-trades. Backed by a new
  pipeline: `build_market_dataset.py` distills the ~200MB laptop treasury download (`market_index.db`)
  into a small indexed **`market.db`** (`market_txns`: buyer/seller/ts/category/gross/to_player/
  to_treasury/burned + **USD priced at each txn's actual minute** via paged 1m GeckoTerminal candles,
  hourly fallback), served read-only by the new
  `GET /api/market-watch` (env `KINTARA_MARKET_DB`, default `/opt/kintara-data/market.db` hosted).
  Categories: marketplace / sink (casino-wheel burn) / payout / other. Captured month totals: 95,716
  txns, ~$582k volume (minute-priced), 4,089 buyers, 5.97M KINS burned. Ship the dataset out of band via `scp` (it's
  gitignored; the data volume isn't touched by git deploys). See "On-chain market dataset" + "Market
  Watch" tab.
- **Fast first-page poller (kill the create-and-sell blind spot).** The listing poll was a single
  full-book sweep every 90s (~143 requests), so a listing created *and* sold inside one sweep was never
  captured and its sale degraded to a detail-less synthetic row. Added a second loop, `firstpage_loop`
  (`FIRSTPAGE_INTERVAL`=3s, env-tunable, `--firstpage-interval`): fetches **only page 1**
  (`fetch_all_active(max_pages=1)`, 1 request) and `reconcile(..., complete=False, record_poll=False)` —
  **capture-only** (upserts newest listings, never marks removals, writes no `polls` row). `poll_loop`
  (full, `complete=True`) still owns removal detection. `fetch_all_active()` gained a `max_pages` arg and
  now returns `complete=False` when it hits the page cap with book remaining, and `reconcile()` gained
  `record_poll` (default True). Result: nearly every listing is logged the instant it appears, so sales
  match to real listing detail instead of becoming synthetic. See "Two-tier listing poll" above.
- **Sales feed: hide synthetic rows + stop mis-attributed (wrong-price) sales.** Two fixes for fake/ugly
  feed entries:
  1. **No more "~est" rows.** Synthetic events (a sale confirmed via the /stats counter but with no captured
     listing → no qty/seller) are now **excluded from `/api/sales-feed`** (`listing_id IS NOT NULL`). They're
     still kept in the DB for the count reconciliation/audit, just not shown as individual feed rows.
  2. **Two-factor sale verification.** The /stats counter already gates *whether* a sale happened
     (cancellations don't move it); now a removed listing is only **attributed** to that sale if its
     per-unit is within `SALE_MATCH_TOL` (±60%) of the actual marginal sold price — so a coincidental
     cancellation at a wild price (e.g. a 50g helm on an 11.7g-average day) is no longer logged as the sale
     (it falls through to a detail-less synthetic, which is hidden). New `purge_implausible_sales()` also
     cleans **existing** mis-attributed rows (price >`SALE_OUTLIER_TOL`×/<1/tol the item-day avg), run once
     on upgrade (`sales_purge_v2`) and every cycle in `sales_audit_loop` (purge → backfill, so the count
     still reconciles). Verified: a 50g cancellation is left unmatched/synthetic+hidden, and an existing
     50g row on an 11.7g-avg day is purged.

- **Treasury KINS ledger builder:** broadened `build_treasury_index.py` from a marketplace-only indexer
  into the raw on-chain treasury ledger Connor wanted. It now stores every decoded KINS balance-changing
  transaction involving the treasury owner in `treasury_txns`, classifies the flow shape, stores program
  IDs/instruction types/raw owner deltas for later reverse-engineering, and also maintains `market_txns`
  as the high-confidence marketplace-trade subset. Includes `--probe`, `--summary`, `--kinds`, and
  `--wallet` query modes. Live probe verified the current KINS mint (`Tqj8yFm...pump`), treasury token
  account (`FawpB6...HFq4`), recent marketplace rows decoding at 500 bps, and the marketplace program
  appearing as `L2TExMFKdjpN9kozasaurPirfHy9P8sbXoAN1qA3S95`. The downloader now tries Helius
  `getTransactionsForAddress` first (`transactionDetails=full`, cursor pagination, `HELIUS_LIMIT=1000`)
  and falls back to standard RPC if unavailable; after Helius returned HTTP 413/403 on batch POSTs, the
  fallback path defaults to `BATCH=1` single transaction calls and recursively splits oversized/rejected
  batches if `BATCH>1` is enabled. Fallback is now governed by `TX_WORKERS`, `TX_CHUNK`, and an adaptive
  `RPC_RPS` throttle; after concurrency triggered persistent 429s, the defaults were moved back to the
  stable single-worker ~5 tx/s path, then given AIMD-style speed discovery (clean chunks increase RPS,
  429s cool down and back off). The run also does a final forward catch-up after backfill so transactions
  created during the long download are captured before exit. This builds `market_index.db`; the web app is
  not yet reading it.

- **Wallet on-chain: full-history incremental backfill (no fixed cap).** `compute_wallet_onchain` was
  reworked from a one-shot 2000-tx cap to an incremental scanner that reaches a wallet's **first** KINS
  transaction: each call does a forward pass (new txns) + a backward chunk of `ONCHAIN_MAX_SIGS` older
  sigs, folding into a persisted aggregate with `_newest`/`_oldest` cursors + `_done` flag (in
  `wallet_onchain`). The Player page auto-repolls until `_done` (shows "backfilling… N so far"). Depth is
  then limited only by the RPC node's history — **set `SOLANA_RPC` to an archival Helius/QuickNode endpoint
  for the complete history** (the public node prunes old `getTransaction`s + rate-limits). Verified with
  mocked RPC: chunked backfill aggregates the whole history with no dupes (count/earned/spent exact),
  forward pass picks up only new txns without re-scanning, fresh-cache short-circuits, and it serves
  partial progress on RPC failure.

- **On-chain KINS wallet stats (Player page):** new `/api/wallet-onchain?wallet=` reads the Solana chain
  via plain JSON-RPC (no `solders`) to capture a wallet's **all-time KINS transactions** — total earned &
  spent (KINS + USD priced at each tx's day), net flow, tx count, first/last activity, recent transfers,
  and a **marketplace earned/spent split** when the treasury wallet is set. The **KINS mint auto-resolves**
  from the GeckoTerminal pool (`kins_mint()` — no config); `_kins_delta()` decodes each tx from
  pre/postTokenBalances; results cached per wallet in the new `wallet_onchain` table (~10 min). The Player
  tab's on-chain panel is now live (loads async after the DB profile). Config: `SOLANA_RPC` (point at
  Helius/QuickNode in prod — public RPC rate-limits), `KINS_TREASURY` (comma-sep override; **defaults to the
  identified treasury so the marketplace split works out of the box**), `KINS_MINT` (override),
  `ONCHAIN_MAX_SIGS`/`ONCHAIN_CACHE_SEC`. **Treasury identified on-chain:** scanning a real wallet's KINS
  history, `4zW4zuZb9rXpvw3cTYyGoQ2iHTtG9E17YpdeNUbwuQVt` took exactly ~5% in 137/140 trades — it's the
  marketplace fee wallet. Verified end-to-end live against mainnet on that wallet (KINS mint auto-resolved
  to `Tqj8yFmagrg7oorpQkVGYR52r96RFTamvWfth9bpump`; earned/spent/net + marketplace split all populate);
  degrades gracefully on invalid input / RPC failure. Still pending: wheel spins + Kintara Club (need their
  program addresses).

- **Killed cancellations-logged-as-sales:** removed the speculative `detect_removal_sales` (it logged a
  "sale" whenever a reserved/collectible listing vanished — but cancellations and abandoned/expired-reserved
  listings vanish too, producing false sales like "$500 wood" / a cancelled $58 cosmetic). Sales are now
  confirmed **only** by kintara's /stats completed-sale counter (cancellations never move it); a removal
  just makes an item **urgent** in the stats queue so it's re-checked fast. Added
  **`purge_overcounted_sales()`**: for any item-day where logged sale events exceed the in-game /stats
  count, it deletes the excess, removing the most price-implausible first (so absurd entries like $500 wood
  go before real ones) and never touching under-counted/legit data. Runs once on upgrade
  (`sales_purge_v1` flag) to clean existing bad rows, and every ~3 min in `sales_audit_loop` (recent days)
  as ongoing insurance. Verified: a 4-event wood day with 2 real /stats sales purged the $500 + worst
  outlier down to the 2 real ones; a 0-real-sale cosmetic cancellation deleted; an under-counted item left
  untouched.

- **Player profile active-listing valuation hardening:** active listings still show if Kintara's public
  API returns them, but player/property totals no longer treat a seller's ask as value. `active_market_unit_usd()`
  values listed stacks from another-seller floor first, then recent fair value, then raw floor/ask only as a
  last resort, and the Player tab flags huge ask outliers. This fixes profiles like LEX1 where 12,911 wood
  listed for 112,321g made a fake ~$477k inventory value.
- **Player profile page (v1 — DB-backed):** new **Player** tab + `GET /api/player?name=&wallet=` that
  aggregates everything we publicly know about a player into one profile: marketplace **earned** (sell
  side — count/units/gross in gold/USD/$KINS/avg from `sales_events`), **top items sold**, **recent
  sales**, **active listings + market-anchored inventory value + category mix** (`listings`), **first seen**, and
  **property owned** (live property-feed owner match). Lookup is by in-game name (case-insensitive);
  `wallet` is accepted + echoed. The **on-chain layer is a clearly-marked pending stub**
  (`onchain.available=false`) — KINS spent/earned, wheel spins, Kintara Club and wallet verification
  need the Solana program/mint/treasury addresses captured from a sample transaction first (the build
  plan + on-chain design is in `PLAYER_PAGE_PLAN.md`, building on the existing `check_pump_rewards.py`
  Solana-RPC pattern). Buy-side "spent" is intentionally absent from the DB part (the kintara API doesn't
  expose buyers — that's exactly what the on-chain layer is for). Uninvasive: public + on-chain only.
- **Live character on the Player page** (`GET /api/player-live?name=`): sweeps all 12 servers' spectate
  streams for the name and returns the FULL matched player (outfit/cosmetics, level, held item, badge, HP,
  area, coords) + server. The Player tab renders their **avatar** (`avatarSvg`) + live stats, polling until
  found or all servers swept. On-demand fan-out like the Live-World search. **Offline fallback:** the
  spectate rosters seen during any Live-World / player lookup are cached into a **`player_seen`** table
  (`record_seen()`, throttled per shard — free, reuses fetched data); when a player isn't online,
  `/api/player-live` returns their **last-seen character + when/where** so the avatar/level/area still
  show, tagged "last seen Xh ago · Server N".

- **Rare-sale capture hardening (Venom Weaver Mount $800 miss):** official Kintara `/stats` had a
  `mount_venom_weaver` token sale for `$800`, but Recent Sales could still be empty if the listing sold
  between listing polls or if the first `/stats` sighting was treated as startup backlog. Fixes:
  - `stats_loop` now polls a wide item universe (`stats_item_universe`) instead of only items captured in
    `listings`: active/historical DB items + `ITEM_LABELS` + `ALWAYS_STATS_ITEMS` (Venomweaver loot), so
    rare/new drops can be discovered from `/stats` even with no captured listing row.
  - Recent official stats rows are forced to `base=0` (`SALES_RECENT_BASE_DAYS`, default 3) and backfilled
    into `sales_events`; older days can still be startup-baselined to avoid replaying ancient backlog.
    Backfilled prior-day events get a day-end timestamp so the global feed is not polluted as "just now."
  - `/api/sales-summary` now builds the Index item list from `listings ∪ sales_daily ∪ sales_events`, so
    stats-discovered items can appear even without a current listing. Added Venomweaver labels/icons/meta.

- **Sales-feed filter/search fix + Index sort options:**
  - The Sales feed's search/currency/category filters were unreliable because **category and `q` were
    filtered in Python over only the most-recent ~600-row slice** (after the SQL LIMIT) — so searching an
    item outside that slice found nothing. Now `category` + `q` (matches itemType OR in-game label) resolve
    to an item_type set and filter **in SQL**, with `ORDER BY ts DESC LIMIT` applied *after* filtering, so
    every filter searches the full dataset. (`/api/sales-feed`.)
  - **Index sort dropdown:** replaced the Most/Least pills with a 6-way sort — Most/Least sold, Cheapest /
    Most expensive (by the computed **$KINS floor**, sort-only — no new column), Newest / Oldest added (by
    first sale date). `/api/sales-summary` now returns `first_sale` (all-time MIN sale date per item).

- **Sales cross-check against the hard in-game number + auto-backfill:** the `/stats` per-day
  completed-sale count (the figure behind the in-game sales graph) is treated as ground truth and we now
  continuously reconcile to it. New `audit_sales()` compares our logged `sales_events` vs the in-game
  count per item-day (`missing = sales − base − logged`); `backfill_sales()` (run by `sales_audit_loop`
  every ~3 min, DB-only) tops up any shortfall so we converge on the in-game number even if a per-poll
  detection slipped. New `/api/sales-audit?days=` returns the comparison, and the **Sales feed** shows a
  coverage line ("✓ in sync with the in-game sales counter" / "⚠ behind by N — backfilling"). A **one-time
  baseline** (`sales_base_init` settings flag) sets `base = sales` on existing `sales_daily` rows so the
  pre-detector historical backlog isn't replayed as "now" (verified: a seeded 3-sale shortfall was
  detected and backfilled to 0; live audit shows missing 0 after baseline).

- **Sale detection overhaul — stop missing sales (top priority):** we were still missing sales (e.g. a
  $333 purple-aura sale). Root cause: the **first sale of an item-day was silently swallowed** — the old
  detector only logged on a /stats COUNT *increment* against an already-baselined day, and the first
  sighting baselined silently unless we'd captured a price-matching removal. Rebuilt detection to be
  multi-signal and self-healing:
  1. **Count-based reconciliation** (`_archive_samples`): for each item-day we keep the number of logged
     `sales_events` equal to `stats_count − base`, where new `sales_daily.base` is the cold-start backlog
     we deliberately skip (set only on a first sighting *within* a short `SALES_STARTUP_GRACE`, so a
     restart doesn't replay the whole day, but a genuine first-sale-of-day/new-day IS logged). Because it
     reconciles by **count of events**, it cooperates with the instant detector below — no double-logging
     — and no longer swallows first sales. `_log_sales` now matches each sale to the closest captured
     removal (real qty/seller/price/time-on-market) and logs a synthetic row for any it can't match.
  2. **No speculative removal→sale logging.** (An earlier `detect_removal_sales` that logged a sale when a
     reserved/collectible listing vanished was REMOVED — it produced false sales from cancellations and
     abandoned/expired-reserved listings, e.g. "$500 wood" / a cancelled $58 cosmetic.) The **only** thing
     that confirms a sale is kintara's /stats counter; removals just supply the *details* for sales the
     counter confirms, and items with a fresh removal are flagged **urgent** in the stats queue so they're
     re-checked fast (speed without ever logging a cancellation).
  3. **Tighter cold polling:** `STATS_STALE_COLD` 900→**420s** so quiet items (auras/cosmetics) get
     re-checked far more often.

- **Mobile layout pass:** added a phone stylesheet (`@media (max-width:680px)` + a ≤400px tweak at the end
  of the `<style>` block) — **desktop is untouched**, everything is an override for small screens.
  Highlights: compact header (drops the brand subtitle + status line, shrinks the mark/title); the tab bar
  becomes a single horizontally-scrollable full-bleed strip; the Index/Collectables/Arbitrage game panel
  (`.gw`) stacks (the category sidebar becomes a horizontal chip strip on top); the Index table drops the
  Sales column on phones to keep the three floor columns legible; item-expand panels go single-column; wide
  raw tables (Arbitrage/Live listings/Sales feed) scroll horizontally instead of squashing; charts get
  shorter; merchant/forecast/listings panels collapse to one column; tooltips/cards clamp to the viewport.
  Note: not visually verified on a real device from here (the preview runtime is sandboxed) — worth a quick
  phone check after deploy.

- **New Venomweaver boss area — Live World + per-server boss counts:** Kintara added a level-20 spider
  boss ("Venomweaver") east of the wilderness. Its internal spectate region key wasn't in our datamine,
  so a new **`BossCensus`** background loop self-resolves it: one short-lived spectate socket at a time,
  round-robining all 12 shards, probing Venomweaver/spider candidate keys (`BOSS_CANDIDATES`) and **locking onto
  whichever actually streams players** (`snap.region == key` with players present), then counting boss
  players per shard. Gentle (single socket, paced by `BOSS_CENSUS_INTERVAL`); set `KINTARA_BOSS_REGION`
  to skip probing once the real key is known. The resolved region is (a) fed into the **server-status
  bubble** as a 🕷 per-server count + a 🕷 total in the header (`/api/servers` now returns per-server
  `boss` + top-level `boss_region`/`boss_total`), and (b) added to the **Live World** round-robin so boss
  players get rostered and grouped under "Venomweaver" (`BOSS_LABEL`); `/api/live` includes the boss
  realm label. No map for the area yet — the per-player view uses the name-only card (no `REALM_MAPS`
  entry). New `BossCensus` class + `_boss_census`, started in `main()`.

- **Sales feed starvation fix:** the urgent stats queue was item-level and liquidity-sorted, so constant
  wood removals kept wood urgent and starved stone/coal/cooked-fish/metal for ~40 minutes. Urgency is now
  tracked per item+currency, expires once stats have been checked after that removal, and urgent selection
  uses stats age before liquidity so busy wood cannot monopolize `/stats`. Also reverted the mistaken
  assumption that `/stats.sales` is item units; it is completed-sale count, with real stack quantity coming
  only from matched vanished listings. `/api/sales-feed` now orders matched rows ahead of synthetic rows.

- **USD floor = raw USD listings only (no gold conversion):** the USD floor was `min(token_usd,
  gold_floor×rate)`, so an item listed only in gold showed its gold price *converted* to USD (weird
  decimals nobody types). Now `item_floors()` returns `usd` = the cheapest **raw token/USD** listing only
  (null if not listed in USD) and a separate `usd_equiv` = the cheaper of USD vs gold→USD, used **only**
  for the KINS floor (which should reflect whichever currency is cheaper) and the cheap/fair/expensive
  verdict. Applied across the scorecard, Index floor columns, and the floor-history chart (the USD line is
  now real listings only; KINS still uses the cheaper path). Floor-chart `usd` for old pre-snapshot points
  is null since the rollup only kept the cheapest-acquisition close.

- **Merchant restock markers on every time chart:** a merchant restock (donation campaign filling / gold
  stock refilling) is a market-wide price shock, so it's now detected (`merchant_events` table, transition
  logic in `merchant_snapshot_loop`), served (`/api/merchant-events`), and **overlaid as a gold dot +
  faint vertical line on the floor charts, the per-item sales charts, and the gold-price chart** — hover
  shows "🪙 Merchant restock · <time>". The overlay is a reusable plain-HTML layer (`restockOverlay` /
  `applyRestock` + `ensureMerchEvents`) positioned over each chart, so it works uniformly over both the
  `<svg>` floor chart and the `<canvas>` sales/gold charts; each chart supplies a `time → x-fraction`
  mapping. Markers refresh when the Merchant tab is opened.

- **Floor chart $KINS bugfix:** the floor chart's $KINS series was dividing every (high-resolution,
  per-tick) point by a single **daily** KINS close, so within a day $KINS was just a scaled copy of USD
  (identical shape). Now it converts each tick by the KINS price **at that tick's time** (intraday, new
  `kins_intraday_ms()` cached ~3 min + `interp_gold()`), falling back to the daily close only for old
  pre-snapshot points / when GeckoTerminal is unreachable. KINS now moves independently of USD, surfacing
  item-alpha-vs-token-beta on the floor chart.

- **Merchant graph hover + table item icons:** added a reusable `svgHover()` (crosshair + floating card,
  same feel as the gold-price chart) and wired it into the Merchant **resource-progress chart**
  (date · % · amount donated) and the **mint-profit sparkline** (date · profit/gold · gold & mint cost) —
  the inline SVG charts that previously had no hover. Added a small item **icon** next to the name in both
  the **Live listings** and **Sales feed** tables (`rowIcon()`, emoji fallback).

- **Floor-centric Index + actual-gold floors + merchant resource history + UI polish:**
  - **Index now shows the live FLOOR** (gold/USD/$KINS) per item instead of avg-sales columns
    (`item_floors()` in `/api/sales-summary`). Cheap commodities render as **items-per-gold** (`24k/g`),
    and **material/food/potion** quote **USD/$KINS per 1,000**. Same floor display on the scorecard.
  - **Bulk-material floor rule:** materials ignore listings smaller than `MIN_BULK_QTY` (1000) when
    pricing the floor — a tiny "100 wood" dump no longer sets the price. Applied in `compute_orderbook_rows`
    (snapshots), `item_floors`, the scorecard, and `/api/item-listings`.
  - **Floor chart uses ACTUAL gold listings** (not a USD→gold conversion): `gold` = cheapest gold-currency
    ask, graphed as **items-per-gold** when <1g/item; `$KINS` = whichever is cheaper at the time (token-USD
    vs gold→USD); hover shows gold/ea + gold/1k. (`/api/floor-history` now returns the real gold floor.)
  - **Merchant resource bars are clickable** → inline chart of that resource's donation **% over time**
    (`/api/merchant-history` + `drawMerchResChart`, from `merchant_snapshots`).
  - **Cheapest live listings** (materials): only ≥1k-unit listings; rows show **price · quantity · $KINS**
    (dropped the per-1k/per-unit clutter).
  - **Pets/furniture icons:** `icon_candidates()` probes likely per-item art paths (cached under a `__art`
    namespace so old generic-paw files don't shadow them), falling back to the paw for pets. *The probed
    paths are unverified guesses* — confirm real ones in-browser and add to `ICON_OVERRIDES` if needed.
  - **Aesthetic + small fixes:** Arbitrage and Collectables are now wrapped in the same framed gold-titled
    game panel (`.gw`) as the Index, so the tabs match. Removed the **per-item** column from Live listings
    and the **per-1k** figure from the Sales feed. Live-World "player isn't on any server" message is now
    **red**.

- **Sales-feed accuracy overhaul + per-1,000 quoting + floor-chart hover + Index-first:**
  - **Sales feed rebuilt for real quantities.** The old `sales_events.units` was a `/stats` *count* of
    listings sold (so "5 stone" meant 5 listings, each a huge stack — misleading). Now each sale is
    **matched to the listing we watched vanish** (`_log_sales`: recently removed, unclaimed, ranked by
    per-unit closeness to the marginal sold price) to recover the real **`qty`, `total` paid, `seller`,
    and `listing_ms`** (time on market). `sales_events` gained those columns + `listing_id`; `listings`
    gained `sold_claimed`. **Don't-miss:** detection now also fires on the *first* baseline of an item-day
    (price-matched removals only, no synthetic rows → no replaying history), and items with a fresh
    unclaimed removal are **prioritised in the stats queue** so high-value sales (e.g. a 500g aura)
    surface within a poll instead of waiting out the cold cadence. Legacy `sales_events` rows were
    dropped on migration; the feed fills forward. `rollup_day`/`recent_fair_usd`/`time_to_sell` now use
    `qty`. The **Sales feed** tab and **Recent sales** panel show qty · total paid · seller · time-listed.
  - **Per-1,000 quoting** for **material/potion/food** (a single unit is sub-cent and untradeable under
    the Solana fee): scorecard floor, floor chart axis/hover, and cheapest-live-listings all quote
    `/1k` for these categories. `qbasis()`/`qbasisSuffix()` (frontend `PER1K` set).
  - **Cheapest-live-listings + recent-sales show total paid + quantity**, not the tiny per-unit number
    (per-unit was unreadable for big stacks); per-1,000 is the secondary figure for commodities.
  - **Floor chart hover:** the per-item floor chart now has a Gold-chart-style **crosshair + hover card**
    (`attachFloorHover`, reuses the `.goldcard` style) showing the point's date and floor in gold/USD/$KINS.
  - **Index moved to the first/default tab** (`TAB="hist"`), since the scorecard is now the main product.

- **Historical order-book pipeline + Item Scorecard + Merchant Forecast (the "market memory" build):**
  The DB now preserves the market's *shape over time*, not just current state. Three new tables +
  background loops (all DB-local — no extra kintara.gg load): **`orderbook_snapshots`** (Substrate A,
  `snapshot_loop` ~5 min: floor + 2nd/3rd asks, listed supply, sellers, 1/5/10/25% depth bands, reserved
  supply, per item×currency — what `reconcile()` used to overwrite), **`item_daily_metrics`** (Substrate B,
  `rollup_loop` hourly: durable daily roll-up of floor open/close/min/max, volume, velocity, volatility,
  undercut count — the cache scorecard/floor-history read from), and **`merchant_snapshots`**
  (`merchant_snapshot_loop` ~5 min). **Guardrails baked in:** snapshot writes are one batched transaction
  per tick, raw snapshots are **pruned after 14d** once rolled up (storage stays bounded), the rollup
  `wal_checkpoint(TRUNCATE)`s, and snapshot cadence is coarser than the poll loop. New endpoints:
  **`/api/scorecard`** (floor in gold/USD/KINS, 24h/7d/30d change, liquidity 0–100 exit score, sells/day,
  **time-to-sell** from listing lifecycle, volatility, listed supply, sellers, gold-anchored cheap/fair/
  expensive verdict, last sale) and **`/api/floor-history`** (floor over time, gold/USD/KINS, ranges). The
  Index expand is now a full **Item Scorecard** (header strip + **floor price chart** with unit toggle +
  ranges) above the existing sales chart. `/api/item-listings` listings now carry **price-memory badges**
  (cheapest-ever/7d-low/below-sale-avg/above-fair/overpriced). The Merchant tab gained a full-width
  **Forecast desk**: completion ETA (gold-trade unlock), bottleneck resource, per-resource demand pressure,
  break-even gold price + mint profitability, and a mint-profit-over-campaign sparkline. New helpers:
  `compute_orderbook_rows`, `rollup_day`, `time_to_sell`, `recent_fair_usd`, `liquidity_score`,
  `merchant_mint_cost`, `merchant_forecast`. Verified end-to-end against the live DB (snapshot tick →
  168 books, rollup → 107 item-days; scorecard velocity/liquidity/time-to-sell; forecast math on synthetic
  campaign data; all frontend render fns runtime-tested). Movers / seller-intelligence / watchlists /
  Collectables overhaul intentionally deferred to later sessions.

- **Live World search compatibility + speed:** fixed DigitalOcean/new-`websockets` failures where
  `extra_headers` leaked into `BaseEventLoop.create_connection()` by selecting `additional_headers`
  when the installed package expects it. `/api/live-search` now also reports `connected` socket count,
  and the manual all-server search polls every ~750ms instead of ~1.8s so matches appear sooner while
  the 12 shard rosters warm.
- **Merchant calculator color pass:** in the Merchant tab's profit summary, **Gold value (sell)**
  is now gold-tinted and **Craft cost (buy)** is neutral white, leaving green/red reserved for the
  actual profit and margin signal.
- **Merchant campaign metal update:** the Merchant page now follows the current game code/API split:
  donation progress uses five resources from `/api/world/merchant-campaign` (`wood`, `stone`, `coal`,
  `cooked_fish_meat`, `metal`) via new `MERCHANT_CAMPAIGN_RESOURCES`, while the cost calculator remains
  tied to the current `MERCHANT_TRADE_COST` gold-trade recipe (wood/stone/coal/cooked fish; no metal in
  the live trade-cost block as of 2026-06-19). Cooked fish labels were updated from "Fish" to
  "Cooked Fish" and calculator copy now says it prices the gold-trade recipe.
- **Public branding pass:** renamed the hosted/site-facing brand from **Kintara Market**
  to **KinScan** in the browser title and header; added a gold-icon brand mark, favicon
  routes (`/favicon.ico`, `/favicon.png`, `/apple-touch-icon.png`), OpenGraph/Twitter
  description metadata, theme color, and `/site.webmanifest`. The icon uses the real
  Kintara `gold` HUD asset via the existing icon cache. DigitalOcean deploy scripts now
  also pre-mark `/opt/kintara` as a Git safe directory before pulling, because the repo
  is intentionally handed from root to the non-root `kintara` service user, and `deploy.sh`
  reinstalls the systemd unit before restart so service metadata changes go live too.
  Added `deploy/publish.sh`, a Mac-side one-command publish wrapper: commit local changes,
  push `main`, SSH to the Droplet, run the server deploy, and return.
- **Hosting-ready + 24/7 politeness:** added `Dockerfile`, `requirements.txt`, `.dockerignore`,
  `.gitignore`, and `DEPLOY.md` (DigitalOcean Droplet / Fly.io / Railway / Oracle-free-VM walkthroughs;
  Vercel/serverless explained as unsuitable). For the chosen **DigitalOcean Droplet** path there's a
  `deploy/` folder — `kintara.service` (systemd: non-root user, port 80 via `CAP_NET_BIND_SERVICE`,
  auto-restart, code in `/opt/kintara` + data in `/opt/kintara-data`), `setup.sh` (one-time bootstrap),
  and `deploy.sh` (one-command update: pull + deps + restart, data untouched). Site is served at the
  Droplet's public IP (`http://<ip>/`). `DB_PATH` now comes from `KINTARA_DB` so the SQLite file lives on a mounted
  volume (survives redeploys); server binds `KINTARA_HOST`/`PORT`. Crucially, **all kintara.gg requests
  now go through a shared global pacer** (`pace_kintara()`, `KINTARA_MIN_GAP` default 0.5s ⇒ ≤~2 req/s
  total across every loop) with **429/403 backoff** (`KINTARA_BACKOFF`) — so running 24/7 can't burst the
  marketplace (was ~5–6 req/s unthrottled). Cadences are env-tunable and default slower (listing poll
  45→**90s**, per-item stats refresh hot/cold **120/900s**). Frontend auto-refreshes relaxed to match
  (arb & Index ~1 min, Live listings/Sales feed ~30s, Live World ~6s) with a **manual ↻ refresh on every
  page** (added to the Index tab). Verified: pacer enforces the gap across threads, 429 → 45s backoff.
- **Sales feed → ACTUAL sales (not removals):** the **Sales feed** tab previously listed *removed*
  listings, which conflated real sales with cancel-and-relist undercutting. Now it logs only **genuine
  completed sales**: the stats loop (`_archive_samples`) watches each item's current-day completed-sale
  counter from kintara's own `/stats`, and when it ticks up, records the increment in a new `sales_events`
  table — `units` sold + the **marginal** avg price of just those units (backed out of the running day
  total). New `/api/sales-feed` drives the tab (item · units · price · currency · when) and a new per-item
  **Recent sales** panel in the Index expand (`loadRecentSales`). `sales_events` was **started from scratch**
  (old removed-listings data dropped); it fills going forward. Actively-traded items now refresh every ~5 min
  (vs 30) so the feed stays granular. Verified: detection logs exact marginal price; live feed caught real
  Gold/Stone/Wood/Fish sales within a minute of startup.
- **"Gold Standard" chart option (item sales graphs):** added a 4th toggle to every item's sales chart
  (Index expand + Collectables dropdown) — `currency=goldstd`. It values **every** sale in gold: gold sales
  as-is, USD/KINS sales converted to the gold that USD would have bought that day (`avg_usd / gold_usd(date)`),
  blended units-weighted into one all-sales gold series — the gold-denominated mirror of the blended `kins`
  series, matching the unit fair value is anchored in.
- **Resilient poller / no more flashing timeout errors:** `fetch_all_active()` now retries each listing
  page up to 3× and returns partial data with `complete=False` instead of raising when kintara.gg times
  out on deep pages (very common); `reconcile()` keeps last-good data on a partial poll. `_state` tracks
  `last_success`/`fail_streak`, and the header `#status` only shows a quiet "reconnecting to kintara…"
  note when the poller is **persistently** down (≥3 misses AND >4 min stale) — transient timeouts are
  hidden and self-heal in the background (was: every deep-page timeout dumped a raw error banner).
- **Arbitrage `spread` → `kins / gold` column:** replaced the (now redundant) spread column with
  **kins/gold** = how many $KINS it costs to buy enough of an item (at its cheapest USD/KINS listing) to
  assemble **1 gold's worth** (`per_gold × kins_unit / kins_price`) — the KINS price of a "manufactured"
  gold, green when below the live market KINS/gold (`gold_rate/kins_price`). The rate line now shows
  `1 gold = $X = N $KINS` as the benchmark. `/api/arbitrage` now returns `kins_price`. Most useful for the
  materials this tab flips (e.g. wood ≈ 101 $KINS to make a gold the market values at ~242 $KINS).
- **Collectables refinements:** (1) fair value now uses only the **most recent ~50%** of each item's
  sale records (`recs[len//2:]`), so launch-day outlier prices stop skewing settled value; (2) the far-
  right column is now **cumulative volume** over those recent trading days with a **per-day breakdown on
  hover** (`trade_days` replaces `recent_days`); (3) the **fair value** numbers are **gold-tinted** in the
  table to break up the color scheme; (4) fixed a lingering bug where the flicker-free morph carried
  arbitrage **deal/sold hover-card handlers** onto Collectables rows (showed the wrong item's card) — the
  render now clears stale `td.onmouse*` handlers.
- **vs $KINS chart now blends gold + USD sales:** the `currency=kins` series (used by every item's
  **vs $KINS** toggle in the Index tab and the Collectables/Index dropdowns) now combines **both** the
  gold and token sale history into one $KINS price instead of token-only — gold sales convert via
  `gold_usd(date)/kins_usd(date)`, token sales via `1/kins_usd(date)`, averaged **units-weighted** per
  day (so items that trade mostly/only in gold finally show a real $KINS line). `vs_token` now uses the
  blended-USD series. See `/api/sales-history` above.
- **Collectables mode + QoL pass:**
  - The 3rd arbitrage mode is now **Collectables** (was "mispricing"), rebuilt on the research finding
    that CMP prices are stickiest in **gold/KINS**, not USD. `compute_mispricing()` now compares the
    cheapest live listing to a **gold-anchored, recency- & volume-weighted fair value** (each recent
    sale re-priced into gold at its day's gold price via new `gold_daily_usd()`, weighted by
    `units × 0.5^(age/7d)`, carried to today), fixing the low-liquidity staleness bug. Added a
    **confidence dot** (recency + volume), a **vol = last 3 sale-days** column, and **farmable-commodity
    exclusion** (`FARMABLE_CMP` + heuristic). The KINS/Gold/Both toggle is now display-unit-only (one
    gold-anchored comparison). Dropped the `fee` param. See the CMP note in *What it does*.
  - **Arbitrage QoL:** removed the **sell-fee** input; direction labels shortened to **gold → KINS** /
    **KINS → gold** (arrow implies buy/sell); "Refresh shown" is now a bare **↻** icon. Fixed a bug
    where a stale deal/sold hover card lingered into the Collectables tab (now `hideDeal()`/`hideSold()`
    on entry).
  - **Header:** added a live **`$KINS` price pill** (`/api/kins-price`, `#kpx`, value-flashes); removed
    the "N active · rows tracked · since …" status line (kept only the green live-light + "updated Xs
    ago"; `#status` now shows a poller error only).
  - **Live World:** added a **player search box** (`#lwsearch`) that filters the current server's roster
    by name and tags each match's area.
- **Live World — Mainland top-down map:** the overworld (`world`) now uses a real top-down render
  (`/maps/mainland.png`: plaza + fountain + shops, the exact `PROPERTY_PLOTS` estate, paths, edge gates,
  trees) instead of the eyeballed isometric `worldmap.jpg` crop. Wired via `centerMap(62)` so players plot
  exactly (verified: x−6.5/z0.5 → 39.3%/50.8%); dropped `worldToMap()`/`MAP_ZOOM`. Every realm including
  the mainland now plots through the same `REALM_MAPS` path.
- **Live World — all realm maps finished:** `render_maps.py` now renders the remaining realms —
  **Whisperwood** (eldergrove, 62×62, forest+path), **Frostmere** (40×40, snow/pines/frozen ponds),
  **The Wilderness** (wild, 50×50), **Deep Wilderness** (wild_ext, 25×25), **Wilderness East**
  (wild_exp, 25×25), **The Mine** (20×20, ore+torches), **Spider Lair** (spider, 20×20 approx), and
  **The Shack** (5×5). Grid sizes/offsets pulled exact from `constants.js`; served at `/maps/<slug>.png`
  and wired via a new `centerMap(N)` transform so players plot exactly (verified: an eldergrove player at
  x22.5/z-3.5 lands at 86.9%/44.3%). Terrain palette + transform are exact; trees/rocks are seeded
  representative scatter (no public prop dump for these realms), shared helpers `_scatter_realm`/`_pine`/
  `_round_tree`/`_rock`. Every instanced realm now shows its own map in the per-player Live World view.
- **Live World — all-server player search:** the search box now also does a **cross-server lookup**. Hit
  **Search all servers** (or Enter) and it sweeps every one of the 12 worlds (new `/api/live-search`,
  which opens + warms a spectate socket per shard and reports `ready` N/12 as rosters fill), then
  **auto-selects the server the player is on**, expands their card, and scrolls to them — or says they're
  not online anywhere. The extra sockets idle-close ~75s after the search. `searchAllServers()` +
  `state.liveSearchBusy/liveSearchStatus`; `loadWorld()` pauses its 2.2s refresh while a search runs.
- **Index: cheapest live listings per item:** every Index item expand now shows a **Cheapest live listings**
  panel (`/api/item-listings`) — up to the **5 cheapest buyable listings in gold** and **5 in $KINS**, side by
  side, with per-item price, stack size, seller, and cross-converted value. Plus `KNOWN_SHOP_PRICE` now drives
  a "Shop price (confirmed)" row (seeded with mog glasses = 10g; add more as confirmed in-game).
- **Index sourcing accuracy pass:** (1) **auras are wheel-only** — Red Aura = the free **daily wheel**
  (~1 in 22), blue/green/gold auras = the **$5 paid wheel** (random); none come from the shop (was wrong).
  (2) Dropped the bogus "~3 gold" (the spider placeholder); `item_index_meta()` now reports the
  **availability cadence** confirmed from game.js shop-payload shapes — mounts = alchemist **weekly** drop
  (7-day window), cosmetics = cosmetic shop **daily slot (24h)** or **weekly bundle (7d)**, pets = **weekly**
  (3/week), wolf = world-tamed (always available). (3) **Shop gold prices**: the real per-rotation prices are
  server-side/auth-gated (configs 403, no public shop API, only placeholder defaults in the client), so the
  panel shows a hand-maintained **`KNOWN_SHOP_PRICE`** (community-confirmed in-shop gold, e.g. mog glasses
  10g) when available, else the **cheapest-ever GOLD sale** as a market-floor proxy (gold = the shop's currency).
- **Sales history → "Index" (item encyclopedia + alpha-vs-beta):** renamed the tab to **Index**.
  Each cosmetic/mount/pet expand now shows an **item-info panel** (`/api/item-meta`, `ITEM_DESC` +
  `MOUNT_SPEED` + `ALCHEMIST_MOUNTS` datamined from game.js): how it's **sourced**, cost, **ride speed**
  (+10%…+50%), special features, and a **derived availability window + supply status** (flooding /
  tapering / dried-up) from `sales_daily`. Added a **vs $KINS** chart toggle (`/api/sales-history?currency=kins`):
  the item's USD history re-priced in $KINS/day (`kins_daily_usd()`), with a banner that says whether
  it's real **alpha** (outpacing the token) or just **token beta** — e.g. tralalero looks ~flat in USD
  but is **−93% in $KINS terms** because the token pumped +949% over the window.
- **Property Map → tilted 2.5D estate (redesign):** the Property Map is now drawn client-side as
  an SVG tilted-2.5D scene — axis-aligned (rectangular box, not an iso diamond), pitched to the same
  slant as in-game, with each mansion/house/trailer extruded (gabled roof + front wall + door) at its
  real `PROPERTY_PLOTS` footprint, drawn back-to-front. Buildings **hover-glow white**, locked = red
  outline, selected = gold glow + owner card. Replaced the flat `/estate.png` backdrop + outline
  overlay (route and asset removed; `render_maps.py` no longer emits the estate PNG).
- **Pond + Arena maps + Live World realm registry:** `render_maps.py` now also renders `The Pond.png`
  (`/pond.png`, RNG-grown lake seed `0x50dcee` + dock + NE tower) and `The Arena.png` (`/arena.png`,
  sand + boxing ring). Live World's per-player map is generalised to a `REALM_MAPS` registry
  (`{img, toMap}`) so beach/pond/arena all plot players on their own backdrop; adding a realm = render
  PNG + serve route + one registry entry. Remaining: Whisperwood/Frostmere/Wild/Mine.
- **Property Map proper render + feature finished:** new `render_maps.py` (Pillow) generates
  `MapImages/Estate (Property).png`, a top-down render of the estate with every mansion/house/
  trailer drawn at its real `PROPERTY_PLOTS` footprint. Served at `/estate.png` and placed as the
  Property Map backdrop **inside** the SVG (`<image>` over the same grid box the overlay computes),
  so the clickable plots line up exactly on the rendered buildings. Plot overlay restyled to
  outline-only (transparent fill, type-coloured/locked-red/selected-gold stroke, haloed label) so
  the buildings show through. Dropped the old `world_map.jpg` cover backdrop on `.pm-map`.
- **The Shores map is now a real generated asset:** `render_maps.py` rebuilds the beach from the
  game's world-gen (40×40, shoreline xorshift `0x9e3779b1`, exact prop tiles, true-flat top-down
  oriented to `shoresToMap()`) and writes `MapImages/The Shores (Beach).png` (replaced the old art).
- **In-game area names:** `SPECTATE_REGIONS` labels are now the names that flash on entry
  (`playRegionIntro`/`*_DISPLAY_NAME`): Overworld→**The Mainland**, Fishing Pond→**The Pond**,
  Eldergrove→**Whisperwood**, Arena→**The Arena**, The Wilds→**The Wilderness**, Deep Wilds→**Deep
  Wilderness**, Far Wilds→**Wilderness East**, Mine→**The Mine** (Shores/Frostmere already correct).
  Fixed the Live World fallback card's doubled article (`In the The Mine` → `In The Mine`).
- **Materials liquidity chart:** the Sales-History item expand now shows a **buy-side liquidity
  depth chart** for `material` items — cumulative units available by USD price per 1000, bucketed
  in $0.10 tranches, with a "cheapest" marker and per-band hover detail. New `GET /api/liquidity`
  endpoint (token listings in USD + gold converted at the live rate; axis auto-capped near market
  so overpriced tail stacks don't squash it) and `liquidityHTML()`/`loadLiquidity()` on the front end.
- **Arbitrage `sold` hover card:** hovering the far-right sold-today column pops a compact,
  deal-card-styled tooltip listing the last **3 days**, each with that day's **units sold + avg
  sale price** in the sell-side currency. `showSold()` / `lastNDays()`, served from the existing
  `/api/sales-history`, cached client-side.
- **Removed the redundant `cat` column** from the Arbitrage and Live-listings tables (category
  filtering/chips unchanged).
- **Premium QoL / design pass (site-wide frontend):** flicker-free in-place DOM morph for all
  re-renders (no more flash/scroll-jump on polling tabs), value-flash on price changes, a ⌘K
  command palette (items/sellers/tabs), design tokens + tabular numerals, toned-down table headers,
  custom tooltips, abbreviated numbers, relative+absolute timestamps, freshness badges, an inline
  gold-rate sparkline, skeleton loaders, tab cross-fades, a live "updated Xs ago" indicator, and
  reduced-motion support. All additive in `INDEX_HTML`; see the "Premium QoL layer" section above.
- **The Shores map (Live World):** renamed the `beach` realm label "Beach" → "The Shores" and made
  its per-player view fully operational. New `/shores.png` route serves `MapImages/The Shores
  (Beach).png`; expanding a player in the shores now shows that top-down map with **every** character
  currently in the shores plotted at exact `(x,z)` (coord system top-right `(-19.5,-19.5)`, bottom-left
  `(19.5,19.5)` → `shoresToMap()`), rendered as their generated avatar icon with a white glow (selected
  player larger/brighter) instead of a plain dot. First of the per-area maps (`MapImages/`); HD art and
  more realms to follow.
- **Startup fix:** the dashboard could print its banner but never bind ("connection refused")
  due to an import-lock deadlock — `poll_loop`'s first lazy `import requests` racing the main
  thread's Flask/Werkzeug import. `main()` now eager-imports `requests` and builds the app on
  the main thread *before* starting any worker thread.
- **All 12 servers:** `sN` in the spectate URL is the server number, and kintara.gg (not just
  the 4-server read-fanout) serves the spectator stream for every server — so the Live World tab
  now covers all 12, labeled with their in-game names. `SPECTATE_WS` points at `kintara.gg`,
  `SHARDS = range(1,13)`.
- **Full per-world roster:** `SpectateHub` now round-robins every realm (`spec_reg`) instead of
  sitting on the hub, so the Live World tab rosters ~75–80 named players per world grouped by
  area (pond/beach/dungeons/…), not just the ~25 at the hub. Player snapshots carry `realm`; the
  per-player map shows a **single** location dot (overworld) or names the instanced area.
- **Live World redesigned** (was a laggy always-on canvas radar): now roster-first per world,
  with a click-to-expand per-player dropdown showing a zoomed world-map crop + location dot +
  full card. Clarified that spectate "shards" s1–s4 are **4 separate worlds/servers** (no
  player overlap) and `onlineTotal` is global; relabeled "World 1–4". Added the world map as a
  served asset (`world_map.jpg` → `/worldmap.jpg`) and used it as the Property Map backdrop.
- Added **Live World** tab (real-time player radar + roster + click-through player cards from
  the public spectate WebSocket `wss://ktra-server-b.onrender.com/ws/spectate/sN`, served via a
  lazy per-shard `SpectateHub` and `/api/live`) and **Property Map** tab (interactive estate
  map from `/api/property-signs/status` placed at real in-game grid coordinates, with owner
  cards cross-referenced to marketplace listings). Needs the optional `websockets` package for
  Live World. New module-level `SpectateHub`, `PROPERTY_PLOTS`, and routes `/api/live`,
  `/api/property`.
- Merchant data now comes from kintara.gg's **own** API (`/api/world/merchant-campaign`) and
  the recipe from its `game.js` — dropped the kintaraai.xyz Supabase dependency entirely.
- Server status is now a **compact header icon → floating bubble** (was a full-width bar).
- Merchant tracker shows a **% next to each resource**; cost calculator is now
  **liquidity-aware** — it walks the live order book per resource, so minting more gold costs
  progressively more as cheap listings are consumed (reports avg & marginal $/gold, caps to
  available liquidity). `/api/merchant` `calc` now returns price ladders instead of a single
  cheapest price.
- Added a **server status bar** from `kintara.gg/api/servers`; a **Merchant tab** with a live
  traveling-merchant progress tracker; and a **merchant cost calculator** (gold-mint recipe:
  2500 wood, 1500 stone, 700 coal, 30 cooked fish per gold) priced vs our gold price. New routes
  `/api/servers` and `/api/merchant`. Inspired by kintaraai.xyz, rebuilt in our own style.
- Gold price is now measured directly: `gold_price_loop` snapshots avg of the 3 cheapest
  per-gold asks into a new `gold_price` table every ~3 min; the gold chart and arbitrage
  gold rate use it, with kintaragold.xyz demoted to backfill/fallback only. Removed the
  Custom SQL tab and `POST /api/sql` route.
- Centered page layout (`.hdr` + `main` at `max-width:1320px; margin:0 auto`).
- Site-wide game retheme; renamed to "Kintara Market".
- Daily-sales archive (`sales_daily`); sales endpoints read from it; gold-history cached
  ~3 min server-side to stop GeckoTerminal rate-limiting.
- Sales history redesigned: category sidebar, window selector (1/7/30d), expandable line
  charts + stats panel, real item icons.
- Gold-price tab: ripped kintaragold's independent gold series so KINS/gold truly moves;
  6 ranges; 3-min resolution for 4H/1D.
- Item display names (`ITEM_LABELS`) and real item art (`/icon/<type>`).
- Arbitrage: per-unit pricing, categories, items/$ + per-gold columns, sold-today volume
  (anchored to the latest game day), min-stack filter, hover deal card, auto-refresh.
