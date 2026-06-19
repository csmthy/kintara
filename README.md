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
`KINTARA_DB` (DB path — point at a volume when hosted), `PORT`, `KINTARA_HOST`,
`POLL_INTERVAL` (90), `KINTARA_MIN_GAP` (0.5 — global min seconds between **any** two
kintara.gg requests, a shared pacer across all loops ⇒ ≈ ≤2 req/s total),
`KINTARA_BACKOFF` (45 — pause after a 429/403), `STATS_STALE_HOT`/`STATS_STALE_COLD`
(120/900 — per-item stats refresh cadence). All kintara.gg requests go through
`pace_kintara()` so it can run continuously without bursting the marketplace.

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
commodity (gold↔KINS) logic, not the CMP mispricing scan. Total world supply per item is **not
publicly exposed** (no kintara.gg supply/index API; game.js has no edition/supply concept).

---

## Data sources (all reverse-engineered)

| What | Endpoint | Notes |
|---|---|---|
| Active listings | `GET kintara.gg/api/marketplace/listings?sort=latest&currency=all&category=all&limit=40&offset=0&q=` | Returns `{ok, listings[], total, limit, offset, hasMore}`. Each listing: `id, sellerId, sellerName, itemType, quantity, priceGold, currency, priceUsd, createdAt, reservedBy, reservedUntilMs, itemDurability`. **No public sales-history here; no category field** (the `category` param is ignored — server returns everything). We build history ourselves. |
| Daily completed sales | `GET kintara.gg/api/marketplace/stats?[currency=token&]itemType=<x>` | `{ok, currency, avg30d, samples:[{date, avgUnitPrice, sales}]}`. **Daily only** (ignores interval params), ~30 days, **sparse** (only days with sales). `avgUnitPrice` is per single item; gold prices are rounded to 2 decimals (sub-cent gold collapses to 0). Omit `currency` = gold; `currency=token` = USD. |
| Live KINS price (USD) | `GET kintara.gg/api/token/blimp-stats` | `{priceUsd, ...}` — kintara's own KINS/USD, matches their index page. |
| Item art | `kintara.gg/assets/hud/<category>/<name>.(png|svg)` | Real in-game icons. Mapping is per-item (see `ICON_OVERRIDES` + `icon_asset()`); cosmetics = `cosmetics/<itemType>.png`, pets = `pets/paw.svg`, keys = bronze/silver/gold. Furniture has no art. |
| Item display names | (ripped from `kintara.gg/game.js` label catalog) | Baked into `ITEM_LABELS` dict (133 entries). `item_label()` resolves itemType→name with a prettify fallback. e.g. `cosmetic_dog_mask`→"Jotchua", `wild_sword`→"Training Sword". |
| **Gold USD price (ours)** | our own `listings` DB | Authoritative while the tracker runs. `gold_price_loop` snapshots one row into `gold_price` every ~3 min = `our_gold_price()` (avg per-gold USD of the 3 cheapest live token gold listings). Drives both the gold chart and the arbitrage gold rate. |
| **Gold USD price history (fallback)** | (ripped from `kintaragold.xyz` HTML) | The page embeds `"history":[{t,price}]` + `"spotPriceUsd"` in its RSC payload (escaped, can straddle chunk boundaries — we regex the `t`/`price` pairs). Independent gold-USD series (~10-min, ~25 days), NOT derived from KINS. `fetch_kintara_gold_history()`, cached ~3 min. Used **only to backfill the stretch before our own `gold_price` data begins** (see `gold_series_for_chart()`) and as the gold-rate fallback when no live gold listings exist. |
| **KINS/USD price history** | `GET api.geckoterminal.com/api/v2/networks/solana/pools/<POOL>/ohlcv/<tf>?aggregate=&limit=&currency=usd&token=base` | Pool `F42tZnKPavq1VUcrL6ymhc6YqVpt84fWwgzbNTv2wb3W` (KINS/SOL on pumpswap). `currency=usd` already converts SOL→USD (no separate SOL feed needed). Valid aggregates: minute 1/5/15, hour 1/4/12, day 1. **Rate-limits if hammered** — we cache (see below). |
| **Server list** | `GET kintara.gg/api/servers` | `{ok, servers:[{id, name, populationLabel, full, queueLength, minLevel}]}`. Live population + queue per game server. Drives the top status bar. `fetch_servers()`, cached ~30s (last-good on failure). |
| **Traveling-merchant state** | `GET kintara.gg/api/world/merchant-campaign` | kintara.gg's **own** public endpoint (no auth; the game client reads it the same way via `KINTARA_READ_FANOUT_ORIGIN`, also reachable at `ktra-server-b.onrender.com`). Returns `{ok, mode, wood, stone, coal, cooked_fish_meat, metal, goals:{...}, complete, goldTradeEnabled, goldStock, goldStockFull}`. **No overall %** — we compute it as the mean of the five per-resource (capped) percentages. `fetch_merchant()`, cached ~60s (last-good on failure). |
| **Merchant gold-mint recipe** | (from `kintara.gg/game.js` `MERCHANT_TRADE_COST`) | `MERCHANT_RECIPE` = resources consumed per 1 gold minted: 2500 wood + 1500 stone + 700 coal + 30 cooked_fish_meat. This is now separate from the **donation campaign** resources (`MERCHANT_CAMPAIGN_RESOURCES`: wood, stone, coal, cooked fish, **metal**); the cost calculator follows the current gold-trade recipe, while the left progress tracker follows the live campaign goals. |
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
  refreshes one item+currency every ~0.4s, prioritized by liquidity (most-listed first),
  re-touching each pair at most every ~30 min, writing to both `sales_daily` (archive)
  and `item_stats` (last-day summary). Cold start fills the high-volume items within a
  couple minutes.
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
- **`sales_events`** — **ACTUAL completed sales** (the truthful sales feed). Detected by the stats
  loop in `_archive_samples()`: when a day we've already baselined shows MORE completed sales than
  before, that increment is real (cancel-and-relist doesn't move kintara's sale counter), so we log
  `(item_type, currency, units, price, day, ts)` — `units` = how many sold since we last looked,
  `price` = the **marginal** avg unit price of just those units, backed out of the running day total
  `(new_sales·new_avg − old_sales·old_avg)/units`. Only logged on an UPDATE (a day already seen), so
  pre-existing sales aren't replayed as "now". Indexed on `ts` and `(item_type, ts)`.
- **`gold_price`** — our own measured gold-USD series: `ts` (epoch ms, PK), `usd` (USD per
  1 gold), `listings` (how many listings the avg used, ≤3). One row per ~3 min from
  `gold_price_loop`.
- **`polls`** — one row per listing poll (ts, active, removed, ok).
- **`settings`** — key/value (notably `gold_item`).

Schema migrations are handled inline in `init_db()` (ALTER + backfill for older DBs).

---

## API routes

- `GET /` — the dashboard (single HTML page).
- `GET /favicon.ico`, `/favicon.png`, `/apple-touch-icon.png`, `/site.webmanifest` — site
  identity assets for the hosted KinScan brand. The favicon/apple icon use the real
  Kintara gold HUD icon through the same disk cache as `/icon/gold`.
- `GET /api/status` — poller state, tracking-since, row count.
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
  `sales_events`, newest first (the truthful sales feed; cancellations excluded). Each row:
  `{item_type, label, category, units, price, currency, day, ts}`. Drives the **Sales feed** tab and
  the per-item **Recent sales** panel in the Index expand.
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
- `GET /api/item-meta?item_type=` — Index info-panel metadata for one cosmetic/mount/pet: sourcing
  channel + **availability cadence** (weekly 7d vs daily 24h, from the game's shop-payload shapes),
  **ride speed** (mounts), special-feature flavor (`ITEM_DESC`), a **cheapest-ever-traded** source/floor
  proxy (exact rotating shop gold prices are server-side/auth-gated, not public), plus the derived
  availability window + supply status from `sales_daily` (`item_index_meta()`).
- `GET /api/sales-summary?window=1|7|30` — per-item totals over the window: sales,
  sales-weighted avg gold/USD, `$KINS` (= avg USD ÷ live KINS price), `ref_day`.
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
  the top status bar.
- `GET /api/live?shard=1|2|3|4` — live world roster for a shard (from the spectate
  WebSocket): `online_total`, `players[]` (id, name, x, z, level, held item, badge, hp,
  outfit), `connected`, `err`. Opens the socket on first hit, keeps it warm while polled.
- `GET /api/live-search?q=<name>` — find a player by name **across all 12 servers**. Opens (and keeps
  warm) a spectate socket on every shard and returns current name matches as `results[]`
  (`{shard, id, name, realm, level}`, exact-name matches first), plus `ready` = how many shards have a
  populated roster yet. Rosters fill over ~20s after a socket opens, so the client polls this until
  `ready == 12` or a match is found; the extra sockets idle-close ~75s after the search stops. **Be
  gentle** — this fans out to all 12 read-fanout sockets, so it's an on-demand action, not polled.
- `GET /api/property` — every mansion/house/trailer: owner, lock state, real map
  coordinates, plus a marketplace cross-reference (the owner's live listing count + total
  ask USD) and how many properties that owner holds.
- `GET /api/merchant` — traveling-merchant tracker **and** cost calculator in one payload:
  `state` (five donation resources: wood, stone, coal, cooked fish, **metal** current/goal/**pct**,
  overall %, mode, gold stock) and `calc` (`gold_rate` + the current `MERCHANT_TRADE_COST`
  gold-trade recipe with per-ingredient **order-book ladder** — cheapest-first `[unit_usd, qty]`
  levels across both currencies, gold converted at the rate). The client walks the ladder so larger
  mints cost more as cheap listings run out (**liquidity-aware**), reports avg & marginal $/gold,
  and caps the mint to the listed liquidity.

---

## Frontend tabs (all in `INDEX_HTML`)

Game-styled aesthetic site-wide (navy gradient, gold **Cinzel** headings, **Fredoka**
body, gold pill tabs, rounded panels). Public branding is **KinScan** with a Kintara
gold-icon brand mark, browser title/description metadata, favicon/apple icon, and web
manifest for a more polished hosted-site shell.

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
2. **Live listings** — current active listings, with item art. **Sales feed** — now shows **actual
   completed sales** (`/api/sales-feed`, from `sales_events`): item · units sold · marginal price · paid-in
   currency · when. This replaced the old removed-listings feed, which counted cancel-and-relist
   undercutting as if it were sales; the misleading old data was dropped (`sales_events` starts empty and
   fills going forward).
3. **Index** (was "Sales history") — game "index" layout: category **sidebar**, **Today / 7d / 30d**
   window selector, **Most/Least sales** sort, columns ITEM · SALES · AVG GOLD · AVG USD ·
   $KINS. Click a row → expands to a **line chart** with a 4-way currency toggle
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
     cheapest per-unit**, with stack size, per-unit (for stacks), seller, and the cross-converted value
     (gold→USD at the rate, USD→$KINS at spot). Many items have <5; that's fine.
   - **Every** item expand also shows a **Recent sales** panel (`loadRecentSales()`, `/api/sales-feed`):
     the item's latest **actual completed sales** (units × marginal price, currency, relative time),
     newest first — cancellations excluded. Empty until real sales are observed (it logs going forward).
   - **For `material` items**, the expand still shows the buy-side **liquidity depth chart**
     (`/api/liquidity`): cumulative units available by USD price per 1000, in $0.10 tranches.
4. **Gold price** — kintaragold-style chart, now driven by our own `gold_price` series.
   Toggle **Gold (USD) ⇆ KINS/gold**, ranges 4H/1D/3D/7D/14D/ALL, % change pill, hover
   card, auto log-scale for extreme ranges.
5. **Merchant** — traveling-merchant desk. Left: progress tracker (overall % + five donation-resource
   current/goal bars **with a % next to each item**: wood, stone, coal, cooked fish, **metal**; mode
   badge donation/gold-trade; gold stock). Right: **cost calculator** — the current gold-trade recipe
   from `MERCHANT_TRADE_COST` priced **liquidity-aware** (walks the live order book, so each additional
   gold costs more as cheap listings are consumed), with a "mint N gold" input, avg & marginal $/gold,
   craft cost vs gold value, profit/margin, and a cap when the mint exceeds listed liquidity.
   Auto-refreshes ~30s (skips while the mint field is focused).

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
- The actual-sales feed is **granular but not per-transaction**: it logs the delta each time the stats
  loop re-checks an item (every ~5 min for actively-traded items, ~30 min for quiet ones), so a busy item
  shows up as "N units sold @ avg $P" per interval, with up to that much latency. Gold-priced sale prices
  inherit the `/stats` 2-dp rounding (coarse for cheap goods); the USD/$KINS prices are exact.
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
  socket. If `websockets` isn't installed, the tab shows a one-line install hint.
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
