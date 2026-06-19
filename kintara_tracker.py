#!/usr/bin/env python3
"""
KinScan — Kintara arbitrage + history tracker.

Two currencies trade on the Kintara marketplace:
  * KINS / token  -> priced in USD (priceUsd), highly liquid
  * gold          -> the in-game currency, also tradeable for KINS

Because the same item can be listed in EITHER currency, a price gap opens up:
an item listed for a few gold can be worth less (in USD) than the cheapest KINS
listing of the same item. This tool surfaces that gap.

Everything is priced PER SINGLE ITEM (price / stack size), because almost every
listing is a stack: 5000 wood for 2 gold is 0.0004 gold/wood, not "2 gold".

Primary page (ARBITRAGE):
  gold_rate = cheapest per-gold token ask (price_usd / quantity) = USD per gold.
  For each item:
     gold_unit_usd = cheapest per-item gold ask * gold_rate
     kins_unit     = cheapest per-item KINS ask (USD)
     profit/item (buy gold -> sell KINS) = kins_unit - gold_unit_usd
  "per_gold" shows how many of an item one gold buys (e.g. 24000 wood/gold).
  A toggle flips the direction (buy KINS -> sell gold). Reserved listings are
  excluded (you can't buy them). Every item with a live ask is shown — a
  "profitable only" toggle hides the rest. Items are categorized from their
  itemType prefix (mount_, cosmetic_, tool_, …) for filtering.

It also keeps the full history engine: it polls the listings endpoint, records
every listing, and infers sales when a listing disappears. Live listings, the
sales feed, and price history live on their own tabs.

Run:
    pip install flask requests
    python kintara_tracker.py
"""

import argparse
import json
import os
import re
import sqlite3
import threading
import time
import webbrowser
from collections import defaultdict
from datetime import datetime, timezone

def _envf(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _envi(name, default):
    try:
        return int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return int(default)


# DB lives wherever KINTARA_DB points (a mounted persistent volume when hosted), so
# the database survives redeploys instead of being wiped with the container.
DB_PATH = os.environ.get("KINTARA_DB", "kintara.db")

# --- 24/7 cadence + politeness (all tunable via env, with hosting-friendly defaults) ---
POLL_INTERVAL = _envi("POLL_INTERVAL", 90)        # listing poll seconds (was 45)
STATS_GAP = _envf("STATS_GAP", 0.5)               # spacing between stats requests
STATS_STALE_HOT = _envi("STATS_STALE_HOT", 120)   # re-check actively-traded items this often
STATS_STALE_COLD = _envi("STATS_STALE_COLD", 900) # re-check quiet items this often
KINTARA_MIN_GAP = _envf("KINTARA_MIN_GAP", 0.5)   # global min gap between ANY two kintara.gg hits
KINTARA_BACKOFF = _envf("KINTARA_BACKOFF", 45)    # pause this long after a 429/403 (rate-limited)

# A single shared pacer so EVERY background loop's requests to kintara.gg are spread
# out — 24/7 operation then can't burst the marketplace no matter how the loops line
# up. KINTARA_MIN_GAP=0.5 ⇒ at most ~2 requests/second total; a 429/403 adds a backoff.
_pace_lock = threading.Lock()
_pace_last = [0.0]
_pace_backoff_until = [0.0]


def pace_kintara():
    """Block until both the global min-gap and any active rate-limit backoff allow
    the next kintara.gg request. Call right before each request to that host."""
    with _pace_lock:
        now = time.monotonic()
        target = max(_pace_last[0] + KINTARA_MIN_GAP, _pace_backoff_until[0])
        wait = target - now
        if wait > 0:
            time.sleep(wait)
        _pace_last[0] = time.monotonic()


def kintara_rate_limited():
    """Note a 429/403 so the pacer holds off for KINTARA_BACKOFF seconds."""
    _pace_backoff_until[0] = max(_pace_backoff_until[0], time.monotonic() + KINTARA_BACKOFF)


BASE = "https://kintara.gg/api/marketplace/listings"
STATS_BASE = "https://kintara.gg/api/marketplace/stats"
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36")
# KINS/SOL pool on Solana (same token kintaragold.xyz tracks). GeckoTerminal
# gives free OHLCV in USD (minute/hour/day), our source for the KINS price.
GECKO_POOL = "F42tZnKPavq1VUcrL6ymhc6YqVpt84fWwgzbNTv2wb3W"
GECKO_OHLCV = (f"https://api.geckoterminal.com/api/v2/networks/solana/"
               f"pools/{GECKO_POOL}/ohlcv")
KINTARAGOLD_URL = "https://kintaragold.xyz/"
# kintara.gg's live server list (population label + queue length per server).
SERVERS_URL = "https://kintara.gg/api/servers"
# kintara.gg's own traveling-merchant campaign state (public, no auth — the game
# client reads it the same way via its read-fanout origin). Returns
# {ok, mode, wood, stone, coal, cooked_fish_meat, metal, goals:{...}, complete,
#  goldTradeEnabled, goldStock, goldStockFull}.
MERCHANT_URL = "https://kintara.gg/api/world/merchant-campaign"
# Traveling-merchant donation campaign resources, from kintara.gg game.js
# (`MERCHANT_CAMPAIGN_GOALS`) and the live `/api/world/merchant-campaign` payload.
# The server can override goal quantities in the payload; this tuple controls order
# and labels for the progress tracker.
# (key, label)
MERCHANT_CAMPAIGN_RESOURCES = (
    ("wood", "Wood"),
    ("stone", "Stone"),
    ("coal", "Coal"),
    ("cooked_fish_meat", "Cooked Fish"),
    ("metal", "Metal"),
)
# Traveling-merchant gold-trade recipe = resources consumed per 1 gold minted,
# taken from kintara.gg's own game.js (`MERCHANT_TRADE_COST`). This is separate
# from the donation goals; the donation drive now also includes metal, while the
# current gold-trade recipe does not.
# (key, label, qty per 1 gold)
MERCHANT_RECIPE = (
    ("wood", "Wood", 2500),
    ("stone", "Stone", 1500),
    ("coal", "Coal", 700),
    ("cooked_fish_meat", "Cooked Fish", 30),
)
# Public live-spectator WebSocket (the game's own read-fanout host). Per shard,
# plain JSON, no auth. Streams {t:"snap", region, onlineTotal, players:[{id,name,
# x,z,ry,avg,eq,bdg,php,outfit{...},...}]} — a live roster near the spectator
# camera plus the global online count. Reverse-engineered from game.js.
# Spectate is per SERVER (the in-game "sN" = server number). The read-fanout host
# (ktra-server-b) only mirrors servers 1-4, but kintara.gg itself serves the live
# spectator stream for all 12 servers, so we point at the authoritative origin.
SPECTATE_WS = "wss://kintara.gg/ws/spectate/s{shard}"
SHARDS = tuple(range(1, 13))   # 12 separate game servers/worlds
# The spectator is only sent players in the realm it's subscribed to (via a
# {t:"spec_reg",region} message), and within the big "world" realm only those near
# the hub camera. To roster a whole world we round-robin every realm, accumulating
# players tagged by where they are. (label, emoji) per realm key.
# Labels are the in-game area names — the title that flashes on screen when you
# enter (playRegionIntro / *_DISPLAY_NAME in game.js + constants.js), NOT our own
# nicknames. e.g. eldergrove displays as "Whisperwood", beach as "The Shores".
SPECTATE_REGIONS = {
    "world": ("The Mainland", "\U0001F3D5"),   # the main map / hub
    "pond": ("The Pond", "\U0001F3A3"),
    "beach": ("The Shores", "\U0001F3D6"),
    "eldergrove": ("Whisperwood", "\U0001F332"),
    "frostmere": ("Frostmere", "❄"),
    "arena": ("The Arena", "⚔"),
    "wild": ("The Wilderness", "\U0001F5E1"),
    "wild_ext": ("Deep Wilderness", "\U0001F5E1"),
    "wild_exp": ("Wilderness East", "\U0001F5E1"),
    "mine": ("The Mine", "⛏"),
    "spider": ("Spider Lair", "\U0001F577"),
    "shack": ("Shack", "\U0001F3DA"),
}
PROPERTY_STATUS_URL = "https://kintara.gg/api/property-signs/status"
# Skin-tone palette (game.js `SKIN_TONE_HEX`) for rendering player avatars.
SKIN_TONE_HEX = ("#f1e8df", "#e3c19e", "#d4a574", "#7e5f49", "#5c4332")
# Property plots in world-grid (col0,col1,row0,row1) from game.js (MANSIONS,
# REGULAR_HOUSES via REGULAR_HOUSE_SLOT_TO_ID=[1,2,5,3,4], TRAILERS). Gives every
# property a real map position so the Property Map matches the in-game estate row.
PROPERTY_PLOTS = {
    "mansion": {  # col0, col1, row0, row1
        1: (23, 26, 42, 46), 2: (23, 26, 49, 53), 3: (23, 26, 56, 60),
    },
    "house": {
        1: (35, 37, 52, 56), 2: (35, 37, 46, 50), 3: (37, 41, 41, 43),
        4: (41, 43, 35, 39), 5: (52, 56, 35, 37),
    },
    "trailer": {
        1: (45, 46, 57, 60), 2: (45, 46, 51, 54), 3: (57, 60, 45, 46),
        4: (51, 54, 45, 46), 5: (51, 52, 56, 59), 6: (57, 60, 51, 52),
        7: (52, 55, 51, 52), 8: (56, 59, 57, 58),
    },
}

# ---------------------------------------------------------------------------
# Item index metadata (cosmetics / mounts / pets) — datamined from kintara.gg
# game.js: MARKETPLACE_ITEM_DESC (flavor + sourcing hints), *_MOUNT_SPEED_FACTOR
# (ride speeds), ALCHEMIST_MOUNT_SHOP_TYPES (weekly mount shop), and
# DAILY_SPINNER_SEGMENT_TYPE (free daily spin = Red Aura only, ~1 in 22).
# Drives the Index tab's per-item info panel. `spd` = ride speed boost %.
# ---------------------------------------------------------------------------
MOUNT_SPEED = {  # +% move speed while riding (getLocalMoveSpeedFactor)
    "mount_eagle": 50, "mount_unicorn": 45, "mount_tralalero_tralala": 45,
    "mount_tiger": 40, "mount_harambe": 40, "mount_whale_gold": 35,
    "mount_whale": 30, "mount_crocodile": 30, "mount_dragon": 25,
    "mount_spider": 25, "mount_wolf": 25, "mount_giraffe": 10,
    "mount_wooly_mammoth": 10,
}
# Mounts that rotate ONE-AT-A-TIME through the alchemist WEEKLY shop (gold price,
# server-set each week; base ~3 gold). Limited window → resale market after rotation.
ALCHEMIST_MOUNTS = {
    "mount_dragon", "mount_whale", "mount_whale_gold", "mount_spider", "mount_tiger",
    "mount_unicorn", "mount_giraffe", "mount_wooly_mammoth", "mount_harambe",
    "mount_tralalero_tralala", "mount_crocodile",
}
# Community-confirmed *in-shop* gold prices. The real per-rotation prices are
# server-side / auth-gated (not in the public client — only placeholder defaults),
# so this is a hand-maintained list of prices observed in-game. Add as confirmed.
KNOWN_SHOP_PRICE = {            # itemType -> gold price seen in the shop
    "cosmetic_mog_glasses": 10,
}
# Per-item flavor / special features (verbatim-ish from MARKETPLACE_ITEM_DESC).
ITEM_DESC = {
    "mount_dragon": "Summonable dragon mount for faster overland travel.",
    "mount_eagle": "Majestic American eagle mount that soars high, wings always beating. (Eagle skin of the dragon mount.)",
    "mount_whale": "Summonable whale mount for crossing deep water quickly.",
    "mount_whale_gold": "Rare golden whale — same ride, faster movement.",
    "mount_spider": "Summonable spider mount for faster overland travel.",
    "mount_wolf": "A loyal wolf mount tamed with roast chicken in Wilderness East.",
    "mount_tiger": "A fierce tiger mount for faster overland travel.",
    "mount_unicorn": "A radiant golden-maned unicorn for magical overland travel.",
    "mount_crocodile": "A hulking crocodile — ride low and slow across land.",
    "mount_giraffe": "A tall giraffe for swift (if gentle) overland travel.",
    "mount_wooly_mammoth": "A sturdy wooly mammoth for heavy overland travel.",
    "mount_harambe": "A mighty silverback gorilla — knuckle-walks fast across land.",
    "mount_tralalero_tralala": "A great-white shark that struts on blue Nike sneakers. Rare catch from The Shores.",
    "cosmetic_red_aura": "A blazing red aura of glowing energy around your character.",
    "cosmetic_blue_aura": "A blazing blue aura of glowing energy around your character.",
    "cosmetic_green_aura": "A blazing green aura of glowing energy around your character.",
    "cosmetic_gold_aura": "A blazing gold aura of glowing energy around your character.",
    "cosmetic_jester_hat": "Split red & yellow jester hat with belled points.",
    "cosmetic_neet_hat": "Rounded navy ball cap repping NEET.",
    "cosmetic_inferno_top_hat": "Cosmetic headwear — pure style.",
    "cosmetic_rainbow_top_hat": "Rainbow cosmetic top hat.",
    "cosmetic_chill_house_hat": "A cozy little house worn over the head — gable roof, red nose, grin.",
    "cosmetic_lava_backward_cap": "Molten backwards cap with looping fire tones.",
    "cosmetic_fnice_baseball_cap": "Animated fire-and-ice baseball cap.",
    "cosmetic_galaxy_cowboy_hat": "Cowboy hat with animated galaxy nebula shader.",
    "cosmetic_dog_mask": "Semi-geometric dog mask over the whole head — tan fur, floppy ears.",
    "cosmetic_json_helmet": "A sturdy helmet — pure style.",
    "cosmetic_alon_durag": "Sleek durag tied over the head with tails down the back.",
    "cosmetic_alon_glasses": "Slim stylish shades, click-on over any look.",
    "cosmetic_unc_glasses": "Black wraparound shades + a few wispy hairs.",
    "cosmetic_mog_glasses": "Oversized single-lens shield shades, rainbow-to-red mirror sheen.",
    "cosmetic_skull_hoodie": "Cosmetic top — no combat stats.",
    "cosmetic_troll_hoodie": "White hoodie with the troll graphic.",
    "cosmetic_pump_fun_hoodie": "Pump.fun green hoodie with the Pump logo.",
    "cosmetic_alon_tank_top": "White sleeveless tank with the Alon graphic.",
    "cosmetic_phantom_tshirt": "Purple tee with the white Phantom ghost.",
    "cosmetic_solana_tshirt": "Gradient tee with the Solana logo.",
    "cosmetic_rainbow_tshirt": "Colorful cosmetic shirt.",
    "cosmetic_lava_tshirt": "Molten shirt with looping fire tones.",
    "cosmetic_galaxy_tank_top": "Tank top with animated galaxy stars & nebula.",
    "cosmetic_canada_jersey": "Red World Cup jersey, Canada maple leaf.",
    "cosmetic_mexico_jersey": "Red World Cup jersey, Mexico eagle crest.",
    "cosmetic_usa_jersey": "White World Cup jersey, starred navy crest.",
    "cosmetic_camo_cargo": "Cosmetic camo legwear.",
    "cosmetic_rainbow_pants": "Colorful cosmetic pants.",
    "cosmetic_lava_pants": "Molten pants with looping fire tones.",
    "cosmetic_galaxy_shorts": "Shorts with animated galaxy nebula & stars.",
    "cosmetic_fnice_shorts": "Shorts with fire-to-ice procedural animation.",
    "cosmetic_fnice_longsleeve": "Long sleeve, rising fire blending into icy blues.",
    "cosmetic_unc_shorts": "Beige khaki cargo shorts — pairs with the Unc Tanline.",
    "cosmetic_rainbow_boots": "Colorful cosmetic footwear.",
    "cosmetic_galaxy_boots": "Boots with a procedural galaxy nebula.",
    "cosmetic_lava_boots": "Molten footwear with looping fire tones.",
    "cosmetic_fnice_shoes": "Footwear with fire & ice shimmer.",
    "cosmetic_unc_sandals": "Brown slides + crisp white socks. Peak Unc energy.",
    "cosmetic_tan_line": "Tank-top tan-line dad bod — translucent tank, beer belly.",
}
# range -> (window seconds, bucket seconds). KINS sets the x-grid; gold is
# interpolated onto it. Mirrors kintaragold's selector, at finer resolution.
GOLD_RANGES = {
    "4H":  (4 * 3600, 180),       # 3-min
    "1D":  (24 * 3600, 180),      # 3-min
    "3D":  (3 * 86400, 900),      # 15-min
    "7D":  (7 * 86400, 3600),     # 1-hour
    "14D": (14 * 86400, 3600),    # 1-hour
    "ALL": (60 * 86400, 14400),   # 4-hour
}
PAGE = 40
MAX_PAGES = 200
HTTP_TIMEOUT = 20


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------

def connect(readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
    else:
        con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    return con


def init_db() -> None:
    con = connect()
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS listings (
            id           INTEGER PRIMARY KEY,
            seller_id    INTEGER,
            seller_name  TEXT,
            item_type    TEXT,
            category     TEXT,
            quantity     INTEGER,
            price_gold   INTEGER,
            currency     TEXT,
            price_usd    REAL,
            unit_price   REAL,
            per_unit     REAL,
            reserved_by      INTEGER,
            reserved_until   INTEGER,
            item_durability  INTEGER,
            created_at   TEXT,
            first_seen   TEXT,
            last_seen    TEXT,
            active       INTEGER DEFAULT 1,
            removed_at   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_item     ON listings(item_type);
        CREATE INDEX IF NOT EXISTS idx_category ON listings(category);
        CREATE INDEX IF NOT EXISTS idx_active   ON listings(active);
        CREATE INDEX IF NOT EXISTS idx_removed  ON listings(removed_at);

        CREATE TABLE IF NOT EXISTS polls (
            ts TEXT, active INTEGER, removed INTEGER, ok INTEGER
        );
        CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT);

        -- cached daily-sales stats from kintara's /marketplace/stats (one row
        -- per item+currency). Refreshed slowly in the poll loop; the data is a
        -- daily aggregate so it doesn't need to be live.
        CREATE TABLE IF NOT EXISTS item_stats (
            item_type  TEXT,
            currency   TEXT,
            day        TEXT,
            day_sales  INTEGER,
            day_avg    REAL,
            avg30d     REAL,
            updated_at TEXT,
            PRIMARY KEY (item_type, currency)
        );

        -- archived per-day completed-sale history (one row per item/currency/date).
        -- Past dates never change, so once stored we serve charts from here and
        -- only the background loop re-touches the current day.
        CREATE TABLE IF NOT EXISTS sales_daily (
            item_type TEXT, currency TEXT, date TEXT,
            sales INTEGER, avg_price REAL,
            PRIMARY KEY (item_type, currency, date)
        );
        CREATE INDEX IF NOT EXISTS idx_sd_item ON sales_daily(item_type, currency);
        CREATE INDEX IF NOT EXISTS idx_sd_date ON sales_daily(date);

        -- our own directly-measured gold price series. The gold_price_loop writes
        -- one row every ~3 min = average per-gold USD ask of the 3 cheapest live
        -- gold listings. This is the authoritative gold price while the tracker
        -- runs; kintaragold.xyz is only a fallback for gaps when it wasn't running.
        CREATE TABLE IF NOT EXISTS gold_price (
            ts        INTEGER PRIMARY KEY,   -- epoch ms
            usd       REAL,                  -- USD per 1 gold
            listings  INTEGER                -- how many listings the avg used (≤3)
        );

        -- ACTUAL completed sales (not removed/cancelled listings). Detected by the
        -- stats loop: when an item's current-day completed-sale count (the
        -- authoritative figure from kintara's /stats) goes UP, that increment is a
        -- real sale. `units` = how many sold since we last looked; `price` = the
        -- marginal avg unit price of just those units (backed out of the day total),
        -- in `currency` (gold or USD/token). This is the truthful sales feed.
        CREATE TABLE IF NOT EXISTS sales_events (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            item_type TEXT, currency TEXT,
            units     INTEGER, price REAL,
            day       TEXT,                  -- game day (UTC) the sale belongs to
            ts        INTEGER                -- epoch ms we observed it
        );
        CREATE INDEX IF NOT EXISTS idx_se_ts ON sales_events(ts);
        CREATE INDEX IF NOT EXISTS idx_se_item ON sales_events(item_type, ts);
        """
    )

    # migrate pre-existing databases: add any columns missing from old schemas,
    # then backfill the derived ones.
    shave = {r["name"] for r in con.execute("PRAGMA table_info(item_stats)")}
    if "day_avg" not in shave:
        con.execute("ALTER TABLE item_stats ADD COLUMN day_avg REAL")
    have = {r["name"] for r in con.execute("PRAGMA table_info(listings)")}
    for col, decl in (("category", "TEXT"), ("per_unit", "REAL"),
                      ("reserved_by", "INTEGER"), ("reserved_until", "INTEGER"),
                      ("item_durability", "INTEGER")):
        if col not in have:
            con.execute(f"ALTER TABLE listings ADD COLUMN {col} {decl}")
    con.execute(
        "UPDATE listings SET per_unit = unit_price*1.0/quantity "
        "WHERE per_unit IS NULL AND quantity > 0 AND unit_price IS NOT NULL")
    for r in con.execute(
            "SELECT DISTINCT item_type FROM listings WHERE category IS NULL"):
        con.execute("UPDATE listings SET category=? WHERE item_type=?",
                    (categorize(r["item_type"]), r["item_type"]))
    con.commit()
    con.close()


def get_setting(con, key, default=None):
    row = con.execute("SELECT v FROM settings WHERE k=?", (key,)).fetchone()
    return row["v"] if row else default


def set_setting(con, key, value):
    con.execute(
        "INSERT INTO settings(k,v) VALUES(?,?) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    con.commit()


def unit_price(L: dict) -> float:
    """Headline price of a listing in its own currency (total, not per item)."""
    return L.get("priceUsd") if L.get("currency") == "token" else L.get("priceGold")


def per_item_price(L: dict):
    """Price for a *single* item = headline price / stack size.

    Gold listings come back in gold, token listings in USD. This is the number
    that actually matters for arbitrage: a 5000-wood stack for 2 gold is
    0.0004 gold/wood, not "2 gold"."""
    hp = unit_price(L)
    q = L.get("quantity") or 0
    if hp is None or q <= 0:
        return None
    return hp / q


# ---------------------------------------------------------------------------
# categories — the API ignores its own ?category= param and never returns a
# category on a listing, so we derive it from the itemType naming scheme.
# Prefix-based first (future items inherit automatically), then a few bare
# names that have no prefix.
# ---------------------------------------------------------------------------

CATEGORY_PREFIXES = (
    ("cosmetic_", "cosmetic"),
    ("mount_", "mount"),
    ("pet_", "pet"),
    ("furniture_", "furniture"),
    ("tool_", "tool"),
    ("potion_", "potion"),
    ("key_", "key"),
    ("wild_", "weapon"),
)
MATERIALS = {"wood", "stone", "coal", "metal"}
FOOD = {"fish", "cooked_fish_meat", "cooked_chicken", "raw_chicken"}
BUILDING = {"shack_kit"}


def categorize(item_type: str) -> str:
    if not item_type:
        return "other"
    if item_type == "gold":
        return "currency"
    for pre, cat in CATEGORY_PREFIXES:
        if item_type.startswith(pre):
            return cat
    if item_type in MATERIALS:
        return "material"
    if item_type in FOOD:
        return "food"
    if item_type in BUILDING:
        return "building"
    return "other"


# ---------------------------------------------------------------------------
# display names — the marketplace API only gives internal itemTypes
# (cosmetic_fnice_longsleeve); these are the in-game labels from kintara.gg's
# item index. Ripped from the game bundle's label catalog.
# ---------------------------------------------------------------------------

ITEM_LABELS = {
    "coal": "Coal",
    "cooked_chicken": "Chicken",
    "cooked_fish_meat": "Cooked Fish Meat",
    "cosmetic_alon_tank_top": "Alon Tank Top",
    "cosmetic_blue_aura": "Blue Aura",
    "cosmetic_camo_cargo": "Camo Cargo Pants",
    "cosmetic_canada_jersey": "Canada Jersey",
    "cosmetic_chill_house_hat": "Chill House Hat",
    "cosmetic_dog_mask": "Jotchua",
    "cosmetic_fnice_baseball_cap": "Fire 'n Ice Baseball Cap",
    "cosmetic_fnice_longsleeve": "Fire 'n Ice Long Sleeve",
    "cosmetic_fnice_shoes": "Fire 'n Ice Shoes",
    "cosmetic_fnice_shorts": "Fire 'n Ice Shorts",
    "cosmetic_galaxy_boots": "Galaxy Boots",
    "cosmetic_galaxy_cowboy_hat": "Galaxy Cowboy Hat",
    "cosmetic_galaxy_shorts": "Galaxy Shorts",
    "cosmetic_galaxy_tank_top": "Galaxy Tank Top",
    "cosmetic_gold_aura": "Gold Aura",
    "cosmetic_green_aura": "Green Aura",
    "cosmetic_inferno_top_hat": "Inferno Top Hat",
    "cosmetic_jester_hat": "Jester Hat",
    "cosmetic_lava_backward_cap": "Molten Backwards Cap",
    "cosmetic_lava_boots": "Molten Shoes",
    "cosmetic_lava_pants": "Molten Pants",
    "cosmetic_lava_tshirt": "Molten T-shirt",
    "cosmetic_mexico_jersey": "Mexico Jersey",
    "cosmetic_mog_glasses": "MOG Glasses",
    "cosmetic_neet_hat": "NEET Hat",
    "cosmetic_phantom_tshirt": "Phantom T-shirt",
    "cosmetic_pump_fun_hoodie": "Pump.fun Hoodie",
    "cosmetic_rainbow_boots": "Rainbow Boots",
    "cosmetic_rainbow_pants": "Rainbow Pants",
    "cosmetic_rainbow_top_hat": "Rainbow Top Hat",
    "cosmetic_rainbow_tshirt": "Rainbow T-shirt",
    "cosmetic_red_aura": "Red Aura",
    "cosmetic_skull_hoodie": "Night Skull Hoodie",
    "cosmetic_solana_tshirt": "Solana T-shirt",
    "cosmetic_tan_line": "Unc Tanline",
    "cosmetic_troll_hoodie": "Troll Hoodie",
    "cosmetic_unc_glasses": "Unc Glasses",
    "cosmetic_unc_sandals": "Unc Sandals",
    "cosmetic_unc_shorts": "Unc Shorts",
    "cosmetic_usa_jersey": "USA Jersey",
    "firepit_kit": "Firepit (Portable)",
    "fish": "Fish",
    "furniture_bed": "Oak Double Bed",
    "furniture_couch": "Modular Couch",
    "furniture_holokin": "Holo Kin",
    "furniture_marble_corner": "Marble Corner",
    "furniture_marble_divider": "Marble Divider",
    "furniture_marble_gate": "Marble Gate",
    "furniture_sidetable": "Oak Side Table",
    "furniture_soccer_ball": "World Cup Football",
    "furniture_sunflower": "Sunflower Pot",
    "furniture_throne": "Throne",
    "furniture_worldcup": "World Cup Trophy",
    "gold": "Gold",
    "key_flat_1": "Flat 1 Key",
    "key_flat_10": "Flat 10 Key",
    "key_flat_11": "Apartment 1 Key",
    "key_flat_12": "Apartment 2 Key",
    "key_flat_13": "Apartment 3 Key",
    "key_flat_14": "Apartment 4 Key",
    "key_flat_15": "Apartment 5 Key",
    "key_flat_16": "Penthouse 1 Key",
    "key_flat_17": "Penthouse 2 Key",
    "key_flat_18": "Penthouse 3 Key",
    "key_flat_2": "Flat 2 Key",
    "key_flat_3": "Flat 3 Key",
    "key_flat_4": "Flat 4 Key",
    "key_flat_5": "Flat 5 Key",
    "key_flat_6": "Flat 6 Key",
    "key_flat_7": "Flat 7 Key",
    "key_flat_8": "Flat 8 Key",
    "key_flat_9": "Flat 9 Key",
    "key_house_1": "House 1 Key",
    "key_house_2": "House 2 Key",
    "key_house_3": "House 3 Key",
    "key_house_4": "House 4 Key",
    "key_house_5": "House 5 Key",
    "key_mansion_1": "Mansion 1 Key",
    "key_mansion_2": "Mansion 2 Key",
    "key_mansion_3": "Mansion 3 Key",
    "key_trailer_1": "Trailer 1 Key",
    "key_trailer_2": "Trailer 2 Key",
    "key_trailer_3": "Trailer 3 Key",
    "key_trailer_4": "Trailer 4 Key",
    "key_trailer_5": "Trailer 5 Key",
    "key_trailer_6": "Trailer 6 Key",
    "key_trailer_7": "Trailer 7 Key",
    "key_trailer_8": "Trailer 8 Key",
    "metal": "Metal",
    "mount_crocodile": "Crocodile Mount",
    "mount_dragon": "Dragon Mount",
    "mount_eagle": "Eagle Mount",
    "mount_giraffe": "Giraffe Mount",
    "mount_harambe": "Harambe Mount",
    "mount_spider": "Spider Mount",
    "mount_tiger": "Tiger Mount",
    "mount_tralalero_tralala": "Tralalero Tralala Mount",
    "mount_unicorn": "Unicorn Mount",
    "mount_whale": "Whale Mount",
    "mount_whale_gold": "Gold Whale Mount",
    "mount_wolf": "Wolf Mount",
    "mount_wooly_mammoth": "Wooly Mammoth Mount",
    "pet_doge": "Doge",
    "pet_golden_puppy": "Golden Puppy",
    "pet_lmao": "LMAO!",
    "pet_nietzschean_penguin": "Nietzschean Penguin",
    "pet_pepe_the_frog": "Pepe the Frog",
    "pet_phantom": "Phantom",
    "pet_pump_pill": "Pump Pill",
    "pet_purple_dragon": "Purple Dragon",
    "pet_rainbow_dragon": "Rainbow Dragon",
    "pet_smoky_cat": "Smoky Cat",
    "pet_tung_sahur": "Tung Sahur",
    "potion_health": "Health Potion",
    "potion_poison": "Poison Potion",
    "potion_shield": "Shield Potion",
    "potion_strength": "Strength Potion",
    "raw_chicken": "Raw Chicken",
    "shack_kit": "Shack (Portable)",
    "stone": "Stone",
    "tool_axe": "Axe",
    "tool_axe_l2": "Axe (Lvl 2)",
    "tool_fishing_rod": "Fishing Rod",
    "tool_hammer": "Hammer",
    "tool_pickaxe": "Pickaxe",
    "tool_pickaxe_l2": "Pickaxe (Lvl 2)",
    "tool_shovel": "Shovel",
    "wild_sword": "Training Sword",
    "wild_sword_l2": "Wild Sword (Lvl 2)",
    "wood": "Wood",
}

_LABEL_PREFIXES = ("cosmetic_", "mount_", "pet_", "furniture_", "tool_",
                   "potion_", "key_", "wild_", "cooked_", "raw_")


def item_label(item_type: str) -> str:
    """In-game display name, falling back to a prettified itemType for anything
    not in the catalog (e.g. a newly-added item)."""
    if not item_type:
        return item_type or ""
    if item_type in ITEM_LABELS:
        return ITEM_LABELS[item_type]
    s = item_type
    for pre in _LABEL_PREFIXES:
        if s.startswith(pre):
            s = s[len(pre):]
            break
    return s.replace("_", " ").title()


# itemType -> kintara HUD asset path (real in-game art). Cosmetics/pets/keys
# follow a pattern; the rest are an explicit map.
ICON_OVERRIDES = {
    "coal": "resources/coal.png", "stone": "resources/stone.png",
    "wood": "resources/wood.png", "metal": "resources/metal.png",
    "gold": "resources/gold.png", "fish": "resources/fish.png",
    "cooked_fish_meat": "resources/cookedfish.png",
    "cooked_chicken": "resources/cookedchicken.png",
    "raw_chicken": "resources/rawchicken.png",
    "mount_crocodile": "mounts/crocodile.png", "mount_dragon": "mounts/dragon.png",
    "mount_eagle": "mounts/mount_eagle.png", "mount_giraffe": "mounts/giraffe.png",
    "mount_harambe": "mounts/harambe.png", "mount_spider": "mounts/spider.png",
    "mount_tiger": "mounts/tiger.png", "mount_tralalero_tralala": "mounts/tralalero_tralala.png",
    "mount_unicorn": "mounts/unicorn.png", "mount_whale": "mounts/whale.png",
    "mount_whale_gold": "mounts/whale_gold.png", "mount_wolf": "mounts/wolf.png",
    "mount_wooly_mammoth": "mounts/wooly_mammoth.png",
    "tool_axe": "tools/axe.png", "tool_axe_l2": "tools/axelvl2.png",
    "tool_fishing_rod": "tools/fishingrod.png", "tool_hammer": "tools/hammer.png",
    "tool_pickaxe": "tools/pickaxe.png", "tool_pickaxe_l2": "tools/pickaxelvl2.png",
    "tool_shovel": "tools/shovel.png", "wild_sword": "tools/sword.png",
    "wild_sword_l2": "tools/swordlvl2.png",
    "potion_health": "potions/health.svg", "potion_poison": "potions/poison.svg",
    "potion_shield": "potions/shield.svg", "potion_strength": "potions/strength.svg",
    "shack_kit": "buildings/shack.svg", "firepit_kit": "buildings/firepit.svg",
}
ICON_DIR = "icons_cache"


def icon_asset(item_type):
    """Relative HUD asset path for an item, or None if there's no art."""
    if not item_type:
        return None
    if item_type in ICON_OVERRIDES:
        return ICON_OVERRIDES[item_type]
    if item_type.startswith("cosmetic_"):
        return f"cosmetics/{item_type}.png"
    if item_type.startswith("pet_"):
        return "pets/paw.svg"
    if item_type.startswith("key_"):
        if "mansion" in item_type:
            return "keys/goldkey.png"
        if "house" in item_type:
            return "keys/silverkey.png"
        return "keys/bronzekey.png"
    return None


# ---------------------------------------------------------------------------
# fetching (runs on the user's machine; needs network)
# ---------------------------------------------------------------------------

def fetch_all_active():
    """Page through the live listings. kintara.gg is flaky on deep pages, so each
    page is retried a couple times; if a page still fails we return what we already
    have with complete=False (never raise). reconcile() only marks listings gone on a
    complete fetch, so a partial poll just keeps the last-good data instead of erroring
    — a transient timeout self-heals on the next cycle without surfacing anything."""
    import requests
    out, offset = [], 0
    headers = {"User-Agent": "kintara-tracker/1.0 (personal market tracker)"}
    for _ in range(MAX_PAGES):
        params = {"sort": "latest", "currency": "all", "category": "all",
                  "limit": PAGE, "offset": offset, "q": ""}
        data = None
        for attempt in range(3):
            try:
                pace_kintara()
                r = requests.get(BASE, params=params, headers=headers, timeout=HTTP_TIMEOUT)
                if r.status_code in (429, 403):
                    kintara_rate_limited()
                r.raise_for_status()
                data = r.json()
                break
            except Exception:
                if attempt == 2:
                    return out, False          # give up on this page; keep partial data
                time.sleep(1.5 * (attempt + 1))
        if not data.get("ok"):
            return out, False
        batch = data.get("listings", [])
        out.extend(batch)
        if not data.get("hasMore") or not batch:
            return out, True
        offset += PAGE
    return out, True


def fetch_kins_ohlcv(timeframe="day", aggregate=1, limit=1000, before=None):
    """KINS close price in USD from GeckoTerminal as [(ts_seconds, close)] ascending.
    GeckoTerminal aggregates: minute 1/5/15, hour 1/4/12, day 1."""
    import requests
    params = {"aggregate": aggregate, "limit": limit, "currency": "usd", "token": "base"}
    if before:
        params["before_timestamp"] = int(before)
    r = requests.get(f"{GECKO_OHLCV}/{timeframe}", params=params,
                     headers={"User-Agent": BROWSER_UA, "Accept": "application/json"},
                     timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    lst = (r.json().get("data", {}).get("attributes", {}).get("ohlcv_list") or [])
    out = [(row[0], row[4]) for row in lst]
    out.sort(key=lambda x: x[0])
    return out


_KINS_DAILY_CACHE = {"ts": 0, "map": {}}


def kins_daily_usd():
    """{ 'YYYY-MM-DD' (UTC): KINS close USD } for the last ~1000 days of daily
    candles. Cached ~5 min. Lets us re-price an item's daily USD sale history in
    $KINS (item_usd / kins_usd) to separate item alpha from token beta."""
    import time as _t
    if _t.time() - _KINS_DAILY_CACHE["ts"] < 300 and _KINS_DAILY_CACHE["map"]:
        return _KINS_DAILY_CACHE["map"]
    try:
        rows = fetch_kins_ohlcv("day", 1, 1000)
    except Exception:
        return _KINS_DAILY_CACHE["map"]
    m = {}
    for ts, close in rows:
        if close and close > 0:
            m[datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")] = close
    if m:
        _KINS_DAILY_CACHE["map"] = m
        _KINS_DAILY_CACHE["ts"] = _t.time()
    return m


def item_index_meta(con, item_type: str) -> dict:
    """Sourcing / availability / special-feature metadata for one cosmetic, mount
    or pet, for the Index info panel. Static facts (source channel, cost, ride
    speed, flavor) are datamined into ITEM_DESC/MOUNT_SPEED/ALCHEMIST_MOUNTS; the
    availability window + supply status are DERIVED from our sales_daily archive."""
    it = item_type
    cat = categorize(it)
    spd = MOUNT_SPEED.get(it)
    note = ITEM_DESC.get(it)
    # --- source channel + availability cadence (code-confirmed from game.js shop
    #     payload shapes; per-rotation gold prices are server-set + auth-gated, so
    #     they are NOT fixed — we show the data-derived cheapest-ever as a proxy). ---
    #   * Alchemist mount shop: ONE mount per WEEKLY rotation (endsAtMs +7d).
    #   * Cosmetic shop: a DAILY single slot (endsAtMs +24h) AND a WEEKLY bundle (+7d).
    #   * Pet shop: WEEKLY bundle of 3 (+7d).
    #   * Daily free spinner: Red Aura only (1 of 22 slices ≈ 4.5%).
    if it == "mount_wolf":
        src = "World — tame in Wilderness East"
        cadence = "Always available (not a timed drop)"
        cost = "Free: feed a wild wolf roast chicken. Grindable → effectively unlimited supply."
    elif it == "mount_tralalero_tralala":
        src = "World drop (The Shores) + alchemist shop"
        cadence = "Rare world catch · also a weekly shop slot"
        cost = "Rare catch while fishing The Shores, or buy it the week it rotates into the alchemist mount shop (gold)."
    elif it == "mount_eagle":
        src = "Eagle skin of the dragon mount"
        cadence = "Limited / premium skin"
        cost = "An eagle reskin of the dragon mount (fastest ride at +50%); scarce, not a normal weekly shop slot."
    elif it in ALCHEMIST_MOUNTS:
        src = "Alchemist weekly mount shop"
        cadence = "Weekly drop — 7-day window, one mount/week"
        cost = "Gold, price set per weekly rotation (varies by mount — premium ones cost far more). Buyable only the week it's featured; resale market after."
    elif it == "cosmetic_red_aura":
        src = "Daily free spinner (the wheel)"
        cadence = "Daily — free spin every 24h (~1 in 22)"
        cost = "ONLY from the free daily wheel — Red Aura is the lone cosmetic slice. Perpetual faucet → it saturates and bleeds."
    elif it.endswith("_aura"):              # blue / green / gold aura
        src = "Paid spinner ($5 wheel)"
        cadence = "Paid spin — random prize, no fixed window"
        cost = "ONLY from the $5 paid wheel (random). No shop, not on the free wheel → far scarcer than Red Aura."
    elif cat == "pet":
        src = "Whisperwood pet shop"
        cadence = "Weekly drop — 7-day window, 3 pets/week"
        cost = "Gold, price set per weekly rotation. Cosmetic companion — follows you, no combat stats."
    elif cat == "cosmetic":
        src = "Cosmetic shop (gold)"
        cadence = "Cosmetic shop: daily slot (24h) or weekly bundle (7d)"
        cost = "Bought with gold during its shop window (a daily 24h feature OR the weekly 7-day bundle). Exact price is set per rotation server-side."
    else:
        src, cadence, cost = ("—", "", "")
    # --- shop price: per-rotation gold prices are server-side/auth-gated and NOT
    #     in the public client (only placeholder defaults). KNOWN_SHOP_PRICE holds
    #     community-confirmed in-shop gold prices; otherwise we fall back to the
    #     cheapest-ever GOLD sale as a market-floor proxy. ---
    shop_gold = KNOWN_SHOP_PRICE.get(it)
    rows = con.execute(
        """SELECT date, SUM(COALESCE(sales,0)) s FROM sales_daily
           WHERE item_type=? GROUP BY date HAVING s>0 ORDER BY date""", (it,)).fetchall()
    lo_g = con.execute(
        """SELECT MIN(avg_price) lo FROM sales_daily
           WHERE item_type=? AND currency='gold' AND sales>0 AND avg_price>0""", (it,)).fetchone()
    lo_u = con.execute(
        """SELECT MIN(avg_price) lo FROM sales_daily
           WHERE item_type=? AND currency='token' AND sales>0 AND avg_price>0""", (it,)).fetchone()
    cheapest_gold = lo_g["lo"] if lo_g else None
    cheapest = lo_u["lo"] if lo_u else None
    first = rows[0]["date"] if rows else None
    last = rows[-1]["date"] if rows else None
    total = sum(r["s"] for r in rows)
    days = len(rows)
    from datetime import date as _date, timedelta as _td
    cutoff = (_date.today() - _td(days=7)).isoformat()
    recent = sum(r["s"] for r in rows if r["date"] >= cutoff)
    share = (recent / total) if total else 0
    # Heuristic supply state: lots of very-recent volume vs the all-time total ⇒
    # still being supplied (flood); little recent volume ⇒ supply dried (resale).
    if total == 0:
        status = "no recorded sales"
    elif recent >= 25 and share >= 0.45:
        status = "flooding — supply still active (don't chase yet)"
    elif recent <= max(2, total * 0.06):
        status = "supply dried up — resale-only (scarcity)"
    else:
        status = "tapering — supply slowing"
    return {
        "item_type": it, "label": item_label(it), "category": cat,
        "source": src, "cadence": cadence, "cost": cost, "speed_pct": spd, "note": note,
        "shop_gold": shop_gold, "cheapest_gold": cheapest_gold, "cheapest_usd": cheapest,
        "first_sale": first, "last_sale": last, "days_traded": days,
        "units_total": total, "units_recent7": recent, "supply_status": status,
    }


def fetch_kins_1min(total_seconds):
    """Page 1-minute KINS candles back far enough to cover total_seconds
    (GeckoTerminal caps each call at 1000)."""
    need = total_seconds // 60 + 2
    out, before = [], None
    while len(out) < need:
        batch = fetch_kins_ohlcv("minute", 1, 1000, before=before)
        if not batch:
            break
        out = batch + out
        before = batch[0][0]            # page further back from the oldest seen
        if len(batch) < 1000:
            break
    return out


def kins_series_for_range(window_sec, bucket_sec):
    """KINS USD series covering the last window_sec, at bucket_sec resolution."""
    cutoff = time.time() - window_sec
    if bucket_sec <= 180:
        raw = fetch_kins_1min(window_sec)            # bucket 1-min -> 3-min below
    elif bucket_sec <= 900:
        raw = fetch_kins_ohlcv("minute", 15, 1000)
    elif bucket_sec <= 3600:
        raw = fetch_kins_ohlcv("hour", 1, 1000)
    else:
        raw = fetch_kins_ohlcv("hour", 4, 1000)
    raw = [(t, c) for t, c in raw if t >= cutoff]
    if bucket_sec <= 180:                            # collapse 1-min into 3-min buckets
        b = {}
        for t, c in raw:
            b[int(t // bucket_sec) * bucket_sec] = c  # last close in each bucket
        raw = sorted(b.items())
    return raw


_kins_px_cache = {"at": 0, "px": None}


def current_kins_usd():
    """Live KINS price in USD (kintara's own figure, matches the index page).
    Cached ~2 min."""
    now = time.time()
    if _kins_px_cache["px"] and now - _kins_px_cache["at"] < 120:
        return _kins_px_cache["px"]
    try:
        import requests
        r = requests.get("https://kintara.gg/api/token/blimp-stats",
                         headers={"User-Agent": BROWSER_UA}, timeout=HTTP_TIMEOUT)
        px = (r.json() or {}).get("priceUsd")
        if px:
            _kins_px_cache.update(at=now, px=px)
    except Exception:
        pass
    return _kins_px_cache["px"]


_kgold_cache = {"at": 0, "data": None, "spot": None}
_goldhist_cache = {}   # range -> (ts, payload), ~3 min TTL (avoids gecko rate limits)


def fetch_kintara_gold_history():
    """Rip kintaragold.xyz's own gold-USD price history (embedded in its page as
    {"history":[{t,price}],"spotPriceUsd":..}). Returns (sorted [(t_ms, usd)], spot).
    This is an independent gold price series (not derived from KINS), which is
    what lets KINS/gold actually move. Cached ~3 min."""
    import requests
    now = time.time()
    if _kgold_cache["data"] and now - _kgold_cache["at"] < 180:
        return _kgold_cache["data"], _kgold_cache["spot"]
    r = requests.get(KINTARAGOLD_URL, headers={"User-Agent": BROWSER_UA}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    html = r.text
    # the series lives in the (escaped) RSC payload and can straddle chunk
    # boundaries, so pull the t/price pairs directly rather than parsing the array
    pairs = re.findall(r'\\"t\\":([\d.]+),\\"price\\":([\d.eE-]+)', html)
    data = sorted(((float(t), float(p)) for t, p in pairs), key=lambda x: x[0])
    m = re.search(r'\\"spotPriceUsd\\":([\d.]+)', html)
    spot = float(m.group(1)) if m else (data[-1][1] if data else None)
    if data:
        _kgold_cache.update(at=now, data=data, spot=spot)
    return data, spot


def interp_gold(data, t_ms):
    """Gold USD at time t (ms) by linear interpolation of the ripped series."""
    if not data:
        return None
    if t_ms <= data[0][0]:
        return data[0][1]
    if t_ms >= data[-1][0]:
        return data[-1][1]
    lo, hi = 0, len(data) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if data[mid][0] < t_ms:
            lo = mid + 1
        else:
            hi = mid
    t1, p1 = data[lo]
    t0, p0 = data[lo - 1]
    return p1 if t1 == t0 else p0 + (p1 - p0) * (t_ms - t0) / (t1 - t0)


def fetch_stats(item_type, currency):
    """One item+currency of completed-sales history from kintara."""
    import requests
    params = {"itemType": item_type}
    if currency == "token":
        params["currency"] = "token"
    pace_kintara()
    r = requests.get(STATS_BASE, params=params,
                     headers={"User-Agent": BROWSER_UA}, timeout=HTTP_TIMEOUT)
    if r.status_code in (429, 403):
        kintara_rate_limited()
    r.raise_for_status()
    return r.json()


def _upsert_stats(con, it, cur, day, day_sales, avg30d, day_avg=None):
    con.execute(
        """INSERT INTO item_stats(item_type,currency,day,day_sales,day_avg,avg30d,updated_at)
           VALUES(?,?,?,?,?,?,?)
           ON CONFLICT(item_type,currency) DO UPDATE SET
             day=excluded.day, day_sales=excluded.day_sales, day_avg=excluded.day_avg,
             avg30d=excluded.avg30d, updated_at=excluded.updated_at""",
        (it, cur, day, day_sales, day_avg, avg30d, now_iso()))
    con.commit()


def _archive_samples(con, it, cur, samples):
    """Persist every daily sample. Past days are immutable; the current day's
    partial figure updates in place. Caller commits.

    Also detects ACTUAL SALES: when a day we've already recorded shows MORE completed
    sales than before, the increment is real (cancellations don't touch /stats' sale
    count), so we log a `sales_events` row for the newly-sold units at their marginal
    price. We only log on an UPDATE (a day we'd already baselined) — the first time we
    see a day we just record the count silently, so pre-existing sales aren't replayed
    as if they happened now."""
    for sm in samples or []:
        d = sm.get("date")
        if not d:
            continue
        new_sales = sm.get("sales")
        new_avg = sm.get("avgUnitPrice")
        prev = con.execute(
            "SELECT sales, avg_price FROM sales_daily WHERE item_type=? AND currency=? AND date=?",
            (it, cur, d)).fetchone()
        if prev is not None and new_sales is not None and prev["sales"] is not None \
                and new_sales > prev["sales"]:
            old_sales, old_avg = prev["sales"], prev["avg_price"]
            units = new_sales - old_sales
            # marginal avg price of just the newly-sold units (back out the running total)
            price = new_avg
            if old_avg is not None and new_avg is not None and units > 0:
                m = (new_sales * new_avg - old_sales * old_avg) / units
                if m and m > 0:
                    price = m
            con.execute(
                "INSERT INTO sales_events(item_type,currency,units,price,day,ts) VALUES(?,?,?,?,?,?)",
                (it, cur, units, price, d, int(time.time() * 1000)))
        con.execute(
            """INSERT INTO sales_daily(item_type,currency,date,sales,avg_price)
               VALUES(?,?,?,?,?)
               ON CONFLICT(item_type,currency,date) DO UPDATE SET
                 sales=excluded.sales, avg_price=excluded.avg_price""",
            (it, cur, d, new_sales, new_avg))


def _mark_stats_attempt(con, it, cur):
    """Record that we tried (so we don't hammer a failing item) WITHOUT clobbering
    any good data already cached for it."""
    con.execute(
        """INSERT INTO item_stats(item_type,currency,updated_at) VALUES(?,?,?)
           ON CONFLICT(item_type,currency) DO UPDATE SET updated_at=excluded.updated_at""",
        (it, cur, now_iso()))
    con.commit()


def _next_stats_pair(con, stale_sec, retry_err_sec=300):
    """Pick the single most worthwhile item+currency to refresh next.

    Priority: most-listed (highest liquidity) items first, so the things you
    actually trade — wood, stone, gold — fill in before low-volume cosmetics.
    A pair is eligible if it's never been fetched, its data is older than
    stale_sec, or a previous fetch errored (retry after retry_err_sec)."""
    liq = {r["item_type"]: r["n"] for r in con.execute(
        "SELECT item_type, COUNT(*) n FROM listings WHERE active=1 GROUP BY item_type")}
    items = [r["item_type"] for r in con.execute("SELECT DISTINCT item_type FROM listings")]
    cached = {(r["item_type"], r["currency"]): r for r in con.execute(
        "SELECT item_type,currency,day_sales,updated_at FROM item_stats")}
    now = time.time()
    best, best_key = None, None
    for it in items:
        for cur in ("gold", "token"):
            row = cached.get((it, cur))
            never = row is None
            if never:
                age = 1e18
            else:
                got = row["day_sales"] is not None  # 0 = confirmed no-sales, still "got"
                try:
                    age = now - datetime.fromisoformat(row["updated_at"]).timestamp()
                except Exception:
                    age = 1e18
                # actively-traded items (lots of live listings) refresh much more often,
                # so the actual-sales feed stays granular for the things that sell a lot;
                # quiet items use the slower cadence. Both env-tunable.
                eff_stale = STATS_STALE_HOT if liq.get(it, 0) >= 15 else stale_sec
                if age < (eff_stale if got else retry_err_sec):
                    continue  # fresh enough, skip
            key = (liq.get(it, 0), never, age)   # liquidity first, then unfetched, then age
            if best_key is None or key > best_key:
                best_key, best = key, (it, cur)
    return best


def stats_loop(stale_sec=None, gap=None):
    """Continuously top up the item_stats cache, one request at a time (paced by the
    global kintara throttle). Decoupled from the listing poll so it fills steadily
    regardless of --interval. Cadence env-tunable (STATS_GAP / STATS_STALE_*)."""
    stale_sec = STATS_STALE_COLD if stale_sec is None else stale_sec
    gap = STATS_GAP if gap is None else gap
    while True:
        con = connect()
        try:
            pair = _next_stats_pair(con, stale_sec)
            if not pair:
                con.close()
                time.sleep(20)
                continue
            it, cur = pair
            try:
                d = fetch_stats(it, cur)
                s = d.get("samples") or []
                day_sales = (s[-1].get("sales", 0) if s else 0)  # 0 = no sales (not missing)
                day = s[-1].get("date") if s else None
                day_avg = s[-1].get("avgUnitPrice") if s else None
                _archive_samples(con, it, cur, s)
                _upsert_stats(con, it, cur, day, day_sales, d.get("avg30d"), day_avg)
            except Exception:
                _mark_stats_attempt(con, it, cur)
        finally:
            con.close()
        time.sleep(gap)


def gold_price_loop(interval=180):
    """Every ~3 min, snapshot our own gold price (avg of the 3 cheapest per-gold
    asks) into the gold_price table. This is what populates the gold chart and
    the arbitrage gold rate while the tracker runs; kintaragold.xyz is only used
    to backfill gaps from when it wasn't running."""
    while True:
        con = connect()
        try:
            gi = get_setting(con, "gold_item")
            px, n = our_gold_price(con, gi)
            if px is not None:
                con.execute(
                    "INSERT OR REPLACE INTO gold_price(ts, usd, listings) VALUES(?,?,?)",
                    (int(time.time() * 1000), round(px, 6), n))
                con.commit()
        except Exception as e:
            print(f"[{now_iso()}] gold price snapshot error: {e}")
        finally:
            con.close()
        time.sleep(interval)


def gold_series_for_chart(con):
    """Gold-USD points for the chart: our own measured series (gold_price table)
    spliced over the kintaragold.xyz fallback. We use kintaragold only for the
    stretch *before* our own data begins; from there on our own snapshots win.
    Returns (sorted [(t_ms, usd)], spot) — spot is our latest price if we have
    one, else kintaragold's."""
    ours = [(r["ts"], r["usd"]) for r in
            con.execute("SELECT ts, usd FROM gold_price ORDER BY ts")]
    try:
        kg, spot = fetch_kintara_gold_history()
    except Exception:
        kg, spot = [], None
    if ours:
        start = ours[0][0]
        merged = [(t, p) for (t, p) in kg if t < start] + ours
        return merged, ours[-1][1]
    return kg, spot


# ---------------------------------------------------------------------------
# servers + traveling merchant (external; cached, last-good on failure)
# ---------------------------------------------------------------------------

_servers_cache = {"at": 0, "data": None}
_merchant_cache = {"at": 0, "data": None}
_property_cache = {"at": 0, "data": None}


def fetch_servers():
    """kintara.gg's live server list (name, populationLabel, full, queueLength,
    minLevel). Cached ~30s; raises on a cold failure (caller serves last-good)."""
    import requests
    now = time.time()
    if _servers_cache["data"] and now - _servers_cache["at"] < 30:
        return _servers_cache["data"]
    r = requests.get(SERVERS_URL, headers={"User-Agent": BROWSER_UA}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    servers = (r.json() or {}).get("servers") or []
    _servers_cache.update(at=now, data=servers)
    return servers


def fetch_merchant():
    """kintara.gg's traveling-merchant campaign state (the same public endpoint the
    game client reads). Cached ~60s; serves last-good on failure."""
    import requests
    now = time.time()
    if _merchant_cache["data"] and now - _merchant_cache["at"] < 60:
        return _merchant_cache["data"]
    try:
        r = requests.get(MERCHANT_URL, headers={"User-Agent": BROWSER_UA},
                         timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        _merchant_cache.update(at=now, data=data)
        return data
    except Exception:
        if _merchant_cache["data"]:
            return _merchant_cache["data"]
        raise


def order_book_usd(con, item_type, gold_rate):
    """The live buy-side depth for item_type as a price ladder: a list of
    [unit_usd, quantity] levels sorted cheapest-first, across BOTH currencies
    (gold listings converted at gold_rate). `quantity` is the stack size of each
    listing = how many units are available at that unit price. Lets the caller
    walk the book to price a large purchase (liquidity-aware), not just take the
    single cheapest ask. Also returns total available units."""
    rclause, rparam = _buyable_clause()
    levels = []
    for r in con.execute(
            f"""SELECT currency, per_unit, quantity FROM listings
                WHERE active=1 AND item_type=? AND per_unit IS NOT NULL
                  AND quantity > 0{rclause}""", (item_type, rparam)):
        usd = r["per_unit"] if r["currency"] == "token" else (
            r["per_unit"] * gold_rate if gold_rate else None)
        if usd is not None:
            levels.append([usd, r["quantity"]])
    levels.sort(key=lambda x: x[0])
    return levels, sum(q for _, q in levels)


def fetch_property_status():
    """kintara.gg's public property ownership board (mansions/houses/trailers →
    ownerName, ownerId, sold, locked). Cached ~30s, last-good on failure."""
    import requests
    now = time.time()
    if _property_cache["data"] and now - _property_cache["at"] < 30:
        return _property_cache["data"]
    try:
        r = requests.get(PROPERTY_STATUS_URL, headers={"User-Agent": BROWSER_UA},
                         timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        _property_cache.update(at=now, data=data)
        return data
    except Exception:
        if _property_cache["data"]:
            return _property_cache["data"]
        raise


# ---------------------------------------------------------------------------
# live spectator hub — one background asyncio loop holding a WebSocket per shard
# (opened lazily, closed when idle). The game's /ws/spectate/sN stream is public
# plain-JSON; we keep a live roster of every player it sends near the spectator
# camera, plus the global online count, and serve it over /api/live.
# ---------------------------------------------------------------------------

class SpectateHub:
    # We revisit each realm once per cycle (~len(regions) * DWELL seconds), so a
    # player must survive longer than a full cycle or they'd flicker out between
    # visits. ~2 cycles.
    PLAYER_TTL = 55      # drop a player not seen in any snapshot for this long
    IDLE_CLOSE = 75      # close a shard's socket if /api/live hasn't asked in this long
    DWELL_WORLD = 5.0    # the big overworld rotates players through view — linger longer
    DWELL_OTHER = 1.6    # instanced realms are small; a short visit catches everyone

    def __init__(self):
        self.loop = None
        self.lock = threading.Lock()
        # shard -> {"players":{id:obj}, "online_total":int|None, "at":ts,
        #           "last_req":ts, "running":bool, "err":str|None}
        self.shards = {}
        self._started = False

    def start(self):
        if self._started:
            return
        self._started = True
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        import asyncio
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _state(self, shard):
        st = self.shards.get(shard)
        if st is None:
            st = {"players": {}, "online_total": None, "at": 0,
                  "last_req": 0, "running": False, "err": None}
            self.shards[shard] = st
        return st

    def request(self, shard):
        """Called from a Flask thread: mark interest and ensure a socket is open."""
        self.start()
        with self.lock:
            st = self._state(shard)
            st["last_req"] = time.time()
            need = not st["running"]
            if need:
                st["running"] = True
        if need and self.loop:
            import asyncio
            asyncio.run_coroutine_threadsafe(self._connect(shard), self.loop)

    async def _connect(self, shard):
        import asyncio
        try:
            import websockets
        except ImportError:
            with self.lock:
                self._state(shard).update(running=False,
                    err="websockets not installed — run: pip install websockets")
            return
        url = SPECTATE_WS.format(shard=shard)
        try:
            async with websockets.connect(
                    url, extra_headers={"User-Agent": BROWSER_UA,
                                        "Origin": "https://kintara.gg"},
                    open_timeout=15, max_size=2 ** 21, ping_interval=None) as ws:
                await ws.send("ping")
                with self.lock:
                    self._state(shard)["err"] = None
                regions = list(SPECTATE_REGIONS.keys())   # "world" is first
                idx = 0
                cur = None
                last_switch = 0.0
                while True:
                    now = time.time()
                    with self.lock:
                        idle = now - self._state(shard)["last_req"]
                    if idle > self.IDLE_CLOSE:
                        break
                    # round-robin the realms so the roster covers the whole world, not
                    # just the hub — lingering on the overworld since its players cycle
                    # through the spectator's limited view.
                    dwell = self.DWELL_WORLD if cur == "world" else self.DWELL_OTHER
                    if cur is None or now - last_switch >= dwell:
                        cur = regions[idx]
                        idx = (idx + 1) % len(regions)
                        last_switch = now
                        try:
                            await ws.send(json.dumps({"t": "spec_reg", "region": cur}))
                        except Exception:
                            break
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    self._ingest(shard, msg)
        except Exception as e:
            with self.lock:
                self._state(shard)["err"] = str(e)[:140]
        finally:
            with self.lock:
                self._state(shard)["running"] = False

    def _ingest(self, shard, msg):
        if isinstance(msg, (bytes, bytearray)):
            return
        try:
            d = json.loads(msg)
        except Exception:
            return
        objs = d if isinstance(d, list) else [d]
        now = time.time()
        with self.lock:
            st = self._state(shard)
            for o in objs:
                if not isinstance(o, dict) or o.get("t") != "snap":
                    continue
                if o.get("onlineTotal") is not None:
                    st["online_total"] = o["onlineTotal"]
                realm = o.get("region")
                for p in o.get("players", []):
                    pid = p.get("id")
                    if pid is None:
                        continue
                    cur = st["players"].get(pid, {})
                    cur.update(p)       # full snaps carry name/outfit; deltas carry pos
                    cur["_seen"] = now
                    if realm:
                        cur["realm"] = realm
                    st["players"][pid] = cur
                st["at"] = now
            stale = [k for k, v in st["players"].items()
                     if now - v.get("_seen", 0) > self.PLAYER_TTL]
            for k in stale:
                del st["players"][k]

    def snapshot(self, shard):
        with self.lock:
            st = self.shards.get(shard)
            if not st:
                return {"online_total": None, "players": [], "connected": False,
                        "err": None, "age": None}
            players = [{k: v for k, v in p.items() if k != "_seen"}
                       for p in st["players"].values()]
            return {"online_total": st["online_total"], "players": players,
                    "connected": st["running"], "err": st["err"],
                    "age": round(time.time() - st["at"], 1) if st["at"] else None}


_spectate_hub = SpectateHub()


# ---------------------------------------------------------------------------
# reconcile (testable offline)
# ---------------------------------------------------------------------------

def reconcile(con, listings, complete):
    ts = now_iso()
    seen = set()
    for L in listings:
        lid = L["id"]; seen.add(lid)
        up = unit_price(L)
        pu = per_item_price(L)
        cat = categorize(L.get("itemType"))
        rb, ru = L.get("reservedBy"), L.get("reservedUntilMs")
        dur = L.get("itemDurability")
        exists = con.execute("SELECT 1 FROM listings WHERE id=?", (lid,)).fetchone()
        if exists is None:
            con.execute(
                """INSERT INTO listings
                   (id,seller_id,seller_name,item_type,category,quantity,price_gold,
                    currency,price_usd,unit_price,per_unit,reserved_by,reserved_until,
                    item_durability,created_at,first_seen,last_seen,active,removed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,NULL)""",
                (lid, L.get("sellerId"), L.get("sellerName"), L.get("itemType"),
                 cat, L.get("quantity"), L.get("priceGold"), L.get("currency"),
                 L.get("priceUsd"), up, pu, rb, ru, dur,
                 L.get("createdAt"), ts, ts))
        else:
            con.execute(
                """UPDATE listings SET last_seen=?,active=1,removed_at=NULL,
                   price_gold=?,currency=?,price_usd=?,unit_price=?,per_unit=?,
                   reserved_by=?,reserved_until=?,item_durability=? WHERE id=?""",
                (ts, L.get("priceGold"), L.get("currency"), L.get("priceUsd"),
                 up, pu, rb, ru, dur, lid))
    newly_removed = 0
    if complete and seen:
        active = con.execute("SELECT id FROM listings WHERE active=1").fetchall()
        gone = [r["id"] for r in active if r["id"] not in seen]
        for lid in gone:
            con.execute("UPDATE listings SET active=0,removed_at=? WHERE id=?",
                        (ts, lid))
        newly_removed = len(gone)
    con.execute("INSERT INTO polls(ts,active,removed,ok) VALUES(?,?,?,?)",
                (ts, len(seen), newly_removed, 1 if complete else 0))
    con.commit()
    return len(seen), newly_removed


# ---------------------------------------------------------------------------
# arbitrage (testable offline)
# ---------------------------------------------------------------------------

def _buyable_clause():
    """SQL fragment + param: exclude listings currently reserved by another
    buyer (you can't purchase those). Reservations with a past expiry are fair
    game again."""
    return " AND (reserved_until IS NULL OR reserved_until < ?)", int(time.time() * 1000)


def our_gold_price(con, gold_item, n=3):
    """Our directly-measured USD value of one gold = the average *per-gold* ask
    of the N (=3) cheapest buyable token listings of the gold item.

    Per-gold (price_usd / quantity) so a 1000-gold stack and a single gold are
    compared on the same footing — this naturally handles stack sizes. Averaging
    the few cheapest (rather than the single MIN) smooths out a lone lowball
    listing. Returns (avg_usd or None, listings_used)."""
    if not gold_item:
        return None, 0
    rclause, rparam = _buyable_clause()
    rows = con.execute(
        f"""SELECT per_unit FROM listings
            WHERE active=1 AND item_type=? AND currency='token'
              AND per_unit IS NOT NULL{rclause}
            ORDER BY per_unit ASC LIMIT ?""",
        (gold_item, rparam, n)).fetchall()
    if not rows:
        return None, 0
    vals = [r["per_unit"] for r in rows]
    return sum(vals) / len(vals), len(vals)


def gold_rate_usd(con, gold_item):
    """USD value of one gold. While the tracker is running this is our own
    measured price (avg of the 3 cheapest per-gold asks, see `our_gold_price`).
    Falls back to kintaragold.xyz's spot price when there are no live gold
    listings to price from (e.g. the poller hasn't populated the DB yet).
    Returns (rate or None, listings_used) — 0 listings_used = the fallback."""
    rate, n = our_gold_price(con, gold_item)
    if rate is not None:
        return rate, n
    try:
        _, spot = fetch_kintara_gold_history()
        if spot:
            return spot, 0
    except Exception:
        pass
    return None, 0


def compute_arbitrage(con, gold_item, direction="gold_to_kins", fee_pct=0.0, min_qty=0):
    """Everything is priced PER SINGLE ITEM (price / stack size). A listing of
    5000 wood for 2 gold is 0.0004 gold/wood; we compare that against the
    cheapest per-item KINS ask of wood."""
    rate, rate_n = gold_rate_usd(con, gold_item)
    kp = current_kins_usd()
    rclause, rparam = _buyable_clause()

    # last-day completed-sale volume in the currency we'd be SELLING into:
    # selling for KINS when buying with gold, and vice-versa.
    sell_cur = "token" if direction == "gold_to_kins" else "gold"
    sold = {r["item_type"]: r for r in con.execute(
        "SELECT item_type, day, day_sales FROM item_stats WHERE currency=?", (sell_cur,))}
    # The stats series is sparse (only days that had sales), so each item's latest
    # sample can be days old. Anchor to the current game day = the newest day seen
    # anywhere in the cache (the most-liquid pairs always sell on the current day).
    # Anything whose latest sale isn't that day sold 0 today.
    ref_row = con.execute(
        "SELECT MAX(day) d FROM item_stats WHERE day IS NOT NULL").fetchone()
    ref_day = ref_row["d"] if ref_row else None

    # cheapest per-item ask + single cheapest listing (the exact deal) per
    # item+currency. `extra` lets us also build a min-stack-filtered variant.
    def asks(currency, extra=""):
        return {r["item_type"]: r for r in con.execute(
            f"""SELECT item_type, MIN(per_unit) p, COUNT(*) n, SUM(quantity) q
                FROM listings
                WHERE active=1 AND currency='{currency}' AND per_unit IS NOT NULL{rclause}{extra}
                GROUP BY item_type""", (rparam,))}

    def lots(currency, extra=""):
        return {r["item_type"]: r for r in con.execute(
            f"""SELECT item_type, quantity, price_gold, price_usd, seller_name FROM (
                  SELECT item_type, quantity, price_gold, price_usd, seller_name,
                    ROW_NUMBER() OVER (PARTITION BY item_type ORDER BY per_unit ASC) rn
                  FROM listings
                  WHERE active=1 AND currency='{currency}' AND per_unit IS NOT NULL{rclause}{extra}
                ) WHERE rn=1""", (rparam,))}

    gold_asks, kins_asks = asks("gold"), asks("token")
    gold_lots, kins_lots = lots("gold"), lots("token")
    # min-stack filter: prefer listings with quantity >= min_qty (skips "100 coal"
    # dust on bulk goods). Falls back to the normal cheapest when an item has no
    # listing that big — so single-unit items (mounts, cosmetics) are untouched
    # and a bulk good is only filtered when a bigger listing actually exists.
    min_qty = int(min_qty or 0)
    if min_qty > 1:
        qextra = f" AND quantity >= {min_qty}"
        gold_asks_f, kins_asks_f = asks("gold", qextra), asks("token", qextra)
        gold_lots_f, kins_lots_f = lots("gold", qextra), lots("token", qextra)

    rows = []
    fee = max(0.0, fee_pct) / 100.0
    for item in set(gold_asks) | set(kins_asks):
        if item == gold_item:
            continue
        g = gold_asks.get(item); k = kins_asks.get(item)
        gl = gold_lots.get(item); kl = kins_lots.get(item)
        # prefer a big-enough listing; fall back to the normal cheapest if none
        if min_qty > 1:
            g = gold_asks_f.get(item) or g; k = kins_asks_f.get(item) or k
            gl = gold_lots_f.get(item) or gl; kl = kins_lots_f.get(item) or kl
        gold_unit = g["p"] if g else None              # gold per item
        kins_unit = k["p"] if k else None              # USD per item
        gold_unit_usd = (gold_unit * rate) if (gold_unit is not None and rate) else None
        # how many of this item one gold buys (the "24000 wood per gold" number)
        per_gold = (1.0 / gold_unit) if gold_unit else None
        # cheapest USD price for one item (either currency) and its inverse,
        # how many you get per dollar
        usd_opts = [v for v in (gold_unit_usd, kins_unit) if v and v > 0]
        usd_each = min(usd_opts) if usd_opts else None
        per_usd = (1.0 / usd_each) if usd_each else None

        spread = None
        if gold_unit_usd is not None and kins_unit is not None:
            spread = kins_unit - gold_unit_usd  # positive => buy-gold/sell-kins wins

        if direction == "gold_to_kins":
            buy_usd, sell_usd = gold_unit_usd, kins_unit
        else:
            buy_usd, sell_usd = kins_unit, gold_unit_usd

        profit = margin = None
        if buy_usd is not None and sell_usd is not None:
            profit = sell_usd * (1 - fee) - buy_usd
            margin = round(profit / buy_usd * 100, 1) if buy_usd else None

        # Gold buys come with a 1-gold minimum spend. For items that cost less
        # than 1 gold each (wood, coal, stone, …) the meaningful figure is
        # profit per *gold spent*, not per item — you always buy a gold's worth
        # at a time. For items >= 1 gold each (cosmetics, mounts) per-item stands.
        sub_gold = gold_unit is not None and gold_unit < 1
        basis = "gold" if sub_gold else "item"
        mult = (1.0 / gold_unit) if sub_gold else 1.0   # items per gold
        profit_disp = profit * mult if profit is not None else None
        spread_disp = spread * mult if spread is not None else None

        srow = sold.get(item)
        if srow is None:
            sold_day = None          # not fetched yet → "—"
        elif srow["day"] == ref_day:
            sold_day = srow["day_sales"]   # sold on the current day
        else:
            sold_day = 0             # last sale was an earlier day → none today
        rows.append({
            "item_type": item,
            "category": categorize(item),
            "gold_unit": gold_unit,
            "gold_unit_usd": gold_unit_usd,
            "kins_unit": kins_unit,
            "per_gold": per_gold,
            "per_usd": per_usd,
            "usd_each": usd_each,
            "gold_lot": ({"qty": gl["quantity"], "price_gold": gl["price_gold"],
                          "seller": gl["seller_name"]} if gl else None),
            "kins_lot": ({"qty": kl["quantity"], "price_usd": kl["price_usd"],
                          "seller": kl["seller_name"]} if kl else None),
            "sold_day": sold_day,
            "sold_date": srow["day"] if srow else None,
            "spread": spread,
            "spread_disp": spread_disp,
            "profit": profit,
            "profit_disp": profit_disp,
            "basis": basis,
            "margin": margin,
            "n_gold": g["n"] if g else 0,
            "n_kins": k["n"] if k else 0,
            "qty_gold": g["q"] if g else 0,
            "qty_kins": k["q"] if k else 0,
            "complete": gold_unit is not None and kins_unit is not None,
        })

    # rank by the displayed profit (per gold for cheap items, per item otherwise);
    # incomplete (one-sided) rows sink to the bottom
    rows.sort(key=lambda r: (r["profit_disp"] is not None,
                             r["profit_disp"] if r["profit_disp"] is not None else -1e9),
              reverse=True)
    return {
        "gold_rate": round(rate, 6) if rate else None,
        "gold_rate_listings": rate_n,
        "kins_price": kp,
        "gold_item": gold_item,
        "direction": direction,
        "sell_cur": sell_cur,
        "ref_day": ref_day,
        "fee_pct": fee_pct,
        "rows": rows,
    }


def gold_daily_usd(con):
    """{ 'YYYY-MM-DD' (UTC): USD value of 1 gold } over the sales window. Uses our
    own measured `gold_price` series where we have it (last of day wins), backfilled
    with kintaragold.xyz's history for earlier dates. Lets us re-express a historical
    USD sale in gold at the time it happened (the empirically stickiest unit)."""
    m = {}
    for r in con.execute("SELECT ts, usd FROM gold_price WHERE usd IS NOT NULL ORDER BY ts"):
        d = datetime.fromtimestamp(r["ts"] / 1000, timezone.utc).strftime("%Y-%m-%d")
        m[d] = r["usd"]
    try:
        hist, _ = fetch_kintara_gold_history()
        for t, usd in hist:
            d = datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%d")
            m.setdefault(d, usd)          # only backfill days we don't already have
    except Exception:
        pass
    return m


# items that look like farmable/grindable commodities, not true collectibles:
# they're USD/utility-anchored (a wolf is worth ~$X of convenience) and their KINS/gold
# value swings wildly, so they don't belong in the Collectables (CMP) mispricing scan.
# Detected by high sales volume + low price; this explicit set is a belt-and-braces guard.
FARMABLE_CMP = {"mount_wolf", "mount_dragon", "mount_whale"}


def compute_mispricing(con, gold_item):
    """Mispricing scan for the collectible markets (CMP = cosmetics / mounts / pets).

    Compares each item's **cheapest current buyable listing** against a **gold-anchored,
    recency- & volume-weighted fair value** built from recent completed sales.

    Why gold-anchored: empirically, settled CMP prices are stickiest in gold (and KINS),
    and least sticky in USD — so carrying a (possibly stale) sale price forward to today
    is most reliable in gold, then converted to the display currency at the live rate.
    This fixes the low-liquidity staleness problem (an item that last sold weeks ago,
    when gold/KINS were cheaper, gets re-priced at today's rates instead of compared in
    raw historical USD). Each recent sale is converted to a gold value at the gold price
    on its own day, weighted by units sold and an exponential recency decay (~7-day
    half-life). Farmable commodities (wolf/dragon/whale-type) are excluded."""
    rate, _ = gold_rate_usd(con, gold_item)
    kp = current_kins_usd()
    rclause, rparam = _buyable_clause()
    gusd = gold_daily_usd(con)
    gdates = sorted(gusd)

    def gold_usd_on(d):
        if d in gusd:
            return gusd[d]
        prior = [x for x in gdates if x <= d]
        return gusd[prior[-1]] if prior else (gusd[gdates[0]] if gdates else None)

    today = con.execute("SELECT MAX(date) d FROM sales_daily").fetchone()["d"]
    today_dt = datetime.strptime(today, "%Y-%m-%d") if today else None
    WINDOW_DAYS, HALF_LIFE = 21, 7.0

    # cheapest buyable per-item ask per currency
    def cheapest(currency):
        return {r["item_type"]: r["p"] for r in con.execute(
            f"""SELECT item_type, MIN(per_unit) p FROM listings
                WHERE active=1 AND currency='{currency}' AND per_unit IS NOT NULL{rclause}
                GROUP BY item_type""", (rparam,))}
    gold_ask, tok_ask = cheapest("gold"), cheapest("token")

    # all recent CMP sales (both currencies), each converted to a gold value at its date
    sales = defaultdict(list)   # item -> [(date, sales, gold_value, usd_value)]
    for r in con.execute(
            "SELECT item_type,currency,date,sales,avg_price FROM sales_daily "
            "WHERE sales>0 AND avg_price>0"):
        it = r["item_type"]
        cat = categorize(it)
        if cat not in ("cosmetic", "mount", "pet"):
            continue
        if today_dt and (today_dt - datetime.strptime(r["date"], "%Y-%m-%d")).days > WINDOW_DAYS:
            continue
        if r["currency"] == "token":
            usd = r["avg_price"]
            gu = gold_usd_on(r["date"])
            gv = usd / gu if gu else None
        else:                                    # gold sale (avg already in gold)
            gv = r["avg_price"]
            gu = gold_usd_on(r["date"])
            usd = gv * gu if gu else None
        if gv:
            sales[it].append((r["date"], r["sales"], gv, usd))

    rows = []
    for item in set(gold_ask) | set(tok_ask):
        if item == gold_item or item in FARMABLE_CMP:
            continue
        cat = categorize(item)
        if cat not in ("cosmetic", "mount", "pet"):
            continue
        recs = sorted(sales.get(item) or [], key=lambda x: x[0])   # oldest -> newest
        if not recs:
            continue

        # farmable heuristic (on the full recent window): high volume + cheap = commodity
        usds = sorted(u for _, _, _, u in recs if u)
        med_usd = usds[len(usds) // 2] if usds else 0
        if sum(s for _, s, _, _ in recs) >= 120 and med_usd < 6:
            continue

        # Use only the most recent ~50% of trading records for fair value: items
        # routinely sell for wild outlier prices in the first day or two after release
        # (hype + thin supply), which don't reflect the settled value. Trimming the
        # older half drops that launch noise; sparse items (<4 records) keep everything.
        kept = recs[len(recs) // 2:] if len(recs) >= 4 else recs

        # gold-anchored, units * recency-weighted fair value over the kept records
        num = den = 0.0
        for d, s, gv, _ in kept:
            age = (today_dt - datetime.strptime(d, "%Y-%m-%d")).days if today_dt else 0
            w = s * (0.5 ** (age / HALF_LIFE))
            num += w * gv
            den += w
        fair_gold = num / den if den else None
        if not fair_gold:
            continue

        # cheapest acquisition, expressed in gold (token listings converted at the live rate)
        cands = []
        if item in gold_ask:
            cands.append((gold_ask[item], "gold"))
        if item in tok_ask and rate:
            cands.append((tok_ask[item] / rate, "kins"))
        if not cands:
            continue
        buy_gold, buy_ccy = min(cands, key=lambda x: x[0])

        spread_gold = fair_gold - buy_gold
        margin = round(spread_gold / buy_gold * 100, 1) if buy_gold else None

        # the kept window's trading days (combining currencies), newest first, +
        # cumulative volume across them — drives the volume column + its hover
        byday = defaultdict(int)
        for d, s, _, _ in kept:
            byday[d] += s
        trade_days = [{"date": d, "sales": byday[d]} for d in sorted(byday, reverse=True)]
        vol_window = sum(byday.values())
        last_sale = trade_days[0]["date"]
        last_age = (today_dt - datetime.strptime(last_sale, "%Y-%m-%d")).days if today_dt else 0

        # confidence: recency of the last sale + how much volume backs the average
        if last_age <= 2 and vol_window >= 5:
            conf = "high"
        elif last_age > 7 or vol_window < 3:
            conf = "low"
        else:
            conf = "med"

        rows.append({
            "item_type": item, "category": cat,
            "buy_gold": buy_gold, "buy_ccy": buy_ccy,
            "fair_gold": fair_gold,
            "spread_gold": spread_gold, "margin": margin,
            "vol_window": vol_window, "trade_days": trade_days,
            "last_sale": last_sale, "last_age": last_age, "conf": conf,
        })

    rows.sort(key=lambda r: r["margin"] if r["margin"] is not None else -1e9, reverse=True)
    return {"gold_rate": round(rate, 6) if rate else None,
            "kins_price": kp, "gold_item": gold_item, "rows": rows}


def guess_gold_item(con):
    """Best-effort default: an item literally called 'gold', else one whose name
    contains 'gold' but isn't a cosmetic/aura. Returns None if unsure."""
    names = [r["item_type"] for r in con.execute(
        "SELECT DISTINCT item_type FROM listings")]
    for n in names:
        if n and n.lower() == "gold":
            return n
    for n in names:
        low = (n or "").lower()
        if "gold" in low and "aura" not in low and "cosmetic" not in low:
            return n
    return None


# ---------------------------------------------------------------------------
# poller
# ---------------------------------------------------------------------------

_state = {"last": None, "last_active": 0, "last_removed": 0, "error": None,
          "last_success": None, "fail_streak": 0}


def poll_loop(interval):
    while True:
        con = connect()
        try:
            listings, complete = fetch_all_active()
            if not listings and not complete:
                # got nothing this cycle (upstream down/slow) — keep last-good data and
                # just note it; the UI only surfaces a problem once this persists.
                _state["fail_streak"] = _state.get("fail_streak", 0) + 1
                _state.update(last=now_iso(), error="upstream unreachable")
                print(f"[{now_iso()}] poll: upstream unreachable (streak {_state['fail_streak']})")
            else:
                a, r = reconcile(con, listings, complete)
                # auto-pick a gold item once, if not set and we can guess
                if not get_setting(con, "gold_item"):
                    g = guess_gold_item(con)
                    if g:
                        set_setting(con, "gold_item", g)
                _state.update(last=now_iso(), last_active=a, last_removed=r,
                              error=None, last_success=now_iso(), fail_streak=0)
                print(f"[{now_iso()}] active={a} newly_removed={r} complete={complete}")
        except Exception as e:
            # unexpected (DB etc.) — count it but keep serving; don't crash the loop
            _state["fail_streak"] = _state.get("fail_streak", 0) + 1
            _state.update(error=str(e), last=now_iso())
            print(f"[{now_iso()}] poll error: {e}")
        finally:
            con.close()
        time.sleep(interval)


# ---------------------------------------------------------------------------
# web app
# ---------------------------------------------------------------------------

def make_app():
    from flask import Flask, jsonify, request, Response, send_file
    app = Flask(__name__)

    def _serve_cached_icon(item_type, status_on_error=502):
        """Serve real Kintara HUD art from the same disk cache used by /icon/<item>."""
        rel = icon_asset(item_type)
        if not rel:
            return Response(status=404)
        ext = rel.rsplit(".", 1)[-1]
        os.makedirs(ICON_DIR, exist_ok=True)
        fp = os.path.join(ICON_DIR, f"{item_type}.{ext}")
        if not os.path.exists(fp):
            try:
                import requests
                r = requests.get(f"https://kintara.gg/assets/hud/{rel}",
                                 headers={"User-Agent": BROWSER_UA}, timeout=HTTP_TIMEOUT)
                r.raise_for_status()
                with open(fp, "wb") as f:
                    f.write(r.content)
            except Exception:
                return Response(status=status_on_error)
        return send_file(os.path.abspath(fp),
                         mimetype="image/svg+xml" if ext == "svg" else f"image/{ext}",
                         max_age=604800)

    @app.route("/")
    def index():
        return Response(INDEX_HTML, mimetype="text/html")

    @app.route("/favicon.ico")
    @app.route("/favicon.png")
    @app.route("/apple-touch-icon.png")
    def favicon():
        return _serve_cached_icon("gold")

    @app.route("/site.webmanifest")
    def webmanifest():
        return Response(json.dumps({
            "name": "KinScan",
            "short_name": "KinScan",
            "description": "Kintara market intelligence for KINS, gold, listings, sales, and live world data.",
            "start_url": "/",
            "scope": "/",
            "display": "standalone",
            "background_color": "#0a0f1a",
            "theme_color": "#0a0f1a",
            "icons": [
                {"src": "/favicon.png", "sizes": "any", "type": "image/png"},
                {"src": "/apple-touch-icon.png", "sizes": "180x180", "type": "image/png"}
            ]
        }), mimetype="application/manifest+json")

    @app.route("/api/status")
    def status():
        con = connect(readonly=True)
        since = con.execute("SELECT MIN(first_seen) m FROM listings").fetchone()["m"]
        n = con.execute("SELECT COUNT(*) c FROM listings").fetchone()["c"]
        con.close()
        return jsonify({**_state, "tracking_since": since, "total_rows": n})

    @app.route("/api/items")
    def items():
        con = connect(readonly=True)
        rows = [r["item_type"] for r in con.execute(
            "SELECT DISTINCT item_type FROM listings ORDER BY item_type")]
        cats = [r["category"] for r in con.execute(
            "SELECT DISTINCT category FROM listings "
            "WHERE category IS NOT NULL ORDER BY category")]
        gold_item = get_setting(con, "gold_item")
        con.close()
        labels = {it: item_label(it) for it in rows}
        return jsonify({"items": rows, "categories": cats,
                        "labels": labels, "gold_item": gold_item})

    @app.route("/api/settings", methods=["GET", "POST"])
    def settings():
        con = connect()
        if request.method == "POST":
            body = request.get_json(force=True) or {}
            if "gold_item" in body:
                set_setting(con, "gold_item", body["gold_item"] or "")
        out = {"gold_item": get_setting(con, "gold_item")}
        con.close()
        return jsonify(out)

    @app.route("/api/arbitrage")
    def arbitrage():
        con = connect(readonly=True)
        gold_item = request.args.get("gold_item") or get_setting(con, "gold_item")
        direction = request.args.get("direction", "gold_to_kins")
        fee = float(request.args.get("fee", 0) or 0)
        min_qty = int(float(request.args.get("min_qty", 0) or 0))
        out = compute_arbitrage(con, gold_item, direction, fee, min_qty)
        con.close()
        return jsonify(out)

    @app.route("/api/mispricing")
    def mispricing():
        con = connect(readonly=True)
        gold_item = request.args.get("gold_item") or get_setting(con, "gold_item")
        out = compute_mispricing(con, gold_item)
        con.close()
        return jsonify(out)

    @app.route("/api/kins-price")
    def kins_price():
        # live KINS/USD (kintara's own figure), cached ~2 min — drives the header pill
        return jsonify({"usd": current_kins_usd()})

    @app.route("/api/liquidity")
    def liquidity():
        """Buy-side liquidity depth for one item, priced in USD per 1000 units.
        Buckets all active, buyable listings into $0.10 (per-1000) price tranches
        so the item page can show how many units are available up to each price
        marker. Token (KINS) listings use their USD price directly; gold listings
        are converted at the current gold rate (excluded if no rate is known)."""
        item = request.args.get("item_type") or ""
        if not item:
            return jsonify({"ok": False, "error": "item_type required"})
        con = connect(readonly=True)
        gold_item = request.args.get("gold_item") or get_setting(con, "gold_item")
        rate, _ = gold_rate_usd(con, gold_item)
        rclause, rparam = _buyable_clause()
        rows = con.execute(
            f"""SELECT currency, per_unit, quantity FROM listings
                WHERE active=1 AND item_type=? AND per_unit IS NOT NULL
                  AND quantity > 0{rclause}""",
            (item, rparam)).fetchall()
        con.close()

        STEP = 0.10
        priced = []                       # (usd_per_1000, units)
        total_units = gold_units = token_units = excluded_gold = 0
        for r in rows:
            qty = r["quantity"] or 0
            if r["currency"] == "token":
                pu = r["per_unit"]; token_units += qty
            else:
                if not rate:
                    excluded_gold += qty; continue
                pu = (r["per_unit"] or 0) * rate; gold_units += qty
            if pu is None or pu < 0:
                continue
            priced.append((pu * 1000.0, qty)); total_units += qty

        if not priced:
            return jsonify({"ok": True, "item_type": item, "gold_rate": rate,
                            "step": STEP, "markers": [], "total_units": 0,
                            "total_listings": 0, "best_per_1000": None,
                            "gold_units": gold_units, "token_units": token_units,
                            "excluded_gold_units": excluded_gold})

        best = min(p for p, _ in priced)
        max_p = max(p for p, _ in priced)
        # Focus the axis near the market: show up to ~3.5x the cheapest price, or
        # the median listing price if that's higher, so overpriced tail stacks
        # (e.g. stone listed far above market) don't stretch and squash the
        # actionable range. Total/depth beyond the axis is still reported below.
        priced.sort()

        def _price_at(frac):
            need, c = frac * total_units, 0.0
            for p, u in priced:
                c += u
                if c >= need:
                    return p
            return max_p

        p_cap = min(max(best * 3.5, _price_at(0.50)), max_p)
        n_markers = int(p_cap / STEP) + (1 if (p_cap / STEP) % 1 > 1e-9 else 0)
        n_markers = max(5, min(50, n_markers))   # keep the axis sane
        markers = []
        for i in range(1, n_markers + 1):
            hi = round(i * STEP, 2)
            lo = round(hi - STEP, 2)
            cum = sum(u for p, u in priced if p <= hi + 1e-9)
            tr = [(p, u) for p, u in priced if lo - 1e-9 < p <= hi + 1e-9]
            markers.append({"price": hi, "cum_units": cum,
                            "tranche_units": sum(u for _, u in tr),
                            "listings": len(tr)})
        return jsonify({"ok": True, "item_type": item, "gold_rate": rate,
                        "step": STEP, "markers": markers,
                        "total_units": total_units, "total_listings": len(priced),
                        "best_per_1000": round(best, 4),
                        "gold_units": gold_units, "token_units": token_units,
                        "excluded_gold_units": excluded_gold})

    @app.route("/api/refresh-stats", methods=["POST"])
    def refresh_stats():
        """Force-refresh the cached sales stats for a specific set of items (the
        ones the user is currently viewing). Keeps the visible sold-today numbers
        live without re-fetching the whole market. `currency` limits the work to
        the side being shown; omit it to refresh both."""
        body = request.get_json(force=True) or {}
        items = list(dict.fromkeys(str(i) for i in (body.get("items") or []) if i))[:60]
        currency = body.get("currency")
        curs = (currency,) if currency in ("gold", "token") else ("gold", "token")
        con = connect()
        done = 0
        for it in items:
            for cur in curs:
                try:
                    d = fetch_stats(it, cur)
                    s = d.get("samples") or []
                    day_sales = (s[-1].get("sales", 0) if s else 0)
                    day = s[-1].get("date") if s else None
                    day_avg = s[-1].get("avgUnitPrice") if s else None
                    _archive_samples(con, it, cur, s)
                    _upsert_stats(con, it, cur, day, day_sales, d.get("avg30d"), day_avg)
                    done += 1
                except Exception:
                    _mark_stats_attempt(con, it, cur)
        ref = con.execute(
            "SELECT MAX(day) d FROM item_stats WHERE day IS NOT NULL").fetchone()["d"]
        con.close()
        return jsonify({"ok": True, "refreshed": done, "ref_day": ref})

    def _filters():
        q = (request.args.get("q") or "").strip()
        currency = request.args.get("currency") or "all"
        item = request.args.get("item_type") or "all"
        category = request.args.get("category") or "all"
        clauses, params = [], []
        if q:
            # match internal itemType / seller, or the in-game display name
            label_its = [it for it, lb in ITEM_LABELS.items() if q.lower() in lb.lower()]
            if label_its:
                ph = ",".join("?" * len(label_its))
                clauses.append(f"(item_type LIKE ? OR seller_name LIKE ? OR item_type IN ({ph}))")
                params += [f"%{q}%", f"%{q}%"] + label_its
            else:
                clauses.append("(item_type LIKE ? OR seller_name LIKE ?)")
                params += [f"%{q}%", f"%{q}%"]
        if currency != "all":
            clauses.append("currency=?"); params.append(currency)
        if item != "all":
            clauses.append("item_type=?"); params.append(item)
        if category != "all":
            clauses.append("category=?"); params.append(category)
        return clauses, params

    @app.route("/api/current")
    def current():
        clauses, params = _filters()
        where = "WHERE active=1" + ("" if not clauses else " AND " + " AND ".join(clauses))
        sort = {"latest": "created_at DESC", "cheapest": "unit_price ASC",
                "expensive": "unit_price DESC"}.get(request.args.get("sort", "latest"),
                                                    "created_at DESC")
        limit = min(int(request.args.get("limit", 300)), 1000)
        con = connect(readonly=True)
        rows = con.execute(f"SELECT * FROM listings {where} ORDER BY {sort} LIMIT ?",
                           params + [limit]).fetchall()
        con.close()
        return jsonify([dict(r) for r in rows])

    @app.route("/api/removed")
    def removed():
        clauses, params = _filters()
        where = "WHERE active=0 AND removed_at IS NOT NULL" + (
            "" if not clauses else " AND " + " AND ".join(clauses))
        limit = min(int(request.args.get("limit", 300)), 1000)
        con = connect(readonly=True)
        rows = con.execute(
            f"""SELECT *, ROUND((julianday(removed_at)-julianday(first_seen))*86400)
                       AS seconds_listed
                FROM listings {where} ORDER BY removed_at DESC LIMIT ?""",
            params + [limit]).fetchall()
        con.close()
        return jsonify([dict(r) for r in rows])

    @app.route("/api/sales-feed")
    def sales_feed():
        """ACTUAL completed sales (from `sales_events`), newest first — the truthful
        replacement for the removed-listings feed (which also counted cancellations).
        Filters: currency (gold|token), item_type, q (item or in-game label). Each row:
        item_type, label, category, units, price, currency, day, ts."""
        item = request.args.get("item_type", "all")
        currency = request.args.get("currency", "all")
        cat = request.args.get("category", "all")
        q = (request.args.get("q") or "").strip().lower()
        limit = min(int(request.args.get("limit", 200)), 1000)
        clauses, params = [], []
        if currency in ("gold", "token"):
            clauses.append("currency=?"); params.append(currency)
        if item != "all":
            clauses.append("item_type=?"); params.append(item)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        con = connect(readonly=True)
        rows = con.execute(
            f"SELECT item_type,currency,units,price,day,ts FROM sales_events {where} "
            f"ORDER BY ts DESC LIMIT ?", params + [limit * 3]).fetchall()
        con.close()
        out = []
        for r in rows:
            it = r["item_type"]
            c = categorize(it)
            if cat != "all" and c != cat:
                continue
            lbl = item_label(it)
            if q and q not in it.lower() and q not in lbl.lower():
                continue
            out.append({"item_type": it, "label": lbl, "category": c,
                        "units": r["units"], "price": r["price"],
                        "currency": r["currency"], "day": r["day"], "ts": r["ts"]})
            if len(out) >= limit:
                break
        return jsonify(out)

    @app.route("/icon/<item_type>")
    def icon(item_type):
        """Real in-game item art, lazily downloaded from kintara and cached on
        disk so we only fetch each icon once. 404 -> frontend uses a fallback."""
        return _serve_cached_icon(item_type)

    @app.route("/worldmap.jpg")
    def worldmap():
        """The isometric Kintara world map (shipped next to the script). Used as the
        Property Map backdrop and the per-player location view."""
        from flask import send_file, Response
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "world_map.jpg")
        if not os.path.exists(fp):
            return Response(status=404)
        return send_file(fp, mimetype="image/jpeg", max_age=604800)

    @app.route("/shores.png")
    def shores_map():
        """Top-down map art for The Shores realm (MapImages/). Used as the per-player
        location backdrop for players in the `beach` realm, with the coordinate system
        top-right=(-19.5,-19.5), bottom-left=(19.5,19.5)."""
        from flask import send_file, Response
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "MapImages", "The Shores (Beach).png")
        if not os.path.exists(fp):
            return Response(status=404)
        return send_file(fp, mimetype="image/png", max_age=604800)

    @app.route("/pond.png")
    def pond_map():
        """Top-down render of The Pond (render_maps.py): RNG-grown central lake +
        dock + NE tower. Live World backdrop for the `pond` realm; players plotted
        via pondToMap u=(x+19.5)/39, v=(z+19.5)/39."""
        from flask import send_file, Response
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MapImages", "The Pond.png")
        return send_file(fp, mimetype="image/png", max_age=604800) if os.path.exists(fp) else Response(status=404)

    @app.route("/arena.png")
    def arena_map():
        """Top-down render of The Arena (render_maps.py): sand floor + central
        boxing ring. Live World backdrop for the `arena` realm; players plotted
        via arenaToMap u=(x+9.5)/19, v=(z+9.5)/19."""
        from flask import send_file, Response
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MapImages", "The Arena.png")
        return send_file(fp, mimetype="image/png", max_age=604800) if os.path.exists(fp) else Response(status=404)

    # remaining per-realm map PNGs (render_maps.py). Each realm is a centred square
    # grid (constants.js), so the frontend plots players via centerMap(N).
    _REALM_MAP_FILES = {
        "mainland": "The Mainland.png",
        "whisperwood": "Whisperwood (Eldergrove).png",
        "frostmere": "Frostmere.png",
        "wild": "The Wilderness (Wild).png",
        "wild-deep": "Deep Wilderness.png",
        "wild-east": "Wilderness East.png",
        "mine": "The Mine.png",
        "spider": "Spider Lair.png",
        "shack": "The Shack.png",
    }

    @app.route("/maps/<slug>.png")
    def realm_map(slug):
        """Serve a per-realm map PNG (Live World per-player backdrops)."""
        from flask import send_file, Response
        fn = _REALM_MAP_FILES.get(slug)
        if not fn:
            return Response(status=404)
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MapImages", fn)
        return send_file(fp, mimetype="image/png", max_age=604800) if os.path.exists(fp) else Response(status=404)

    @app.route("/api/sales-history")
    def sales_history():
        """Daily completed-sale history, served from our archive (sales_daily).
        Only falls back to a live Kintara fetch when we have nothing cached, then
        stores it — so past data is never re-fetched."""
        item = (request.args.get("item_type") or "").strip()
        currency = request.args.get("currency", "gold")
        if not item:
            return jsonify({"ok": False, "error": "item_type required"}), 400

        # currency=kins → ONE blended $KINS series combining BOTH gold and USD
        # (token) sales. Each day, every recorded sale is converted to $KINS:
        #   token (USD) sale  -> avg_usd                / kins_usd(date)
        #   gold       sale   -> avg_gold * gold_usd(date) / kins_usd(date)
        # then averaged across the two currencies weighted by units sold, so the
        # chart shows the item's full market price in $KINS (rising line = real
        # alpha vs the token; flat/falling = just tracking it). Because both
        # currencies divide by the same kins_usd(date), the blend reduces to a
        # units-weighted USD price ÷ kins_usd — we build that USD blend first.
        if currency == "kins":
            con = connect(readonly=True)
            grows = con.execute(
                """SELECT date, sales, avg_price FROM sales_daily
                   WHERE item_type=? AND currency='gold' ORDER BY date""", (item,)).fetchall()
            trows = con.execute(
                """SELECT date, sales, avg_price FROM sales_daily
                   WHERE item_type=? AND currency='token' ORDER BY date""", (item,)).fetchall()
            gusd = gold_daily_usd(con)
            con.close()
            kmap = kins_daily_usd()
            gdates, kdates = sorted(gusd), sorted(kmap)

            def _on(m, keys, d):           # exact day, else carry the nearest prior forward
                if d in m:
                    return m[d]
                prior = [x for x in keys if x <= d]
                return m[prior[-1]] if prior else None

            # date -> [units-weighted USD sum, total units] across both currencies
            blend = {}
            for r in trows:                # token sales are already USD
                if r["avg_price"] and r["avg_price"] > 0:
                    s = r["sales"] or 0
                    a = blend.setdefault(r["date"], [0.0, 0])
                    a[0] += r["avg_price"] * s; a[1] += s
            for r in grows:                # gold sales -> USD at that day's gold price
                gu = _on(gusd, gdates, r["date"])
                if r["avg_price"] and r["avg_price"] > 0 and gu:
                    s = r["sales"] or 0
                    a = blend.setdefault(r["date"], [0.0, 0])
                    a[0] += r["avg_price"] * gu * s; a[1] += s

            samples, usd_by_day = [], {}
            for d in sorted(blend):
                wsum_usd, units = blend[d]
                k = _on(kmap, kdates, d)
                if units <= 0 or not k:
                    continue
                usd = wsum_usd / units                 # blended USD price that day
                usd_by_day[d] = usd
                samples.append({"date": d, "sales": units, "avgUnitPrice": usd / k})

            tot = sum((s["sales"] or 0) for s in samples)
            wsum = sum((s["avgUnitPrice"] or 0) * (s["sales"] or 0) for s in samples)
            avg = (wsum / tot) if tot else None
            # headline: item (blended-USD) return vs KINS's own USD return over the span
            vs = None
            if len(samples) >= 2:
                d0, d1 = samples[0]["date"], samples[-1]["date"]
                u0, u1 = usd_by_day.get(d0), usd_by_day.get(d1)
                k0, k1 = _on(kmap, kdates, d0), _on(kmap, kdates, d1)
                if u0 and u1 and k0 and k1:
                    vs = {"item_usd_pct": (u1 / u0 - 1) * 100,
                          "kins_usd_pct": (k1 / k0 - 1) * 100,
                          "rel_pct": (samples[-1]["avgUnitPrice"] / samples[0]["avgUnitPrice"] - 1) * 100,
                          "from": d0, "to": d1}
            return jsonify({"ok": True, "currency": "kins", "avg30d": avg,
                            "samples": samples, "vs_token": vs})

        # currency=goldstd ("Gold Standard") → ONE blended series of every sale valued
        # in GOLD: gold sales as-is, and each USD/token sale converted to the amount of
        # gold that USD would have bought on that day (avg_usd / gold_usd(date)), then
        # averaged across both currencies weighted by units sold. Same units-weighted
        # USD blend as currency=kins, but divided by gold_usd(date) instead of kins_usd.
        if currency == "goldstd":
            con = connect(readonly=True)
            grows = con.execute(
                """SELECT date, sales, avg_price FROM sales_daily
                   WHERE item_type=? AND currency='gold' ORDER BY date""", (item,)).fetchall()
            trows = con.execute(
                """SELECT date, sales, avg_price FROM sales_daily
                   WHERE item_type=? AND currency='token' ORDER BY date""", (item,)).fetchall()
            gusd = gold_daily_usd(con)
            con.close()
            gdates = sorted(gusd)

            def _on(m, keys, d):
                if d in m:
                    return m[d]
                prior = [x for x in keys if x <= d]
                return m[prior[-1]] if prior else None

            # date -> [units-weighted USD sum, total units] across both currencies
            blend = {}
            for r in trows:                # token sales are already USD
                if r["avg_price"] and r["avg_price"] > 0:
                    s = r["sales"] or 0
                    a = blend.setdefault(r["date"], [0.0, 0])
                    a[0] += r["avg_price"] * s; a[1] += s
            for r in grows:                # gold sales -> USD at that day's gold price
                gu = _on(gusd, gdates, r["date"])
                if r["avg_price"] and r["avg_price"] > 0 and gu:
                    s = r["sales"] or 0
                    a = blend.setdefault(r["date"], [0.0, 0])
                    a[0] += r["avg_price"] * gu * s; a[1] += s

            samples = []
            for d in sorted(blend):
                wsum_usd, units = blend[d]
                gu = _on(gusd, gdates, d)
                if units <= 0 or not gu:
                    continue
                samples.append({"date": d, "sales": units,
                                "avgUnitPrice": (wsum_usd / units) / gu})   # blended USD -> gold
            tot = sum((s["sales"] or 0) for s in samples)
            wsum = sum((s["avgUnitPrice"] or 0) * (s["sales"] or 0) for s in samples)
            avg = (wsum / tot) if tot else None
            return jsonify({"ok": True, "currency": "goldstd", "avg30d": avg, "samples": samples})

        con = connect(readonly=True)
        rows = con.execute(
            """SELECT date, sales, avg_price FROM sales_daily
               WHERE item_type=? AND currency=? ORDER BY date""", (item, currency)).fetchall()
        con.close()
        samples = [{"date": r["date"], "sales": r["sales"],
                    "avgUnitPrice": r["avg_price"]} for r in rows]
        if not samples:                       # archive miss -> fetch once and store
            try:
                d = fetch_stats(item, currency)
                samples = [{"date": s.get("date"), "sales": s.get("sales"),
                            "avgUnitPrice": s.get("avgUnitPrice")} for s in (d.get("samples") or [])]
                w = connect(); _archive_samples(w, item, currency, d.get("samples") or [])
                w.commit(); w.close()
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 502
        tot = sum((s["sales"] or 0) for s in samples)
        wsum = sum((s["avgUnitPrice"] or 0) * (s["sales"] or 0) for s in samples)
        avg = (wsum / tot) if tot else None
        return jsonify({"ok": True, "currency": currency, "avg30d": avg, "samples": samples})

    @app.route("/api/item-meta")
    def item_meta():
        """Index info-panel metadata for one cosmetic/mount/pet: how it's sourced,
        cost, ride speed, special features, and the derived availability window +
        supply status from our sales archive."""
        item = (request.args.get("item_type") or "").strip()
        if not item:
            return jsonify({"ok": False, "error": "item_type required"}), 400
        con = connect(readonly=True)
        try:
            meta = item_index_meta(con, item)
        finally:
            con.close()
        return jsonify({"ok": True, **meta})

    @app.route("/api/item-listings")
    def item_listings():
        """Up to the 5 cheapest live, buyable (non-reserved) listings for one item,
        per currency — gold and KINS (token) — for the Index order-book panel.
        Cross-converted (gold↔USD via the gold rate, USD↔$KINS via spot)."""
        item = (request.args.get("item_type") or "").strip()
        if not item:
            return jsonify({"ok": False, "error": "item_type required"}), 400
        rclause, now_ms = _buyable_clause()
        con = connect(readonly=True)
        try:
            gold_rate, _ = gold_rate_usd(con, get_setting(con, "gold_item"))

            def cheapest(cur):
                rows = con.execute(
                    f"""SELECT per_unit, quantity, seller_name, price_gold, price_usd
                        FROM listings
                        WHERE item_type=? AND currency=? AND active=1 AND per_unit IS NOT NULL
                        {rclause} ORDER BY per_unit ASC LIMIT 5""", (item, cur, now_ms)).fetchall()
                return [{"per_unit": r["per_unit"], "qty": r["quantity"],
                         "seller": r["seller_name"],
                         "price": r["price_gold"] if cur == "gold" else r["price_usd"]}
                        for r in rows]

            gold, token = cheapest("gold"), cheapest("token")
        finally:
            con.close()
        kins_px = current_kins_usd()
        return jsonify({"ok": True, "gold": gold, "token": token,
                        "gold_rate": gold_rate, "kins_price": kins_px})

    @app.route("/api/sales-summary")
    def sales_summary():
        """Per-item marketplace summary over a window (1/7/30 days ending on the
        most recent trading day): total sales, sales-weighted avg gold & USD
        price, and the USD price in $KINS. Reads the archive."""
        from datetime import date as _date, timedelta as _td
        window = max(1, int(float(request.args.get("window", 1) or 1)))
        con = connect(readonly=True)
        ref = con.execute("SELECT MAX(date) d FROM sales_daily").fetchone()["d"]
        items = [r["item_type"] for r in con.execute(
            "SELECT DISTINCT item_type FROM listings ORDER BY item_type")]
        agg = {}
        if ref:
            start = (_date.fromisoformat(ref) - _td(days=window - 1)).isoformat()
            for r in con.execute(
                """SELECT item_type, currency,
                          SUM(COALESCE(sales,0)) tot,
                          SUM(COALESCE(avg_price,0)*COALESCE(sales,0)) wp,
                          SUM(CASE WHEN sales>0 THEN sales ELSE 0 END) sw
                   FROM sales_daily WHERE date>=? GROUP BY item_type, currency""", (start,)):
                agg[(r["item_type"], r["currency"])] = r
        con.close()
        kins_px = current_kins_usd()
        out = []
        for it in items:
            g, t = agg.get((it, "gold")), agg.get((it, "token"))
            gs = (g["tot"] or 0) if g else 0
            ts = (t["tot"] or 0) if t else 0
            ga = (g["wp"] / g["sw"]) if (g and g["sw"]) else None
            ta = (t["wp"] / t["sw"]) if (t and t["sw"]) else None
            kins = (ta / kins_px) if (ta and kins_px) else None
            out.append({
                "item_type": it, "label": item_label(it), "category": categorize(it),
                "sales": gs + ts, "avg_gold": ga, "avg_usd": ta, "kins": kins,
            })
        return jsonify({"ok": True, "ref_day": ref, "window": window,
                        "kins_price": kins_px, "items": out})

    @app.route("/api/gold-history")
    def gold_history():
        """Gold price chart like kintaragold. gold_usd is kintaragold's own
        (independent) series; kins_usd is the live KINS/SOL pool at the range's
        resolution; kins_per_gold = gold_usd / kins_usd (truly moves intraday).
        range = 4H | 1D | 3D | 7D | 14D | ALL."""
        rng = (request.args.get("range") or "1D").upper()
        window_sec, bucket_sec = GOLD_RANGES.get(rng, GOLD_RANGES["1D"])
        # serve a cached payload for ~3 min so toggling/auto-refresh never bursts
        # GeckoTerminal (the source of the earlier rate-limiting).
        cached = _goldhist_cache.get(rng)
        if cached and time.time() - cached[0] < 180:
            return jsonify(cached[1])
        con = connect(readonly=True)
        try:
            gold, spot = gold_series_for_chart(con)   # our series, kintaragold fallback
        finally:
            con.close()
        if not gold:
            return jsonify({"ok": False, "error": "no gold price data yet"}), 502
        try:
            candles = kins_series_for_range(window_sec, bucket_sec)
        except Exception as e:
            if cached:                       # rate-limited? serve last good payload
                return jsonify(cached[1])
            return jsonify({"ok": False, "error": "kins price source: " + str(e)}), 502
        gstart = gold[0][0] if gold else None
        series = []
        for ts, ku in candles:
            tms = ts * 1000
            if gstart and tms < gstart:     # before gold history begins
                continue
            gu = interp_gold(gold, tms)
            kpg = (gu / ku) if (gu and ku) else None
            series.append({"t": int(tms), "gold_usd": gu, "kins_usd": ku,
                           "kins_per_gold": kpg})
        payload = {"ok": True, "range": rng, "spot": spot, "series": series}
        _goldhist_cache[rng] = (time.time(), payload)
        return jsonify(payload)

    @app.route("/api/servers")
    def servers():
        """Live server list for the top status bar. Normalizes kintara.gg's shape
        and adds rollup counts (open / full / queued, total in queue)."""
        try:
            raw = fetch_servers()
        except Exception as e:
            if _servers_cache["data"]:
                raw = _servers_cache["data"]
            else:
                return jsonify({"ok": False, "error": str(e)}), 502
        out = []
        for s in raw:
            out.append({
                "id": s.get("id"),
                "name": s.get("name"),
                "population": s.get("populationLabel"),
                "full": bool(s.get("full")),
                "queue": s.get("queueLength") or 0,
                "min_level": s.get("minLevel") or 0,
            })
        n_full = sum(1 for s in out if s["full"])
        n_queue = sum(1 for s in out if s["queue"] > 0)
        return jsonify({"ok": True, "servers": out, "total": len(out),
                        "full": n_full, "open": len(out) - n_full,
                        "queued": n_queue,
                        "queue_total": sum(s["queue"] for s in out)})

    @app.route("/api/merchant")
    def merchant():
        """Traveling-merchant tracker + cost calculator in one payload.
        `state` = current donation/gold-trade progress (per-resource current/goal/pct,
                  overall %). `calc` = the gold-mint recipe plus, per ingredient, the
                  live buy-side price `ladder` (cheapest-first [unit_usd, qty] levels).
                  The client walks the ladder so a larger mint costs more as the cheap
                  listings are exhausted (liquidity-aware), not a flat cheapest price."""
        try:
            m = fetch_merchant()
        except Exception as e:
            return jsonify({"ok": False, "error": "merchant source: " + str(e)}), 502
        raw = m or {}
        goals = raw.get("goals") or {}
        # per-resource progress (current vs goal, %). The official endpoint gives no
        # overall %, so we average the per-resource (capped) percentages.
        resources, pcts = [], []
        for key, label in MERCHANT_CAMPAIGN_RESOURCES:
            cur = raw.get(key)
            goal = goals.get(key)
            pct = (min(100.0, cur / goal * 100) if (cur is not None and goal) else None)
            if pct is not None:
                pcts.append(pct)
            resources.append({"key": key, "label": label, "current": cur,
                              "goal": goal, "pct": pct})
        overall = round(sum(pcts) / len(pcts), 2) if pcts else None
        state = {
            "mode": raw.get("mode"),
            "gold_trade": bool(raw.get("goldTradeEnabled")),
            "complete": bool(raw.get("complete")),
            "overall_pct": overall,
            "gold_stock": raw.get("goldStock"),
            "gold_stock_full": raw.get("goldStockFull"),
            "resources": resources,
        } if raw else None

        # cost calculator: the per-1-gold recipe + live buy-side depth per ingredient,
        # so the frontend can price any mint quantity walking the order book.
        con = connect(readonly=True)
        try:
            gold_rate, _ = gold_rate_usd(con, get_setting(con, "gold_item"))
            recipe = []
            for key, label, qty in MERCHANT_RECIPE:
                ladder, avail = order_book_usd(con, key, gold_rate)
                recipe.append({"item_type": key, "label": label, "qty": qty,
                               "ladder": ladder, "available": avail})
        finally:
            con.close()
        calc = {"gold_rate": round(gold_rate, 6) if gold_rate else None, "recipe": recipe}
        return jsonify({"ok": True, "state": state, "calc": calc})

    @app.route("/api/live")
    def live():
        """Live world roster for a shard, from the public spectate WebSocket. Opens
        the socket on first request (kept warm while polled). `players` are those in
        the spectator's view (near the world hub); `online_total` is the global count."""
        try:
            shard = int(request.args.get("shard", 1))
        except (TypeError, ValueError):
            shard = 1
        if shard not in SHARDS:
            shard = 1
        _spectate_hub.request(shard)
        snap = _spectate_hub.snapshot(shard)
        keep = ("id", "name", "x", "z", "ry", "y", "avg", "eq", "bdg",
                "php", "mov", "act", "outfit", "pr", "realm")
        snap["players"] = [{k: p[k] for k in keep if k in p} for p in snap["players"]]
        snap.update(ok=True, shard=shard, shards=list(SHARDS),
                    realms={k: {"l": v[0], "e": v[1]} for k, v in SPECTATE_REGIONS.items()})
        return jsonify(snap)

    @app.route("/api/live-search")
    def live_search():
        """Find a player by name across ALL servers. Opens (and keeps warm) a spectate
        socket on every shard and returns the current name matches with the shard they're
        on. Rosters fill over ~20s after a socket opens, so the client polls this a few
        times; `ready` = how many shards have a populated roster yet, so it knows when the
        sweep is complete. The extra sockets idle-close ~75s after the search stops."""
        q = (request.args.get("q") or "").strip().lower()
        if not q:
            return jsonify({"ok": False, "error": "name required"})
        results, ready = [], 0
        for shard in SHARDS:
            _spectate_hub.request(shard)          # ensure open + keep warm
            snap = _spectate_hub.snapshot(shard)
            players = snap.get("players") or []
            if players:
                ready += 1
            for p in players:
                nm = p.get("name") or ""
                if q in nm.lower():
                    results.append({"shard": shard, "id": p.get("id"), "name": nm,
                                    "realm": p.get("realm"), "level": p.get("avg")})
        # exact matches first, then by shard
        results.sort(key=lambda r: (r["name"].lower() != q, r["shard"]))
        return jsonify({"ok": True, "q": q, "results": results,
                        "shards": list(SHARDS), "ready": ready})

    @app.route("/api/property")
    def property_map():
        """Every mansion/house/trailer: owner, lock state, real map coordinates, and
        a cross-reference into our marketplace DB (the owner's live listing count +
        total ask value) plus how many properties that owner holds."""
        try:
            raw = fetch_property_status()
        except Exception as e:
            return jsonify({"ok": False, "error": "property source: " + str(e)}), 502
        plots, owners = [], {}
        for kind, key in (("mansion", "mansions"), ("house", "houses"),
                          ("trailer", "trailers")):
            for num, info in (raw.get(key) or {}).items():
                try:
                    num = int(num)
                except (TypeError, ValueError):
                    continue
                c = PROPERTY_PLOTS.get(kind, {}).get(num)
                oid = info.get("ownerId")
                if oid is not None:
                    owners[oid] = owners.get(oid, 0) + 1
                plots.append({
                    "kind": kind, "num": num, "owner": info.get("ownerName"),
                    "owner_id": oid, "sold": bool(info.get("sold")),
                    "locked": bool(info.get("locked")),
                    "col0": c[0] if c else None, "col1": c[1] if c else None,
                    "row0": c[2] if c else None, "row1": c[3] if c else None})
        # cross-ref the marketplace: each owner's active-listing count + USD value
        names = sorted({p["owner"] for p in plots if p["owner"]})
        market = {}
        if names:
            con = connect(readonly=True)
            try:
                gold_rate, _ = gold_rate_usd(con, get_setting(con, "gold_item"))
                qmarks = ",".join("?" * len(names))
                for r in con.execute(
                        f"""SELECT seller_name, currency, per_unit, quantity FROM listings
                            WHERE active=1 AND per_unit IS NOT NULL
                              AND seller_name IN ({qmarks})""", names):
                    usd = r["per_unit"] if r["currency"] == "token" else (
                        r["per_unit"] * gold_rate if gold_rate else 0)
                    m = market.setdefault(r["seller_name"], {"n": 0, "v": 0.0})
                    m["n"] += 1
                    m["v"] += (usd or 0) * (r["quantity"] or 1)
            finally:
                con.close()
        for p in plots:
            mk = market.get(p["owner"])
            p["listings"] = mk["n"] if mk else 0
            p["market_value"] = round(mk["v"], 2) if mk else 0
            p["owner_properties"] = owners.get(p["owner_id"], 0)
        return jsonify({"ok": True, "plots": plots,
                        "counts": {k: sum(1 for p in plots if p["kind"] == k)
                                   for k in ("mansion", "house", "trailer")}})

    return app


# ---------------------------------------------------------------------------
# frontend
# ---------------------------------------------------------------------------

INDEX_HTML = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>KinScan · Kintara Market Intelligence</title>
<meta name="description" content="KinScan tracks Kintara marketplace prices, KINS/gold arbitrage, completed sales, merchant data, property ownership, and live world activity.">
<meta name="application-name" content="KinScan">
<meta name="theme-color" content="#0a0f1a">
<meta property="og:site_name" content="KinScan">
<meta property="og:title" content="KinScan · Kintara Market Intelligence">
<meta property="og:description" content="Kintara market intelligence for KINS, gold, listings, sales, and live world data.">
<meta property="og:type" content="website">
<meta property="og:image" content="/favicon.png">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="KinScan · Kintara Market Intelligence">
<meta name="twitter:description" content="Kintara market intelligence for KINS, gold, listings, sales, and live world data.">
<link rel="icon" type="image/png" href="/favicon.png">
<link rel="shortcut icon" href="/favicon.ico">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="manifest" href="/site.webmanifest">
<style>
@import url('https://fonts.googleapis.com/css2?family=Cinzel:wght@600;700;800&family=Fredoka:wght@400;500;600;700&display=swap');
:root{
  --bg:#0a0f1a; --panel:#131c2b; --panel2:#1a263a; --line:#243349;
  --ink:#e9eef5; --mut:#8aa0bd; --gold:#e8b54a; --gold2:#f6d68a;
  --buy:#34d39a; --sell:#f06a6a;
  --mono:ui-monospace,"SF Mono",SFMono-Regular,Menlo,Consolas,monospace;
  --ui:'Fredoka',system-ui,Segoe UI,Roboto,sans-serif;
  /* design tokens — one spacing rhythm, two radii, three elevations */
  --s1:4px; --s2:8px; --s3:12px; --s4:16px; --s5:24px; --s6:32px;
  --r1:9px; --r2:14px;
  --sh1:0 2px 8px rgba(0,0,0,.30); --sh2:0 10px 30px rgba(0,0,0,.50); --sh3:0 20px 50px rgba(0,0,0,.62);
}
*{box-sizing:border-box}
body{margin:0;background:
  radial-gradient(1100px 520px at 84% -12%, rgba(70,92,134,.30), transparent 60%),
  linear-gradient(170deg,#0e1626 0%, #0b1322 55%, #090f1a 100%) fixed;
  color:var(--ink);font:14px/1.5 var(--ui);font-variant-numeric:tabular-nums lining-nums}
header{border-bottom:1px solid var(--line);
  background:linear-gradient(180deg,rgba(25,38,58,.5),transparent)}
.hdr{display:flex;align-items:center;gap:16px;flex-wrap:wrap;
  max-width:1320px;margin:0 auto;padding:16px 28px}
.brand{display:flex;align-items:center;gap:12px;margin-right:2px}
.brand-mark{width:38px;height:38px;border-radius:11px;padding:5px;
  background:linear-gradient(180deg,rgba(246,214,138,.20),rgba(232,181,74,.05));
  border:1px solid rgba(232,181,74,.42);box-shadow:0 10px 26px rgba(0,0,0,.35)}
.brand-copy{display:flex;flex-direction:column;gap:1px}
h1{margin:0;font-family:'Cinzel',serif;font-weight:800;font-size:22px;letter-spacing:.08em;
  text-transform:uppercase;
  background:linear-gradient(180deg,#fbe9b6,#e8b54a 60%,#c98a2e);-webkit-background-clip:text;
  background-clip:text;color:transparent}
h1 b{color:var(--gold)}
.brand-sub{font:700 9.5px var(--mono);letter-spacing:.15em;text-transform:uppercase;color:var(--mut)}
.meta{font:12px/1.4 var(--mono);color:var(--mut)}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--buy);
  box-shadow:0 0 8px var(--buy);margin-right:6px}
.dot.err{background:var(--sell);box-shadow:0 0 8px var(--sell)}
main{padding:22px 28px 40px;max-width:1320px;margin:0 auto}
.tabs{display:flex;gap:6px;margin-bottom:18px;flex-wrap:wrap}
.tab{padding:9px 18px;cursor:pointer;color:var(--mut);border:1px solid transparent;border-radius:999px;
  font:600 13.5px var(--ui);letter-spacing:.01em;transition:background .12s,color .12s}
.tab:hover{color:var(--ink);background:rgba(255,255,255,.04)}
.tab.on{color:var(--gold2);border-color:rgba(232,181,74,.45);
  background:linear-gradient(180deg,rgba(232,181,74,.18),rgba(232,181,74,.04))}
.controls{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;align-items:center}
input,select,button,textarea{background:var(--panel2);color:var(--ink);
  border:1px solid var(--line);border-radius:9px;padding:8px 11px;font:inherit;font-family:var(--ui)}
input[type=number]{width:88px}
input::placeholder{color:var(--mut)}
button{cursor:pointer;font-weight:600}
button:hover{border-color:var(--gold)}
button.go{background:linear-gradient(180deg,var(--gold2),var(--gold));color:#241803;border-color:var(--gold)}
.seg button.on,.chip.on{color:#241803}
.chip{padding:5px 12px;border:1px solid var(--line);border-radius:999px;background:var(--panel2);
  color:var(--mut);font-size:12px;cursor:pointer;text-transform:capitalize;font-family:var(--ui)}
.chip.on{background:linear-gradient(180deg,var(--gold2),var(--gold));border-color:var(--gold)}
tr.part td{opacity:.62}
td.isd{cursor:help}
.dealcard{position:fixed;z-index:60;display:none;pointer-events:none;max-width:330px;
  background:var(--panel2);border:1px solid var(--gold);border-radius:10px;
  padding:10px 12px;font:12px/1.5 var(--mono);color:var(--ink);
  box-shadow:0 10px 30px rgba(0,0,0,.55)}
.dealcard .dh{color:var(--gold);font-weight:700;margin-bottom:5px;text-transform:none}
.dealcard .row{display:flex;justify-content:space-between;gap:14px;white-space:nowrap}
.dealcard .row.drv{color:var(--buy)}
.dealcard .tag{color:var(--mut)}
.dealcard .sel{color:var(--mut);font-size:11px;margin-top:4px}
.goldcard{position:fixed;z-index:60;display:none;pointer-events:none;
  background:#0f141b;border:1px solid #28323d;border-radius:10px;padding:8px 12px;
  font:12px/1.55 var(--mono);box-shadow:0 12px 34px rgba(0,0,0,.6)}
.goldcard .gd{color:var(--mut);font-size:11px}
.goldcard .gv{font-weight:700;font-size:13px;margin:1px 0}
.gpill{display:inline-flex;align-items:center;border:1px solid;border-radius:999px;
  padding:3px 11px;font:12px var(--mono);font-weight:700}
.gtitle2{letter-spacing:.16em;color:#c9d4e0;font-weight:600;font-size:13px;text-transform:uppercase}
/* hover card on the arbitrage "sold today" column: per-day sales + avg price */
.soldcard{position:fixed;z-index:65;display:none;pointer-events:none;max-width:250px;
  background:var(--panel2);border:1px solid var(--gold);border-radius:10px;
  padding:10px 12px;font:12px/1.5 var(--mono);color:var(--ink);box-shadow:0 10px 30px rgba(0,0,0,.55)}
.soldcard .sh{color:var(--gold);font-weight:700;margin-bottom:5px}
.soldcard .row{display:flex;justify-content:space-between;gap:16px;white-space:nowrap}
.soldcard .row .d{color:var(--mut)}
.soldcard .row b{color:#dbe5f0}
.soldcard .none{color:var(--mut)}

/* ===== server status widget (compact icon → floating bubble) ===== */
.srv{position:relative;margin-left:8px}
.kpx{margin-left:auto;display:inline-flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;
  border:1px solid var(--line);background:rgba(255,255,255,.03);font:600 12px var(--ui);user-select:none}
.kpx .kpx-t{color:var(--gold2);font:700 10px var(--mono);letter-spacing:.06em}
.kpx .kpx-v{color:#cdd9e6;font:600 13px var(--mono);border-radius:6px;padding:0 2px}
.srv-btn{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border-radius:999px;
  border:1px solid var(--line);background:rgba(255,255,255,.03);cursor:pointer;color:#cdd9e6;
  font:600 12px var(--ui);user-select:none;transition:border-color .12s,background .12s}
.srv-btn:hover{border-color:var(--gold);background:rgba(255,255,255,.06)}
.srv.open .srv-btn{border-color:var(--gold);background:rgba(232,181,74,.08)}
.srv-btn .ic{display:flex;color:var(--gold2)}
.srv-btn .q{padding:1px 8px;border-radius:999px;font:600 11px var(--mono);color:var(--gold2);
  border:1px solid rgba(232,181,74,.4);background:rgba(232,181,74,.08)}
.srv-btn .q.z{color:var(--buy);border-color:rgba(52,211,154,.3);background:rgba(52,211,154,.06)}
.srv-pop{position:absolute;right:0;top:calc(100% + 10px);z-index:70;width:min(390px,86vw);display:none;
  border:1px solid var(--line);border-radius:14px;overflow:hidden;
  background:linear-gradient(180deg,#18243a,#0e1626);box-shadow:0 18px 46px rgba(0,0,0,.62)}
.srv.open .srv-pop{display:block}
.srv-pop:before{content:"";position:absolute;top:-6px;right:18px;width:11px;height:11px;
  background:#18243a;border-left:1px solid var(--line);border-top:1px solid var(--line);transform:rotate(45deg)}
.srv-pop-h{display:flex;align-items:center;gap:7px;flex-wrap:wrap;padding:13px 15px 11px;
  border-bottom:1px solid var(--line)}
.srv-pop-h .ttl{font:700 11px var(--ui);letter-spacing:.16em;text-transform:uppercase;color:#c9d4e0;margin-right:auto}
.sb{display:inline-flex;align-items:center;padding:3px 9px;border-radius:999px;font:600 11px var(--mono);border:1px solid}
.sb-open{color:var(--buy);border-color:rgba(52,211,154,.4);background:rgba(52,211,154,.08)}
.sb-full{color:var(--sell);border-color:rgba(240,106,106,.4);background:rgba(240,106,106,.08)}
.sb-queue{color:var(--gold2);border-color:rgba(232,181,74,.4);background:rgba(232,181,74,.08)}
.sb-mut{color:var(--mut);border-color:var(--line);background:rgba(255,255,255,.02)}
.srv-grid{max-height:58vh;overflow:auto;padding:12px;display:grid;
  grid-template-columns:repeat(auto-fill,minmax(162px,1fr));gap:9px}
.srv-card{border:1px solid var(--line);border-radius:11px;padding:10px 12px;
  background:rgba(255,255,255,.025);display:flex;flex-direction:column;gap:6px}
.srv-card .nm{display:flex;align-items:center;gap:7px;font:600 13px var(--ui);color:var(--ink)}
.srv-card .meta2{display:flex;justify-content:space-between;align-items:center;
  font:11.5px var(--mono);color:var(--mut)}
.pdot{width:8px;height:8px;border-radius:50%;flex:0 0 8px}
.pop-High{background:var(--sell);box-shadow:0 0 7px var(--sell)}
.pop-Medium{background:var(--gold);box-shadow:0 0 7px var(--gold)}
.pop-Low{background:var(--buy);box-shadow:0 0 7px var(--buy)}
.pop-na{background:var(--mut)}
.qbadge{padding:2px 8px;border-radius:999px;font:600 11px var(--mono);
  color:var(--gold2);border:1px solid rgba(232,181,74,.35);background:rgba(232,181,74,.07)}
.qbadge.zero{color:var(--buy);border-color:rgba(52,211,154,.3);background:rgba(52,211,154,.06)}

/* ===== traveling merchant tab ===== */
.mwrap{display:grid;grid-template-columns:1.25fr 1fr;gap:18px;align-items:start}
@media(max-width:980px){.mwrap{grid-template-columns:1fr}}
.mpanel{border:1px solid var(--line);border-radius:16px;padding:20px 22px;
  background:radial-gradient(900px 400px at 10% -10%,rgba(70,92,134,.22),transparent 60%),
    linear-gradient(160deg,#111a2c,#0b1322 60%,#090f1a)}
.mhead{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:4px}
.mtitle{font-family:'Cinzel',serif;font-weight:800;font-size:24px;letter-spacing:.04em;
  text-transform:uppercase;background:linear-gradient(180deg,#fbe9b6,#e8b54a 60%,#c98a2e);
  -webkit-background-clip:text;background-clip:text;color:transparent}
.msub{color:var(--mut);font:400 13px var(--ui);margin-bottom:16px}
.mode-badge{padding:5px 13px;border-radius:999px;font:700 11px var(--mono);letter-spacing:.1em;
  border:1px solid rgba(232,181,74,.5);color:var(--gold2);background:rgba(232,181,74,.1)}
.mode-badge.don{color:#9fd0ff;border-color:rgba(120,170,230,.45);background:rgba(120,170,230,.1)}
.moverall{margin:6px 0 18px}
.moverall .lab{display:flex;justify-content:space-between;font:600 13px var(--ui);
  color:#cdd9e6;margin-bottom:6px}
.moverall .lab strong{color:var(--gold2);font-variant-numeric:tabular-nums}
.mtrack{height:13px;border-radius:999px;background:rgba(255,255,255,.06);
  border:1px solid var(--line);overflow:hidden}
.mtrack.sm{height:8px}
.mfill{height:100%;border-radius:999px;
  background:linear-gradient(90deg,#c98a2e,#e8b54a 60%,#f6d68a);
  box-shadow:0 0 12px rgba(232,181,74,.35);transition:width .4s}
.mfill.done{background:linear-gradient(90deg,#1f8f63,#34d39a)}
.mres{display:flex;flex-direction:column;gap:14px}
.mres-row .rh{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}
.mres-row .rh .ico{width:22px;height:22px;border-radius:6px;margin-right:8px;vertical-align:middle}
.mres-row .nm{font:600 14px var(--ui);color:var(--ink);display:flex;align-items:center}
.mres-row .nums{font:13px var(--mono);color:var(--mut);font-variant-numeric:tabular-nums;
  display:flex;align-items:center;gap:9px}
.mres-row .nums b{color:#dbe5f0}
.mres-row .rpct{font:700 12px var(--mono);color:var(--gold2);min-width:46px;text-align:right}
.mres-row .rpct.done{color:var(--buy)}
.gold-stock{display:flex;justify-content:space-between;align-items:center;margin-top:16px;
  padding-top:14px;border-top:1px solid var(--line);font:600 14px var(--ui);color:#cdd9e6}
.gold-stock strong{color:var(--gold2);font-family:var(--mono)}
.mevents{margin-top:18px;padding-top:14px;border-top:1px solid var(--line)}
.mevents h3{margin:0 0 8px;font:700 11px var(--ui);letter-spacing:.16em;text-transform:uppercase;color:var(--mut)}
.mevents li{list-style:none;padding:7px 0;border-top:1px solid rgba(255,255,255,.05);
  font:12.5px var(--ui);color:#bcc9d8}
.mevents li:first-child{border-top:0}
.mevents li .et{color:var(--gold2);font-weight:600}
.mevents li .ed{color:var(--mut);font-size:11.5px;font-family:var(--mono)}
/* calculator */
.calc .crow{display:grid;grid-template-columns:1.5fr .9fr .9fr;gap:8px;align-items:center;
  padding:9px 4px;border-top:1px solid rgba(255,255,255,.05);font-variant-numeric:tabular-nums}
.calc .crow:first-of-type{border-top:0}
.calc .chead{color:var(--mut);font:600 10.5px var(--ui);letter-spacing:.12em;text-transform:uppercase;
  border-top:0;padding-bottom:4px}
.calc .ci{display:flex;align-items:center;gap:9px;font:500 14px var(--ui);color:var(--ink)}
.calc .ci .ico{width:24px;height:24px;border-radius:7px}
.calc .ci small{color:var(--mut);font-family:var(--mono);font-size:11px}
.calc .r{text-align:right;font:13px var(--mono)}
.calc .r.mut{color:var(--mut)}
.calc-tot{display:flex;justify-content:space-between;align-items:baseline;margin-top:6px;
  padding-top:12px;border-top:1px solid var(--line);font:600 14px var(--ui);color:#cdd9e6}
.calc-tot .v{font-family:var(--mono);font-weight:700}
.spread-box{margin-top:16px;border-radius:13px;border:1px solid;padding:14px 16px;
  display:grid;grid-template-columns:1fr 1fr;gap:10px 16px}
.spread-box.pos{border-color:rgba(52,211,154,.4);background:rgba(52,211,154,.06)}
.spread-box.neg{border-color:rgba(240,106,106,.4);background:rgba(240,106,106,.06)}
.spread-box .k{color:var(--mut);font:12px var(--ui)}
.spread-box .v{text-align:right;font:700 15px var(--mono)}
.spread-box .v.pos{color:var(--buy)}.spread-box .v.neg{color:var(--sell)}
.mintctl{display:flex;align-items:center;gap:9px;margin:2px 0 14px;color:var(--mut);font:13px var(--ui)}
.mintctl input{width:96px}

/* ===== live world (roster-first; per-player map dropdown) ===== */
.lw-head{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:8px}
.lw-shards{display:flex;gap:6px;flex-wrap:wrap}
.lw-shard{padding:7px 15px;border-radius:999px;border:1px solid var(--line);background:var(--panel2);
  color:var(--mut);font:600 13px var(--ui);cursor:pointer}
.lw-shard.on{background:linear-gradient(180deg,rgba(232,181,74,.2),rgba(232,181,74,.05));
  color:var(--gold2);border-color:rgba(232,181,74,.45)}
.lw-online{font:13px var(--mono);color:var(--mut);margin-left:auto}
.lw-online b{color:var(--buy)}
.lw-online .live{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--buy);
  box-shadow:0 0 8px var(--buy);margin-right:5px;animation:pulse 1.6s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.lw-note{color:var(--mut);font:12px var(--ui);margin:0 0 14px}
.lw-search{display:flex;align-items:center;gap:8px;margin:0 0 10px}
.lw-search input{flex:1;max-width:360px;border:1px solid var(--line);border-radius:999px;
  background:rgba(255,255,255,.03);padding:8px 14px;font:500 13px var(--ui);color:var(--ink)}
.lw-search input:focus{outline:none;border-color:var(--gold)}
.lw-search button{border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.03);
  color:var(--mut);cursor:pointer;font:600 12px var(--ui);padding:7px 11px}
.lw-search button:hover{border-color:var(--sell);color:var(--sell)}
.lw-search button.go{background:linear-gradient(180deg,var(--gold2),var(--gold));color:#241803;border-color:var(--gold)}
.lw-search button.go:hover{border-color:var(--gold);color:#241803}
.lw-search button.go:disabled{opacity:.6;cursor:default}
.lw-srch-status{color:var(--mut);font:12px var(--ui);margin-left:2px}
.lw-roster{border:1px solid var(--line);border-radius:14px;overflow:hidden;
  background:linear-gradient(180deg,rgba(26,38,58,.4),rgba(15,24,40,.25))}
.lw-roster h3{margin:0;padding:12px 15px;font:700 11px var(--ui);letter-spacing:.16em;
  text-transform:uppercase;color:var(--mut);border-bottom:1px solid var(--line);display:flex;justify-content:space-between}
.lw-sec{display:flex;align-items:center;gap:8px;padding:9px 15px;background:rgba(232,181,74,.06);
  border-top:1px solid var(--line);font:700 12.5px var(--ui);letter-spacing:.02em;color:#e7eef6}
.lw-sec:first-child{border-top:0}
.lw-sec span{margin-left:auto;color:var(--gold2);font-family:var(--mono);font-size:12px;font-weight:600}
.lw-p{display:flex;align-items:center;gap:11px;padding:10px 14px;cursor:pointer;border-top:1px solid rgba(255,255,255,.04)}
.lw-p:first-child{border-top:0}
.lw-p:hover{background:rgba(255,255,255,.04)}
.lw-p.open{background:rgba(232,181,74,.08)}
.lw-p .av{flex:0 0 32px}
.lw-p .nm{font:600 14.5px var(--ui);color:var(--ink);flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.lw-p .lvl{font:600 12px var(--mono);color:var(--gold2);background:rgba(232,181,74,.1);
  border:1px solid rgba(232,181,74,.3);border-radius:999px;padding:1px 8px}
.lw-p .chev{color:#5f748f;transition:transform .15s;font-size:12px}
.lw-p.open .chev{transform:rotate(90deg);color:var(--gold)}
.lw-exp{border-top:1px solid rgba(255,255,255,.06);background:rgba(0,0,0,.18);
  display:grid;grid-template-columns:1.05fr .95fr;gap:16px;padding:16px}
@media(max-width:760px){.lw-exp{grid-template-columns:1fr}}
.lw-map{position:relative;border:1px solid var(--line);border-radius:12px;overflow:hidden;
  aspect-ratio:16/9;background:#9fd6e6 center/cover no-repeat}
.lw-map .you{position:absolute;width:15px;height:15px;border-radius:50%;background:var(--gold2);
  border:2px solid #1a1206;box-shadow:0 0 0 3px rgba(232,181,74,.45),0 0 14px var(--gold);
  transform:translate(-50%,-50%);z-index:3}
.lw-map .you:after{content:"";position:absolute;inset:-7px;border:2px solid var(--gold2);border-radius:50%;
  animation:ping 1.7s ease-out infinite}
@keyframes ping{0%{transform:scale(.5);opacity:.85}100%{transform:scale(2);opacity:0}}
.lw-map .other{position:absolute;width:9px;height:9px;border-radius:50%;background:#7fb0ff;
  border:1.5px solid #0a1019;transform:translate(-50%,-50%);z-index:2}
.lw-map .other.mov{background:#34d39a}
.lw-map.shore{aspect-ratio:1/1;background:#cfe3ee center/cover no-repeat}
.lw-map.shore img{image-rendering:pixelated}
.shore-mk{position:absolute;transform:translate(-50%,-60%);z-index:2;pointer-events:none;
  filter:drop-shadow(0 0 2px #fff) drop-shadow(0 0 5px rgba(255,255,255,.85))}
.shore-mk.sel{z-index:3;filter:drop-shadow(0 0 3px #fff) drop-shadow(0 0 8px #fff) drop-shadow(0 0 13px rgba(255,255,255,.9))}
.shore-mk .nm{display:block;text-align:center;font:600 9px var(--ui);color:#fff;margin-top:-2px;
  text-shadow:0 0 3px #000,0 1px 2px #000;white-space:nowrap}
.shore-mk.sel .nm{color:var(--gold2);font-weight:800}
.lw-map .biome{position:absolute;left:8px;top:8px;font:600 11px var(--ui);color:#fff;
  background:rgba(0,0,0,.5);padding:3px 9px;border-radius:999px;z-index:4}
.lw-map .coord{position:absolute;right:8px;bottom:8px;font:11px var(--mono);color:#fff;
  background:rgba(0,0,0,.5);padding:2px 8px;border-radius:999px;z-index:4}
.lw-info{display:flex;flex-direction:column;gap:12px}
.lw-info .top{display:flex;gap:13px;align-items:center}
.lw-info .nm{font-family:'Cinzel',serif;font-weight:800;font-size:20px;color:var(--gold2)}
.lw-info .sub{color:var(--mut);font:12px var(--mono)}
.lw-grid{display:grid;grid-template-columns:auto 1fr;gap:7px 14px;font:13px var(--ui)}
.lw-grid .k{color:var(--mut)} .lw-grid .v{text-align:right;color:#dbe5f0;font-family:var(--mono)}
.tag{display:inline-block;padding:1px 8px;border-radius:999px;font:600 11px var(--mono);
  border:1px solid rgba(232,181,74,.35);color:var(--gold2);background:rgba(232,181,74,.08)}
.hpbar{height:7px;border-radius:999px;background:rgba(240,106,106,.18);overflow:hidden;border:1px solid var(--line)}
.hpbar>div{height:100%;background:linear-gradient(90deg,#f06a6a,#34d39a)}

/* ===== property map ===== */
.pm-head{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:14px}
.pm-legend{display:flex;gap:14px;flex-wrap:wrap;font:12px var(--ui);color:var(--mut)}
.pm-legend span{display:inline-flex;align-items:center;gap:6px}
.pm-sw{width:13px;height:13px;border-radius:4px;display:inline-block;border:1px solid rgba(255,255,255,.2)}
.pm{display:grid;grid-template-columns:1.4fr .9fr;gap:18px;align-items:start}
@media(max-width:980px){.pm{grid-template-columns:1fr}}
.pm-map{position:relative;border:1px solid var(--line);border-radius:16px;overflow:hidden;
  background:#15331c}
.pm-map svg{display:block;width:100%;height:auto}
.plot{cursor:pointer;transition:filter .12s}
.plot:hover{filter:brightness(1.25)}
.plot.sel{filter:brightness(1.35) drop-shadow(0 0 6px var(--gold))}
.pm-bld{cursor:pointer;transition:filter .1s}
.pm-bld:hover{filter:drop-shadow(0 0 4px #fff) drop-shadow(0 0 9px rgba(255,255,255,.85)) brightness(1.08)}
.pm-bld.sel{filter:drop-shadow(0 0 5px var(--gold)) drop-shadow(0 0 11px rgba(232,181,74,.7)) brightness(1.12)}
.pm-bld .lab{font:800 8.5px var(--mono);paint-order:stroke;stroke:#0a1019;stroke-width:2.4px;stroke-linejoin:round;fill:#fff;pointer-events:none;opacity:0;transition:opacity .1s}
.pm-bld:hover .lab,.pm-bld.sel .lab{opacity:1}
.pm-side{border:1px solid var(--line);border-radius:14px;padding:18px;min-height:200px;
  background:linear-gradient(160deg,#16223a,#0e1626)}
.pm-card .ttl{font-family:'Cinzel',serif;font-weight:800;font-size:22px;color:var(--gold2);text-transform:capitalize}
.pm-card .owner{font:600 15px var(--ui);color:var(--ink);margin:2px 0 14px}
.pm-card .owner small{color:var(--mut);font:12px var(--mono)}
.pm-row{display:flex;justify-content:space-between;padding:8px 0;border-top:1px solid rgba(255,255,255,.06);
  font:13px var(--ui);color:var(--mut)}
.pm-row b{color:#dbe5f0;font-family:var(--mono);font-weight:600}
.pm-empty{color:var(--mut);font:13px var(--ui);text-align:center;padding:60px 10px}
.pm-stat{display:flex;gap:10px;margin-bottom:12px}
.pm-stat .box{flex:1;border:1px solid var(--line);border-radius:11px;padding:9px 11px;text-align:center}
.pm-stat .box .n{font:700 18px var(--mono);color:var(--gold2)}
.pm-stat .box .l{font:11px var(--ui);color:var(--mut);text-transform:uppercase;letter-spacing:.08em}

/* ===== game-styled sales index (matches the in-game menus) ===== */
.gw{--gold:#e8b54a;--gold2:#f6d68a;font-family:'Fredoka',system-ui,sans-serif;
  display:flex;gap:0;border:1px solid #243349;border-radius:18px;overflow:hidden;min-height:600px;
  background:radial-gradient(1100px 520px at 12% -10%, rgba(70,92,134,.32), transparent 60%),
    linear-gradient(160deg,#101a2c 0%,#0b1322 55%,#090f1a 100%);
  box-shadow:0 24px 60px rgba(0,0,0,.45)}
.gw-side{flex:0 0 206px;padding:20px 12px;border-right:1px solid #1c2b40;
  background:linear-gradient(180deg,rgba(30,46,70,.5),rgba(15,24,40,.25))}
.gw-eyebrow{font:600 11px 'Fredoka';letter-spacing:.22em;color:#6f86a6;text-transform:uppercase;
  text-align:center;margin:2px 0 16px}
.gw-cat{display:block;width:100%;text-align:left;padding:11px 14px;margin:3px 0;border-radius:11px;
  border:1px solid transparent;background:transparent;color:#aebbcd;font:500 14.5px 'Fredoka';cursor:pointer;
  transition:background .12s,color .12s}
.gw-cat:hover{background:rgba(255,255,255,.045);color:#eef3f9}
.gw-cat.on{background:linear-gradient(90deg,rgba(232,181,74,.18),rgba(232,181,74,.02));
  color:var(--gold2);border-color:rgba(232,181,74,.4);box-shadow:inset 2px 0 0 var(--gold)}
.gw-cat .c{float:right;color:#5f748f;font-size:12px;font-weight:400}
.gw-main{flex:1;padding:24px 28px 28px;position:relative;min-width:0}
.gw-corner{position:absolute;top:12px;width:9px;height:9px;background:var(--gold);opacity:.7;
  transform:rotate(45deg);box-shadow:0 0 10px rgba(232,181,74,.5)}
.gw-corner.l{left:14px}.gw-corner.r{right:14px}
.gw-title{font-family:'Cinzel',serif;font-weight:800;font-size:30px;letter-spacing:.05em;margin:2px 0 6px;text-transform:uppercase;
  background:linear-gradient(180deg,#fbe9b6 0%,#e8b54a 55%,#c98a2e 100%);-webkit-background-clip:text;
  background-clip:text;color:transparent;text-shadow:0 2px 14px rgba(232,181,74,.12)}
.gw-sub{color:#8aa0bd;font:400 13.5px 'Fredoka';margin-bottom:14px}
.gw-pills{display:flex;gap:8px;margin-bottom:6px}
.gw-pill{padding:7px 16px;border-radius:999px;border:1px solid #2c3c56;background:rgba(255,255,255,.03);
  color:#9fb1c8;font:500 13px 'Fredoka';cursor:pointer}
.gw-pill:hover{border-color:#3d5274;color:#e7ecf2}
.gw-pill.on{background:linear-gradient(180deg,rgba(232,181,74,.22),rgba(232,181,74,.06));
  color:var(--gold2);border-color:rgba(232,181,74,.45)}
.gw-note{color:#6f86a6;font:400 12px 'Fredoka';margin:10px 0 6px}
.gw-head,.gw-row{display:grid;grid-template-columns:1.6fr .8fr .9fr .9fr .9fr;align-items:center;gap:8px}
.gw-head{padding:6px 12px;color:#6f86a6;font:600 11px 'Fredoka';letter-spacing:.14em;text-transform:uppercase;
  border-bottom:1px solid rgba(255,255,255,.07)}
.gw-head .r,.gw-row .r{text-align:right}
.gw-row{padding:13px 12px;border-top:1px solid rgba(255,255,255,.05);cursor:pointer;transition:background .1s}
.gw-row:first-of-type{border-top:0}
.gw-row:hover{background:rgba(255,255,255,.035)}
.gw-row.open{background:rgba(232,181,74,.06)}
.gw-item{display:flex;align-items:center;gap:11px;min-width:0}
.gw-ico{flex:0 0 34px;width:34px;height:34px;border-radius:9px;display:grid;place-items:center;font-size:18px;
  background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.08)}
.gw-name{color:#e9eef5;font:500 15px 'Fredoka';white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.gw-num{color:#dbe5f0;font:500 14.5px 'Fredoka';text-align:right;font-variant-numeric:tabular-nums}
.gw-num.mut{color:#6f86a6}
.gw-kins{color:var(--gold2)}
.gw-chev{color:#5f748f;transition:transform .15s;display:inline-block}
.gw-row.open .gw-chev{transform:rotate(90deg);color:var(--gold)}
.gw-exp{grid-column:1/-1;overflow:hidden}
.gw-expinner{padding:8px 4px 18px;display:grid;grid-template-columns:1fr 232px;gap:18px}
.gw-statpanel{border:1px solid #243349;border-radius:13px;padding:14px;
  background:linear-gradient(180deg,rgba(25,38,58,.5),rgba(15,24,40,.3))}
.gw-statpanel h4{margin:0 0 10px;font:700 12px 'Fredoka';letter-spacing:.12em;color:#8aa0bd;text-transform:uppercase}
.gw-stat{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px dashed rgba(255,255,255,.06);
  font:400 13px 'Fredoka';color:#9fb1c8}
.gw-stat:last-child{border-bottom:0}
.gw-stat b{color:#e9eef5;font-weight:600}
.gw-cseg{display:inline-flex;border:1px solid #2c3c56;border-radius:9px;overflow:hidden;margin-bottom:10px}
.gw-cseg button{border:0;background:rgba(255,255,255,.03);color:#9fb1c8;font:500 12.5px 'Fredoka';
  padding:6px 14px;cursor:pointer}
.gw-cseg button.on{background:linear-gradient(180deg,rgba(232,181,74,.2),rgba(232,181,74,.05));color:var(--gold2)}
.gw-empty{color:#6f86a6;font:400 13px 'Fredoka';padding:30px;text-align:center}
/* item index info panel (cosmetic/mount/pet expand) */
.gw-meta{margin-top:14px;border-top:1px solid rgba(255,255,255,.07);padding-top:13px}
.gw-meta-h{font:700 12px 'Cinzel',serif;letter-spacing:.06em;color:var(--gold2);text-transform:uppercase;margin-bottom:7px}
.gw-meta-note{color:#c4d2e4;font:400 13px 'Fredoka';line-height:1.5;margin-bottom:11px;font-style:italic}
.gw-meta-grid{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;font:13px 'Fredoka'}
.gw-meta-grid .k{color:#7f93ad} .gw-meta-grid .v{color:#dbe5f0}
/* cheapest live listings (gold / $KINS) in the item expand */
.gw-listings{margin-top:14px;border-top:1px solid rgba(255,255,255,.07);padding-top:13px}
.ll-cols{display:grid;grid-template-columns:1fr 1fr;gap:10px 22px}
@media(max-width:560px){.ll-cols{grid-template-columns:1fr}}
.ll-col{min-width:0}
.ll-h{font:600 11px 'Fredoka';letter-spacing:.05em;text-transform:uppercase;color:#7f93ad;margin-bottom:6px}
.ll-row{display:flex;align-items:baseline;gap:8px;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.05);font:13px 'Fredoka'}
.ll-p{color:var(--gold2);font-weight:700;font-family:var(--mono)}
.ll-q{color:#9fb1c8;font-size:11px} .ll-x{color:#8aa0bd;font-size:11.5px;font-family:var(--mono)}
.ll-sel{margin-left:auto;color:#7f93ad;font-size:12px;max-width:46%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ll-none{color:#6f86a6;font:400 12.5px 'Fredoka';padding:6px 0}
.kins-vs{font:500 12.5px 'Fredoka';line-height:1.5;border-radius:9px;padding:8px 12px;margin-bottom:10px;
  background:rgba(52,211,154,.08);border:1px solid rgba(52,211,154,.3);color:#bdeedd}
.kins-vs.down{background:rgba(240,106,106,.08);border-color:rgba(240,106,106,.3);color:#f3c0c0}
.kins-vs b{font-size:14px} .kins-vs span{color:#8aa0bd}
/* buy-side liquidity depth chart (materials, in the item expand) */
.liq-wrap{grid-column:1/-1;margin-top:6px;border-top:1px solid rgba(255,255,255,.07);padding-top:14px}
.liq-h{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:9px}
.liq-t{font:800 13px 'Cinzel',serif;color:var(--gold2);letter-spacing:.05em;text-transform:uppercase}
.liq-sub{color:#8aa0bd;font:12px 'Fredoka'}
.liq-sub b{color:#dbe5f0}
.liq-svg{width:100%;height:auto;display:block;
  background:linear-gradient(180deg,rgba(25,38,58,.35),rgba(15,24,40,.15));
  border:1px solid var(--line);border-radius:12px}
.liq-svg rect[data-tip]{transition:opacity .1s}
.liq-svg rect[data-tip]:hover{opacity:.82}
.liq-foot{color:#6f86a6;font:11.5px 'Fredoka';margin-top:8px;line-height:1.5}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:9px;overflow:hidden}
.seg button{border:0;border-radius:0;background:var(--panel2)}
.seg button.on{background:linear-gradient(180deg,var(--gold2),var(--gold));color:#241803}
/* mispricing mode */
.mp-sec{font-size:11px;color:var(--mut);font-weight:400;margin-top:1px}
.mp-tag{font-size:10px;color:var(--mut);border:1px solid var(--line);border-radius:5px;padding:0 4px;margin-left:4px}
.mp-exp{background:var(--panel2);border:1px solid var(--line);border-radius:7px;color:var(--mut);
  cursor:pointer;font-size:12px;line-height:1;padding:3px 7px}
.mp-exp:hover,.mp-exp.on{color:var(--gold2);border-color:var(--gold2)}
.mp-exprow td{padding:0;border-bottom:1px solid var(--line)}
.mp-exprow tr:hover td,.mp-exprow td:hover{background:none}
.mp-exp-box{padding:6px 14px 16px}
table{width:100%;border-collapse:collapse;font:12.5px/1.4 var(--mono);
  background:linear-gradient(180deg,rgba(25,38,58,.4),rgba(15,24,40,.2));
  border:1px solid var(--line);border-radius:13px}
th{text-align:left;color:var(--mut);font-weight:600;padding:10px 12px;letter-spacing:.08em;
  text-transform:uppercase;font-size:11px;
  border-bottom:1px solid var(--line);position:sticky;top:0;
  background:linear-gradient(180deg,#16223400,#16223490),var(--panel)}
td{padding:9px 12px;border-bottom:1px solid rgba(255,255,255,.05)}
tr:hover td{background:rgba(255,255,255,.035)}
.num{text-align:right}
.gold{color:var(--gold2)} .usd{color:var(--buy)} .mut{color:var(--mut)}
.pos{color:var(--buy);font-weight:700} .neg{color:var(--sell)}
tr.win td{background:rgba(52,211,154,.08)}
tr.win td:first-child{box-shadow:inset 3px 0 0 var(--buy)}
.rate{display:flex;align-items:baseline;gap:10px;
  background:linear-gradient(180deg,rgba(25,38,58,.6),rgba(15,24,40,.35));
  border:1px solid var(--line);border-radius:13px;padding:13px 18px;margin-bottom:14px;flex-wrap:wrap}
.rate .big{font:700 22px/1 var(--ui);color:var(--gold2)}
.rate small{color:var(--mut);font:12px var(--mono)}
.warn{color:var(--sell)}
.card{background:linear-gradient(180deg,rgba(25,38,58,.5),rgba(15,24,40,.3));
  border:1px solid var(--line);border-radius:14px;padding:16px 18px;margin-bottom:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:10px}
.kpi{font:700 20px/1 var(--ui);color:var(--gold2)} .klab{font:600 11px/1.4 var(--ui);color:var(--mut);text-transform:uppercase;letter-spacing:.08em}
canvas{width:100%;height:210px;display:block}
textarea{width:100%;min-height:90px;font:12.5px/1.5 var(--mono);resize:vertical}
.hint{color:var(--mut);font:12.5px/1.6 var(--ui);margin:8px 0 12px}
.empty{color:var(--mut);padding:30px;text-align:center;font:13px var(--ui)}
a{color:var(--gold2)}

/* ===== premium QoL layer ===== */
/* honour reduced-motion (#11) */
@media (prefers-reduced-motion: reduce){
  *,*:before,*:after{animation-duration:.001ms!important;animation-iteration-count:1!important;transition-duration:.001ms!important}
}
/* custom tooltip engine (#16) */
.tipbox{position:fixed;z-index:95;display:none;pointer-events:none;max-width:280px;
  background:#0f141b;border:1px solid var(--line);border-radius:var(--r1);padding:6px 10px;
  font:12px/1.45 var(--ui);color:var(--ink);box-shadow:var(--sh2)}
[data-tip]{cursor:help;text-decoration:underline dotted rgba(255,255,255,.18);text-underline-offset:3px}
td [data-tip],.num [data-tip]{text-decoration:none}
/* live "updated" indicator (#12) */
.live-upd{display:inline-flex;align-items:center;gap:6px;font:12px var(--mono);color:var(--mut)}
.live-upd .d{width:7px;height:7px;border-radius:50%;background:var(--buy);box-shadow:0 0 8px var(--buy);animation:pulse 1.6s infinite}
/* freshness badges (#20) */
.fresh{font:600 10px var(--mono);padding:1px 6px;border-radius:999px;border:1px solid;white-space:nowrap}
.fresh.aging{color:var(--gold2);border-color:rgba(232,181,74,.4);background:rgba(232,181,74,.07)}
.fresh.stale{color:var(--sell);border-color:rgba(240,106,106,.4);background:rgba(240,106,106,.08)}
/* inline sparkline (#14) */
.spark{vertical-align:middle;overflow:visible}
/* value flash on change (#8) */
@keyframes fup{0%{background:rgba(52,211,154,.30)}100%{background:transparent}}
@keyframes fdn{0%{background:rgba(240,106,106,.30)}100%{background:transparent}}
.flash-up{animation:fup .7s ease-out}
.flash-dn{animation:fdn .7s ease-out}
/* tab cross-fade + row expand (#10) */
@keyframes viewfade{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}
#view.swap{animation:viewfade .22s ease}
.gw-exp,.lw-exp{animation:viewfade .2s ease}
/* skeleton loaders (#9) */
@keyframes shim{0%{background-position:-260px 0}100%{background-position:260px 0}}
.skel{border-radius:var(--r1);background:linear-gradient(90deg,rgba(255,255,255,.04) 25%,rgba(255,255,255,.11) 37%,rgba(255,255,255,.04) 63%);
  background-size:520px 100%;animation:shim 1.25s infinite linear}
.skel-wrap{padding:6px 2px}
.skel-bar{height:40px;margin:9px 0}
/* command palette (#7) */
.kbtn{display:inline-flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;
  border:1px solid var(--line);background:rgba(255,255,255,.03);cursor:pointer;color:#cdd9e6;
  font:600 12px var(--ui);transition:border-color .12s,background .12s}
.kbtn:hover{border-color:var(--gold);background:rgba(255,255,255,.06)}
kbd{font:11px var(--mono);background:rgba(255,255,255,.06);border:1px solid var(--line);
  border-bottom-width:2px;border-radius:5px;padding:1px 6px;color:#cdd9e6}
.cmdk{position:fixed;inset:0;z-index:100;display:none;background:rgba(6,10,18,.55);
  -webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px);justify-content:center}
.cmdk.on{display:flex}
.cmdk-box{margin-top:11vh;height:max-content;width:min(560px,92vw);
  background:linear-gradient(180deg,#16223a,#0e1626);border:1px solid var(--line);
  border-radius:var(--r2);overflow:hidden;box-shadow:var(--sh3)}
.cmdk-box input{width:100%;border:0;border-bottom:1px solid var(--line);border-radius:0;
  background:transparent;padding:16px 18px;font:16px var(--ui);color:var(--ink)}
.cmdk-box input:focus{outline:none;border-color:var(--line)}
.cmdk-list{max-height:50vh;overflow:auto;padding:6px}
.cmdk-row{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:var(--r1);cursor:pointer}
.cmdk-row.sel,.cmdk-row:hover{background:rgba(232,181,74,.10)}
.cmdk-t{font:600 9.5px var(--mono);text-transform:uppercase;letter-spacing:.1em;color:var(--mut);
  border:1px solid var(--line);border-radius:999px;padding:2px 7px;flex:0 0 auto;min-width:42px;text-align:center}
.cmdk-l{color:var(--ink);font:500 14px var(--ui);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cmdk-s{margin-left:auto;color:var(--mut);font:11px var(--mono);flex:0 0 auto}
.cmdk-empty{padding:24px;text-align:center;color:var(--mut)}
.cmdk-hint{display:flex;justify-content:space-between;padding:9px 14px;border-top:1px solid var(--line);
  color:var(--mut);font:11px var(--mono)}
</style></head>
<body>
<header><div class="hdr">
  <div class="brand">
    <img class="brand-mark" src="/favicon.png" alt="" width="38" height="38">
    <div class="brand-copy">
      <h1>KinScan</h1>
      <div class="brand-sub">Kintara market intelligence</div>
    </div>
  </div>
  <div class="meta" id="status">connecting…</div>
  <span class="live-upd"><i class="d"></i><span id="upd">live</span></span>
  <button class="kbtn" id="cmdkBtn" data-tip="Search items, sellers &amp; tabs">🔍 Search <kbd>⌘K</kbd></button>
  <div class="kpx" id="kpx" data-tip="live $KINS price (USD)"></div>
  <div class="srv" id="srv"></div>
</div></header>
<div class="cmdk" id="cmdk"><div class="cmdk-box">
  <input id="cmdkInput" placeholder="Search items, sellers, or jump to a tab…" autocomplete="off" spellcheck="false">
  <div class="cmdk-list" id="cmdkList"></div>
  <div class="cmdk-hint"><span><kbd>↑</kbd><kbd>↓</kbd> navigate &nbsp; <kbd>↵</kbd> open</span><span><kbd>esc</kbd> close</span></div>
</div></div>
<main>
  <div class="tabs">
    <div class="tab on" data-t="arb">Arbitrage</div>
    <div class="tab" data-t="live">Live listings</div>
    <div class="tab" data-t="removed">Sales feed</div>
    <div class="tab" data-t="hist">Index</div>
    <div class="tab" data-t="gold">Gold price</div>
    <div class="tab" data-t="merchant">Merchant</div>
    <div class="tab" data-t="world">Live World</div>
    <div class="tab" data-t="props">Property Map</div>
  </div>
  <div id="view"></div>
</main>

<script>
const $=s=>document.querySelector(s);
const lbl=it=>(state.labels&&state.labels[it])||it;   // itemType -> in-game name
let TAB="arb", timer=null;
const state={dir:"gold_to_kins", fee:0, goldItem:null, items:[], cats:[], labels:{},
  mpCur:"kins", mpOpen:null, mpRowMap:{}, mpCatOff:new Set(), mpCatInit:false,
  catOff:new Set(), profitableOnly:false, soldOnly:false, search:"", minQty:0, viewSet:[],
  histItem:null, histCur:"token", histCat:"all", histSort:"most", histOpen:null, histWindow:1,
  goldMode:"gold_usd", goldRange:"1D", srvOpen:false, mintQty:1,
  liveShard:1, liveSel:null, liveSearch:"", liveSearchBusy:false, liveSearchStatus:"", propSel:null, servers:[]};

/* force-refresh cached sales for a set of items (the side currently shown) */
async function refreshVisible(items){
  items=[...new Set(items||[])];
  if(!items.length) return;
  const sellCur = state.dir==="mispricing" ? (state.mpCur==="gold"?"gold":"token")
                : state.dir==="gold_to_kins" ? "token" : "gold";
  try{
    await fetch("/api/refresh-stats",{method:"POST",
      headers:{"content-type":"application/json"},
      body:JSON.stringify({items, currency:sellCur})});
  }catch(e){/* offline / unreachable — leave cache as-is */}
}

/* ----- hover "deal card": the cheapest listing behind the items/$ column ----- */
let dealEl=null;
function dealNode(){ if(!dealEl){dealEl=document.createElement('div');
  dealEl.className='dealcard'; document.body.appendChild(dealEl);} return dealEl; }
function hideDeal(){ if(dealEl) dealEl.style.display='none'; }
function dealHtml(r){
  const rate=state.rate||0;
  let goldEa = r.gold_lot && rate ? (r.gold_lot.price_gold/r.gold_lot.qty)*rate : null;
  let kinsEa = r.kins_lot ? r.kins_lot.price_usd/r.kins_lot.qty : null;
  const drv = (goldEa!=null && (kinsEa==null||goldEa<=kinsEa)) ? 'gold'
            : (kinsEa!=null ? 'kins' : null);
  let h=`<div class="dh">${lbl(r.item_type)} · cheapest listing${r.gold_lot&&r.kins_lot?'s':''}</div>`;
  let seller=null;
  if(r.gold_lot){ const g=r.gold_lot, tot=rate?` <span class="tag">≈${fmtU(g.price_gold*rate)}</span>`:'';
    h+=`<div class="row ${drv==='gold'?'drv':''}"><span>GOLD &nbsp;<b>${(g.qty||0).toLocaleString()}</b> × `+
       `<span class="gold">${(g.price_gold||0).toLocaleString()}g</span>${tot}</span>`+
       `<span class="tag">${goldEa==null?'':fmtU(goldEa)+'/ea'}${drv==='gold'?' ◄':''}</span></div>`;
    if(drv==='gold') seller=g.seller; }
  if(r.kins_lot){ const k=r.kins_lot;
    h+=`<div class="row ${drv==='kins'?'drv':''}"><span>KINS &nbsp;<b>${(k.qty||0).toLocaleString()}</b> × `+
       `<span class="usd">${fmtU(k.price_usd)}</span></span>`+
       `<span class="tag">${fmtU(k.price_usd/k.qty)}/ea${drv==='kins'?' ◄':''}</span></div>`;
    if(drv==='kins') seller=k.seller; }
  if(!r.gold_lot && !r.kins_lot) h+=`<div class="tag">no live listing</div>`;
  if(seller) h+=`<div class="sel">seller: <b>${seller}</b> &nbsp; ◄ drives items/$</div>`;
  return h;
}
function showDeal(r,x,y){ const el=dealNode(); el.innerHTML=dealHtml(r); el.style.display='block';
  const b=el.getBoundingClientRect(); let L=x+16, T=y+16;
  if(L+b.width>innerWidth-8) L=Math.max(8,x-b.width-16);
  if(T+b.height>innerHeight-8) T=Math.max(8,innerHeight-b.height-8);
  el.style.left=L+'px'; el.style.top=T+'px'; }

/* ----- hover "sold" card: last-7-days units sold + avg price mini-chart ----- */
let soldEl=null; const soldCache={};
function soldNode(){ if(!soldEl){soldEl=document.createElement('div');soldEl.className='soldcard';document.body.appendChild(soldEl);} return soldEl; }
function hideSold(){ if(soldEl) soldEl.style.display='none'; }
function positionSold(x,y){ const el=soldEl; if(!el||el.style.display==='none')return;
  const b=el.getBoundingClientRect(); let L=x-b.width-16, T=y+16;
  if(L<8) L=x+16; if(L+b.width>innerWidth-8) L=Math.max(8,innerWidth-b.width-8);
  if(T+b.height>innerHeight-8) T=Math.max(8,innerHeight-b.height-8);
  el.style.left=L+'px'; el.style.top=T+'px'; }
/* build a fixed 7-slot window of calendar days ending today (UTC), mapping the
   sparse daily archive onto it so every day shows individually (0 if no sales). */
function lastNDays(samples,n){
  const byDate={}; (samples||[]).forEach(s=>byDate[s.date]=s);
  const t=new Date(), endMs=Date.UTC(t.getUTCFullYear(),t.getUTCMonth(),t.getUTCDate()), out=[];
  for(let i=n-1;i>=0;i--){ const dMs=endMs-i*864e5, ds=new Date(dMs).toISOString().slice(0,10), s=byDate[ds];
    out.push({date:ds,t:dMs,sales:s?(s.sales||0):0,price:s?s.avgUnitPrice:null}); }
  return out;
}
async function showSold(it,cur,x,y){
  const el=soldNode(), goldOn=cur==='gold', key=it+'|'+cur;
  el.style.display='block'; el.__tok=key;
  el.innerHTML=`<div class="sh">${esc(lbl(it))} · last 7 days</div><div class="ssub">loading…</div>`;
  positionSold(x,y);
  let d=soldCache[key];
  if(!d){ try{ d=await (await fetch(`/api/sales-history?item_type=${encodeURIComponent(it)}&currency=${cur}`)).json(); soldCache[key]=d; }
    catch(e){ if(el.__tok===key) el.innerHTML=`<div class="sh">${esc(lbl(it))}</div><div class="ssub">couldn't load history</div>`; return; } }
  if(el.__tok!==key || el.style.display==='none') return;   // moved on while loading
  const days=lastNDays((d&&d.samples)||[],3), hasPx=days.some(p=>p.price!=null);
  const unit=v=> v==null?'—':(goldOn?(+Number(v).toPrecision(3))+'g':'$'+Number(v).toFixed(v>=1?2:4));
  const fmtDay=ms=>new Date(ms).toLocaleDateString('en-US',{month:'short',day:'numeric',timeZone:'UTC'});
  const rows=days.slice().reverse().map(p=> p.price!=null
    ? `<div class="row"><span class="d">${fmtDay(p.t)}</span><span><b>${(p.sales||0).toLocaleString()}</b> sold · <span class="${goldOn?'gold':'usd'}">${unit(p.price)}</span></span></div>`
    : `<div class="row"><span class="d">${fmtDay(p.t)}</span><span class="none">no sales</span></div>`).join('');
  el.innerHTML=`<div class="sh">${esc(lbl(it))} · last 3 days</div>`+
    (hasPx?rows:`<div class="none">no recorded sales in this window</div>`);
  positionSold(x,y);
}

/* arbitrage auto-refresh: re-pull live sales for the shown items every few sec */
let arbTimer=null, arbBusy=false, arbHover=false;
async function arbTick(){
  if(TAB!=="arb" || document.hidden || arbBusy) return;
  if(state.dir==="mispricing" && state.mpOpen) return;   // freeze while inspecting a dropdown
  const typing = document.activeElement && document.activeElement.id==="asearch";
  if(arbHover || typing) return;            // don't disrupt the hover card or typing
  arbBusy=true;
  try{ await refreshVisible(state.viewSet); }catch(e){}
  arbBusy=false;
  if(TAB==="arb" && !arbHover) loadArb();
}

function ago(iso){ if(!iso) return "";
  const s=(Date.now()-new Date(iso))/1000;
  if(s<60)return Math.floor(s)+"s"; if(s<3600)return Math.floor(s/60)+"m";
  if(s<86400)return Math.floor(s/3600)+"h"; return Math.floor(s/86400)+"d"; }
function dur(sec){ if(sec==null)return "";
  if(sec<60)return sec+"s"; if(sec<3600)return Math.round(sec/60)+"m";
  if(sec<86400)return (sec/3600).toFixed(1)+"h"; return (sec/86400).toFixed(1)+"d"; }
function money(v){ return v==null?"—":"$"+Number(v).toFixed(2); }
/* adaptive precision — per-item prices are often tiny ($0.0004 / 0.00009g) */
function dec(v){ const a=Math.abs(v); return a>=100?2:a>=1?3:a>=0.01?4:6; }
function fmtU(v){ return v==null?"—":"$"+Number(v).toFixed(dec(v)); }
function fmtG(v){ return v==null?"—":(+Number(v).toFixed(dec(v)))+"g"; }
function fmtN(v){ if(v==null)return "—"; v=Number(v);
  return v>=1000?Math.round(v).toLocaleString():v.toFixed(v>=1?1:3); }
function fmtKins(v){ if(v==null)return "—"; v=Number(v);
  return v>=1000?Math.round(v).toLocaleString():(+v.toFixed(v>=10?1:v>=1?2:3)).toString(); }
function pct(v){ return v==null?"—":v+"%"; }

/* ============================================================
   premium QoL layer: flicker-free morph, value flash, tooltips,
   command palette, sparklines, freshness, live "updated" ticker
   ============================================================ */
const RM = matchMedia('(prefers-reduced-motion: reduce)').matches;

/* (#13) abbreviate big numbers; full value on hover */
function abbr(n){ if(n==null||n===''||isNaN(n)) return '—'; n=+n;
  const a=Math.abs(n); let s;
  if(a>=1e9) s=(n/1e9).toFixed(a>=1e10?0:1)+'B';
  else if(a>=1e6) s=(n/1e6).toFixed(a>=1e7?0:1)+'M';
  else if(a>=1e4) s=(n/1e3).toFixed(a>=1e5?0:1)+'k';
  else return Math.round(n).toLocaleString();
  return `<span data-tip="${n.toLocaleString()}">${s}</span>`;
}
/* (#17) relative time + absolute on hover */
function relAbs(ms){ if(!ms) return ''; const t=+new Date(ms); if(isNaN(t)) return '';
  const s=Math.max(0,(Date.now()-t)/1000); let r;
  if(s<60)r=Math.floor(s)+'s'; else if(s<3600)r=Math.floor(s/60)+'m';
  else if(s<86400)r=Math.floor(s/3600)+'h'; else r=Math.floor(s/86400)+'d';
  return `<span data-tip="${new Date(t).toLocaleString()}">${r} ago</span>`;
}
/* (#20) freshness badge for an age in hours */
function freshness(ms){ if(!ms) return ''; const h=(Date.now()-+new Date(ms))/3.6e6;
  if(isNaN(h)||h<1) return ''; const lvl=h>6?'stale':'aging';
  return ` <span class="fresh ${lvl}" data-tip="this listing is ${h<2?h.toFixed(1):Math.round(h)}h old — may be stale">${h<2?h.toFixed(1):Math.round(h)}h</span>`; }
/* (#14) inline sparkline */
function sparkline(vals,w,h,col){ vals=(vals||[]).filter(v=>v!=null&&!isNaN(v));
  if(vals.length<2) return ''; w=w||74; h=h||20;
  const mn=Math.min(...vals), mx=Math.max(...vals), rng=(mx-mn)||1, up=vals[vals.length-1]>=vals[0];
  const c=col||(up?'var(--buy)':'var(--sell)');
  const pts=vals.map((v,i)=>`${(i/(vals.length-1)*w).toFixed(1)},${(h-2-((v-mn)/rng)*(h-4)).toFixed(1)}`).join(' ');
  return `<svg class="spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">`+
    `<polyline points="${pts}" fill="none" stroke="${c}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/></svg>`; }

/* (#16) one floating tooltip for everything tagged data-tip */
let tipEl=null;
function tipNode(){ if(!tipEl){tipEl=document.createElement('div');tipEl.className='tipbox';document.body.appendChild(tipEl);} return tipEl; }
document.addEventListener('mouseover',e=>{ const t=e.target.closest&&e.target.closest('[data-tip]'); if(!t) return;
  const el=tipNode(); el.textContent=t.getAttribute('data-tip'); el.style.display='block';
  const r=t.getBoundingClientRect(), b=el.getBoundingClientRect();
  let L=r.left+r.width/2-b.width/2, T=r.top-b.height-8; if(T<6) T=r.bottom+8;
  el.style.left=Math.max(6,Math.min(L,innerWidth-b.width-6))+'px'; el.style.top=T+'px'; });
document.addEventListener('mouseout',e=>{ if(tipEl && e.target.closest&&e.target.closest('[data-tip]')) tipEl.style.display='none'; });

/* (#1) flicker-free DOM morph + (#8) value flash, installed as an
   innerHTML interceptor on #view / #ltable so every render updates in
   place instead of nuking the DOM. Falls back to native on any error. */
const NATIVE_HTML=Object.getOwnPropertyDescriptor(Element.prototype,'innerHTML');
function defineMorph(el){ if(!el||el.__morph) return; el.__morph=true;
  Object.defineProperty(el,'innerHTML',{ configurable:true,
    get(){ return NATIVE_HTML.get.call(this); },
    set(html){ try{ const tpl=document.createElement('template'); tpl.innerHTML=html;
        morphChildren(this,tpl.content); recordUpdate(); }
      catch(err){ NATIVE_HTML.set.call(this,html); } } }); }
function numish(s){ if(s==null) return null;
  const m=String(s).replace(/[, ]/g,'').match(/-?\d+(\.\d+)?/); return m?parseFloat(m[0]):null; }
function morphChildren(from,to){
  const fc=from.childNodes, tc=to.childNodes;
  for(let i=0;i<tc.length;i++){ const n=tc[i], o=fc[i];
    if(!o){ from.appendChild(document.importNode(n,true)); continue; } morphNode(o,n); }
  while(fc.length>tc.length) from.removeChild(fc[fc.length-1]); }
function morphNode(o,n){
  if(o.nodeType!==n.nodeType||o.nodeName!==n.nodeName){ o.replaceWith(document.importNode(n,true)); return; }
  if(o.nodeType===3){ if(o.nodeValue!==n.nodeValue){
      const a=numish(o.nodeValue), b=numish(n.nodeValue); o.nodeValue=n.nodeValue;
      if(!RM && a!=null && b!=null && a!==b){ const host=o.parentElement&&o.parentElement.closest('.flashable'); if(host) flash(host,b>a); } }
    return; }
  if(o.nodeType!==1 || o.nodeName==='CANVAS') return;
  const oa=o.attributes, na=n.attributes;
  for(let j=oa.length-1;j>=0;j--){ if(!n.hasAttribute(oa[j].name)) o.removeAttribute(oa[j].name); }
  for(let j=0;j<na.length;j++){ if(o.getAttribute(na[j].name)!==na[j].value) o.setAttribute(na[j].name,na[j].value); }
  if(o.nodeName==='INPUT'||o.nodeName==='TEXTAREA'||o.nodeName==='SELECT') return; // don't disturb live form state
  morphChildren(o,n); }
const _flashT=new WeakMap();
function flash(el,up){ el.classList.remove('flash-up','flash-dn'); void el.offsetWidth;
  el.classList.add(up?'flash-up':'flash-dn');
  clearTimeout(_flashT.get(el)); _flashT.set(el,setTimeout(()=>el.classList.remove('flash-up','flash-dn'),720)); }

/* (#10) cross-fade the view on tab switches */
function fadeView(){ if(RM) return; const v=$('#view'); if(!v) return;
  v.classList.remove('swap'); void v.offsetWidth; v.classList.add('swap'); }

/* (#9) skeleton placeholder */
function skel(rows){ rows=rows||6; let s='<div class="skel-wrap">';
  for(let i=0;i<rows;i++) s+=`<div class="skel skel-bar" style="width:${88-(i%3)*9}%"></div>`;
  return s+'</div>'; }

/* (#12) "updated Xs ago" ticker */
function recordUpdate(){ window.__upd=Date.now(); }
function shortAgo(ms){ const s=(Date.now()-ms)/1000;
  if(s<2)return 'just now'; if(s<60)return Math.floor(s)+'s ago';
  if(s<3600)return Math.floor(s/60)+'m ago'; return Math.floor(s/3600)+'h ago'; }
setInterval(()=>{ const u=$('#upd'); if(u) u.textContent = window.__upd?shortAgo(window.__upd):'live'; },1000);

/* (#7) command palette — ⌘K to search items / sellers / tabs */
const CMD_TABS=[['arb','Arbitrage'],['live','Live listings'],['removed','Sales feed'],
  ['hist','Index'],['gold','Gold price'],['merchant','Merchant'],
  ['world','Live World'],['props','Property Map']];
let cmdkList=[], cmdkSel=0;
function gotoTab(t){ document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('on',x.dataset.t===t));
  TAB=t; fadeView(); render(); schedule(); }
function openCmdk(){ const o=$('#cmdk'); o.classList.add('on'); const i=$('#cmdkInput'); i.value=''; cmdkRender(''); i.focus(); }
function closeCmdk(){ $('#cmdk').classList.remove('on'); }
function cmdkRender(q){ q=(q||'').trim().toLowerCase(); const res=[];
  CMD_TABS.forEach(([t,l])=>{ if(!q||l.toLowerCase().includes(q)) res.push({t:'Tab',l,act:()=>{closeCmdk();gotoTab(t);}}); });
  if(q) (state.items||[]).forEach(it=>{ const name=lbl(it);
    if(it.toLowerCase().includes(q)||name.toLowerCase().includes(q))
      res.push({t:'Item',l:name,sub:it,act:()=>{ closeCmdk(); state.search=name; state.catOff.clear(); gotoTab('arb'); }}); });
  cmdkList=res.slice(0,40); cmdkSel=0;
  $('#cmdkList').innerHTML = cmdkList.length
    ? cmdkList.map((r,i)=>`<div class="cmdk-row ${i===0?'sel':''}" data-i="${i}">
        <span class="cmdk-t">${r.t}</span><span class="cmdk-l">${esc(r.l)}</span>
        ${r.sub?`<span class="cmdk-s">${esc(r.sub)}</span>`:''}</div>`).join('')
    : `<div class="cmdk-empty">No matches for “${esc(q)}”</div>`;
  document.querySelectorAll('.cmdk-row').forEach(el=>el.onclick=()=>runCmdk(+el.dataset.i)); }
function cmdkMove(d){ if(!cmdkList.length) return; cmdkSel=(cmdkSel+d+cmdkList.length)%cmdkList.length;
  document.querySelectorAll('.cmdk-row').forEach((el,i)=>el.classList.toggle('sel',i===cmdkSel));
  const sel=document.querySelector('.cmdk-row.sel'); if(sel) sel.scrollIntoView({block:'nearest'}); }
function runCmdk(i){ const r=cmdkList[i]; if(r) r.act(); }
addEventListener('keydown',e=>{
  if((e.metaKey||e.ctrlKey)&&e.key.toLowerCase()==='k'){ e.preventDefault();
    $('#cmdk').classList.contains('on')?closeCmdk():openCmdk(); return; }
  if(!$('#cmdk').classList.contains('on')) return;
  if(e.key==='Escape') closeCmdk();
  else if(e.key==='ArrowDown'){ e.preventDefault(); cmdkMove(1); }
  else if(e.key==='ArrowUp'){ e.preventDefault(); cmdkMove(-1); }
  else if(e.key==='Enter'){ e.preventDefault(); runCmdk(cmdkSel); } });

async function loadStatus(){
  // The live light + "updated Xs ago" indicator (.live-upd) is all we normally show.
  // kintara.gg often times out on deep listing pages; those are transient and the
  // poller keeps serving last-good data, so we stay silent and let it self-heal.
  // Only surface a quiet note once it's *persistently* failing (several misses AND no
  // successful update in a few minutes) — i.e. the data is actually going stale.
  let s; try{ s=await (await fetch("/api/status")).json(); }catch(e){ return; }
  const staleMs = s.last_success ? (Date.now()-new Date(s.last_success)) : Infinity;
  const persistent = (s.fail_streak||0) >= 3 && staleMs > 4*60*1000;
  $("#status").innerHTML = persistent
    ? `<span class="dot err"></span>reconnecting to kintara… <span class="mut">data ${s.last_success?ago(s.last_success)+' old':'unavailable'}</span>`
    : "";
}
async function loadItems(){
  const d=await (await fetch("/api/items")).json();
  state.items=d.items; state.cats=d.categories||[]; state.labels=d.labels||{};
  if(state.goldItem==null) state.goldItem=d.gold_item;
}

/* ---------------- arbitrage (primary) ---------------- */
/* pull the last ~24h gold series once for the header sparkline (#14) */
async function ensureGoldSpark(){ if(state.goldSpark!==undefined) return; state.goldSpark=null;
  try{ const d=await (await fetch("/api/gold-history?range=1D")).json();
    const arr=(d.series||[]).map(p=>p.gold_usd).filter(v=>v!=null);
    if(arr.length>1){ state.goldSpark=arr; if(TAB==="arb") loadArb(); }
  }catch(e){/* leave null */} }
/* the 3-way mode toggle shared by the arbitrage + mispricing renders */
function modeSeg(){ const m=state.dir;
  return `<span class="seg">
    <button id="d1" class="${m==='gold_to_kins'?'on':''}">gold → KINS</button>
    <button id="d2" class="${m==='kins_to_gold'?'on':''}">KINS → gold</button>
    <button id="d3" class="${m==='mispricing'?'on':''}">Collectables</button>
  </span>`; }
function wireModeSeg(){
  const a=$("#d1"),b=$("#d2"),c=$("#d3");
  if(a)a.onclick=()=>{state.dir="gold_to_kins";state.mpOpen=null;loadArb();};
  if(b)b.onclick=()=>{state.dir="kins_to_gold";state.mpOpen=null;loadArb();};
  if(c)c.onclick=()=>{state.dir="mispricing";loadArb();};
}
async function loadArb(){
  if(state.dir==="mispricing") return loadMispricing();
  if(TAB==="arb" && !$("#view").querySelector(".controls")) $("#view").innerHTML=skel(8);
  await loadItems(); ensureGoldSpark();
  const p=new URLSearchParams({direction:state.dir, min_qty:state.minQty||0});
  if(state.goldItem) p.set("gold_item", state.goldItem);
  const d=await (await fetch("/api/arbitrage?"+p)).json();
  state.rate=d.gold_rate;
  state.rowMap={}; (d.rows||[]).forEach(r=>state.rowMap[r.item_type]=r);
  const dirA=state.dir==="gold_to_kins";
  const KP=d.kins_price;
  // market value of 1 gold in $KINS — the benchmark the "kins/gold" column is read against
  const marketKpg = (d.gold_rate && KP) ? d.gold_rate/KP : null;
  const rateTxt = d.gold_rate!=null
    ? `<span class="big flashable">1 gold = ${fmtU(d.gold_rate)}</span>
       ${marketKpg!=null?`<span class="big" style="color:var(--gold2)">= ${fmtKins(marketKpg)} $KINS</span>`:''}
       ${state.goldSpark?`<span data-tip="gold price (USD), last ~24h">${sparkline(state.goldSpark,96,24)}</span>`:''}
       <small>${d.gold_rate_listings>0
           ? `avg of the ${d.gold_rate_listings} cheapest per-gold ask${d.gold_rate_listings===1?'':'s'}`
           : `kintaragold.xyz spot (no live gold listings)`}</small>`
    : `<span class="big warn">no gold rate</span>
       <small>pick the gold item above; need a live token (KINS) listing of it</small>`;

  /* category chips */
  const cats=(d.rows||[]).reduce((s,r)=>s.add(r.category),new Set());
  state.cats.forEach(c=>cats.add(c));
  const chips=[...cats].sort().map(c=>{
    const on=!state.catOff.has(c);
    return `<button class="chip ${on?'on':''}" data-cat="${c}">${c}</button>`;}).join("");

  /* client-side filtering: category + search define the "viewing set" (what the
     Refresh button targets); profitable / sold-today narrow the display only. */
  const q=state.search.trim().toLowerCase();
  let rows=(d.rows||[]).filter(r=>!state.catOff.has(r.category));
  if(q) rows=rows.filter(r=>r.item_type.toLowerCase().includes(q)||lbl(r.item_type).toLowerCase().includes(q));
  state.viewSet=rows.map(r=>r.item_type);
  if(state.profitableOnly) rows=rows.filter(r=>r.profit!=null && r.profit>0);
  if(state.soldOnly) rows=rows.filter(r=>r.sold_day!=null && r.sold_day>0);
  const shown=rows.length, total=(d.rows||[]).length;

  const head=`
    <div class="controls">
      ${modeSeg()}
      <input id="asearch" placeholder="filter item…" style="min-width:150px" value="${state.search}">
      <label class="meta"><input type="checkbox" id="profonly" ${state.profitableOnly?'checked':''}> profitable only</label>
      <label class="meta"><input type="checkbox" id="soldonly" ${state.soldOnly?'checked':''}> sold today only</label>
      <span class="meta">min stack</span><input type="number" id="minqty" placeholder="any" min="0" step="100" value="${state.minQty||''}" style="width:84px" title="Only count stackable-good listings (qty>1 items like wood/coal) with at least this many items; single items unaffected">

      <button class="go" id="arbref" data-tip="Refresh shown rows">↻</button>
    </div>
    <div class="controls" style="margin-top:-6px">
      <span class="meta">categories</span>${chips}
      <button class="chip" id="catall">all</button><button class="chip" id="catnone">none</button>
    </div>
    <div class="rate">${rateTxt}</div>`;

  const soldHdr = (dirA ? "sold KINS" : "sold gold")+(d.ref_day?` · ${d.ref_day.slice(5)}`:"");
  const intN = v => v==null ? "—" : Number(v).toLocaleString();

  const table = (d.gold_rate==null) ?
    `<div class="empty">Set the gold rate first: choose which item is tradeable gold above. Need at least one live KINS (token) listing of it.</div>`
    : (!rows.length ?
      `<div class="empty">Nothing matches the current filters. Clear the search, enable more categories, or turn off "profitable only".</div>`
      : `<table><thead><tr>
          <th>item</th>
          <th class="num" style="color:var(--buy)">items / $</th>
          <th class="num" style="color:var(--gold)">per gold</th>
          <th class="num" data-tip="how many $KINS it costs (at the cheapest USD/KINS listing) to buy enough of this item to sell for 1 gold — i.e. the KINS price of a manufactured gold. Green = cheaper than buying gold outright (${marketKpg!=null?fmtKins(marketKpg)+' $KINS':'—'}).">kins / gold</th><th class="num">margin</th>
          <th class="num">profit</th><th class="num">${soldHdr}</th>
          </tr></thead><tbody>`+
        rows.map(r=>{
          const win=r.profit!=null && r.profit>0;
          const incomplete=!r.complete;
          const sfx=r.basis==="gold"?' <span class="mut">/gold</span>':' <span class="mut">/ea</span>';
          const profCell = r.profit_disp==null?'—':fmtU(r.profit_disp)+sfx;
          // $KINS to acquire 1 gold's worth of this item at the cheapest USD/KINS price
          const kpg = (r.per_gold!=null && r.kins_unit!=null && KP) ? r.per_gold*r.kins_unit/KP : null;
          const kpgCell = kpg==null?'—':fmtKins(kpg)+' <span class="mut">$K</span>';
          const kpgCls = (kpg!=null && marketKpg!=null) ? (kpg<marketKpg?'pos':'neg') : '';
          return `<tr class="${win?'win':''}${incomplete?' part':''}" data-item="${r.item_type}">
            <td title="${r.item_type}">${lbl(r.item_type)}</td>
            <td class="num usd isd flashable" style="font-weight:700">${r.per_usd==null?'—':(r.per_usd>=1?fmtN(r.per_usd):fmtU(r.usd_each))}</td>
            <td class="num gold flashable" style="font-weight:700">${fmtN(r.per_gold)}</td>
            <td class="num flashable ${kpgCls}" data-tip="${kpg!=null&&marketKpg!=null?(kpg<marketKpg?'cheaper than the '+fmtKins(marketKpg)+' $KINS it costs to buy 1 gold outright — assembling gold from this item is favourable':'dearer than buying 1 gold outright ('+fmtKins(marketKpg)+' $KINS)'):''}">${kpgCell}</td>
            <td class="num flashable ${r.margin>0?'pos':r.margin<0?'neg':''}">${pct(r.margin)}</td>
            <td class="num flashable ${win?'pos':r.profit_disp<0?'neg':''}">${profCell}</td>
            <td class="num mut soldc" title="${r.sold_date||''}">${intN(r.sold_day)}</td></tr>`;
        }).join("")+`</tbody></table>
        <div class="hint">Showing ${shown} of ${total} items. <b class="usd">items / $</b> = how many
          you get per dollar at the cheapest price (for items over $1 each it shows the per-item
          price instead, e.g. $3.60); <b class="gold">per gold</b> = how many one gold buys.
          <b>kins / gold</b> = how many <b>$KINS</b> it costs to buy enough of this item (at its cheapest
          USD/KINS listing) to assemble <b>1 gold's worth</b> — i.e. the KINS price of a "manufactured"
          gold. <b style="color:var(--buy)">Green</b> = cheaper than buying 1 gold outright
          (${marketKpg!=null?fmtKins(marketKpg)+' $KINS':'—'}), so turning this item into gold is favourable;
          red = dearer. <b>sold ${dirA?'KINS':'gold'}</b> = units sold so far on the current game day
          (${d.ref_day||'—'}) in the currency you'd sell into — 0 means no sales yet today, —
          means not loaded yet. The current day is partial, so it climbs through the day; hit
          <b>↻ Refresh shown</b> to pull live sales for just the items you're viewing. The shown
          rows also auto-refresh every ~7s. <b>Hover the items / $ cell</b> to see the exact
          cheapest listing it's pricing from (stack size, price &amp; seller) to find it in game.
          Because spending gold has a <b>1-gold minimum</b>, profit is <b>per gold
          spent</b> for items under 1 gold each and <b>per item</b> otherwise (/gold vs /ea tag).
          <b>min stack</b> ignores stackable-good listings (wood, coal, … — anything sold in qty &gt;1)
          smaller than the number you enter — e.g. set 1000 to skip "100 coal" dust listings; single
          items like mounts are never affected. Reserved listings are excluded;
          rows with one side missing (—) can't be arbitraged yet.</div>`);

  $("#view").innerHTML=head+table;

  wireModeSeg();
  $("#profonly").onchange=e=>{state.profitableOnly=e.target.checked;loadArb();};
  $("#soldonly").onchange=e=>{state.soldOnly=e.target.checked;loadArb();};
  $("#minqty").onchange=e=>{state.minQty=parseInt(e.target.value)||0;loadArb();};
  const sb=$("#asearch"); sb.oninput=e=>{state.search=e.target.value;loadArb();
    const s=$("#asearch"); s.focus(); s.setSelectionRange(s.value.length,s.value.length);};
  document.querySelectorAll(".chip[data-cat]").forEach(b=>b.onclick=()=>{
    const c=b.dataset.cat, turningOn=state.catOff.has(c);
    if(turningOn){
      state.catOff.delete(c);
      // newly visible category → refresh its items so sold-today is current
      const its=(d.rows||[]).filter(r=>r.category===c).map(r=>r.item_type);
      loadArb();                                  // show immediately from cache
      refreshVisible(its).then(()=>{ if(!state.catOff.has(c)) loadArb(); });
    } else { state.catOff.add(c); loadArb(); }
  });
  $("#catall").onclick=()=>{state.catOff.clear();loadArb();};
  $("#catnone").onclick=()=>{[...cats].forEach(c=>state.catOff.add(c));loadArb();};
  $("#arbref").onclick=async()=>{
    const b=$("#arbref"); if(!b) return;
    b.disabled=true; const t=b.textContent; b.textContent="Refreshing…";
    await refreshVisible(state.viewSet);
    b.disabled=false; b.textContent=t;
    loadArb();
  };

  // hover card on the items/$ cells + pause auto-refresh while hovering the table
  document.querySelectorAll("#view td.isd").forEach(td=>{
    const r=state.rowMap[td.parentElement.dataset.item];
    if(!r) return;
    td.onmousemove=e=>showDeal(r,e.clientX,e.clientY);
    td.onmouseleave=hideDeal;
  });
  // hover the "sold today" cell → 7-day sales + avg price mini-chart (in the currency you'd sell into)
  const soldCur=dirA?"token":"gold";
  document.querySelectorAll("#view td.soldc").forEach(td=>{
    const it=td.parentElement.dataset.item; if(!it) return;
    td.onmouseenter=e=>showSold(it,soldCur,e.clientX,e.clientY);
    td.onmousemove=e=>positionSold(e.clientX,e.clientY);
    td.onmouseleave=hideSold;
  });
  const tbl=$("#view table");
  if(tbl){ tbl.onmouseenter=()=>{arbHover=true;};
           tbl.onmouseleave=()=>{arbHover=false; hideDeal(); hideSold();}; }
}

/* ---------------- Collectables (3rd arbitrage mode) ---------------- */
/* Each CMP item's cheapest live listing vs a gold-anchored, recency- & volume-
   weighted fair value carried to today's rates (see compute_mispricing). Display
   currency (KINS/Gold/Both) is just a unit choice — the comparison is one number. */
async function loadMispricing(){
  if(TAB==="arb" && !$("#view").querySelector(".controls")) $("#view").innerHTML=skel(8);
  hideDeal(); hideSold();                         // clear any stale arb hover card
  await loadItems();
  const p=new URLSearchParams();
  if(state.goldItem) p.set("gold_item", state.goldItem);
  const d=await (await fetch("/api/mispricing?"+(p.toString()))).json();
  state.rate=d.gold_rate;
  const RATE=d.gold_rate, KP=d.kins_price;
  const cur=state.mpCur;

  // category chips — independent of the arbitrage tab; default to CMP, hide the rest
  const cats=(d.rows||[]).reduce((s,r)=>s.add(r.category),new Set());
  if(!state.mpCatInit){ const INV=new Set(["cosmetic","mount","pet"]);
    [...cats].forEach(c=>{ if(!INV.has(c)) state.mpCatOff.add(c); }); state.mpCatInit=true; }
  const chips=[...cats].sort().map(c=>{const on=!state.mpCatOff.has(c);
    return `<button class="chip ${on?'on':''}" data-cat="${c}">${c}</button>`;}).join("");

  const q=state.search.trim().toLowerCase();
  let rows=(d.rows||[]).filter(r=>!state.mpCatOff.has(r.category));
  if(q) rows=rows.filter(r=>r.item_type.toLowerCase().includes(q)||lbl(r.item_type).toLowerCase().includes(q));
  state.viewSet=rows.map(r=>r.item_type);
  if(state.profitableOnly) rows=rows.filter(r=>r.spread_gold>0);
  rows.sort((a,b)=>(b.margin??-1e9)-(a.margin??-1e9));   // largest price error first
  state.mpRowMap={}; rows.forEach(r=>state.mpRowMap[r.item_type]={r,cat:r.category});
  const shown=rows.length, total=(d.rows||[]).length;

  // gold value -> display currency; "both" stacks the secondary denomination
  const inK=g=> (RATE&&KP)? g*RATE/KP : null;     // gold -> KINS
  const one=(g,c)=> g==null?"—":(c==="gold"?fmtG(g):fmtKins(inK(g))+' <span class="mut">$K</span>');
  const cell=g=>{ if(cur!=="both") return one(g,cur);
    return `${one(g,"kins")}<div class="mp-sec">${one(g,"gold")}</div>`; };
  const CONF={high:["●","var(--buy)","fresh sale + solid volume — trust the fair value"],
              med:["●","var(--gold2)","a few days old or light volume — treat as a guide"],
              low:["●","var(--sell)","stale or very thin volume — fair value is unreliable"]};

  const rateTxt = (RATE!=null||KP!=null)
    ? `<span class="big">${RATE!=null?`1 gold = ${fmtU(RATE)}`:''}${RATE!=null&&KP!=null?' · ':''}${KP!=null?`1 $KINS = ${fmtU(KP)}`:''}</span>
       <small>cheapest listing vs a gold-anchored, recency- &amp; volume-weighted fair value (carried to today) — sorted by largest gap</small>`
    : `<span class="big warn">no rate</span><small>need a live gold/KINS price to convert</small>`;

  const head=`
    <div class="controls">
      ${modeSeg()}
      <span class="seg" data-tip="which currency to price in">
        <button id="mpk" class="${cur==='kins'?'on':''}">KINS</button>
        <button id="mpg" class="${cur==='gold'?'on':''}">Gold</button>
        <button id="mpb" class="${cur==='both'?'on':''}">Both</button>
      </span>
      <input id="asearch" placeholder="filter item…" style="min-width:150px" value="${state.search}">
      <label class="meta"><input type="checkbox" id="profonly" ${state.profitableOnly?'checked':''}> profitable only</label>
      <button class="go" id="arbref" data-tip="Refresh shown rows">↻</button>
    </div>
    <div class="controls" style="margin-top:-6px">
      <span class="meta">categories</span>${chips}
      <button class="chip" id="catall">all</button><button class="chip" id="catnone">none</button>
    </div>
    <div class="rate">${rateTxt}</div>`;

  const priceHdr = cur==="gold"?"price (gold)":cur==="kins"?"price ($KINS)":"price";
  const table = !rows.length
    ? `<div class="empty">Nothing matches the current filters. Clear the search, enable more categories, or turn off "profitable only". (An item appears once it has a live listing and a recent recorded sale.)</div>`
    : `<table><thead><tr>
        <th>item</th>
        <th class="num">${priceHdr}</th>
        <th class="num">fair value</th>
        <th class="num">spread</th><th class="num">margin</th>
        <th class="num" style="color:var(--buy)">profit</th>
        <th class="num" data-tip="total units sold across the recent trading days used for fair value — hover a row's value for the day-by-day split">volume</th>
        <th class="num"></th>
        </tr></thead><tbody>`+
      rows.map(r=>{
        const win=r.spread_gold>0, open=state.mpOpen===r.item_type;
        const [dot,col,tip]=CONF[r.conf]||CONF.med;
        const tag = cur==="both"?` <span class="mp-tag">via ${r.buy_ccy==='gold'?'gold':'$K'}</span>`:"";
        const days=r.trade_days||[];
        const breakdown=days.map(x=>x.date+': '+x.sales+'u').join(' · ');
        const vol=`<b data-tip="${breakdown}">${(r.vol_window||0).toLocaleString()}</b><span class="mut" data-tip="${breakdown}"> ⌄</span>`;
        const main=`<tr class="${win?'win':''}" data-item="${r.item_type}">
            <td title="${r.item_type}"><span class="mp-conf" style="color:${col}" data-tip="${r.conf} confidence — ${tip} (last sale ${r.last_age}d ago, ${r.vol_window} sold across ${days.length} recent day${days.length===1?'':'s'})">${dot}</span> ${lbl(r.item_type)}${tag}</td>
            <td class="num" style="font-weight:700">${cell(r.buy_gold)}</td>
            <td class="num" style="color:var(--gold2)">${cell(r.fair_gold)}</td>
            <td class="num ${r.spread_gold>0?'pos':r.spread_gold<0?'neg':''}">${cell(r.spread_gold)}</td>
            <td class="num ${r.margin>0?'pos':r.margin<0?'neg':''}">${pct(r.margin)}</td>
            <td class="num ${win?'pos':'neg'}" style="font-weight:700">${cell(r.spread_gold)}</td>
            <td class="num">${vol}</td>
            <td class="num"><button class="mp-exp ${open?'on':''}" data-item="${r.item_type}" data-tip="show item info + index chart">${open?'▾':'▸'}</button></td></tr>`;
        return main + (open?`<tr class="mp-exprow"><td colspan="8"><div id="mpexp" class="mp-exp-box"></div></td></tr>`:"");
      }).join("")+`</tbody></table>
      <div class="hint">Showing ${shown} of ${total} CMP items (cosmetics/mounts/pets; farmable commodities like wolf/dragon/whale are excluded — use the <b>gold → KINS</b> mode for those).
        <b>price</b> = cheapest buyable live listing (whichever currency is cheaper — the <b>via</b> tag in Both shows which).
        <b>fair value</b> (gold-tinted) = recent sales re-priced into <b>gold</b> (the empirically stickiest unit), weighted by units sold and recency
        (~7-day half-life), then carried to today's rate — so a stale low-liquidity sale is re-valued at current prices instead of
        compared in raw old USD. Only the <b>most recent ~50%</b> of each item's sales are used, so launch-day outlier prices don't skew it.
        <b>spread/margin/profit</b> = how far the listing sits below fair value. The colored dot is
        <b>confidence</b> (green = fresh + liquid, red = stale or thin). <b>volume</b> = total units across those recent trading days
        (hover for the per-day split). Use <b>KINS / Gold / Both</b> to change the display unit. This is an upper bound — fair value is historical and you'd undercut to sell.</div>`;

  $("#view").innerHTML=head+table;
  // morph reuses DOM nodes across renders, so cells can keep stale mouse handlers
  // from the arbitrage tab (e.g. a leftover deal-card hover bound to the old row).
  // Clear them and hide any open card so Collectables shows nothing stale.
  document.querySelectorAll('#view td, #view table').forEach(el=>{ el.onmousemove=el.onmouseenter=el.onmouseleave=null; });
  hideDeal(); hideSold();
  wireModeSeg();
  $("#mpk").onclick=()=>{state.mpCur="kins";state.mpOpen=null;loadMispricing();};
  $("#mpg").onclick=()=>{state.mpCur="gold";state.mpOpen=null;loadMispricing();};
  $("#mpb").onclick=()=>{state.mpCur="both";state.mpOpen=null;loadMispricing();};
  $("#profonly").onchange=e=>{state.profitableOnly=e.target.checked;loadMispricing();};
  const sb=$("#asearch"); if(sb) sb.oninput=e=>{state.search=e.target.value;loadMispricing();
    const s=$("#asearch"); s.focus(); s.setSelectionRange(s.value.length,s.value.length);};
  document.querySelectorAll(".chip[data-cat]").forEach(bt=>bt.onclick=()=>{
    const c=bt.dataset.cat; if(state.mpCatOff.has(c)) state.mpCatOff.delete(c); else state.mpCatOff.add(c);
    loadMispricing(); });
  $("#catall").onclick=()=>{state.mpCatOff.clear();loadMispricing();};
  $("#catnone").onclick=()=>{[...cats].forEach(c=>state.mpCatOff.add(c));loadMispricing();};
  $("#arbref").onclick=async()=>{const b=$("#arbref"); if(!b)return;
    b.disabled=true; await refreshVisible(state.viewSet); b.disabled=false; loadMispricing(); };
  document.querySelectorAll(".mp-exp").forEach(b=>b.onclick=ev=>{ev.stopPropagation();
    const it=b.dataset.item; state.mpOpen=(state.mpOpen===it)?null:it; loadMispricing(); });
  if(state.mpOpen && state.mpRowMap[state.mpOpen]) mpRenderExpand(state.mpOpen, state.mpRowMap[state.mpOpen].cat);
}
/* the per-item dropdown body — reuses the Index tab's chart / meta / listings */
function mpExpInner(it,cat){
  const cur=state.histCur||"token";
  const curLabel=(cur==="gold"||cur==="goldstd")?"Gold":cur==="kins"?"$KINS":"USD";
  const isMat=cat==='material', showMeta=['cosmetic','mount','pet'].includes(cat);
  return `<div class="gw-expinner">
    <div style="min-width:0">
      <div class="gw-cseg">
        <button data-c="token" class="${cur==='token'?'on':''}">USD ($KINS)</button>
        <button data-c="kins" class="${cur==='kins'?'on':''}">vs $KINS</button>
        <button data-c="gold" class="${cur==='gold'?'on':''}">Gold</button>
        <button data-c="goldstd" class="${cur==='goldstd'?'on':''}" data-tip="every sale valued in gold — gold sales as-is, USD/KINS sales converted to the gold they'd have bought that day">Gold Standard</button></div>
      <div id="kinsbanner"></div>
      <canvas id="schart" width="1100" height="230" style="width:100%;height:230px"></canvas>
      <div id="stip" style="height:16px;color:#8aa0bd;font:12px 'Fredoka';margin-top:4px"></div>
    </div>
    <div class="gw-statpanel"><h4>Cumulative · ${curLabel}</h4><div id="sstats"></div></div>
    ${isMat?`<div id="liqwrap" class="liq-wrap">${skel(3)}</div>`:''}
  </div>${showMeta?`<div id="gwmeta" class="gw-meta">${skel(2)}</div>`:''}<div id="gwlist" class="gw-listings">${skel(2)}</div>`;
}
function mpRenderExpand(it,cat){
  const exp=$("#mpexp"); if(!exp) return;
  exp.innerHTML=mpExpInner(it,cat);
  const cur=state.histCur||"token";
  exp.querySelectorAll("[data-c]").forEach(b=>b.onclick=ev=>{ev.stopPropagation();state.histCur=b.dataset.c;mpRenderExpand(it,cat);});
  drawSalesChart(it,cur);
  if(cat==='material') loadLiquidity(it);
  if(['cosmetic','mount','pet'].includes(cat)) loadItemMeta(it);
  loadItemListings(it);
}

/* ---------------- shared listing filters ---------------- */
function listingControls(){
  const catOpts=`<option value="all">all categories</option>`+
    state.cats.map(c=>`<option ${fstate.category===c?'selected':''}>${c}</option>`).join("");
  return `<div class="controls">
    <input id="q" placeholder="search item / seller…" style="min-width:200px" value="${fstate.q}">
    <select id="currency">
      <option value="all">all currencies</option>
      <option value="gold" ${fstate.currency==='gold'?'selected':''}>gold</option>
      <option value="token" ${fstate.currency==='token'?'selected':''}>token (KINS/USD)</option>
    </select>
    <select id="category">${catOpts}</select>
    <select id="sort">
      <option value="latest">newest</option>
      <option value="cheapest" ${fstate.sort==='cheapest'?'selected':''}>cheapest</option>
      <option value="expensive" ${fstate.sort==='expensive'?'selected':''}>most expensive</option>
    </select>
    <button class="go" id="lref">Refresh</button>
    <label class="meta"><input type="checkbox" id="auto" ${fstate.auto?'checked':''}> auto</label>
  </div><div id="ltable"></div>`;
}
const fstate={q:"",currency:"all",category:"all",sort:"latest",auto:true};
function bindListingControls(reload){
  defineMorph($("#ltable"));   // flicker-free table refreshes
  $("#q").addEventListener("input",e=>{fstate.q=e.target.value;reload();});
  $("#currency").addEventListener("change",e=>{fstate.currency=e.target.value;reload();});
  $("#category").addEventListener("change",e=>{fstate.category=e.target.value;reload();});
  $("#sort").addEventListener("change",e=>{fstate.sort=e.target.value;reload();});
  $("#lref").onclick=reload;
  $("#auto").onchange=e=>{fstate.auto=e.target.checked;schedule();};
}
function lqs(){ return `q=${encodeURIComponent(fstate.q)}&currency=${fstate.currency}`+
  `&category=${fstate.category}&sort=${fstate.sort}`; }
function fmtPrice(r){ return r.currency==="token"
  ? `<span class="usd">$${r.price_usd}</span>` : `<span class="gold">${r.price_gold}g</span>`; }

async function loadLive(){
  if(!$("#ltable")){ await loadItems();
    $("#view").innerHTML=listingControls(); bindListingControls(loadLive); }
  const rows=await (await fetch("/api/current?"+lqs())).json();
  $("#ltable").innerHTML = !rows.length
    ? `<div class="empty">No live listings match.</div>`
    : `<table><thead><tr><th>item</th><th>seller</th><th class="num">qty</th>
        <th class="num">price</th><th class="num">per item</th><th class="num">listed</th>
        </tr></thead><tbody>`+rows.map(r=>`<tr>
        <td title="${r.item_type}">${lbl(r.item_type)}</td>
        <td class="mut">${r.seller_name||""}</td>
        <td class="num">${abbr(r.quantity||0)}</td><td class="num">${fmtPrice(r)}</td>
        <td class="num mut">${r.per_unit==null?"":(r.currency==="token"?fmtU(r.per_unit):fmtG(r.per_unit))}</td>
        <td class="num mut">${relAbs(r.created_at)}${freshness(r.created_at)}</td></tr>`).join("")+`</tbody></table>`;
}
/* price of an actual sale, in its own currency */
function salePrice(r){ if(r.price==null) return "—";
  return r.currency==='gold' ? (+Number(r.price).toPrecision(3))+'g'
    : '$'+Number(r.price).toFixed(r.price>=1?2:4); }
async function loadRemoved(){   // "Sales feed" tab — now ACTUAL completed sales
  if(!$("#ltable")){ await loadItems();
    $("#view").innerHTML=listingControls(); bindListingControls(loadRemoved); }
  const rows=await (await fetch("/api/sales-feed?"+lqs())).json();
  $("#ltable").innerHTML = !rows.length
    ? `<div class="empty">No sales recorded yet. This feed logs <b>actual completed sales</b> — detected from kintara's own sale counter, so cancel-and-relist undercutting is ignored — and fills in going forward (the old removed-listings data was dropped).</div>`
    : `<table><thead><tr><th>item</th><th class="num">units sold</th><th class="num">price</th>
        <th>paid in</th><th class="num">when</th></tr></thead><tbody>`+
      rows.map(r=>`<tr><td title="${r.item_type}">${lbl(r.item_type)}</td>
        <td class="num"><b>${(r.units||0).toLocaleString()}</b></td>
        <td class="num ${r.currency==='gold'?'gold':'usd'}">${salePrice(r)}</td>
        <td class="mut">${r.currency==='gold'?'gold':'$KINS / USD'}</td>
        <td class="num mut">${relAbs(r.ts)}</td></tr>`).join("")+`</tbody></table>`;
}

/* ---------------- sales index (game-styled, real Kintara sales) ----------- */
let SUMMARY=null;
const CAT_ORDER=[["all","All Items"],["material","Materials"],["currency","Gold"],
  ["cosmetic","Cosmetics"],["mount","Mounts"],["pet","Pets"],["furniture","Furniture"],
  ["tool","Tools"],["weapon","Weapons"],["potion","Potions"],["food","Food"],
  ["key","Keys"],["building","Building"],["other","Other"]];
const CAT_EMO={material:"🪨",currency:"🪙",cosmetic:"👕",mount:"🐴",pet:"🐾",
  furniture:"🪑",tool:"⛏️",weapon:"⚔️",potion:"🧪",food:"🍖",key:"🔑",building:"🏠",other:"📦"};
function fmtK(v){ return v>=1000?(+(v/1000).toFixed(2))+"K":(""+v); }
async function loadHist(){
  await loadItems();
  try{ SUMMARY=await (await fetch("/api/sales-summary?window="+(state.histWindow||1))).json(); }
  catch(e){ SUMMARY={ok:false}; }
  renderHist();
}
function iconImg(r){
  const fb=(CAT_EMO[r.category]||"📦").replace(/'/g,"");
  return `<span class="gw-ico"><img src="/icon/${r.item_type}" alt="" loading="lazy" `+
    `onerror="this.parentElement.textContent='${fb}'" `+
    `style="width:26px;height:26px;object-fit:contain"></span>`;
}
function renderHist(){
  const d=SUMMARY;
  if(!d||d.ok===false){ $("#view").innerHTML=`<div class="gw-empty">Couldn't load sales data.</div>`; return; }
  const items=d.items||[];
  const counts={}; items.forEach(r=>counts[r.category]=(counts[r.category]||0)+1);
  const cat=state.histCat||"all";
  const cats=CAT_ORDER.filter(([k])=>k==="all"||counts[k]);
  const side=cats.map(([k,name])=>`<button class="gw-cat ${k===cat?'on':''}" data-cat="${k}">${name}`+
    `${k!=="all"?`<span class="c">${counts[k]||0}</span>`:`<span class="c">${items.length}</span>`}</button>`).join("");
  const catName=(CAT_ORDER.find(([k])=>k===cat)||[,"All Items"])[1];
  let rows=cat==="all"?items.slice():items.filter(r=>r.category===cat);
  rows.sort((a,b)=> state.histSort==="least" ? a.sales-b.sales : b.sales-a.sales);
  const usd=v=> v==null?"—":"$"+Number(v).toFixed(v>=1?2:v>=.01?4:5);
  const goldv=v=> (v==null||v===0)?"—":(+Number(v).toFixed(2))+"g";
  const kinsv=v=> v==null?"—":(v>=1?Math.round(v).toLocaleString():(+v.toFixed(2)))+" $KINS";
  const body=rows.map(r=>{
    const open=r.item_type===state.histOpen;
    return `<div class="gw-row ${open?'open':''}" data-item="${r.item_type}">
      <div class="gw-item"><span class="gw-chev">▸</span>${iconImg(r)}
        <span class="gw-name" title="${r.item_type}">${r.label}</span></div>
      <div class="gw-num r">${r.sales?fmtK(r.sales):"—"}</div>
      <div class="gw-num mut r">${goldv(r.avg_gold)}</div>
      <div class="gw-num r">${usd(r.avg_usd)}</div>
      <div class="gw-num gw-kins r">${kinsv(r.kins)}</div>
    </div>`+(open?`<div class="gw-exp" id="gwexp"></div>`:"");
  }).join("");
  const kp=d.kins_price?("$"+(+d.kins_price.toFixed(4))):"—";
  $("#view").innerHTML=`<div class="gw">
    <div class="gw-side"><div class="gw-eyebrow">— Index —</div>${side}</div>
    <div class="gw-main">
      <div class="gw-corner l"></div><div class="gw-corner r"></div>
      <div class="gw-title">${cat==="all"?"Marketplace":catName}</div>
      <div class="gw-sub">Completed marketplace sales. Average gold, USD ($KINS listings) and live $KINS prices.</div>
      <div class="gw-pills">
        <button class="gw-pill ${state.histWindow==1?'on':''}" data-win="1">Today</button>
        <button class="gw-pill ${state.histWindow==7?'on':''}" data-win="7">Last 7 days</button>
        <button class="gw-pill ${state.histWindow==30?'on':''}" data-win="30">Last 30 days</button>
        <span style="width:14px"></span>
        <button class="gw-pill ${state.histSort!=="least"?'on':''}" data-sort="most">Most sales</button>
        <button class="gw-pill ${state.histSort==="least"?'on':''}" data-sort="least">Least sales</button>
        <span style="width:14px"></span>
        <button class="gw-pill" id="histRefresh" data-tip="Refresh now">↻</button>
      </div>
      <div class="gw-note">${state.histWindow==1?"Most recent trading day":("Last "+state.histWindow+" days")} · through ${d.ref_day||"—"} · live $KINS price ${kp} per token</div>
      <div class="gw-head"><span>Item</span><span class="r">Sales</span><span class="r">Avg Gold</span><span class="r">Avg USD</span><span class="r">$KINS</span></div>
      <div id="gwrows">${body||`<div class="gw-empty">No items in this category.</div>`}</div>
    </div></div>`;
  document.querySelectorAll(".gw-cat").forEach(b=>b.onclick=()=>{state.histCat=b.dataset.cat;state.histOpen=null;renderHist();});
  document.querySelectorAll(".gw-pill[data-sort]").forEach(b=>b.onclick=()=>{state.histSort=b.dataset.sort;renderHist();});
  const hr=$("#histRefresh"); if(hr) hr.onclick=()=>loadHist();
  document.querySelectorAll(".gw-pill[data-win]").forEach(b=>b.onclick=()=>{state.histWindow=+b.dataset.win;state.histOpen=null;loadHist();});
  document.querySelectorAll("#gwrows .gw-row").forEach(row=>row.onclick=()=>{
    const it=row.dataset.item; state.histOpen=(state.histOpen===it)?null:it; renderHist();
    if(state.histOpen) document.querySelector(`.gw-row[data-item="${it}"]`)?.scrollIntoView({block:"nearest",behavior:"smooth"});
  });
  if(state.histOpen) openHist(state.histOpen);
}
async function openHist(it){
  const exp=$("#gwexp"); if(!exp) return;
  const cur=state.histCur||"token";
  const curLabel=(cur==="gold"||cur==="goldstd")?"Gold":cur==="kins"?"$KINS":"USD";
  const row=((SUMMARY&&SUMMARY.items)||[]).find(r=>r.item_type===it);
  const isMat=row&&row.category==='material';
  const showMeta=row&&['cosmetic','mount','pet'].includes(row.category);
  exp.innerHTML=`<div class="gw-expinner">
    <div style="min-width:0">
      <div class="gw-cseg">
        <button data-c="token" class="${cur==='token'?'on':''}">USD ($KINS)</button>
        <button data-c="kins" class="${cur==='kins'?'on':''}">vs $KINS</button>
        <button data-c="gold" class="${cur==='gold'?'on':''}">Gold</button>
        <button data-c="goldstd" class="${cur==='goldstd'?'on':''}" data-tip="every sale valued in gold — gold sales as-is, USD/KINS sales converted to the gold they'd have bought that day">Gold Standard</button></div>
      <div id="kinsbanner"></div>
      <canvas id="schart" width="1100" height="230" style="width:100%;height:230px"></canvas>
      <div id="stip" style="height:16px;color:#8aa0bd;font:12px 'Fredoka';margin-top:4px"></div>
    </div>
    <div class="gw-statpanel"><h4>Cumulative · ${curLabel}</h4><div id="sstats"></div></div>
    ${isMat?`<div id="liqwrap" class="liq-wrap">${skel(3)}</div>`:''}
  </div>${showMeta?`<div id="gwmeta" class="gw-meta">${skel(2)}</div>`:''}<div id="gwlist" class="gw-listings">${skel(2)}</div><div id="gwsales" class="gw-listings">${skel(2)}</div>`;
  exp.querySelectorAll("[data-c]").forEach(b=>b.onclick=ev=>{ev.stopPropagation();state.histCur=b.dataset.c;openHist(it);});
  exp.onclick=ev=>ev.stopPropagation();   // clicks inside don't collapse the row
  drawSalesChart(it, cur);
  if(isMat) loadLiquidity(it);
  if(showMeta) loadItemMeta(it);
  loadItemListings(it);
  loadRecentSales(it);
}
/* per-item recent ACTUAL sales (from /api/sales-feed), for the Index item expand */
async function loadRecentSales(it){
  const w=$("#gwsales"); if(!w) return;
  let rows; try{ rows=await (await fetch("/api/sales-feed?limit=12&item_type="+encodeURIComponent(it))).json(); }
  catch(e){ w.innerHTML=`<div class="gw-empty">Couldn't load recent sales.</div>`; return; }
  if(!rows||!rows.length){ w.innerHTML=`<div class="gw-meta-h">Recent sales <span style="color:#7f93ad;font-weight:400">— actual completed sales</span></div>
      <div class="gw-empty">No sales observed yet — this logs real sales as they happen (cancellations ignored).</div>`; return; }
  const when=ms=>{ const s=(Date.now()-ms)/1000;
    if(s<60)return Math.floor(s)+'s ago'; if(s<3600)return Math.floor(s/60)+'m ago';
    if(s<86400)return Math.floor(s/3600)+'h ago'; return Math.floor(s/86400)+'d ago'; };
  const body=rows.map(r=>`<div class="ll-row">
      <span class="ll-p">${r.units.toLocaleString()}×</span>
      <span class="${r.currency==='gold'?'gold':'usd'}" style="font-weight:700">${salePrice(r)}</span>
      <span class="ll-x">${r.currency==='gold'?'gold':'$KINS/USD'}</span>
      <span class="ll-sel" data-tip="${new Date(r.ts).toLocaleString()}">${when(r.ts)}</span></div>`).join("");
  w.innerHTML=`<div class="gw-meta-h">Recent sales <span style="color:#7f93ad;font-weight:400">— actual completed sales (newest first), cancellations excluded</span></div>${body}`;
}
const listCache={};
async function loadItemListings(it){
  let d=listCache[it];
  if(!d){ try{ d=await (await fetch("/api/item-listings?item_type="+encodeURIComponent(it))).json(); listCache[it]=d; }
    catch(e){ const w=$("#gwlist"); if(w)w.innerHTML=`<div class="gw-empty">Couldn't load listings.</div>`; return; } }
  const w=$("#gwlist"); if(w) w.innerHTML=itemListingsHTML(d);
}
function itemListingsHTML(d){
  if(!d||d.ok===false) return `<div class="gw-empty">No live listings.</div>`;
  const rate=d.gold_rate, kp=d.kins_price;
  const sig=v=> v>=1?(+v.toFixed(2)):(+v.toPrecision(3));   // keep small per-unit prices readable
  const sel=s=>s?`<span class="ll-sel">${esc(s)}</span>`:'';
  // actual listing price (total), with stack + per-unit only when it's a stack
  const goldRows=(d.gold||[]).map(r=>{
    const tot=r.price!=null?r.price:r.per_unit*(r.qty||1);
    const stack=r.qty>1?`<span class="ll-q">×${r.qty.toLocaleString()}</span><span class="ll-x">${sig(r.per_unit)}g/ea</span>`:'';
    const usd=rate?`<span class="ll-x">≈$${(tot*rate).toFixed(2)}</span>`:'';
    return `<div class="ll-row"><span class="ll-p">${sig(tot)}g</span>${stack}${usd}${sel(r.seller)}</div>`;}).join("");
  const tokRows=(d.token||[]).map(r=>{
    const tot=r.price!=null?r.price:r.per_unit*(r.qty||1);
    const stack=r.qty>1?`<span class="ll-q">×${r.qty.toLocaleString()}</span><span class="ll-x">$${sig(r.per_unit)}/ea</span>`:'';
    const kins=kp?`<span class="ll-x">${(tot/kp>=1?Math.round(tot/kp).toLocaleString():(+(tot/kp).toFixed(2)))} $KINS</span>`:'';
    return `<div class="ll-row"><span class="ll-p">$${sig(tot)}</span>${stack}${kins}${sel(r.seller)}</div>`;}).join("");
  const col=(title,rows,empty)=>`<div class="ll-col"><div class="ll-h">${title}</div>${rows||`<div class="ll-none">${empty}</div>`}</div>`;
  return `<div class="gw-meta-h">Cheapest live listings <span style="color:#7f93ad;font-weight:400">— actual price, sorted by cheapest per-unit, up to 5 each</span></div>
    <div class="ll-cols">
      ${col("In gold",goldRows,"none listed in gold")}
      ${col("In $KINS",tokRows,"none listed in $KINS")}
    </div>`;
}
const metaCache={};
async function loadItemMeta(it){
  let m=metaCache[it];
  if(!m){ try{ m=await (await fetch("/api/item-meta?item_type="+encodeURIComponent(it))).json(); metaCache[it]=m; }
    catch(e){ const w=$("#gwmeta"); if(w)w.innerHTML=`<div class="gw-empty">Couldn't load item info.</div>`; return; } }
  const w=$("#gwmeta"); if(w) w.innerHTML=itemMetaHTML(m);
}
function itemMetaHTML(m){
  if(!m||m.ok===false) return `<div class="gw-empty">No item info.</div>`;
  const stClr=/dried/.test(m.supply_status)?"#34d39a":/flooding/.test(m.supply_status)?"#f06a6a":"#e8b54a";
  const win = m.first_sale?`${m.first_sale.slice(5)} → ${m.last_sale.slice(5)} · ${m.days_traded} day${m.days_traded===1?'':'s'} traded`:"—";
  const isShop=/shop/i.test(m.source||"");
  const cg=m.cheapest_gold!=null?(+m.cheapest_gold.toFixed(2))+"g":null;
  const cu=m.cheapest_usd!=null?"$"+m.cheapest_usd.toFixed(2):null;
  const floor=[cg,cu].filter(Boolean).join(" · ")||"—";
  const shopRow = m.shop_gold!=null
    ? ['Shop price', `<b style="color:var(--gold2)">${m.shop_gold}g</b> <span style="color:#7f93ad">(confirmed in-shop)</span>`]
    : (isShop ? ['Shop price', `<span style="color:#7f93ad">not public — exact gold price rotates server-side; floor below is a proxy</span>`] : null);
  const rows=[
    ['Source', esc(m.source)],
    m.cadence?['Availability', `<b style="color:var(--gold2)">${esc(m.cadence)}</b>`]:null,
    ['How to get it', esc(m.cost)],
    m.speed_pct!=null?['Ride speed', `<b style="color:var(--gold2)">+${m.speed_pct}%</b> move speed`]:null,
    shopRow,
    ['Cheapest ever traded', `${floor} <span style="color:#7f93ad">(market floor)</span>`],
    ['First seen → last sale', win],
    ['Units sold (all-time · last 7d)', `${(m.units_total||0).toLocaleString()} · ${(m.units_recent7||0).toLocaleString()}`],
    ['Supply status', `<b style="color:${stClr}">${esc(m.supply_status)}</b>`],
  ].filter(Boolean);
  return `<div class="gw-meta-h">${esc(m.label)} — item index</div>
    ${m.note?`<div class="gw-meta-note">${esc(m.note)}</div>`:''}
    <div class="gw-meta-grid">${rows.map(([k,v])=>`<span class="k">${k}</span><span class="v">${v}</span>`).join("")}</div>`;
}
/* buy-side liquidity depth: units available by USD price per 1000 (materials) */
async function loadLiquidity(it){
  if(!$("#liqwrap")) return;
  let d; try{ d=await (await fetch("/api/liquidity?item_type="+encodeURIComponent(it))).json(); }
  catch(e){ const w=$("#liqwrap"); if(w) w.innerHTML=`<div class="gw-empty">Couldn't load liquidity.</div>`; return; }
  const w=$("#liqwrap"); if(w) w.innerHTML=liquidityHTML(d);
}
function liquidityHTML(d){
  if(!d||d.ok===false) return `<div class="gw-empty">Couldn't load liquidity.</div>`;
  if(!d.markers||!d.markers.length) return `<div class="gw-empty">No buyable $-priced listings to chart right now.</div>`;
  const m=d.markers, W=720,H=210, PL=52,PR=14,PT=14,PB=30, plotW=W-PL-PR, plotH=H-PT-PB;
  const maxU=Math.max(1,...m.map(x=>x.cum_units)), n=m.length, bw=plotW/n;
  const Y=u=>PT+plotH-(u/maxU)*plotH;
  const fU=v=> v>=1e6?(+(v/1e6).toFixed(1))+'M':v>=1e3?(+(v/1e3).toFixed(1))+'k':(''+Math.round(v));
  let grid=''; [0,.25,.5,.75,1].forEach(f=>{const u=maxU*f,yy=Y(u);
    grid+=`<line x1="${PL}" y1="${yy.toFixed(1)}" x2="${W-PR}" y2="${yy.toFixed(1)}" stroke="rgba(120,140,165,.12)"/>`+
      `<text x="${PL-7}" y="${(yy+3).toFixed(1)}" text-anchor="end" fill="#6f86a6" font-size="10" font-family="monospace">${fU(u)}</text>`;});
  let bars=''; m.forEach((x,i)=>{const bx=PL+i*bw, by=Y(x.cum_units), bh=PT+plotH-by;
    bars+=`<rect x="${(bx+1).toFixed(1)}" y="${by.toFixed(1)}" width="${Math.max(0,bw-2).toFixed(1)}" height="${Math.max(0,bh).toFixed(1)}" rx="2" fill="url(#liqg)" data-tip="≤ $${x.price.toFixed(2)} / 1000  —  ${x.cum_units.toLocaleString()} units available${x.tranche_units?`  ·  +${x.tranche_units.toLocaleString()} in this 10¢ band (${x.listings} listing${x.listings===1?'':'s'})`:''}"></rect>`;});
  const every=n>10?2:1; let xlab='';
  m.forEach((x,i)=>{ if(i%every===0){ const bx=PL+i*bw+bw/2;
    xlab+=`<text x="${bx.toFixed(1)}" y="${H-10}" text-anchor="middle" fill="#6f86a6" font-size="10" font-family="monospace">$${x.price.toFixed(2)}</text>`;}});
  let bestLine=''; if(d.best_per_1000!=null){ let bi=m.findIndex(x=>x.price>=d.best_per_1000-1e-9); if(bi<0)bi=n-1;
    const bx=PL+bi*bw+bw/2;
    bestLine=`<line x1="${bx.toFixed(1)}" y1="${PT}" x2="${bx.toFixed(1)}" y2="${PT+plotH}" stroke="var(--gold)" stroke-width="1.5" stroke-dasharray="4 3"/>`+
      `<text x="${bx.toFixed(1)}" y="${PT-3}" text-anchor="middle" fill="var(--gold2)" font-size="9.5" font-family="monospace">cheapest</text>`;}
  const best=d.best_per_1000!=null?`$${d.best_per_1000.toFixed(2)}`:'—';
  return `<div class="liq-h"><span class="liq-t">Buy-side liquidity</span>
      <span class="liq-sub"><b>${d.total_units.toLocaleString()}</b> units available · cheapest <b>${best}</b>/1000 · ${d.total_listings} listings</span></div>
    <svg class="liq-svg" viewBox="0 0 ${W} ${H}"><defs>
      <linearGradient id="liqg" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0" stop-color="rgba(52,211,154,.85)"/><stop offset="1" stop-color="rgba(52,211,154,.22)"/></linearGradient></defs>
      ${grid}${bars}${bestLine}${xlab}</svg>
    <div class="liq-foot">Cumulative units you could buy at or below each price per 1000 (KINS listings${d.gold_units?` + gold listings converted at $${d.gold_rate?d.gold_rate.toFixed(2):'—'}/gold`:''}). Each bar = total depth up to that 10¢ marker. Hover a bar for the band detail.${d.excluded_gold_units?` ${d.excluded_gold_units.toLocaleString()} gold-priced units excluded (no gold rate).`:''}</div>`;
}
async function drawSalesChart(it,cur){
  const c=$("#schart"); if(!c) return;
  const x=c.getContext("2d"),W=c.width,H=c.height;
  const note=t=>{x.clearRect(0,0,W,H);x.fillStyle="#6f86a6";x.font="13px 'Fredoka'";x.textAlign="left";x.fillText(t,60,H/2);};
  note("loading…");
  let d; try{ d=await (await fetch(`/api/sales-history?item_type=${encodeURIComponent(it)}&currency=${cur}`)).json(); }
  catch(e){ note("Couldn't reach Kintara."); return; }
  if(!d||d.ok===false){ note(d&&d.error?d.error:"No data."); return; }
  const s=d.samples||[];
  const goldOn=cur==="gold"||cur==="goldstd", kinsOn=cur==="kins";
  const kfmt=v=> v>=1000?(+(v/1000).toFixed(1))+"k":(+v.toFixed(v>=10?0:1))+"";
  const gfmt=v=> v>=1000?(+(v/1000).toFixed(1))+"kg":(+Number(v).toPrecision(3))+"g";
  const unit=v=> v==null?"—":(goldOn?gfmt(v):kinsOn?kfmt(v)+" $KINS":"$"+Number(v).toFixed(v>=1?2:4));
  // "vs $KINS" headline: is the item beating the token, or just riding it?
  const kb=$("#kinsbanner");
  if(kb){ if(kinsOn&&d.vs_token){ const v=d.vs_token, beat=v.rel_pct>=0;
      kb.innerHTML=`<div class="kins-vs ${beat?'up':'down'}">In $KINS terms the item is <b>${beat?'+':''}${v.rel_pct.toFixed(0)}%</b> ${v.from.slice(5)}→${v.to.slice(5)} `+
        `<span>(item ${v.item_usd_pct>=0?'+':''}${v.item_usd_pct.toFixed(0)}% USD vs $KINS ${v.kins_usd_pct>=0?'+':''}${v.kins_usd_pct.toFixed(0)}% USD)</span> — `+
        `${beat?'real alpha: outpacing the token':'lagging the token: gains are mostly token beta'}.</div>`; }
    else kb.innerHTML=""; }
  const total=s.reduce((a,b)=>a+(b.sales||0),0);
  const traded=s.filter(p=>p.sales>0);
  const prices=traded.map(p=>p.avgUnitPrice||0);
  const peak=prices.length?Math.max(...prices):null, low=prices.length?Math.min(...prices):null;
  const peakDay=(s.find(p=>p.avgUnitPrice===peak)||{}).date;
  const last=traded.length?traded[traded.length-1].avgUnitPrice:null;
  const sst=$("#sstats");
  if(sst) sst.innerHTML=
    `<div class="gw-stat"><span>Total sales</span><b>${total.toLocaleString()}</b></div>`+
    `<div class="gw-stat"><span>Avg price</span><b>${unit(d.avg30d)}</b></div>`+
    `<div class="gw-stat"><span>Latest</span><b>${unit(last)}</b></div>`+
    `<div class="gw-stat"><span>Peak</span><b>${unit(peak)}${peakDay?` <span style="color:#6f86a6">${peakDay.slice(5)}</span>`:''}</b></div>`+
    `<div class="gw-stat"><span>Low</span><b>${unit(low)}</b></div>`+
    `<div class="gw-stat"><span>Days traded</span><b>${traded.length}</b></div>`;
  // line chart over time, only days with sales (real prices)
  const pts=traded.map(p=>({t:Date.parse(p.date+"T00:00:00Z"),v:p.avgUnitPrice||0,sales:p.sales,date:p.date}));
  if(pts.length<2){ note("Not enough sales to chart."); return; }
  x.clearRect(0,0,W,H);
  const PL=60,PR=12,top=12,bot=H-22;
  const t0=pts[0].t, t1=pts[pts.length-1].t, tspan=(t1-t0)||1;
  const X=t=>PL+(t-t0)/tspan*(W-PL-PR);
  let lo=Math.min(...pts.map(p=>p.v)), hi=Math.max(...pts.map(p=>p.v));
  const hh=hi*1.08, S=(hh-(lo=0))||1; const Y=v=>bot-(v-lo)/S*(bot-top);
  const yt=niceTicks(0,hh,4);
  x.font="11px 'Fredoka'";x.textAlign="right";x.textBaseline="middle";
  yt.forEach(tv=>{const yy=Y(tv); if(yy<top-2||yy>bot+2)return;
    x.strokeStyle="rgba(120,140,165,.10)";x.setLineDash([4,4]);x.beginPath();x.moveTo(PL,yy);x.lineTo(W-PR,yy);x.stroke();x.setLineDash([]);
    x.fillStyle="#6f86a6";x.fillText(unit(tv),PL-8,yy);});
  x.textAlign="center";x.textBaseline="alphabetic";x.fillStyle="#6f86a6";
  const tk=Math.min(6,pts.length);
  for(let j=0;j<tk;j++){const p=pts[Math.round(j*(pts.length-1)/(tk-1))];
    x.fillText(new Date(p.t).toLocaleDateString('en-US',{month:'short',day:'numeric',timeZone:'UTC'}),X(p.t),H-7);}
  const g=x.createLinearGradient(0,top,0,bot);
  g.addColorStop(0,"rgba(232,181,74,.30)"); g.addColorStop(1,"rgba(232,181,74,0)");
  x.beginPath(); pts.forEach((p,i)=>{const xx=X(p.t),yy=Y(p.v);i?x.lineTo(xx,yy):x.moveTo(xx,yy);});
  x.lineTo(X(t1),bot);x.lineTo(X(t0),bot);x.closePath();x.fillStyle=g;x.fill();
  const paint=hovT=>{
    x.strokeStyle="#e8b54a";x.lineWidth=2;x.lineJoin="round";x.beginPath();
    pts.forEach((p,i)=>{const xx=X(p.t),yy=Y(p.v);i?x.lineTo(xx,yy):x.moveTo(xx,yy);});x.stroke();
    if(hovT!=null){const p=pts[hovT],xx=X(p.t),yy=Y(p.v);
      x.strokeStyle="rgba(180,200,220,.35)";x.setLineDash([5,4]);x.beginPath();x.moveTo(xx,top);x.lineTo(xx,bot);x.stroke();x.setLineDash([]);
      x.fillStyle="#e8b54a";x.beginPath();x.arc(xx,yy,4,0,7);x.fill();x.strokeStyle="#0c0f13";x.lineWidth=2;x.beginPath();x.arc(xx,yy,4,0,7);x.stroke();}
  };
  paint(null);
  c.onmousemove=ev=>{
    const r=c.getBoundingClientRect(),mx=(ev.clientX-r.left)*(W/r.width);
    let bi=0,bd=1e18; pts.forEach((p,i)=>{const dd=Math.abs(X(p.t)-mx);if(dd<bd){bd=dd;bi=i;}});
    x.clearRect(0,0,W,H);
    // redraw grid+area first
    yt.forEach(tv=>{const yy=Y(tv);if(yy<top-2||yy>bot+2)return;x.strokeStyle="rgba(120,140,165,.10)";x.setLineDash([4,4]);x.beginPath();x.moveTo(PL,yy);x.lineTo(W-PR,yy);x.stroke();x.setLineDash([]);x.fillStyle="#6f86a6";x.textAlign="right";x.textBaseline="middle";x.fillText(unit(tv),PL-8,yy);});
    x.textAlign="center";x.textBaseline="alphabetic";x.fillStyle="#6f86a6";
    for(let j=0;j<tk;j++){const p=pts[Math.round(j*(pts.length-1)/(tk-1))];x.fillText(new Date(p.t).toLocaleDateString('en-US',{month:'short',day:'numeric',timeZone:'UTC'}),X(p.t),H-7);}
    x.beginPath();pts.forEach((p,i)=>{const xx=X(p.t),yy=Y(p.v);i?x.lineTo(xx,yy):x.moveTo(xx,yy);});x.lineTo(X(t1),bot);x.lineTo(X(t0),bot);x.closePath();x.fillStyle=g;x.fill();
    paint(bi);
    const p=pts[bi], card=gcardNode();
    card.innerHTML=`<div class="gd">${new Date(p.t).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric',timeZone:'UTC'})}</div>`+
      `<div class="gv" style="color:#e8b54a">avg ${unit(p.v)}</div>`+
      `<div class="gd">${p.sales.toLocaleString()} sale${p.sales===1?'':'s'}</div>`;
    card.style.display="block";
    const b=card.getBoundingClientRect();let L=ev.clientX+16,T=ev.clientY+16;
    if(L+b.width>innerWidth-8)L=ev.clientX-b.width-16; if(T+b.height>innerHeight-8)T=innerHeight-b.height-8;
    card.style.left=L+"px";card.style.top=T+"px";
  };
  c.onmouseleave=()=>{x.clearRect(0,0,W,H);
    yt.forEach(tv=>{const yy=Y(tv);if(yy<top-2||yy>bot+2)return;x.strokeStyle="rgba(120,140,165,.10)";x.setLineDash([4,4]);x.beginPath();x.moveTo(PL,yy);x.lineTo(W-PR,yy);x.stroke();x.setLineDash([]);x.fillStyle="#6f86a6";x.textAlign="right";x.textBaseline="middle";x.fillText(unit(tv),PL-8,yy);});
    x.textAlign="center";x.textBaseline="alphabetic";x.fillStyle="#6f86a6";for(let j=0;j<tk;j++){const p=pts[Math.round(j*(pts.length-1)/(tk-1))];x.fillText(new Date(p.t).toLocaleDateString('en-US',{month:'short',day:'numeric',timeZone:'UTC'}),X(p.t),H-7);}
    x.beginPath();pts.forEach((p,i)=>{const xx=X(p.t),yy=Y(p.v);i?x.lineTo(xx,yy):x.moveTo(xx,yy);});x.lineTo(X(t1),bot);x.lineTo(X(t0),bot);x.closePath();x.fillStyle=g;x.fill();
    paint(null); if(gcardEl)gcardEl.style.display="none"; };
}

/* ---------------- gold price (USD ⇆ KINS/gold), kintaragold-style ---------- */
let GOLD={};           // cache keyed by range
const GRANGES=["4H","1D","3D","7D","14D","ALL"];
let gcardEl=null;
function gcardNode(){ if(!gcardEl){gcardEl=document.createElement('div');
  gcardEl.className='goldcard'; document.body.appendChild(gcardEl);} return gcardEl; }
function niceTicks(lo,hi,n){
  const span=(hi-lo)||1, raw=span/n, mag=Math.pow(10,Math.floor(Math.log10(raw)));
  const norm=raw/mag, step=(norm<1.5?1:norm<3?2:norm<7?5:10)*mag, out=[];
  for(let v=Math.ceil(lo/step)*step; v<=hi+1e-9; v+=step) out.push(v);
  return out;
}
async function loadGold(){
  const seg=(id,cur,opts)=>`<span class="seg">`+opts.map(o=>
    `<button data-${id}="${o[0]}" class="${o[0]===cur?'on':''}">${o[1]}</button>`).join("")+`</span>`;
  $("#view").innerHTML=`<div class="card" style="padding:16px 18px">
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <span class="gtitle2" id="gtitle">KINTARA GOLD PRICE HISTORY</span>
      <span id="gpill"></span>
      <span style="flex:1 1 20px"></span>
      ${seg('gm', state.goldMode, [['gold_usd','Gold (USD)'],['kins_per_gold','KINS / gold']])}
      ${seg('gr', state.goldRange, GRANGES.map(r=>[r,r]))}
      <button class="go" id="gref">↻</button>
    </div>
    <div id="gsub" style="color:var(--mut);font:11px var(--mono);letter-spacing:.12em;margin:4px 0 10px">PRICED VIA MARKETPLACE · CURATED DAILY</div>
    <canvas id="gchart" width="1200" height="360" style="width:100%;height:360px"></canvas>
    <div class="hint">Gold (USD) is our own measured series (avg of the 3 cheapest per-gold asks,
      snapshotted ~every 3 min; kintaragold.xyz backfills only older gaps); KINS/USD is live from
      the KINS/SOL pool (GeckoTerminal converts SOL→USD). <b>KINS / gold</b> = gold_usd ÷ kins_usd —
      moves intraday. 4H/1D are 3-minute resolution. Hover for the value.</div></div>`;
  document.querySelectorAll("[data-gm]").forEach(b=>b.onclick=()=>{state.goldMode=b.dataset.gm;loadGold();});
  document.querySelectorAll("[data-gr]").forEach(b=>b.onclick=()=>{state.goldRange=b.dataset.gr;loadGold();});
  $("#gref").onclick=()=>{delete GOLD[state.goldRange];drawGold();};
  drawGold();
}
async function drawGold(){
  const c=$("#gchart"); if(!c) return;
  const x=c.getContext("2d"),W=c.width,H=c.height;
  const note=t=>{x.clearRect(0,0,W,H);x.fillStyle="#8893a2";x.font="14px monospace";x.textAlign="left";x.fillText(t,70,H/2);};
  const rng=state.goldRange;
  if(!GOLD[rng]){
    note("loading "+rng+"…");
    try{ GOLD[rng]=await (await fetch("/api/gold-history?range="+rng)).json(); }
    catch(e){ note("Couldn't load gold history."); return; }
    if(rng!==state.goldRange) return;
  }
  const d=GOLD[rng];
  $("#gpill").innerHTML="";
  if(!d || d.ok===false){ note(d&&d.error?d.error:"No data."); return; }
  const kpg = state.goldMode!=="gold_usd", key = kpg?"kins_per_gold":"gold_usd";
  const pts=(d.series||[]).filter(p=>p[key]!=null);
  $("#gtitle").textContent = kpg?"KINS PER GOLD":"KINTARA GOLD PRICE HISTORY";
  $("#gsub").textContent = kpg?"GOLD_USD ÷ KINS_USD · LIVE":"PRICED VIA MARKETPLACE · CURATED DAILY";
  if(pts.length<2){ note("Not enough data for "+rng+"."); return; }
  const fmtV=v=> kpg ? Math.round(v).toLocaleString()+" KINS" : "$"+Number(v).toFixed(v>=100?0:v>=1?3:5);
  const intraday=(rng==="4H"||rng==="1D"||rng==="3D");
  const fmtT=ms=>{const t=new Date(ms);return t.toLocaleString('en-US',intraday
      ?{hour:'numeric',minute:'2-digit'}:{month:'short',day:'numeric'});};
  const fmtFull=ms=>new Date(ms).toLocaleString('en-US',{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});
  const n=pts.length, ys=pts.map(p=>p[key]);
  const PL=64,PR=14,PT=16,PB=26, left=PL, right=W-PR, top=PT, bot=H-PB;
  const X=i=>left+i*(right-left)/(n-1);
  let lo=Math.min(...ys), hi=Math.max(...ys);
  const useLog = kpg && lo>0 && hi/lo>20;
  let toY, yticks;
  if(useLog){ const L=Math.log(lo*0.85),Hh=Math.log(hi*1.15),S=(Hh-L)||1;
    toY=v=>bot-(Math.log(v)-L)/S*(bot-top);
    yticks=[]; for(let e=Math.floor(Math.log10(lo));e<=Math.ceil(Math.log10(hi));e++){const b=Math.pow(10,e);[1,2,5].forEach(m=>{const v=b*m;if(v>=lo*0.85&&v<=hi*1.15)yticks.push(v);});}
  } else {
    if(!kpg) lo=0;                       // gold USD axis from $0 like kintaragold
    const hh=hi*1.06, S=(hh-lo)||1; toY=v=>bot-(v-lo)/S*(bot-top);
    yticks=niceTicks(lo,hh,5);
  }
  const last=pts[n-1][key], first=pts[0][key], chg=first?((last-first)/first*100):0;
  const up=chg>=0, line=up?"#34d39a":"#f06a6a";
  $("#gpill").innerHTML=`<span class="gpill" style="color:${line};border-color:${line}66">`+
    `${up?'▲':'▼'} ${up?'+':''}${chg.toFixed(2)}% · ${rng}</span>`;

  function paint(hov){
    x.clearRect(0,0,W,H);
    // gridlines + y labels
    x.font="11px ui-monospace,monospace"; x.textAlign="right"; x.textBaseline="middle";
    yticks.forEach(tv=>{ const yy=toY(tv); if(yy<top-2||yy>bot+2)return;
      x.strokeStyle="rgba(120,140,165,.10)"; x.setLineDash([4,4]);
      x.beginPath(); x.moveTo(left,yy); x.lineTo(right,yy); x.stroke(); x.setLineDash([]);
      x.fillStyle="#6a7a8f"; x.fillText(fmtV(tv), left-8, yy); });
    // x labels
    x.textAlign="center"; x.textBaseline="alphabetic"; x.fillStyle="#6a7a8f";
    const tk=Math.min(6,n);
    for(let j=0;j<tk;j++){ const i=Math.round(j*(n-1)/(tk-1)); x.fillText(fmtT(pts[i].t),X(i),H-8); }
    // area gradient
    const g=x.createLinearGradient(0,top,0,bot);
    g.addColorStop(0, up?"rgba(52,211,154,.28)":"rgba(240,106,106,.26)");
    g.addColorStop(1, "rgba(52,211,154,0)");
    x.beginPath(); pts.forEach((p,i)=>{const xx=X(i),yy=toY(p[key]); i?x.lineTo(xx,yy):x.moveTo(xx,yy);});
    x.lineTo(X(n-1),bot); x.lineTo(X(0),bot); x.closePath(); x.fillStyle=g; x.fill();
    // line
    x.strokeStyle=line; x.lineWidth=2; x.lineJoin="round"; x.beginPath();
    pts.forEach((p,i)=>{const xx=X(i),yy=toY(p[key]); i?x.lineTo(xx,yy):x.moveTo(xx,yy);}); x.stroke();
    // crosshair + dot
    if(hov!=null){ const p=pts[hov], xx=X(hov), yy=toY(p[key]);
      x.strokeStyle="rgba(180,200,220,.35)"; x.setLineDash([5,4]);
      x.beginPath(); x.moveTo(xx,top); x.lineTo(xx,bot); x.stroke(); x.setLineDash([]);
      x.fillStyle=line; x.beginPath(); x.arc(xx,yy,4,0,7); x.fill();
      x.strokeStyle="#0c0f13"; x.lineWidth=2; x.beginPath(); x.arc(xx,yy,4,0,7); x.stroke();
    }
  }
  paint(null);

  const idxAt=ev=>{ const r=c.getBoundingClientRect(), px=(ev.clientX-r.left)*(W/r.width);
    let i=Math.round((px-left)/((right-left)/(n-1))); return Math.max(0,Math.min(n-1,i)); };
  c.onmousemove=ev=>{
    const i=idxAt(ev), p=pts[i]; paint(i);
    const card=gcardNode();
    card.innerHTML=`<div class="gd">${fmtFull(p.t)}</div>`+
      `<div class="gv" style="color:${line}">${kpg?'KINS / gold':'Gold'} : ${fmtV(p[key])}</div>`+
      (kpg?`<div class="gd">gold $${(p.gold_usd||0).toFixed(3)} · kins $${(p.kins_usd||0).toFixed(6)}</div>`
          :`<div class="gd">kins $${(p.kins_usd||0).toFixed(6)}</div>`);
    card.style.display="block";
    const b=card.getBoundingClientRect(); let L=ev.clientX+16, T=ev.clientY+16;
    if(L+b.width>innerWidth-8) L=ev.clientX-b.width-16;
    if(T+b.height>innerHeight-8) T=innerHeight-b.height-8;
    card.style.left=L+"px"; card.style.top=T+"px";
  };
  c.onmouseleave=()=>{ paint(null); if(gcardEl) gcardEl.style.display="none"; };
}

/* ---------------- server status widget (compact icon → floating bubble) -- */
const SRV_ICON=`<svg class="ic" width="15" height="15" viewBox="0 0 24 24" fill="none"
  stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <rect x="3" y="3" width="18" height="7" rx="1.5"></rect><rect x="3" y="14" width="18" height="7" rx="1.5"></rect>
  <line x1="7" y1="6.5" x2="7.01" y2="6.5"></line><line x1="7" y1="17.5" x2="7.01" y2="17.5"></line></svg>`;
async function loadKinsPx(){
  const el=$("#kpx"); if(!el) return;
  let d; try{ d=await (await fetch("/api/kins-price")).json(); }catch(e){ return; }
  if(d.usd==null){ if(!el.innerHTML) el.innerHTML=`<span class="kpx-t">$KINS</span><span class="kpx-v">—</span>`; return; }
  const prev=el.__px;
  el.innerHTML=`<span class="kpx-t">$KINS</span><span class="kpx-v">$${(+d.usd).toFixed(4)}</span>`;
  if(prev!=null && d.usd!==prev && !RM){ const v=el.querySelector(".kpx-v");
    v.classList.add(d.usd>prev?'flash-up':'flash-dn'); setTimeout(()=>v.classList.remove('flash-up','flash-dn'),700); }
  el.__px=d.usd;
}
async function loadServers(){
  let d; try{ d=await (await fetch("/api/servers")).json(); }catch(e){ return; }
  if(d.ok && d.servers) state.servers=d.servers;   // id -> name, for the Live World labels
  const el=$("#srv"); if(!el) return;
  if(!d.ok){ el.innerHTML=`<button class="srv-btn">${SRV_ICON}<span>servers n/a</span></button>`; return; }
  const popDot=p=>`<span class="pdot pop-${["High","Medium","Low"].includes(p)?p:'na'}"></span>`;
  const cards=d.servers.map(s=>`<div class="srv-card">
      <div class="nm">${popDot(s.population)}${s.name||('Server '+s.id)}
        ${s.min_level?`<small style="color:var(--mut);font:11px var(--mono);margin-left:auto">Lv ${s.min_level}+</small>`:''}</div>
      <div class="meta2"><span>${s.full?'<span style="color:var(--sell)">Full</span>':'<span style="color:var(--buy)">Open</span>'} · ${s.population||'—'}</span>
        <span class="qbadge ${s.queue?'':'zero'}">${s.queue?('queue '+s.queue):'no queue'}</span></div>
    </div>`).join("");
  el.classList.toggle("open", state.srvOpen);
  el.innerHTML=`
    <button class="srv-btn" id="srvBtn" title="Server status">
      ${SRV_ICON}<span>${d.total}</span>
      <span class="q ${d.queue_total?'':'z'}">⏳ ${d.queue_total}</span>
    </button>
    <div class="srv-pop">
      <div class="srv-pop-h"><span class="ttl">Servers</span>
        <span class="sb ${d.open?'sb-open':'sb-mut'}">${d.open} open</span>
        <span class="sb ${d.full?'sb-full':'sb-mut'}">${d.full} full</span>
        <span class="sb ${d.queue_total?'sb-queue':'sb-mut'}">${d.queue_total} queued</span>
      </div>
      <div class="srv-grid">${cards}</div>
    </div>`;
  $("#srvBtn").onclick=(e)=>{ e.stopPropagation();
    state.srvOpen=!state.srvOpen; el.classList.toggle("open",state.srvOpen); };
}
/* close the bubble on any outside click */
document.addEventListener("click",e=>{
  const el=$("#srv");
  if(state.srvOpen && el && !el.contains(e.target)){ state.srvOpen=false; el.classList.remove("open"); }
});

/* ---------------- traveling merchant (tracker + cost calculator) --------- */
let MERCH=null;
async function loadMerchant(){
  if(!document.querySelector('.mwrap')) $("#view").innerHTML=skel(5);
  try{ MERCH=await (await fetch("/api/merchant")).json(); }
  catch(e){ $("#view").innerHTML=`<div class="empty warn">Couldn't load merchant data.</div>`; return; }
  renderMerchant();
}
function mIcon(it){ return `<img class="ico" src="/icon/${it}" alt="" loading="lazy" `+
  `onerror="this.style.visibility='hidden'">`; }
function renderMerchant(){
  const d=MERCH||{};
  if(d.ok===false){ $("#view").innerHTML=`<div class="empty warn">${d.error||'No merchant data.'}</div>`; return; }
  const s=d.state, c=d.calc||{};
  /* ---- left: progress tracker (per-resource % shown) ---- */
  let track=`<section class="mpanel"><div class="empty">Waiting for merchant data…</div></section>`;
  if(s){
    const gold=s.gold_trade;
    const ov=s.overall_pct||0;
    const rows=(s.resources||[]).map(r=>{
      const p=r.pct==null?0:r.pct, done=p>=100;
      return `<div class="mres-row">
        <div class="rh"><span class="nm">${mIcon(r.key)}${r.label}</span>
          <span class="nums"><span><b>${r.current==null?'—':r.current.toLocaleString()}</b> / ${r.goal==null?'—':r.goal.toLocaleString()}</span>
            <span class="rpct ${done?'done':''}">${r.pct==null?'—':p.toFixed(1)+'%'}</span></span></div>
        <div class="mtrack sm"><div class="mfill ${done?'done':''}" style="width:${Math.min(100,p)}%"></div></div>
      </div>`; }).join("");
    const goldStock=(gold && s.gold_stock!=null)
      ? `<div class="gold-stock"><span>Gold stock</span><strong>${(s.gold_stock||0).toLocaleString()} / ${(s.gold_stock_full||0).toLocaleString()}</strong></div>` : "";
    track=`<section class="mpanel">
      <div class="mhead"><div class="mtitle">Traveling Merchant</div>
        <span class="mode-badge ${gold?'':'don'}">${gold?'GOLD TRADE':'DONATION'}</span></div>
      <div class="msub">${gold?'Gold trading active':'Resource collection · donate to fill each goal'}</div>
      <div class="moverall"><div class="lab"><span>Overall progress</span><strong>${ov.toFixed(1)}%</strong></div>
        <div class="mtrack"><div class="mfill ${ov>=100?'done':''}" style="width:${Math.min(100,ov)}%"></div></div></div>
      <div class="mres">${rows}</div>${goldStock}
    </section>`;
  }
  /* ---- right: cost calculator (liquidity-aware: walks the order book) ---- */
  const rate=c.gold_rate, recipe=(c.recipe||[]);
  // walk a cheapest-first [unit_usd, qty] ladder to buy `need` units
  const walk=(ladder,need)=>{ let cost=0,got=0;
    for(let i=0;i<ladder.length && got<need-1e-9;i++){
      const take=Math.min(ladder[i][1], need-got); cost+=take*ladder[i][0]; got+=take; }
    return {cost,got,full:got>=need-1e-9}; };
  const craftAt=n=>{ let cost=0,full=true; const lines=[];
    for(const r of recipe){ const w=walk(r.ladder||[], r.qty*n);
      if(!w.full) full=false; cost+=w.cost; lines.push({r,need:r.qty*n,...w}); }
    return {cost,full,lines}; };
  // how many gold the listed liquidity can actually supply
  const maxMint=recipe.length? Math.min(...recipe.map(r=>Math.floor((r.available||0)/r.qty))):0;
  const want=Math.max(1,Math.floor(Number(state.mintQty)||1));
  const eff=Math.min(want,maxMint);
  const capped=eff<want, canCalc=eff>=1;
  const cur=canCalc?craftAt(eff):null, prev=canCalc?craftAt(eff-1):null;
  const craftN=cur?cur.cost:null;
  const avg=craftN!=null?craftN/eff:null;
  const marginal=(cur&&prev)?(cur.cost-prev.cost):null;   // cost of the eff-th gold (rises as liquidity dries up)
  const rateN=rate==null?null:rate*eff;
  const spreadN=(rateN!=null&&craftN!=null)?(rateN-craftN):null;
  const pos=spreadN!=null&&spreadN>=0;
  const marginPct=(spreadN!=null&&craftN)?(spreadN/craftN*100):null;
  const lineByKey={}; if(cur) cur.lines.forEach(l=>lineByKey[l.r.item_type]=l);
  const recRows=recipe.map(r=>{ const l=lineByKey[r.item_type];
    const need=r.qty*eff, unit=(l&&l.got>0)?l.cost/l.got:null;
    return `<div class="crow">
      <span class="ci">${mIcon(r.item_type)}<span>${r.label} <small>×${need.toLocaleString()}</small></span></span>
      <span class="r mut">${unit==null?'—':fmtU(unit)}</span>
      <span class="r">${l==null?'—':fmtU(l.cost)}</span>
    </div>`; }).join("");
  const calc=`<section class="mpanel calc">
    <div class="mhead"><div class="mtitle" style="font-size:20px">Cost Calculator</div></div>
    <div class="msub">When gold trading is active, buy the trade recipe on the open market, mint gold, then compare against the live gold price. Cost walks the order book — bigger mints pay up as the cheap listings run out.</div>
    <div class="mintctl"><span>Mint</span>
      <input type="number" min="1" id="mintQty" value="${want}"> <span>gold${capped?` · <span style="color:var(--sell)">capped to ${eff} by liquidity</span>`:''}</span></div>
    ${!canCalc?`<div class="msub" style="color:var(--sell)">Not enough market liquidity (or gold price) to price a mint right now.</div>`:`
    <div class="crow chead"><span>Recipe · for ${eff===1?'1 gold':eff.toLocaleString()+' gold'}</span><span class="r">avg unit</span><span class="r">cost</span></div>
    ${recRows}
    <div class="calc-tot"><span>Craft cost</span><span class="v">${fmtU(craftN)}</span></div>
    <div style="display:flex;justify-content:space-between;color:var(--mut);font:12px var(--mono);margin-top:7px">
      <span>avg ${fmtU(avg)}/gold</span><span>${eff>1?`${eff}th gold costs ${fmtU(marginal)}`:''}</span></div>
    <div class="spread-box ${pos?'pos':'neg'}">
      <span class="k">Gold value (sell ${eff})</span><span class="v">${rateN==null?'—':fmtU(rateN)}</span>
      <span class="k">Craft cost (buy)</span><span class="v">${craftN==null?'—':fmtU(craftN)}</span>
      <span class="k">Profit</span><span class="v ${pos?'pos':'neg'}">${spreadN==null?'—':(pos?'+':'')+fmtU(spreadN)}</span>
      <span class="k">Margin</span><span class="v ${pos?'pos':'neg'}">${marginPct==null?'—':(marginPct>=0?'+':'')+marginPct.toFixed(1)+'%'}</span>
    </div>`}
    <div class="msub" style="margin-top:12px;font-size:12px">Gold price = our live rate (avg of the 3 cheapest per-gold asks). Trade recipe = current game.js MERCHANT_TRADE_COST. Resource cost = actually buying that many units cheapest-first across all live listings (gold listings converted at the gold rate); listed liquidity supports ~${maxMint.toLocaleString()} gold.</div>
  </section>`;
  $("#view").innerHTML=`<div class="mwrap">${track}${calc}</div>`;
  const q=$("#mintQty");
  if(q) q.oninput=()=>{ state.mintQty=q.value; renderMerchant();
    const nq=$("#mintQty"); if(nq){ nq.focus(); nq.setSelectionRange(nq.value.length,nq.value.length);} };
}

/* ---------------- live world + property map ---------------- */
const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const badgeLabel=b=>String(b).replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
const SKINS=["#f1e8df","#e3c19e","#d4a574","#7e5f49","#5c4332"];
const hexC=(c,def)=>c==null?def:'#'+(c&0xffffff).toString(16).padStart(6,'0');
function avatarSvg(o,s){ o=o||{}; s=s||30;
  const skin=SKINS[o.skinTone]||SKINS[1], top=hexC(o.topC,'#3a6ea5'), pants=hexC(o.pantsC,'#2c3e57'),
    hat=hexC(o.hatC,'#8a5a2b'), shoe=hexC(o.shoeC,'#222'), hasHat=o.hat>0, aura=o.aura!=null;
  return `<svg width="${s}" height="${s}" viewBox="0 0 32 40" style="display:block">
    ${aura?`<ellipse cx="16" cy="21" rx="15" ry="18" fill="none" stroke="var(--gold)" stroke-width="1.5" opacity=".55"/>`:''}
    <rect x="10" y="29" width="5" height="8" rx="1" fill="${pants}"/><rect x="17" y="29" width="5" height="8" rx="1" fill="${pants}"/>
    <rect x="9" y="36" width="7" height="3" rx="1.5" fill="${shoe}"/><rect x="16" y="36" width="7" height="3" rx="1.5" fill="${shoe}"/>
    <rect x="8.5" y="17" width="15" height="14" rx="3" fill="${top}"/>
    <circle cx="16" cy="11.5" r="6.5" fill="${skin}"/>
    ${hasHat?`<path d="M8.5 11 a7.5 7.5 0 0 1 15 0 z" fill="${hat}"/><rect x="6" y="9.6" width="20" height="2.6" rx="1.3" fill="${hat}"/>`:''}
  </svg>`; }

let WORLD=null; const playerExtra={};
/* The Shores realm: top-down, axis-aligned square, rotated 90° clockwise from the
   raw (x,z) so it matches the in-game view. z is horizontal (right→left as z grows),
   x is vertical (bottom→top as x grows). Exact, not eyeballed. */
function shoresToMap(x,z){
  return {u:(19.5-(z||0))/39, v:((x||0)+19.5)/39};
}
/* Per-realm Live World maps: backdrop image (from render_maps.py) + an (x,z)->(u,v)
   transform onto it. Add a realm here once its PNG + route exist. Each transform is
   the inverse of how that realm's PNG was drawn, so player dots land exactly. */
/* generic transform for a centred square grid of N tiles (off = -(N-1)/2):
   tile (c,r) sits at world (x=c+off, z=r+off), so u=(x+(N-1)/2)/(N-1), v likewise. */
function centerMap(N){ const H=(N-1)/2, S=N-1; return (x,z)=>({u:((x||0)+H)/S, v:((z||0)+H)/S}); }
const REALM_MAPS={
  world:{img:'/maps/mainland.png', toMap:centerMap(62)},
  beach:{img:'/shores.png', toMap:shoresToMap},
  pond:{img:'/pond.png',   toMap:centerMap(40)},
  arena:{img:'/arena.png', toMap:centerMap(20)},
  eldergrove:{img:'/maps/whisperwood.png', toMap:centerMap(62)},
  frostmere:{img:'/maps/frostmere.png',    toMap:centerMap(40)},
  wild:{img:'/maps/wild.png',              toMap:centerMap(50)},
  wild_ext:{img:'/maps/wild-deep.png',     toMap:centerMap(25)},
  wild_exp:{img:'/maps/wild-east.png',     toMap:centerMap(25)},
  mine:{img:'/maps/mine.png',              toMap:centerMap(20)},
  spider:{img:'/maps/spider.png',          toMap:centerMap(20)},
  shack:{img:'/maps/shack.png',            toMap:centerMap(5)},
};
async function loadWorld(){
  if(state.liveSearchBusy) return;        // don't disrupt an in-progress all-server search
  if(!document.querySelector('.lw-roster') && TAB==="world") $("#view").innerHTML=skel(7);
  let d; try{ d=await (await fetch("/api/live?shard="+state.liveShard)).json(); }
  catch(e){ if(TAB==="world") $("#view").innerHTML=`<div class="empty warn">Couldn't reach the live world.</div>`; return; }
  if(TAB!=="world") return; WORLD=d; renderWorld();
}
function realmInfo(r){ return (WORLD&&WORLD.realms&&WORLD.realms[r])||{l:(r||'Overworld'),e:'📍'}; }
function serverName(n){ const s=(state.servers||[]).find(x=>x.id===n); return s&&s.name?s.name:('Server '+n); }
function renderWorld(){
  const d=WORLD; if(!d) return;
  const shards=d.shards||[1,2,3,4];
  const all=[...d.players];
  const qq=(state.liveSearch||"").trim().toLowerCase();
  const ps = qq ? all.filter(p=>(p.name||'').toLowerCase().includes(qq)) : all;
  const groups={}; ps.forEach(p=>{ const r=p.realm||'world'; (groups[r]=groups[r]||[]).push(p); });
  const order=Object.keys(groups).sort((a,b)=>
    a==='world'?-1:(b==='world'?1:groups[b].length-groups[a].length));
  const rowOf=p=>{ const open=p.id===state.liveSel, info=realmInfo(p.realm||'world');
    return `<div class="lw-p ${open?'open':''}" data-id="${p.id}">
        <span class="av">${avatarSvg(p.outfit,32)}</span>
        <span class="nm">${esc(p.name||('#'+p.id))}</span>
        ${qq?`<span class="tag">${info.e} ${esc(info.l)}</span>`:''}
        ${p.bdg?`<span class="tag">${esc(badgeLabel(p.bdg))}</span>`:''}
        <span class="lvl">Lv ${p.avg??'?'}</span><span class="chev">▶</span></div>
      ${open?`<div class="lw-exp">${playerMap(p)}${playerInfo(p)}</div>`:''}`; };
  const body = ps.length ? order.map(r=>{ const info=realmInfo(r);
      const list=groups[r].sort((a,b)=>(b.avg||0)-(a.avg||0)).map(rowOf).join("");
      return `<div class="lw-sec">${info.e} ${esc(info.l)}<span>${groups[r].length}</span></div>${list}`;
    }).join("")
    : (qq? `<div class="empty" style="padding:34px">No player matching “${esc(state.liveSearch)}” is rostered on ${esc(serverName(state.liveShard))} right now. They may be on another server or in an area we haven't swept this cycle.</div>`
        : `<div class="empty" style="padding:34px">Scanning ${esc(serverName(state.liveShard))}… players appear as we sweep every area (give it ~20s).</div>`);
  const online = d.online_total!=null
    ? `<span class="live"></span><b class="flashable">${abbr(d.online_total)}</b> online (all servers) · <b class="flashable" style="color:var(--gold2)">${all.length}</b> rostered on ${esc(serverName(state.liveShard))}${qq?` · <b style="color:var(--gold2)">${ps.length}</b> match`:''}`
    : (d.err? `<span style="color:var(--sell)">${esc(d.err)}</span>` : '<span class="live"></span>connecting…');
  $("#view").innerHTML=`<div class="lw-head">
      <div class="lw-shards">${shards.map(n=>`<div class="lw-shard ${n===state.liveShard?'on':''}" data-sh="${n}" title="${esc(serverName(n))}">${esc(serverName(n))}</div>`).join("")}</div>
      <div class="lw-online">${online}</div></div>
    <div class="lw-search">
      <input id="lwsearch" placeholder="🔍 find a player by name…" value="${esc(state.liveSearch)}" autocomplete="off" spellcheck="false">
      <button id="lwgo" class="go" ${state.liveSearchBusy?'disabled':''}>${state.liveSearchBusy?'Searching…':'Search all servers'}</button>
      ${(qq||state.liveSearchStatus)?`<button id="lwclear" title="clear">✕</button>`:''}
      <span id="lwstatus" class="lw-srch-status">${esc(state.liveSearchStatus||'')}</span>
    </div>
    <p class="lw-note">Each server is a separate world (all 12). We sweep every area — hub, pond, shores, dungeons — to roster who's where (fills over the first ~20s). Typing filters this server's roster live; hit <b>Search all servers</b> (or Enter) to sweep every server and jump to whoever matches. Click anyone to expand; players out on the overworld also pin to the map.</p>
    <div class="lw-roster">${body}</div>`;
  document.querySelectorAll('[data-sh]').forEach(b=>b.onclick=()=>{ state.liveShard=+b.dataset.sh; state.liveSel=null; state.liveSearchStatus=""; WORLD=null; loadWorld(); });
  const ls=$("#lwsearch"); if(ls){ ls.oninput=e=>{ state.liveSearch=e.target.value; renderWorld();
      const s=$("#lwsearch"); if(s){ s.focus(); s.setSelectionRange(s.value.length,s.value.length); } };
    ls.onkeydown=e=>{ if(e.key==='Enter'){ e.preventDefault(); searchAllServers(); } }; }
  const go=$("#lwgo"); if(go) go.onclick=()=>searchAllServers();
  const lc=$("#lwclear"); if(lc) lc.onclick=()=>{ state.liveSearch=""; state.liveSearchStatus=""; renderWorld(); };
  document.querySelectorAll('.lw-p').forEach(el=>el.onclick=()=>{
    const id=+el.dataset.id; state.liveSel=(state.liveSel===id?null:id);
    if(state.liveSel) selectPlayer(); else renderWorld(); });
}
/* sweep every server for a player name, then auto-open the one they're on */
async function searchAllServers(){
  const q=(state.liveSearch||'').trim();
  if(!q){ state.liveSearchStatus=""; renderWorld(); return; }
  if(state.liveSearchBusy) return;
  state.liveSearchBusy=true; state.liveSearchStatus="searching all 12 servers…";
  renderWorld();
  const setStatus=t=>{ state.liveSearchStatus=t; const el=$("#lwstatus"); if(el) el.textContent=t; };
  let found=null;
  const deadline=Date.now()+18000;          // rosters fill over ~20s; give it that long
  try{
    while(Date.now()<deadline && !found){
      let d; try{ d=await (await fetch("/api/live-search?q="+encodeURIComponent(q))).json(); }
      catch(e){ break; }
      if(d&&d.ok&&d.results&&d.results.length){ found=d.results[0]; break; }
      const ready=d?d.ready:0, total=(d&&d.shards)?d.shards.length:12;
      setStatus(`searching… swept ${ready}/${total} servers`);
      if(ready>=total) break;                // all populated, no match → stop early
      await new Promise(r=>setTimeout(r,1800));
    }
  } finally { state.liveSearchBusy=false; }
  if(found){
    state.liveSearchStatus=`found ${found.name} on ${serverName(found.shard)} — opening…`;
    state.liveShard=found.shard; state.liveSel=found.id; WORLD=null;
    await loadWorld();                       // renders the shard (filtered to the match)
    if(state.liveSel) selectPlayer();        // load their market/property card
    setTimeout(()=>{ const el=document.querySelector(`.lw-p[data-id="${found.id}"]`);
      if(el) el.scrollIntoView({block:'center',behavior:'smooth'}); }, 380);
  } else {
    state.liveSearchStatus=`"${q}" isn't on any of the 12 servers right now.`;
    renderWorld();
  }
}
function playerMap(p){
  const r=p.realm||'world', info=realmInfo(r);
  const rm=REALM_MAPS[r];
  if(rm){
    // every character currently in this realm, plotted from exact (x,z) coords.
    const here=((WORLD&&WORLD.players)||[]).filter(q=>(q.realm||'world')===r);
    const mk=here.map(q=>{ const m=rm.toMap(q.x,q.z), sel=q.id===p.id;
      return `<div class="shore-mk ${sel?'sel':''}" style="left:${(m.u*100).toFixed(2)}%;top:${(m.v*100).toFixed(2)}%">
        ${avatarSvg(q.outfit,sel?40:28)}<span class="nm">${esc(q.name||('#'+q.id))}</span></div>`; }).join("");
    return `<div class="lw-map shore">
      <img src="${rm.img}" alt="" style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover">
      ${mk}
      <span class="biome">${info.e} ${esc(info.l)}</span>
      <span class="coord">x ${(p.x||0).toFixed(1)}, z ${(p.z||0).toFixed(1)}</span></div>`;
  }
  return `<div class="lw-map">
    <img src="/worldmap.jpg" alt="" style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:.45">
    <div style="position:absolute;inset:0;display:grid;place-items:center;text-align:center;padding:14px">
      <div><div style="font-size:34px;line-height:1">${info.e}</div>
        <div style="font:800 17px 'Cinzel',serif;color:var(--gold2);letter-spacing:.04em;margin-top:4px">In ${esc(info.l)}</div>
        <div class="hint" style="margin-top:4px">a separate instanced area — the live map pin shows when they're out on the overworld</div></div></div>
  </div>`;
}
function playerInfo(p){
  const ex=playerExtra[p.name]||{}, hp=p.php==null?100:p.php, info=realmInfo(p.realm||'world');
  return `<div class="lw-info">
    <div class="top"><div>${avatarSvg(p.outfit,72)}</div>
      <div><div class="nm">${esc(p.name||('#'+p.id))}</div>
        <div class="sub">Lv ${p.avg??'?'} · #${p.id}${p.bdg?` · ${esc(badgeLabel(p.bdg))}`:''}</div>
        <div class="hpbar" style="margin-top:7px;width:150px"><div style="width:${hp}%"></div></div></div></div>
    <div class="lw-grid">
      <span class="k">Area</span><span class="v">${info.e} ${esc(info.l)}</span>
      <span class="k">Status</span><span class="v">${p.mov?'Moving':'Idle'}${p.act?' · '+esc(p.act):''}</span>
      <span class="k">Holding</span><span class="v">${p.eq?esc(lbl(p.eq)):'—'}</span>
      <span class="k">Position</span><span class="v">${(p.x||0).toFixed(1)}, ${(p.z||0).toFixed(1)}</span>
      <span class="k">On market</span><span class="v">${ex.loading?'…':(ex.listings?`${ex.listings} · ${fmtU(ex.value)}`:'—')}</span>
      <span class="k">Properties</span><span class="v">${ex.loading?'…':((ex.props&&ex.props.length)?ex.props.map(x=>x.kind+' '+x.num).join(', '):'none')}</span>
    </div></div>`;
}
async function selectPlayer(){
  renderWorld();
  const p=WORLD&&WORLD.players.find(q=>q.id===state.liveSel); if(!p||!p.name) return;
  if(playerExtra[p.name]) return;
  playerExtra[p.name]={loading:true};
  try{
    const [cur,prop]=await Promise.all([
      fetch("/api/current?q="+encodeURIComponent(p.name)+"&limit=300").then(r=>r.json()),
      fetch("/api/property").then(r=>r.json())]);
    const rows=Array.isArray(cur)?cur:(cur.rows||[]);
    const mine=rows.filter(L=>L.seller_name===p.name);
    let val=0,n=0; mine.forEach(L=>{ val+=(L.currency==='token'?(L.price_usd||0):((L.per_unit||0)*(state.rate||0)*(L.quantity||1))); n++; });
    const props=(prop.plots||[]).filter(x=>x.owner===p.name);
    playerExtra[p.name]={listings:n,value:val,props};
  }catch(e){ playerExtra[p.name]={listings:0,value:0,props:[]}; }
  if(state.liveSel===p.id) renderWorld();
}

let PROP=null;
async function loadProperty(){
  if(!document.querySelector('.pm') && TAB==="props") $("#view").innerHTML=skel(6);
  let d; try{ d=await (await fetch("/api/property")).json(); }
  catch(e){ if(TAB==="props") $("#view").innerHTML=`<div class="empty warn">Couldn't load properties.</div>`; return; }
  if(TAB!=="props") return; PROP=d; renderProperty();
}
const KIND_COLOR={mansion:'#e8b54a',house:'#5aa9e6',trailer:'#9b7bd8'};
function renderProperty(){
  const d=PROP; if(!d||d.ok===false){ $("#view").innerHTML=`<div class="empty warn">${esc((d&&d.error)||'No property data.')}</div>`; return; }
  const plots=(d.plots||[]).filter(p=>p.col0!=null);
  let minc=1e9,maxc=-1e9,minr=1e9,maxr=-1e9;
  plots.forEach(p=>{minc=Math.min(minc,p.col0);maxc=Math.max(maxc,p.col1);minr=Math.min(minr,p.row0);maxr=Math.max(maxr,p.row1);});
  const pad=1; minc-=pad;maxc+=pad;minr-=pad;maxr+=pad;
  const sold=plots.filter(p=>p.sold).length, locked=plots.filter(p=>p.locked).length;
  // Tilted 2.5D estate: axis-aligned grid (rectangular box, not an iso diamond), pitched
  // down so you see roofs + south-facing fronts — same slant as in-game, rotated so it
  // isn't a diamond (pond entrance toward the bottom). Buildings extruded at their real
  // grid footprints; hover glows white, click selects.
  const TW=26, TH=15;                       // px per grid col / row (rows foreshortened)
  const HT={mansion:[24,13],house:[16,10],trailer:[10,6]};       // [wallH, roofH] px
  const BLD={mansion:{wall:'#8b94a4',ws:'#6c7484',rf:'#4a5160',rb:'#363c49'},
             house:{wall:'#e3cfa3',ws:'#c4a978',rf:'#8a5a3b',rb:'#5f3d27'},
             trailer:{wall:'#d9cdb1',ws:'#bcae8d',rf:'#6b6353',rb:'#4d473b'}};
  const maxH=37, cols=maxc-minc+1, rows=maxr-minr+1, W=cols*TW, H=rows*TH+maxH+10;
  const P=(gx,gy,h)=>`${(gx*TW).toFixed(1)},${(maxH+gy*TH-h).toFixed(1)}`;
  const ground=`<defs><linearGradient id="grs" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0" stop-color="#3f6f28"/><stop offset="1" stop-color="#5d8a39"/></linearGradient></defs>
      <rect x="0" y="${maxH}" width="${W}" height="${rows*TH}" fill="url(#grs)"/>`;
  let grid='';
  for(let c=0;c<=cols;c++) grid+=`<line x1="${c*TW}" y1="${maxH}" x2="${c*TW}" y2="${maxH+rows*TH}" stroke="rgba(20,45,12,.16)"/>`;
  for(let r=0;r<=rows;r++) grid+=`<line x1="0" y1="${maxH+r*TH}" x2="${W}" y2="${maxH+r*TH}" stroke="rgba(20,45,12,.16)"/>`;
  const order=[...plots].sort((a,b)=>(a.row0-b.row0)||(a.col0-b.col0));   // far (north) first
  const blds=order.map(p=>{
    const gx0=p.col0-minc,gx1=p.col1+1-minc,gy0=p.row0-minr,gy1=p.row1+1-minr,mid=(gy0+gy1)/2;
    const ht=HT[p.kind],wh=ht[0],rh=ht[1],k=BLD[p.kind],sel=state.propSel===p.kind+p.num;
    const st=p.locked?'#f06a6a':'rgba(0,0,0,.4)',sw=p.locked?1.6:0.8;
    const FL=P(gx0,gy1,0),FR=P(gx1,gy1,0),FLt=P(gx0,gy1,wh),FRt=P(gx1,gy1,wh);
    const BLt=P(gx0,gy0,wh),BRt=P(gx1,gy0,wh),ML=P(gx0,mid,wh+rh),MR=P(gx1,mid,wh+rh);
    const lab=P((gx0+gx1)/2,mid,wh+rh+2).split(',');
    const dl=gx0*0.62+gx1*0.38,dr=gx0*0.38+gx1*0.62;
    const door=`${P(dl,gy1,0)} ${P(dr,gy1,0)} ${P(dr,gy1,wh*0.6)} ${P(dl,gy1,wh*0.6)}`;
    return `<g class="pm-bld ${sel?'sel':''}" data-key="${p.kind}${p.num}">
      <polygon points="${P(gx0+0.16,gy1+0.34,0)} ${P(gx1+0.16,gy1+0.34,0)} ${P(gx1+0.16,gy0+0.34,0)} ${P(gx0+0.16,gy0+0.34,0)}" fill="rgba(0,0,0,.22)"/>
      <polygon points="${FL} ${FR} ${FRt} ${FLt}" fill="${k.wall}" stroke="${st}" stroke-width="${sw}"/>
      <polygon points="${door}" fill="${k.ws}"/>
      <polygon points="${ML} ${MR} ${BRt} ${BLt}" fill="${k.rb}" stroke="${st}" stroke-width="${sw}"/>
      <polygon points="${FLt} ${FRt} ${MR} ${ML}" fill="${k.rf}" stroke="${st}" stroke-width="${sw}"/>
      <text class="lab" x="${lab[0]}" y="${lab[1]}" text-anchor="middle">${p.kind[0].toUpperCase()}${p.num}</text>
    </g>`;}).join("");
  $("#view").innerHTML=`<div class="pm-head">
      <div class="pm-stat" style="margin:0">
        <div class="box"><div class="n">${d.counts.mansion}</div><div class="l">Mansions</div></div>
        <div class="box"><div class="n">${d.counts.house}</div><div class="l">Houses</div></div>
        <div class="box"><div class="n">${d.counts.trailer}</div><div class="l">Trailers</div></div>
        <div class="box"><div class="n">${locked}</div><div class="l">Locked</div></div></div>
      <div class="pm-legend">
        <span><span class="pm-sw" style="background:#4a5160"></span>Mansion</span>
        <span><span class="pm-sw" style="background:#8a5a3b"></span>House</span>
        <span><span class="pm-sw" style="background:#6b6353"></span>Trailer</span>
        <span><span class="pm-sw" style="border-color:#f06a6a;background:transparent"></span>Locked</span></div></div>
    <div class="pm">
      <div class="pm-map"><svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet">${ground}${grid}${blds}</svg></div>
      <div class="pm-side" id="pmSide"></div></div>`;
  document.querySelectorAll('.pm-bld').forEach(g=>g.onclick=()=>{ state.propSel=g.dataset.key; renderPropCard();
    document.querySelectorAll('.pm-bld').forEach(x=>x.classList.toggle('sel',x.dataset.key===state.propSel)); });
  renderPropCard();
}
function renderPropCard(){
  const el=$("#pmSide"); if(!el) return;
  const p=(PROP.plots||[]).find(x=>x.kind+x.num===state.propSel);
  if(!p){ el.innerHTML=`<div class="pm-empty">Click a property on the map to see who owns it and what they're worth.</div>`; return; }
  const onlineHere=WORLD&&WORLD.players.find(q=>q.name===p.owner);
  el.innerHTML=`<div class="pm-card">
    <div class="ttl" style="color:${KIND_COLOR[p.kind]}">${p.kind} ${p.num}</div>
    <div class="owner">${p.owner?esc(p.owner):'<span style="color:var(--mut)">Unowned</span>'} ${p.owner?`<small>#${p.owner_id}</small>`:''}</div>
    <div class="pm-row"><span>Status</span><b>${p.sold?'Owned':'For sale'}</b></div>
    <div class="pm-row"><span>Access</span><b style="color:${p.locked?'var(--sell)':'var(--buy)'}">${p.locked?'Locked':'Open'}</b></div>
    <div class="pm-row"><span>Owner's properties</span><b>${p.owner_properties||0}</b></div>
    <div class="pm-row"><span>Active listings</span><b>${p.listings||0}</b></div>
    <div class="pm-row"><span>Listed market value</span><b>${p.market_value?fmtU(p.market_value):'—'}</b></div>
    ${onlineHere?`<div class="pm-row"><span>Live now</span><b style="color:var(--buy)">online on shard ${state.liveShard}</b></div>`:''}
    ${p.owner?`<div class="controls" style="margin-top:14px"><button class="go" onclick="jumpToSeller('${esc(p.owner).replace(/'/g,"")}')">View ${esc(p.owner)}'s listings</button></div>`:''}
  </div>`;
}
function jumpToSeller(name){ fstate.q=name; fstate.currency='all'; fstate.category='all'; TAB='live';
  document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('on',x.dataset.t==='live'));
  render(); schedule(); }

/* ---------------- routing ---------------- */
function render(){
  if(TAB==="arb")loadArb();
  else if(TAB==="live")loadLive();
  else if(TAB==="removed")loadRemoved();
  else if(TAB==="hist")loadHist();
  else if(TAB==="gold")loadGold();
  else if(TAB==="merchant")loadMerchant();
  else if(TAB==="world")loadWorld();
  else loadProperty();
}
function schedule(){
  clearInterval(timer); clearInterval(arbTimer);
  // Auto-refresh cadences are deliberately gentle (this can run 24/7); every page
  // also has a manual refresh. The backend loops keep the DB current regardless —
  // these just decide how often the open tab re-reads it.
  if(TAB==="arb"){
    arbTimer=setInterval(arbTick,60000);   // ~1 min (manual "↻ Refresh shown" for now)
  } else if(TAB==="hist"){
    // Index ~1 min, but don't disrupt an expanded row's chart while you're reading it
    timer=setInterval(()=>{ if(!state.histOpen) loadHist(); },60000);
  } else if(TAB==="merchant"){
    timer=setInterval(()=>{ if(document.activeElement!==$("#mintQty")) loadMerchant(); },30000);
  } else if(TAB==="world"){
    timer=setInterval(loadWorld,6000);     // live roster (search sweeps all servers on demand)
  } else if(TAB==="props"){
    timer=setInterval(loadProperty,30000);
  } else if(fstate.auto && (TAB==="live"||TAB==="removed")){
    timer=setInterval(()=>{loadStatus();render();},30000);   // Live listings / Sales feed ~30s
  }
}
document.querySelectorAll(".tab").forEach(t=>t.onclick=()=>{
  document.querySelectorAll(".tab").forEach(x=>x.classList.remove("on"));
  t.classList.add("on"); TAB=t.dataset.t; fadeView(); render(); schedule();
});
$("#cmdkBtn").onclick=openCmdk;
$("#cmdkInput").oninput=e=>cmdkRender(e.target.value);
$("#cmdk").onclick=e=>{ if(e.target.id==="cmdk") closeCmdk(); };
defineMorph($("#view"));   // flicker-free re-renders across every tab
loadStatus(); loadServers(); loadKinsPx(); render(); schedule();
setInterval(loadStatus,6000); setInterval(loadServers,30000); setInterval(loadKinsPx,30000);
</script>
</body></html>"""


def main():
    ap = argparse.ArgumentParser(description="KinScan")
    ap.add_argument("--interval", type=int, default=POLL_INTERVAL,
                    help=f"listing poll seconds (default {POLL_INTERVAL}; env POLL_INTERVAL)")
    ap.add_argument("--port", type=int, default=_envi("PORT", 8765),
                    help="port (default 8765; env PORT — set by most hosts)")
    ap.add_argument("--host", default=os.environ.get("KINTARA_HOST", "127.0.0.1"),
                    help="bind address (use 0.0.0.0 when hosted; env KINTARA_HOST)")
    ap.add_argument("--gold-item", help="itemType that represents tradeable gold")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    init_db()
    if args.gold_item:
        con = connect(); set_setting(con, "gold_item", args.gold_item); con.close()

    # Eager-import the heavy deps and build the Flask app on the MAIN thread BEFORE
    # spawning any worker thread. The workers lazily `import requests` on first run;
    # if that first import races the main thread's Flask/Werkzeug import (inside
    # make_app/run) they can deadlock on Python's import lock — Werkzeug then never
    # binds and the dashboard "refuses to connect" despite the banner printing.
    # Importing both single-threaded here removes that race.
    import requests  # noqa: F401  (force the import tree to load now)
    app = make_app()

    threading.Thread(target=poll_loop, args=(args.interval,), daemon=True).start()
    threading.Thread(target=stats_loop, daemon=True).start()
    threading.Thread(target=gold_price_loop, daemon=True).start()
    _spectate_hub.start()   # live-world spectator hub (connects per shard on demand)
    hosted = args.host not in ("127.0.0.1", "localhost")
    url = f"http://{'127.0.0.1' if not hosted else args.host}:{args.port}"
    print(f"Dashboard: {url}   (listing poll {args.interval}s · kintara min-gap "
          f"{KINTARA_MIN_GAP}s; Ctrl+C to stop)")
    if not args.no_browser and not hosted:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
