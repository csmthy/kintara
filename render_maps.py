#!/usr/bin/env python3
"""
render_maps.py — generate top-down 2D map art for Kintara realms as PNG assets.

These are *not* screenshots. Each realm is rebuilt from the game's own world-gen
data (kintara.gg/src/constants.js + game.js): grid dimensions, the seeded RNG that
shapes terrain, and the hardcoded prop/building coordinates. The output PNGs are
served by kintara_tracker.py and used as the Live World / Property Map backdrops.

Run:  pip install pillow && python render_maps.py
Writes into  MapImages/ .

Provenance (reverse-engineered, see README "Data sources"):
  - The Shores (beach): BEACH_COLS/ROWS=40, off=-19.5, shoreline xorshift seed
    0x9e3779b1, prop tiles from game.js beach block. Orientation matches the
    tracker's shoresToMap()  (u=(19.5-z)/39, v=(x+19.5)/39) so live player dots
    land correctly: pond entrance at top, open ocean at the bottom.
  - Estate (property): mainland 62-grid; mansions/houses/trailers drawn at their
    real PROPERTY_PLOTS footprints over the same bounding box the Property Map
    SVG overlay uses, so the clickable plots line up on the rendered buildings.
"""
import math
import os

from PIL import Image, ImageDraw

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "MapImages")
SS = 2  # supersample factor; rendered big then LANCZOS-downscaled for clean edges


def rgb(h):
    return ((h >> 16) & 255, (h >> 8) & 255, h & 255)


def mix(h1, h2, t):
    a, b = rgb(h1), rgb(h2)
    return tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _hash(c, r):
    h = (c * 73856093) ^ (r * 19349663)
    h = (h ^ (h >> 13)) & 0xFFFFFFFF
    return (h % 1000) / 1000.0


# ── The Shores (beach) ──────────────────────────────────────────────────────

def render_beach(tile):
    """Returns a PIL image of The Shores at `tile` px/tile (before downscale)."""
    COLS = ROWS = 40
    # shoreline: faithful xorshift replay (game seed 0x9e3779b1), one rand per row
    rng = [0x9e3779b1]

    def rnd():
        x = rng[0]
        x ^= (x << 13) & 0xFFFFFFFF
        x ^= (x >> 17)
        x ^= (x << 5) & 0xFFFFFFFF
        x &= 0xFFFFFFFF
        # JS stores a signed-coerced value across ops, but the >>>0 read makes it
        # unsigned; replicate by carrying unsigned state (matches game output).
        rng[0] = x
        return x / 0x100000000

    base = round(COLS * 0.58)
    shore = []
    for r in range(ROWS):
        wig = math.sin(r * 0.42) * 1.7 + math.sin(r * 0.17 + 1.3) * 1.3 + (rnd() - 0.5) * 1.2
        s = round(base + wig)
        s = max(round(COLS * 0.5), min(COLS - 2, s))
        shore.append(s)

    RET_ROWS, RET_COL, CHEB = (19, 20, 21), 0, 3

    def near_exit(c, r):
        return any(max(abs(c - RET_COL), abs(r - pr)) <= CHEB for pr in RET_ROWS)

    water, wet = set(), set()
    for r in range(ROWS):
        sh = shore[r]
        for c in range(COLS):
            if c >= sh:
                water.add((c, r))
            elif (c == sh - 1 or c == sh - 2) and not near_exit(c, r):
                wet.add((c, r))

    UMB = [0xe4572e, 0xf4a259, 0x2a9d8f, 0xe76f51, 0x4d9de0, 0xf25f5c, 0xffca3a]
    umbrellas = [(7, 6), (12, 13), (8, 24), (14, 31), (6, 17), (16, 9), (11, 35)]
    solo = [(10, 4), (18, 16), (9, 29), (15, 22), (5, 33)]
    palms = [(3, 3), (4, 36), (18, 2), (17, 37), (2, 12), (3, 28), (2, 20), (19, 26)]
    towels = [(9, 10), (13, 19), (6, 27), (12, 6), (16, 33)]
    towel_cols = [(0xe63946, 0xf1faee), (0x118ab2, 0xffd166), (0x06d6a0, 0xffffff), (0xef476f, 0xffd166)]
    castles = [(17, 12), (19, 30), (18, 21)]
    coolers = [(8, 7), (13, 32), (7, 18)]
    balls = [(11, 16), (5, 22), (14, 26)]
    ball_cols = [0xe4572e, 0x4d9de0, 0xffca3a]
    loung_cols = [0xf1faee, 0xffd6a5, 0xa8dadc, 0xfcd5ce, 0xe9edc9]
    solo_cols = [0xffadad, 0xbdb2ff, 0xcaffbf, 0xffd6a5, 0x9bf6ff]

    blocked = set(umbrellas)

    def dry_free(c, r):
        return (1 <= c < COLS and 0 <= r < ROWS and (c, r) not in water
                and (c, r) not in wet and (c, r) not in blocked and not near_exit(c, r))

    umb_loung = []
    for i, (c, r) in enumerate(umbrellas):
        for nc, nr in ((c + 1, r), (c, r + 1), (c, r - 1), (c - 1, r)):
            if dry_free(nc, nr):
                umb_loung.append((nc, nr, loung_cols[i % len(loung_cols)]))
                blocked.add((nc, nr))
                break

    T = tile
    W = H = COLS * T
    img = Image.new("RGB", (W, H), rgb(0xcdeafe))
    d = ImageDraw.Draw(img, "RGBA")

    def tx(r):
        return (COLS - 1 - r) * T

    def ty(c):
        return c * T

    def cx(c, r):
        return tx(r) + T / 2

    def cy(c, r):
        return ty(c) + T / 2

    for r in range(ROWS):
        sh = shore[r]
        for c in range(COLS):
            x, y = tx(r), ty(c)
            if (c, r) in water:
                t = min(1.0, (c - sh) / max(1, (COLS - sh)))
                d.rectangle([x, y, x + T, y + T], fill=mix(0x2fb0db, 0x0f5d8a, t))
                if _hash(c, r) > 0.82:
                    d.rectangle([x + T * .12, y + T * .18, x + T * .88, y + T * .34],
                                fill=(255, 255, 255, 26))
            elif (c, r) in wet:
                d.rectangle([x, y, x + T, y + T], fill=mix(0xcbb27f, 0xb89f6c, _hash(c, r)))
            else:
                n = _hash(c, r)
                d.rectangle([x, y, x + T, y + T], fill=mix(0xe9d8a4, 0xddc88c, n * 0.7))
                if n > 0.93:
                    d.rectangle([x + T * .4, y + T * .4, x + T * .4 + 2 * SS, y + T * .4 + 2 * SS],
                                fill=(150, 120, 70, 40))

    # faint tile grid on sand only
    for r in range(ROWS):
        sh = shore[r]
        for c in range(sh):
            x, y = tx(r), ty(c)
            d.rectangle([x, y, x + T, y + T], outline=(28, 24, 12, 16), width=max(1, SS // 2))

    # shoreline foam
    pts = [(tx(r) + T / 2, ty(shore[r])) for r in range(ROWS)]
    d.line(pts, fill=(245, 252, 255, 205), width=round(2.4 * SS), joint="curve")

    def palm(c, r):
        X, Y = cx(c, r), cy(c, r)
        for i in range(10):
            a = i / 10 * math.tau
            col = 0x47b04e if i % 2 else 0x2f8f3a
            d.line([X, Y, X + math.cos(a) * T * 1.25, Y + math.sin(a) * T * 1.25],
                   fill=rgb(col), width=round(4.5 * SS))
        d.ellipse([X - T * .32, Y - T * .32, X + T * .32, Y + T * .32], fill=rgb(0x256b2c))
        d.ellipse([X - T * .16, Y - T * .16, X + T * .16, Y + T * .16], fill=rgb(0x8a6239))

    def umbrella(c, r, col):
        X, Y = cx(c, r), cy(c, r)
        R = T * 1.2
        d.ellipse([X - R, Y - R + 1.5 * SS, X + R, Y + R + 1.5 * SS], fill=(40, 30, 15, 40))
        for i in range(8):
            a0, a1 = i / 8 * 360, (i + 1) / 8 * 360
            d.pieslice([X - R, Y - R, X + R, Y + R], a0, a1,
                       fill=rgb(col) if i % 2 else rgb(0xfbf4e6))
        d.ellipse([X - R, Y - R, X + R, Y + R], outline=(60, 40, 20, 115), width=max(1, SS))
        d.ellipse([X - 2.2 * SS, Y - 2.2 * SS, X + 2.2 * SS, Y + 2.2 * SS], fill=rgb(0x7a6048))

    def lounger(c, r, col):
        X, Y = cx(c, r), cy(c, r)
        w, h = T * 0.7, T * 1.5
        x, y, rad = X - w / 2, Y - h / 2, 3 * SS
        d.rounded_rectangle([x + 1.5 * SS, y + 2 * SS, x + 1.5 * SS + w, y + 2 * SS + h], rad, fill=(40, 30, 15, 40))
        d.rounded_rectangle([x - 1.5 * SS, y - 1.5 * SS, x + w + 1.5 * SS, y + h + 1.5 * SS], rad + SS, fill=rgb(0xded4bd))
        d.rounded_rectangle([x, y, x + w, y + h], rad, fill=rgb(col), outline=(80, 70, 50, 90), width=max(1, SS // 2))
        d.rounded_rectangle([x, y, x + w, y + h * 0.26], rad, fill=(255, 255, 255, 115))

    def towel(c, r, a, b):
        X, Y = cx(c, r), cy(c, r)
        w, h = T * 0.92, T * 1.5
        x, y = X - w / 2, Y - h / 2
        for i in range(7):
            d.rectangle([x, y + i * h / 7, x + w, y + (i + 1) * h / 7 + 0.6 * SS],
                        fill=rgb(a) if i % 2 else rgb(b))

    def castle(c, r):
        X, Y = cx(c, r), cy(c, r)
        s = T * 0.8
        d.rectangle([X - s / 2 + 1.5 * SS, Y - s / 2 + 2 * SS, X + s / 2 + 1.5 * SS, Y + s / 2 + 2 * SS], fill=(40, 30, 15, 36))
        d.rectangle([X - s / 2, Y - s / 2, X + s / 2, Y + s / 2], fill=rgb(0xd9c084))
        for dx, dy in ((-1, -1), (1, -1), (-1, 1), (1, 1)):
            rr = s * 0.18
            px, py = X + dx * s * 0.32, Y + dy * s * 0.32
            d.ellipse([px - rr, py - rr, px + rr, py + rr], fill=rgb(0xc9ad6e))
        d.rectangle([X - s * .16, Y - s * .16, X + s * .16, Y + s * .16], fill=rgb(0xe6d29a))

    def cooler(c, r):
        X, Y = cx(c, r), cy(c, r)
        w, h = T * 0.7, T * 0.5
        x, y = X - w / 2, Y - h / 2
        d.rectangle([x + 1.5 * SS, y + 2 * SS, x + w + 1.5 * SS, y + h + 2 * SS], fill=(40, 30, 15, 36))
        d.rectangle([x, y, x + w, y + h], fill=rgb(0x3a86c8))
        d.rectangle([x, y, x + w, y + h * 0.34], fill=rgb(0xeef3f6))

    def ball(c, r, col):
        X, Y = cx(c, r), cy(c, r)
        R = T * 0.4
        d.ellipse([X - R + SS, Y - R + 1.5 * SS, X + R + SS, Y + R + 1.5 * SS], fill=(40, 30, 15, 36))
        d.ellipse([X - R, Y - R, X + R, Y + R], fill=rgb(col), outline=(0, 0, 0, 38), width=max(1, SS // 2))
        d.pieslice([X - R, Y - R, X + R, Y + R], -90, -90 + 46, fill=rgb(0xffffff))

    for i, (c, r) in enumerate(towels):
        towel(c, r, *towel_cols[i % len(towel_cols)])
    for c, r in castles:
        castle(c, r)
    for c, r in coolers:
        cooler(c, r)
    for i, (c, r) in enumerate(balls):
        ball(c, r, ball_cols[i % len(ball_cols)])
    for c, r, col in umb_loung:
        lounger(c, r, col)
    for i, (c, r) in enumerate(solo):
        lounger(c, r, solo_cols[i % len(solo_cols)])
    for i, (c, r) in enumerate(umbrellas):
        umbrella(c, r, UMB[i % len(UMB)])
    for c, r in palms:
        palm(c, r)

    return img


# ── The Pond (fishing) ──────────────────────────────────────────────────────
# 40×40, off -19.5. Lake grown from centre by the game's pondRand xorshift
# (seed 0x50dcee): a 10×10 seed block then 70 flood-grow steps. Wooden dock
# (cols 15-19 × rows 18-19) and the NE luxury tower / construction lot
# (cols 32-38 × rows 1-6) carve the water. The NE resource-exclude zone is
# approximated (its exact bounds are game.js-only), so the lake edge nearest the
# NE corner is representative; the central lake shape is the exact RNG replay.
def render_pond(tile):
    COLS = ROWS = 40
    rng = [0x50dcee]

    def rnd():
        x = rng[0]
        x ^= (x << 13) & 0xFFFFFFFF
        x ^= (x >> 17)
        x ^= (x << 5) & 0xFFFFFFFF
        x &= 0xFFFFFFFF
        rng[0] = x
        return x / 0x100000000

    RET_ROWS, RET_COL, CHEB = (19, 20, 21), 0, 3

    def near_exit(c, r):
        return any(max(abs(c - RET_COL), abs(r - pr)) <= CHEB for pr in RET_ROWS)

    def excluded(c, r):          # ~NE construction/tower zone (approx)
        return c >= 30 and r <= 8

    cx, cy = COLS // 2, ROWS // 2
    water = set()
    for c in range(cx - 5, cx + 5):
        for r in range(cy - 5, cy + 5):
            if 0 <= c < COLS and 0 <= r < ROWS and not near_exit(c, r):
                water.add((c, r))

    def can_grow(c, r):
        return (0 <= c < COLS and 0 <= r < ROWS and not near_exit(c, r)
                and not excluded(c, r) and (c, r) not in water
                and c < COLS - 3 and c > 2 and max(abs(c - cx), abs(r - cy)) <= 14)

    neigh = [(1, 0), (-1, 0), (0, 1), (0, -1)]
    for _ in range(70):
        tiles = list(water)
        if not tiles:
            break
        pc, pr = tiles[int(rnd() * len(tiles))]
        dc, dr = neigh[int(rnd() * 4)]
        nc, nr = pc + dc, pr + dr
        if can_grow(nc, nr) and rnd() < 0.72:
            water.add((nc, nr))
    water = {k for k in water if not near_exit(*k)}
    dock = {(c, r) for c in range(15, 20) for r in range(18, 20)}
    tower = {(c, r) for c in range(32, 39) for r in range(1, 7)}
    water -= dock
    water -= tower

    T = tile
    W = H = COLS * T
    img = Image.new("RGB", (W, H), rgb(0x5d8a39))
    d = ImageDraw.Draw(img, "RGBA")

    def edge(c, r):
        return any((c + dc, r + dr) not in water for dc, dr in neigh)

    for r in range(ROWS):
        for c in range(COLS):
            x, y = c * T, r * T
            n = _hash(c, r)
            if (c, r) in water:
                d.rectangle([x, y, x + T, y + T],
                            fill=mix(0x2f93c4, 0x1c6a99, 0.25 if edge(c, r) else min(1.0, n)))
            else:
                d.rectangle([x, y, x + T, y + T], fill=mix(0x5d8a39, 0x4f7d30, n))
    for k in range(COLS + 1):
        d.line([k * T, 0, k * T, H], fill=(20, 45, 12, 14), width=max(1, SS // 2))
        d.line([0, k * T, W, k * T], fill=(20, 45, 12, 14), width=max(1, SS // 2))

    # wooden dock
    for (c, r) in dock:
        x, y = c * T, r * T
        d.rectangle([x, y, x + T, y + T], fill=mix(0x9b6b3f, 0x83592f, _hash(c, r)))
    for c in range(15, 20):                  # plank seams
        d.line([c * T, 18 * T, c * T, 20 * T], fill=(60, 40, 20, 120), width=max(1, SS // 2))

    # NE luxury tower (on its dirt lot)
    tx0, ty0, tx1, ty1 = 32 * T, 1 * T, 39 * T, 7 * T
    d.rectangle([tx0, ty0, tx1, ty1], fill=rgb(0x8a8377))                      # dirt lot
    d.rectangle([tx0 + 4 * SS, ty0 + 5 * SS, tx1 + 4 * SS, ty1 + 5 * SS], fill=(0, 0, 0, 70))
    d.rounded_rectangle([tx0 + T, ty0 + T, tx1 - T, ty1 - T], T * 0.3,
                        fill=rgb(0x6b7079), outline=rgb(0x4c5159), width=max(1, SS))

    # representative trees/rocks on the grass (decorative; not the exact scatter)
    trng = [0x1234abcd]

    def trnd():
        x = trng[0]
        x ^= (x << 13) & 0xFFFFFFFF
        x ^= (x >> 17)
        x ^= (x << 5) & 0xFFFFFFFF
        x &= 0xFFFFFFFF
        trng[0] = x
        return x / 0x100000000

    placed = 0
    blocked = water | dock | tower
    while placed < 90:
        c, r = int(trnd() * COLS), int(trnd() * ROWS)
        if (c, r) in blocked or near_exit(c, r) or excluded(c, r):
            continue
        X, Y = c * T + T / 2, r * T + T / 2
        if trnd() < 0.78:
            R = T * (0.5 + trnd() * 0.3)
            d.ellipse([X - R - 1.5 * SS, Y - R + 2 * SS, X + R - 1.5 * SS, Y + R + 2 * SS], fill=(20, 40, 15, 70))
            d.ellipse([X - R, Y - R, X + R, Y + R], fill=mix(0x3f8b3a, 0x2f6f2c, trnd()))
            d.ellipse([X - R * 0.45, Y - R * 0.45, X + R * 0.45, Y + R * 0.45], fill=rgb(0x57a84c))
        else:
            R = T * 0.34
            d.ellipse([X - R, Y - R, X + R, Y + R], fill=mix(0x8d8f93, 0x6f7175, trnd()))
        placed += 1

    return img


# ── The Arena (PvP) ─────────────────────────────────────────────────────────
# 20×20, off -9.5. Sand floor with a central boxing ring (blue mat, red ropes,
# corner posts). arenaToMap: u=(x+9.5)/19, v=(z+9.5)/19.
def render_arena(tile):
    COLS = ROWS = 20
    T = tile
    W = H = COLS * T
    img = Image.new("RGB", (W, H), rgb(0xb59169))
    d = ImageDraw.Draw(img, "RGBA")
    for r in range(ROWS):
        for c in range(COLS):
            x, y = c * T, r * T
            d.rectangle([x, y, x + T, y + T], fill=mix(0xbb976e, 0xa9855c, _hash(c, r)))
    for k in range(COLS + 1):
        d.line([k * T, 0, k * T, H], fill=(60, 40, 20, 16), width=max(1, SS // 2))
        d.line([0, k * T, W, k * T], fill=(60, 40, 20, 16), width=max(1, SS // 2))

    # central ring cols 7-12 rows 7-12
    rx0, ry0, rx1, ry1 = 7 * T, 7 * T, 13 * T, 13 * T
    d.rectangle([rx0 + 4 * SS, ry0 + 5 * SS, rx1 + 4 * SS, ry1 + 5 * SS], fill=(0, 0, 0, 60))
    d.rectangle([rx0, ry0, rx1, ry1], fill=rgb(0x8a8f98))                       # apron
    pad = T * 0.55
    d.rectangle([rx0 + pad, ry0 + pad, rx1 - pad, ry1 - pad], fill=rgb(0x356b9e))  # blue mat
    d.rectangle([rx0 + pad, ry0 + pad, rx1 - pad, ry1 - pad],
                outline=rgb(0xd64545), width=max(2, SS * 2))                    # red ropes
    for cx, cy in ((rx0 + pad, ry0 + pad), (rx1 - pad, ry0 + pad),
                   (rx0 + pad, ry1 - pad), (rx1 - pad, ry1 - pad)):
        d.ellipse([cx - T * 0.3, cy - T * 0.3, cx + T * 0.3, cy + T * 0.3], fill=rgb(0xd64545))
    return img


# ── shared helpers for the scatter realms ───────────────────────────────────
# These realms (Whisperwood/Frostmere/Wilderness/Mine/Spider/Shack) don't have a
# faithful prop-coordinate dump in the public client like the beach does, so —
# exactly as the Pond/Arena do — the terrain palette + grid + coordinate transform
# are exact (from constants.js: each is a centred square grid, off = -(N-1)/2,
# tile (c,r) -> world (x=c+off, z=r+off)), while the trees/rocks/features are a
# seeded representative scatter rather than the game's exact placement.

def _rng_fn(seed):
    st = [seed & 0xFFFFFFFF]

    def r():
        x = st[0]
        x ^= (x << 13) & 0xFFFFFFFF
        x ^= (x >> 17)
        x ^= (x << 5) & 0xFFFFFFFF
        x &= 0xFFFFFFFF
        st[0] = x
        return x / 0x100000000
    return r


def _ground(d, COLS, ROWS, T, c1, c2, grid):
    for r in range(ROWS):
        for c in range(COLS):
            x, y = c * T, r * T
            d.rectangle([x, y, x + T, y + T], fill=mix(c1, c2, _hash(c, r)))
    for k in range(COLS + 1):
        d.line([k * T, 0, k * T, ROWS * T], fill=grid, width=max(1, SS // 2))
    for k in range(ROWS + 1):
        d.line([0, k * T, COLS * T, k * T], fill=grid, width=max(1, SS // 2))


def _shadow(d, X, Y, R):
    d.ellipse([X - R - 1.2 * SS, Y - R + 2.2 * SS, X + R - 1.2 * SS, Y + R + 2.2 * SS], fill=(18, 26, 14, 70))


def _round_tree(d, X, Y, T, c1, c2, trunk=0x5a3d22, snow=False, size=1.0):
    R = T * 0.42 * size
    _shadow(d, X, Y, R)
    d.rectangle([X - T * 0.07, Y + R * 0.2, X + T * 0.07, Y + R * 0.85], fill=rgb(trunk))
    d.ellipse([X - R, Y - R, X + R, Y + R], fill=mix(c1, c2, 0.45))
    d.ellipse([X - R * 0.62, Y - R * 0.72, X + R * 0.5, Y + R * 0.42], fill=rgb(c2))
    if snow:
        d.ellipse([X - R * 0.6, Y - R * 0.9, X + R * 0.55, Y - R * 0.1], fill=(255, 255, 255, 170))


def _pine(d, X, Y, T, snow=False, size=1.0):
    R = T * 0.5 * size
    _shadow(d, X, Y, R * 0.75)
    d.rectangle([X - T * 0.06, Y + R * 0.45, X + T * 0.06, Y + R * 0.85], fill=rgb(0x5a3d22))
    for i, sc in enumerate((1.0, 0.72, 0.46)):
        yy = Y - R * 0.55 + i * R * 0.48
        d.polygon([(X, yy - R * 0.55), (X - R * sc, yy + R * 0.34), (X + R * sc, yy + R * 0.34)],
                  fill=mix(0x2f6b3a, 0x234f2c, i / 3))
        if snow:
            d.polygon([(X, yy - R * 0.55), (X - R * sc * 0.5, yy - R * 0.06), (X + R * sc * 0.5, yy - R * 0.06)],
                      fill=(255, 255, 255, 150))


def _rock(d, X, Y, T, c1=0x8d8f93, c2=0x6f7175, size=1.0):
    R = T * 0.32 * size
    _shadow(d, X, Y, R)
    d.ellipse([X - R, Y - R, X + R, Y + R], fill=mix(c1, c2, 0.5))
    d.ellipse([X - R * 0.5, Y - R * 0.62, X + R * 0.22, Y], fill=rgb(c1))


def _scatter_realm(COLS, T, *, ground, grid, tree=(0x2f8f3a, 0x47b04e), rock=(0x8d8f93, 0x6f7175),
                   density=0.16, rock_freq=0.2, snow=False, pine=False, seed=0xE1DE, bg=None):
    """A centred square realm with seeded tree/rock scatter (the Pond approach)."""
    ROWS = COLS
    img = Image.new("RGB", (COLS * T, ROWS * T), rgb(bg if bg is not None else ground[0]))
    d = ImageDraw.Draw(img, "RGBA")
    _ground(d, COLS, ROWS, T, ground[0], ground[1], grid)
    rnd = _rng_fn(seed)
    occ = set()
    for _ in range(int(COLS * ROWS * density)):
        c, r = int(rnd() * COLS), int(rnd() * ROWS)
        if (c, r) in occ:
            continue
        occ.add((c, r))
        X, Y = c * T + T / 2, r * T + T / 2
        k = rnd()
        if k < rock_freq:
            _rock(d, X, Y, T, rock[0], rock[1], size=0.75 + rnd() * 0.5)
        elif pine:
            _pine(d, X, Y, T, snow=snow, size=0.85 + rnd() * 0.4)
        else:
            _round_tree(d, X, Y, T, tree[0], tree[1], snow=snow, size=0.82 + rnd() * 0.5)
    return img, d, rnd


# ── Whisperwood (eldergrove) — 62×62 lush forest ────────────────────────────
def render_whisperwood(tile):
    COLS = 62
    img, d, rnd = _scatter_realm(COLS, tile, ground=(0x4c7a2e, 0x3c6724),
                                 grid=(20, 45, 12, 12), tree=(0x2f8f3a, 0x53b84e),
                                 density=0.2, rock_freq=0.12, seed=0xE1DE6203)
    T = tile
    # a soft dirt path winding down the grove
    for r in range(COLS):
        pc = 31 + round(math.sin(r * 0.18) * 7 + math.sin(r * 0.07) * 4)
        for c in (pc - 1, pc, pc + 1):
            x, y = c * T, r * T
            d.rectangle([x, y, x + T, y + T], fill=mix(0x8a6b43, 0x76592f, _hash(c, r)))
    return img


# ── Frostmere — 40×40 snowfield ─────────────────────────────────────────────
def render_frostmere(tile):
    COLS = 40
    T = tile
    img, d, rnd = _scatter_realm(COLS, T, ground=(0xe2edf4, 0xccdcea), grid=(120, 150, 180, 22),
                                 rock=(0xb7c3d0, 0x93a2b2), density=0.14, rock_freq=0.22,
                                 snow=True, pine=True, seed=0xF305E3, bg=0xe2edf4)
    # frozen ponds (light ice ellipses)
    for (cc, cr, rr) in ((11, 27, 5), (29, 12, 4), (20, 33, 3.4)):
        X, Y, R = cc * T, cr * T, rr * T
        d.ellipse([X - R, Y - R, X + R, Y + R], fill=mix(0xbcdcea, 0x9ec6dc, 0.4),
                  outline=(255, 255, 255, 150), width=max(1, SS))
        d.line([X - R * 0.5, Y - R * 0.2, X + R * 0.3, Y + R * 0.4], fill=(255, 255, 255, 120), width=max(1, SS))
    return img


# ── The Wilderness (wild) and its extensions — wild scrubland ───────────────
def render_wild(tile, cols=50, *, deep=False, east=False):
    if deep:
        g1, g2, tr, seed = 0x365526, 0x29401c, (0x276b2c, 0x3f8f3a), 0xD3E6
    elif east:
        g1, g2, tr, seed = 0x5a7a30, 0x496627, (0x4a8f2f, 0x6fae42), 0xEA57
    else:
        g1, g2, tr, seed = 0x4a6f2a, 0x3b5b20, (0x357f30, 0x57a84c), 0x217DEE
    img, d, rnd = _scatter_realm(cols, tile, ground=(g1, g2), grid=(20, 40, 12, 14),
                                 tree=tr, rock=(0x8a8470, 0x6d6856), density=0.13,
                                 rock_freq=0.34, seed=seed)
    T = tile
    # tufts of tall grass
    r2 = _rng_fn(seed ^ 0x9999)
    for _ in range(int(cols * cols * 0.05)):
        c, r = int(r2() * cols), int(r2() * cols)
        X, Y = c * T + T / 2, r * T + T / 2
        for dx in (-T * 0.18, 0, T * 0.18):
            d.line([X + dx, Y + T * 0.25, X + dx * 1.4, Y - T * 0.3], fill=mix(tr[1], 0x86b35a, r2()), width=max(1, SS))
    return img


# ── The Mine — 20×20 rock cavern with ore ───────────────────────────────────
def render_mine(tile):
    COLS = 20
    T = tile
    img = Image.new("RGB", (COLS * T, COLS * T), rgb(0x3a3530))
    d = ImageDraw.Draw(img, "RGBA")
    _ground(d, COLS, COLS, T, 0x423b34, 0x322c27, (0, 0, 0, 40))
    rnd = _rng_fn(0x312E)
    ORE = [(0xf0c64b, 0xb8902a), (0x6db4e0, 0x3f7fae), (0xe06a6a, 0xa83f3f), (0xcfd3d8, 0x9aa0a8)]
    for _ in range(int(COLS * COLS * 0.5)):
        c, r = int(rnd() * COLS), int(rnd() * COLS)
        X, Y = c * T + T / 2, r * T + T / 2
        k = rnd()
        if k < 0.5:
            _rock(d, X, Y, T, 0x6a635b, 0x4d4842, size=0.7 + rnd() * 0.6)
        elif k < 0.72:                                   # an ore-flecked boulder
            a, b = ORE[int(rnd() * len(ORE))]
            _rock(d, X, Y, T, 0x6a635b, 0x4d4842, size=0.8 + rnd() * 0.4)
            for _ in range(4):
                ox, oy = X + (rnd() - 0.5) * T * 0.5, Y + (rnd() - 0.5) * T * 0.5
                rr = T * 0.07
                d.ellipse([ox - rr, oy - rr, ox + rr, oy + rr], fill=mix(a, b, rnd()))
    # a couple of wall torches (warm glow)
    for (tc, tr_) in ((3, 3), (16, 5), (5, 16), (15, 16)):
        X, Y = tc * T + T / 2, tr_ * T + T / 2
        R = T * 0.9
        d.ellipse([X - R, Y - R, X + R, Y + R], fill=(255, 170, 60, 30))
        d.ellipse([X - T * 0.12, Y - T * 0.12, X + T * 0.12, Y + T * 0.12], fill=rgb(0xffb347))
    return img


# ── Spider Lair (spider) — dark dungeon, webs + egg sacs (grid approximate) ──
def render_spider(tile):
    COLS = 20
    T = tile
    img = Image.new("RGB", (COLS * T, COLS * T), rgb(0x241f2b))
    d = ImageDraw.Draw(img, "RGBA")
    _ground(d, COLS, COLS, T, 0x2a2433, 0x201b28, (10, 6, 16, 50))
    W = COLS * T
    # cobwebs fanning from each corner
    for (ox, oy, sx, sy) in ((0, 0, 1, 1), (W, 0, -1, 1), (0, W, 1, -1), (W, W, -1, -1)):
        for i in range(6):
            f = (i + 1) / 6
            d.line([ox, oy + sy * W * 0.42 * f, ox + sx * W * 0.42 * f, oy], fill=(220, 220, 235, 55), width=max(1, SS))
        for ring in (0.14, 0.26, 0.38):
            d.arc([ox - W * ring, oy - W * ring, ox + W * ring, oy + W * ring], 0, 360, fill=(210, 210, 230, 45), width=max(1, SS))
    rnd = _rng_fn(0x5719)
    for _ in range(14):                                  # pale egg sacs
        c, r = int(rnd() * COLS), int(rnd() * COLS)
        X, Y = c * T + T / 2, r * T + T / 2
        R = T * (0.28 + rnd() * 0.18)
        _shadow(d, X, Y, R)
        d.ellipse([X - R, Y - R, X + R, Y + R], fill=mix(0xe7e2d6, 0xc9c3b2, rnd()),
                  outline=(120, 110, 95, 120), width=max(1, SS // 2))
        d.ellipse([X - R * 0.4, Y - R * 0.5, X + R * 0.15, Y], fill=(255, 255, 255, 90))
    return img


# ── The Mainland (world) — 62×62 overworld hub + estate ─────────────────────
# Centred 62-grid (GRID_COLS=62, off -30.5) → centerMap(62) plots players exactly.
# The property estate is drawn at its REAL footprints (PROPERTY_PLOTS, same data the
# Property Map uses); the plaza/fountain/shops/portals are representative placement.
MAINLAND_PLOTS = {
    "mansion": {1: (23, 26, 42, 46), 2: (23, 26, 49, 53), 3: (23, 26, 56, 60)},
    "house": {1: (35, 37, 52, 56), 2: (35, 37, 46, 50), 3: (37, 41, 41, 43),
              4: (41, 43, 35, 39), 5: (52, 56, 35, 37)},
    "trailer": {1: (45, 46, 57, 60), 2: (45, 46, 51, 54), 3: (57, 60, 45, 46),
                4: (51, 54, 45, 46), 5: (51, 52, 56, 59), 6: (57, 60, 51, 52),
                7: (52, 55, 51, 52), 8: (56, 59, 57, 58)},
}


def _building(d, T, c0, c1, r0, r1, roof, wall):
    x0, y0, x1, y1 = c0 * T, r0 * T, (c1 + 1) * T, (r1 + 1) * T
    d.rectangle([x0 + 3 * SS, y0 + 4 * SS, x1 + 3 * SS, y1 + 4 * SS], fill=(0, 0, 0, 60))
    d.rectangle([x0, y0, x1, y1], fill=rgb(wall))                      # walls
    pad = max(T * 0.16, 2 * SS)
    d.rectangle([x0 + pad, y0 + pad, x1 - pad, y1 - pad], fill=rgb(roof),
                outline=mix(roof, 0x000000, 0.35), width=max(1, SS))   # roof
    if (x1 - x0) >= (y1 - y0):
        yy = (y0 + y1) / 2
        d.line([x0 + pad, yy, x1 - pad, yy], fill=mix(roof, 0xffffff, 0.3), width=max(1, SS))
    else:
        xx = (x0 + x1) / 2
        d.line([xx, y0 + pad, xx, y1 - pad], fill=mix(roof, 0xffffff, 0.3), width=max(1, SS))


def render_mainland(tile):
    COLS = 62
    T = tile
    img = Image.new("RGB", (COLS * T, COLS * T), rgb(0x4f7d30))
    d = ImageDraw.Draw(img, "RGBA")
    _ground(d, COLS, COLS, T, 0x4f7d30, 0x447028, (20, 45, 12, 12))

    estate_cells = set()
    for kind in MAINLAND_PLOTS.values():
        for (c0, c1, r0, r1) in kind.values():
            for c in range(c0, c1 + 1):
                for r in range(r0, r1 + 1):
                    estate_cells.add((c, r))

    plaza = (24, 39, 5, 19)                       # cobblestone hub (upper-centre)
    fcx, fcy = 31.5, 12                           # fountain centre

    def in_plaza(c, r):
        return plaza[0] <= c <= plaza[1] and plaza[2] <= r <= plaza[3]

    # dirt path: plaza → estate, plus a ring road through the estate row
    path = set()
    for r in range(18, 41):
        path |= {(31, r), (32, r)}
    for c in range(24, 58):
        path |= {(c, 40), (c, 41)}

    # plaza cobblestone
    for r in range(plaza[2], plaza[3] + 1):
        for c in range(plaza[0], plaza[1] + 1):
            x, y = c * T, r * T
            d.rectangle([x, y, x + T, y + T], fill=mix(0x9aa0a8, 0x7e848c, _hash(c, r)))
    # dirt paths
    for (c, r) in path:
        x, y = c * T, r * T
        d.rectangle([x, y, x + T, y + T], fill=mix(0x8a6b43, 0x76592f, _hash(c, r)))

    # scattered trees on open grass
    rnd = _rng_fn(0x4A1D0072)
    occ = set()
    for _ in range(int(COLS * COLS * 0.14)):
        c, r = int(rnd() * COLS), int(rnd() * COLS)
        if (c, r) in occ or in_plaza(c, r) or (c, r) in estate_cells or (c, r) in path:
            continue
        occ.add((c, r))
        X, Y = c * T + T / 2, r * T + T / 2
        if rnd() < 0.16:
            _rock(d, X, Y, T, size=0.7 + rnd() * 0.4)
        else:
            _round_tree(d, X, Y, T, 0x2f8f3a, 0x53b84e, size=0.85 + rnd() * 0.45)

    # fountain
    X, Y, R = fcx * T, fcy * T, 2.6 * T
    d.ellipse([X - R - 3 * SS, Y - R + 3 * SS, X + R - 3 * SS, Y + R + 3 * SS], fill=(0, 0, 0, 55))
    d.ellipse([X - R, Y - R, X + R, Y + R], fill=rgb(0x9aa3ad), outline=rgb(0x6f7780), width=max(2, SS * 2))
    d.ellipse([X - R * 0.66, Y - R * 0.66, X + R * 0.66, Y + R * 0.66], fill=mix(0x3f93c4, 0x2f7aa8, 0.4))
    d.ellipse([X - R * 0.22, Y - R * 0.22, X + R * 0.22, Y + R * 0.22], fill=rgb(0xbfd8e6))

    # a couple of shop/market buildings flanking the plaza
    _building(d, T, 25, 28, 7, 10, 0xd98c3a, 0xb9772f)     # market stall (orange)
    _building(d, T, 35, 38, 7, 10, 0x4d7ea8, 0x3c648a)     # bank (blue)
    _building(d, T, 25, 27, 15, 18, 0x7a5aa0, 0x5f4680)    # alchemist (purple)

    # the real property estate
    for num, plot in MAINLAND_PLOTS["mansion"].items():
        _building(d, T, *plot, 0xe0b34a, 0xb78f30)         # mansions: gold roof
    for num, plot in MAINLAND_PLOTS["house"].items():
        _building(d, T, *plot, 0xcc5b4a, 0xa3463a)         # houses: terracotta
    for num, plot in MAINLAND_PLOTS["trailer"].items():
        _building(d, T, *plot, 0x9aa0a8, 0x767c84)         # trailers: metal grey

    # portal gates at the edges (representative positions), colour-keyed to the realm
    def gate(c, r, col, label):
        x, y = c * T, r * T
        w = 3 * T
        d.rectangle([x, y, x + w, y + T * 1.4], fill=rgb(col), outline=(0, 0, 0, 120), width=max(1, SS))
        d.rectangle([x + T * 0.3, y + T * 0.3, x + w - T * 0.3, y + T * 1.1], fill=mix(col, 0x000000, 0.45))
    gate(16, 0, 0x2f7aa8, "Pond")          # top edge
    gate(30, 0, 0x2f6b3a, "Wild")
    gate(44, 0, 0xc0492f, "Arena")
    gate(30, 60, 0x356b2c, "Whisperwood")  # bottom edge
    gate(0, 30, 0xe0c46a, "Shores")        # left edge (drawn as a vertical-ish marker)
    gate(59, 30, 0x6a635b, "Mine")         # right edge

    return img


# ── The Shack (shack) — 5×5 wooden interior ─────────────────────────────────
def render_shack(tile):
    COLS = 5
    T = tile
    img = Image.new("RGB", (COLS * T, COLS * T), rgb(0x6f4f2c))
    d = ImageDraw.Draw(img, "RGBA")
    for r in range(COLS):
        for c in range(COLS):
            x, y = c * T, r * T
            d.rectangle([x, y, x + T, y + T], fill=mix(0x7a5733, 0x654526, _hash(c, r)))
    for c in range(COLS + 1):                            # plank seams
        d.line([c * T, 0, c * T, COLS * T], fill=(40, 26, 12, 130), width=max(1, SS))
    # a small rug in the middle
    rx0, ry0, rx1, ry1 = 1.3 * T, 1.3 * T, 3.7 * T, 3.7 * T
    d.rectangle([rx0, ry0, rx1, ry1], fill=rgb(0x9c3b3b), outline=rgb(0xd9b25a), width=max(2, SS))
    d.rectangle([rx0 + T * 0.4, ry0 + T * 0.4, rx1 - T * 0.4, ry1 - T * 0.4], outline=rgb(0xd9b25a), width=max(1, SS))
    return img


def save(name, img, tile_render, tile_out_scale=1.0):
    final = img.resize((round(img.width / SS), round(img.height / SS)), Image.LANCZOS)
    path = os.path.join(OUT, name)
    final.save(path)
    print(f"wrote {path}  ({final.width}x{final.height})")


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    save("The Shores (Beach).png", render_beach(40 * SS), 40)
    save("The Pond.png", render_pond(40 * SS), 40)
    save("The Arena.png", render_arena(70 * SS), 70)
    # remaining realms (centred-grid transform exact from constants.js; scatter representative)
    save("Whisperwood (Eldergrove).png", render_whisperwood(24 * SS), 24)   # 62×62
    save("Frostmere.png", render_frostmere(38 * SS), 38)                    # 40×40
    save("The Wilderness (Wild).png", render_wild(30 * SS, 50), 30)         # 50×50
    save("Deep Wilderness.png", render_wild(58 * SS, 25, deep=True), 58)    # 25×25
    save("Wilderness East.png", render_wild(58 * SS, 25, east=True), 58)    # 25×25
    save("The Mine.png", render_mine(70 * SS), 70)                          # 20×20
    save("Spider Lair.png", render_spider(70 * SS), 70)                     # 20×20 (approx)
    save("The Shack.png", render_shack(150 * SS), 150)                      # 5×5
    save("The Mainland.png", render_mainland(24 * SS), 24)                  # 62×62 overworld
