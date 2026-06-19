# Codex instructions

**Read `README.md` first, in full.** It is the source of truth for this project
(what it is, architecture, data sources, DB schema, API routes, UI tabs, caching,
known caveats). Do that before answering questions or making changes — it will save
you from re-deriving things that were already reverse-engineered (endpoints, asset
paths, data quirks).

## One-line orientation
Kintara Market: a self-hosted tracker + arbitrage scanner + price-history desk for the
kintara.gg MMO marketplace. Everything is in one file, `kintara_tracker.py` (Flask +
sqlite3 + requests backend, with the whole frontend as an embedded `INDEX_HTML` string).
Run: `pip install flask requests && python kintara_tracker.py` → http://127.0.0.1:8765.

## Keep the docs current (important)
When you make a change that alters behavior, data sources, the DB schema, API routes,
the tabs/UI, caching, or run instructions, **update `README.md` in the same change** —
the relevant section AND the "Changelog / current state" list at the bottom (newest
first). If you reverse-engineer a new endpoint, asset path, or data quirk, record it in
`README.md` so it never has to be re-derived. Not every tiny tweak needs an entry, but
anything a fresh session would need to know does. This `AGENTS.md` rarely needs changes;
`README.md` is where the living detail goes.

## Working notes
- Don't "fix" the items in README's "Known caveats" section — they're upstream data
  limitations, not bugs.
- The app fetches from kintara.gg, kintaragold.xyz, and GeckoTerminal. Be gentle with
  request volume (GeckoTerminal rate-limits); prefer the existing caches/archive.
- After changes, sanity-check with `python -c "import ast; ast.parse(open('kintara_tracker.py').read())"`
  and, when feasible, run the server and curl the affected endpoint.

Output only the modified or requested code block. Do not provide line-by-line explanations, setup guides, introductory, concluding remarks, or markdown commentary unless explicitly asked. Adopt an ultra-concise, high-density communication style