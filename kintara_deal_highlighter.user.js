// ==UserScript==
// @name         Kintara Market — Deal Highlighter
// @namespace    kintara.local.dealhighlighter
// @version      1.0
// @description  Highlights $KINS marketplace listings (KINS only) that meet your per-item units/$ and minimum-order rules. Read-only, no network calls, no automation.
// @match        https://kintara.gg/play*
// @match        https://www.kintara.gg/play*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

/*
  WHAT THIS IS
  A personal, client-side visual aid. It only restyles listing rows that are
  ALREADY on your screen (turns the background a faint bright-yellow when a
  listing beats your price). It sends nothing to the server, makes no network
  requests, and never clicks/buys for you. Server-side it is invisible — there
  is no traffic that differs from normal browsing. You still buy manually.

  HOW TO USE
  1. Install Tampermonkey (or Violentmonkey) in your browser.
  2. Create a new script, paste this whole file, save.
  3. Open the game (kintara.gg/play). A small "Deals: OFF" pill appears
     top-left. Click it to turn highlighting ON; click ⚙ to set your items.
  4. Open the Marketplace, switch the currency dropdown to $KINS, browse —
     matching rows light up yellow. Toggle OFF (or Alt+D) to go back to normal.

  RULES
  Per item: a name to match (e.g. "Wood"), a minimum units-per-dollar
  (higher = cheaper; a row qualifies when qty/price >= this), and a minimum
  order size (only highlight listings whose stack is at least this many units).
  Example: Wood, units/$ = 10000, min qty = 3000  -> a "Wood x10000 — $1.45"
  listing is 6897 units/$, so it would NOT light up (below 10000); a
  "Wood x5000 — $0.40" listing is 12500 units/$ and >=3000 qty, so it WOULD.

  NOTE: matching relies on the current game's marketplace markup
  (.kintara-mp__row) and the row text format ("Name xQty — seller   $price").
  If the game re-skins and rows stop highlighting, the selector/parse may need
  a small tweak.
*/

(function () {
  'use strict';

  // ---------- settings (persisted in localStorage) ----------
  const LS_KEY = 'kdh_settings_v1';
  const DEFAULTS = {
    enabled: false,
    rules: [
      { name: 'Wood',  upd: 10000, minQty: 3000, on: true  },
      { name: 'Stone', upd: 2000,  minQty: 0,    on: false },
      { name: 'Coal',  upd: 3500,  minQty: 0,    on: false },
    ],
  };
  let S = loadSettings();

  function loadSettings() {
    try {
      const raw = JSON.parse(localStorage.getItem(LS_KEY));
      if (raw && Array.isArray(raw.rules)) return { enabled: !!raw.enabled, rules: raw.rules };
    } catch (e) {}
    return JSON.parse(JSON.stringify(DEFAULTS));
  }
  function saveSettings() { try { localStorage.setItem(LS_KEY, JSON.stringify(S)); } catch (e) {} }

  // ---------- styles ----------
  const css = `
  .kdh-deal { background: rgba(255,221,0,0.22) !important;
    box-shadow: inset 5px 0 0 #ffd400, 0 0 0 1px rgba(255,221,0,0.5) !important; }
  #kdh-launch { position: fixed; top: 90px; left: 14px; z-index: 2147483647;
    display: flex; gap: 6px; align-items: center; font-family: system-ui, sans-serif; }
  .kdh-btn { cursor: pointer; border: 1px solid #3a4150; border-radius: 999px;
    background: #1b2230; color: #e7ecf2; font: 600 12px system-ui, sans-serif;
    padding: 6px 12px; line-height: 1; user-select: none; }
  .kdh-btn:hover { border-color: #ffd400; }
  #kdh-toggle.on { background: #2a2400; border-color: #ffd400; color: #ffe45e; }
  #kdh-toggle .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    background: #6b7280; margin-right: 6px; vertical-align: middle; }
  #kdh-toggle.on .dot { background: #ffd400; box-shadow: 0 0 8px #ffd400; }
  .kdh-gear { padding: 6px 9px; }
  #kdh-panel { position: fixed; top: 128px; left: 14px; z-index: 2147483647; width: 300px;
    background: #141a25; color: #e7ecf2; border: 1px solid #2c3445; border-radius: 14px;
    box-shadow: 0 18px 46px rgba(0,0,0,0.6); padding: 14px 14px 12px;
    font-family: system-ui, sans-serif; }
  #kdh-panel[hidden] { display: none; }
  .kdh-h { font: 700 14px system-ui; display: flex; justify-content: space-between; align-items: center; }
  .kdh-h #kdh-x { cursor: pointer; color: #8a97a8; padding: 2px 6px; border-radius: 6px; }
  .kdh-h #kdh-x:hover { background: rgba(255,255,255,0.08); color: #fff; }
  .kdh-sub { color: #8a97a8; font: 11.5px system-ui; margin: 3px 0 10px; line-height: 1.4; }
  .kdh-master { display: flex; align-items: center; gap: 8px; font: 600 13px system-ui;
    padding: 8px 10px; background: rgba(255,221,0,0.06); border: 1px solid #2c3445;
    border-radius: 9px; margin-bottom: 11px; cursor: pointer; }
  .kdh-cols { display: grid; grid-template-columns: 1fr 64px 64px 22px; gap: 6px;
    font: 600 10px system-ui; letter-spacing: .04em; text-transform: uppercase;
    color: #6f7c8e; padding: 0 2px 5px; }
  .kdh-cols span:nth-child(2), .kdh-cols span:nth-child(3) { text-align: right; }
  .kdh-rule { display: grid; grid-template-columns: 18px 1fr 64px 64px 22px; gap: 6px;
    align-items: center; margin-bottom: 6px; }
  .kdh-rule input[type=text], .kdh-rule input:not([type]) , .kdh-rule input[type=number] {
    background: #0e131c; color: #e7ecf2; border: 1px solid #2c3445; border-radius: 7px;
    padding: 6px 7px; font: 12px system-ui; width: 100%; }
  .kdh-rule input[type=number] { text-align: right; }
  .kdh-rule .r-del { cursor: pointer; color: #8a97a8; background: none; border: 0; font: 14px system-ui; padding: 0; }
  .kdh-rule .r-del:hover { color: #f06a6a; }
  .kdh-add { width: 100%; margin-top: 4px; text-align: center; border-style: dashed; }
  .kdh-foot { color: #6f7c8e; font: 10.5px system-ui; line-height: 1.5; margin-top: 9px; }
  `;
  const styleEl = document.createElement('style');
  styleEl.textContent = css;
  document.documentElement.appendChild(styleEl);

  // ---------- UI ----------
  const ui = document.createElement('div');
  ui.innerHTML = `
    <div id="kdh-launch">
      <button id="kdh-toggle" class="kdh-btn"><span class="dot"></span>Deals: OFF</button>
      <button id="kdh-gear" class="kdh-btn kdh-gear" title="settings">⚙</button>
    </div>
    <div id="kdh-panel" hidden>
      <div class="kdh-h">Deal Highlighter <span id="kdh-x">✕</span></div>
      <div class="kdh-sub">Lights up $KINS material listings that beat your price. KINS only · read-only · nothing sent to the server.</div>
      <label class="kdh-master"><input type="checkbox" id="kdh-en"> Highlighting enabled</label>
      <div class="kdh-cols"><span>Item</span><span>units / $</span><span>min qty</span><span></span></div>
      <div id="kdh-rules"></div>
      <button id="kdh-add" class="kdh-btn kdh-add">+ Add item</button>
      <div class="kdh-foot">"units / $" is your minimum (higher = cheaper). A row qualifies when its quantity ÷ $price is at least that, and its stack is ≥ "min qty". Shortcut: <b>Alt+D</b> toggles on/off.</div>
    </div>`;
  document.body.appendChild(ui);

  const $ = (id) => document.getElementById(id);
  const escAttr = (s) => String(s == null ? '' : s).replace(/"/g, '&quot;');

  function syncUI() {
    const tog = $('kdh-toggle');
    tog.classList.toggle('on', S.enabled);
    tog.lastChild.textContent = 'Deals: ' + (S.enabled ? 'ON' : 'OFF');
    $('kdh-en').checked = S.enabled;
  }
  function setEnabled(v) { S.enabled = v; saveSettings(); syncUI(); scan(); }

  function renderRules() {
    const box = $('kdh-rules');
    box.innerHTML = S.rules.map((r, i) => `
      <div class="kdh-rule" data-i="${i}">
        <input type="checkbox" class="r-on" ${r.on ? 'checked' : ''} title="enable this item">
        <input type="text" class="r-name" value="${escAttr(r.name)}" placeholder="Wood">
        <input type="number" class="r-upd" value="${r.upd}" min="0" step="100">
        <input type="number" class="r-qty" value="${r.minQty}" min="0" step="100">
        <button class="r-del" title="remove">✕</button>
      </div>`).join('');
    box.querySelectorAll('.kdh-rule').forEach((el) => {
      const i = +el.dataset.i;
      el.querySelector('.r-on').onchange  = (e) => { S.rules[i].on = e.target.checked; saveSettings(); scan(); };
      el.querySelector('.r-name').oninput = (e) => { S.rules[i].name = e.target.value; saveSettings(); scan(); };
      el.querySelector('.r-upd').oninput  = (e) => { S.rules[i].upd = parseFloat(e.target.value) || 0; saveSettings(); scan(); };
      el.querySelector('.r-qty').oninput  = (e) => { S.rules[i].minQty = parseFloat(e.target.value) || 0; saveSettings(); scan(); };
      el.querySelector('.r-del').onclick  = () => { S.rules.splice(i, 1); saveSettings(); renderRules(); scan(); };
    });
  }

  $('kdh-toggle').onclick = () => setEnabled(!S.enabled);
  $('kdh-gear').onclick   = () => { const p = $('kdh-panel'); p.hidden = !p.hidden; };
  $('kdh-x').onclick      = () => { $('kdh-panel').hidden = true; };
  $('kdh-en').onchange    = (e) => setEnabled(e.target.checked);
  $('kdh-add').onclick    = () => { S.rules.push({ name: '', upd: 1000, minQty: 0, on: true }); saveSettings(); renderRules(); scan(); };
  document.addEventListener('keydown', (e) => {
    if (e.altKey && (e.key === 'd' || e.key === 'D')) { e.preventDefault(); setEnabled(!S.enabled); }
    if (e.key === 'Escape') $('kdh-panel').hidden = true;
  });

  // ---------- parsing + highlight ----------
  // Parse a marketplace row that is already on screen. KINS-only ($ price),
  // skip reserved/locked rows. Returns null if it isn't a usable $KINS listing.
  function parseRow(row) {
    const t = (row.innerText || '').replace(/\s+/g, ' ').trim();
    if (!t) return null;
    if (/locked|reserved/i.test(t)) return null;     // can't buy these
    if (t.indexOf('$') === -1) return null;           // gold listing -> ignore (KINS only)
    const qm = t.match(/×\s*([\d.,]+)/) || t.match(/\b[xX]\s*([\d.,]+)/);
    const pm = t.match(/\$\s*([\d.,]+)/);
    if (!qm || !pm) return null;
    const qty = parseFloat(qm[1].replace(/,/g, ''));
    const price = parseFloat(pm[1].replace(/,/g, ''));
    if (!(qty > 0) || !(price > 0)) return null;
    let name = t.slice(0, t.indexOf(qm[0])).replace(/^[^A-Za-z]+/, '').trim();
    let itemType = '';
    if (row.dataset && row.dataset.kintaraItemType) itemType = row.dataset.kintaraItemType;
    else { const c = row.querySelector('[data-kintara-item-type]'); if (c) itemType = c.dataset.kintaraItemType || ''; }
    return { name, itemType, qty, price, upd: qty / price };
  }

  function matchRule(rule, info) {
    const key = (rule.name || '').trim().toLowerCase();
    if (!key) return false;
    return info.name.toLowerCase().startsWith(key) ||
           (info.itemType && info.itemType.toLowerCase().indexOf(key) !== -1);
  }

  function scan() {
    const rows = document.querySelectorAll('.kintara-mp__row');
    rows.forEach((row) => {
      let hit = false;
      if (S.enabled) {
        const info = parseRow(row);
        if (info) {
          const rule = S.rules.find((r) => r.on && matchRule(r, info));
          if (rule && info.qty >= (+rule.minQty || 0) && info.upd >= (+rule.upd || 0)) hit = true;
        }
      }
      row.classList.toggle('kdh-deal', hit);
    });
  }

  // Re-scan when the marketplace DOM changes (throttled to one animation frame).
  // The 3D world is WebGL/canvas and does not mutate the DOM, so this stays cheap.
  let pending = false;
  const schedule = () => { if (pending) return; pending = true; requestAnimationFrame(() => { pending = false; scan(); }); };
  new MutationObserver(schedule).observe(document.documentElement, { childList: true, subtree: true });
  // Safety net for virtualized/text-only updates the observer might miss (local CPU only, no network).
  setInterval(() => { if (S.enabled && document.querySelector('.kintara-mp__list')) scan(); }, 600);

  // ---------- boot ----------
  renderRules();
  syncUI();
  scan();
})();
