# Deploying KinScan (24/7)

This app is **stateful and always-on**: one long-running process serves the dashboard
*and* runs the background pollers that build `kintara.db`. That rules out serverless
hosts (Vercel/Netlify/Cloudflare Pages) — they have no persistent process or disk.

What you need from a host:
1. **An always-on container/VM** (runs the process continuously — not request-triggered).
2. **A persistent volume** mounted where the DB lives, so it survives redeploys.

The repo already ships everything: `Dockerfile`, `requirements.txt`, the embedded
frontend, `world_map.jpg`, and the realm PNGs in `MapImages/`.

---

## Important: do NOT use gunicorn / multiple workers
The background loops are threads started in `main()`. Run it as the single process
`python kintara_tracker.py` (the Docker `CMD` already does this). Forking workers would
multiply the pollers and hammer kintara.

## Persistence (the one thing you must get right)
Mount a volume and point `KINTARA_DB` at it. The Dockerfile defaults to
`KINTARA_DB=/data/kintara.db` and `WORKDIR /data`, so both the DB **and** `icons_cache/`
land on the volume. If you skip the volume, the DB is wiped on every deploy/restart.

## Config (env vars — all optional, defaults are 24/7-friendly)
| Var | Default | What |
|---|---|---|
| `KINTARA_DB` | `/data/kintara.db` | DB path — put on the volume |
| `PORT` | `8765` | listen port (most hosts inject this) |
| `KINTARA_HOST` | `0.0.0.0` (in Docker) | bind address |
| `POLL_INTERVAL` | `90` | listing poll seconds |
| `KINTARA_MIN_GAP` | `0.5` | global min seconds between **any** two kintara.gg requests (≈ ≤2 req/s total) |
| `KINTARA_BACKOFF` | `45` | pause after a 429/403 rate-limit |
| `STATS_STALE_HOT` | `120` | re-check actively-traded items this often (sales feed granularity) |
| `STATS_STALE_COLD` | `900` | re-check quiet items this often |

Politeness is enforced globally by `KINTARA_MIN_GAP` (a shared pacer across all loops)
plus 429/403 backoff, so 24/7 operation can't burst the marketplace. Raise the gaps if
you ever see 403/429 in the logs.

---

## Option 0 — DigitalOcean Droplet (the chosen setup)
A $6 Droplet is a full Linux VM with a real persistent disk, so the SQLite DB just
lives on it — no volume config needed. Scripts are in `deploy/`.

**Layout it creates:** code in `/opt/kintara` (the git clone + venv), data in
`/opt/kintara-data` (DB + `icons_cache/`, so updates never touch your data). Runs as a
non-root `kintara` user under systemd, auto-restart + start-on-boot, bound to port 80.

### First-time setup (≈10 min)
1. Get the code onto a GitHub repo (recommended) — from your Mac in this folder:
   ```bash
   git init && git add -A && git commit -m "kintara market"
   git remote add origin git@github.com:<you>/kintara.git && git push -u origin main
   ```
   (`kintara.db` / `icons_cache/` are already git-ignored.)
   *No GitHub?* skip this and rsync instead:
   `rsync -avz --exclude-from=.gitignore ./ root@<DROPLET_IP>:/opt/kintara/`
2. SSH into the Droplet and run the bootstrap:
   ```bash
   ssh root@<DROPLET_IP>
   # with GitHub:
   curl -fsSL https://raw.githubusercontent.com/<you>/kintara/main/deploy/setup.sh -o setup.sh
   bash setup.sh https://github.com/<you>/kintara.git
   # (rsync route: you already copied the code up, so just: bash /opt/kintara/deploy/setup.sh)
   ```
3. It prints the URL. **Open `http://<DROPLET_IP>/` in any browser — that's your site.**

### Updating later from your Mac (one command)
After editing files locally, run:
```bash
bash deploy/publish.sh "what changed"
```
This commits local changes, pushes `main` to GitHub, SSHes into the Droplet, runs the
server deploy script, and returns you to your Mac. It defaults to
`root@159.203.132.20`; override with `KINSCAN_DEPLOY_HOST=root@<ip>` if the Droplet
changes.

### Updating later on the Droplet
If you're already SSH'd into the Droplet, run:
```bash
bash /opt/kintara/deploy/deploy.sh      # git pull (or your rsync) + deps + restart; data untouched
```
*(Optional "just `git push`" auto-deploy: add a GitHub Action that SSHes in and runs that
script — ask and I'll generate `.github/workflows/deploy.yml`.)*

### Seeing it / managing it
- **The website:** `http://<DROPLET_IP>/` (find the IP on the Droplet's page in the DO panel).
- **Live logs:** `journalctl -u kintara -f`
- **Restart / stop:** `systemctl restart kintara` / `systemctl stop kintara`
- **A nice URL + HTTPS (optional):** point a domain's A-record at the Droplet IP, then put
  Caddy in front (`apt install caddy`, one `reverse_proxy localhost:80` line) for automatic
  HTTPS at `https://yourdomain`. Not required — the IP works immediately.
- **Firewall:** the setup opens port 80 if `ufw` is active. If you use a DO Cloud Firewall,
  allow inbound TCP **80** (and **443** if you add HTTPS).

---

## Option A — Fly.io (cheap ~$2–4/mo, push with one command)
```bash
# one-time
brew install flyctl          # or: curl -L https://fly.io/install.sh | sh
fly auth signup              # or: fly auth login
fly launch --no-deploy       # detects the Dockerfile; pick a name/region, say NO to DBs
fly volume create data --size 1 --region <your-region>   # 1 GB persistent volume
```
Add the volume mount to the generated `fly.toml`:
```toml
[mounts]
  source = "data"
  destination = "/data"

[env]
  KINTARA_DB = "/data/kintara.db"
  KINTARA_HOST = "0.0.0.0"

[http_service]
  internal_port = 8765
  force_https = true
  auto_stop_machines = false   # IMPORTANT: stay always-on so the DB keeps building
  min_machines_running = 1
```
Then, every time you change code:
```bash
fly deploy
```
That's the whole "push to the website" loop. The volume keeps `kintara.db` across deploys.

## Option B — Railway (easiest, ~$5/mo)
1. Push this repo to GitHub.
2. railway.app → New Project → Deploy from GitHub repo (it builds the Dockerfile).
3. Add a **Volume**, mount path `/data`.
4. Variables: `KINTARA_DB=/data/kintara.db` (Railway sets `PORT` for you).
5. Every `git push` auto-deploys. Done.

## Option C — Oracle Cloud "Always Free" (free forever, more setup)
A genuinely free always-on VM (Ampere ARM) with free block storage.
```bash
# on the VM (Ubuntu), after installing docker:
git clone <your-repo> kintara && cd kintara
docker build -t kintara .
docker run -d --restart=always -p 80:8765 \
  -v /opt/kintara-data:/data -e PORT=8765 kintara
# redeploy after code changes:
git pull && docker build -t kintara . && docker restart <container>
```
$0/month, but you manage the VM and deploys are `git pull` + rebuild (scriptable).

## Render
Works only on a **paid** Web Service ($7) + a **persistent disk** ($1+) mounted at
`/data`. The free tier sleeps after 15 min idle *and* has no persistent disk, so the
DB-building stops and the DB is wiped — don't use free here.

---

## After it's up
- First boot creates an empty DB and starts filling it; the dashboard is usable
  immediately and the sales/history data accrues over the first ~30–60 min.
- It's **public** by default (anyone with the URL). Ask to add a password gate if wanted.
- Watch the logs for `429/403`; if you see them, bump `KINTARA_MIN_GAP` / `POLL_INTERVAL`.
