# KinScan — laptop → live website with your domain (foolproof guide)

You'll do three things: (1) put the code on GitHub, (2) set it up on your DigitalOcean
Droplet, (3) point your domain at it with HTTPS. Copy/paste the commands exactly,
replacing the **ALL-CAPS placeholders**:

- `GITHUB_USER`  — your GitHub username (make one free at github.com if needed)
- `DROPLET_IP`   — your Droplet's public IP (DigitalOcean → Droplets → your droplet)
- `yourdomain.com` — the domain you bought

Lines you run **on your Mac** vs **on the Droplet (after SSH)** are labeled.

---

## Phase 1 — Put the code on GitHub (on your Mac)

1. Open **Terminal** (Cmd-Space, type "Terminal", Enter).

2. Install the GitHub command-line tool (and git). Paste this; if it says
   `brew: command not found`, first install Homebrew with the one-liner from https://brew.sh,
   then re-run it:
   ```bash
   brew install gh git
   ```

3. Log in to GitHub from the terminal (opens your browser to approve):
   ```bash
   gh auth login
   ```
   Choose: **GitHub.com** → **HTTPS** → **Yes** (authenticate Git) → **Login with a web
   browser** → copy the one-time code → approve in the browser.

4. Tell git who you are (once ever):
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "you@example.com"
   ```

5. Go to the project and publish it to a new GitHub repo:
   ```bash
   cd /Users/connorsmith/Desktop/kintara
   git init
   git branch -M main
   git add .
   git commit -m "KinScan"
   gh repo create kintara --public --source=. --remote=origin --push
   ```
   (Public so the Droplet can pull it with no extra login. Nothing secret is in the repo,
   and `kintara.db` is already excluded.)

   ✅ **Check:** `gh repo view --web` opens the repo — you should see your files there.

---

## Phase 2 — Get into your Droplet (on your Mac)

```bash
ssh root@DROPLET_IP
```
- First time it asks "are you sure…" → type `yes`, Enter.
- If it asks for a password, use the root password DigitalOcean emailed you (it may make
  you set a new one). If you added an SSH key when creating the droplet, it just logs in.

Your prompt now starts with `root@...` — you're **on the Droplet**. (Everything in Phase 3
and 5 runs here. Type `exit` to return to your Mac.)

---

## Phase 3 — Install and run it (on the Droplet)

```bash
curl -fsSL https://raw.githubusercontent.com/GITHUB_USER/kintara/main/deploy/setup.sh -o setup.sh
bash setup.sh https://github.com/GITHUB_USER/kintara.git
```
This installs Python, pulls your code, sets up the auto-restarting service, and prints a URL.

✅ **Check:** open **`http://DROPLET_IP:8765/`** in your browser — the dashboard loads and
starts filling its database. (If it doesn't load, see Troubleshooting → firewall.)

---

## Phase 4 — Point your domain at the Droplet (in your domain registrar)

Log in wherever you bought the domain (GoDaddy / Namecheap / Cloudflare / Google, etc.),
find **DNS settings / records**, and add **two A records**:

| Type | Host / Name | Value (points to) | TTL |
|------|-------------|-------------------|-----|
| A | `@`  (means the root domain) | `DROPLET_IP` | default |
| A | `www` | `DROPLET_IP` | default |

Save. DNS usually updates within minutes (can take up to a couple hours).

✅ **Check (on your Mac):** `dig +short yourdomain.com` should print your `DROPLET_IP`.
(Or use https://dnschecker.org.) Wait until it does before Phase 5.

---

## Phase 5 — Add the domain + HTTPS (on the Droplet)

Back in your SSH session:
```bash
bash /opt/kintara/deploy/setup-domain.sh yourdomain.com
```
This installs Caddy, which automatically gets a free HTTPS certificate and serves your
site at your domain.

✅ **Check:** open **`https://yourdomain.com`** — padlock 🔒 + your dashboard. The very
first load can take ~30 seconds while the certificate is issued. Done! 🎉

---

## Updating the site later (after you change code)

**On your Mac**, from the project folder:
```bash
cd /Users/connorsmith/Desktop/kintara
git add .
git commit -m "what you changed"
git push
```
**Then on the Droplet:**
```bash
ssh root@DROPLET_IP
bash /opt/kintara/deploy/deploy.sh
```
The code updates and the service restarts; your collected data (`kintara.db`) is untouched.

*(Want this fully automatic — site updates the moment you `git push`, no SSH? Ask for the
GitHub Action and I'll add it.)*

---

## Troubleshooting

- **`http://DROPLET_IP:8765` won't load** — you likely have a **DigitalOcean Cloud Firewall**
  attached. In DO → Networking → Firewalls, allow inbound TCP on **22, 80, 443, 8765**.
- **Domain not loading / no certificate** — DNS hasn't propagated yet (re-check `dig`), or
  ports 80/443 are blocked (firewall above). Watch Caddy: `journalctl -u caddy -f`.
- **See what the app is doing** — `journalctl -u kintara -f` (live logs). Restart with
  `systemctl restart kintara`.
- **Rate-limited by kintara (403/429 in logs)** — edit `/etc/systemd/system/kintara.service`,
  raise `KINTARA_MIN_GAP` (e.g. to `1.0`) and/or `POLL_INTERVAL`, then
  `systemctl daemon-reload && systemctl restart kintara`.
