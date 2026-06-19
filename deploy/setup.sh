#!/usr/bin/env bash
# One-time DigitalOcean Droplet bootstrap for KinScan.
# Run as root on the Droplet:   bash setup.sh <git-repo-url>
# (or, if you copied the code up with rsync instead of git, run:  bash setup.sh)
set -euo pipefail

REPO="${1:-}"
CODE=/opt/kintara
DATA=/opt/kintara-data

echo "==> installing system packages"
apt-get update -y
apt-get install -y python3 python3-venv git

echo "==> creating service user + dirs"
id kintara &>/dev/null || useradd -r -s /usr/sbin/nologin kintara
mkdir -p "$DATA"

echo "==> fetching code into $CODE"
if [ -n "$REPO" ]; then
  if [ -d "$CODE/.git" ]; then git -C "$CODE" pull; else git clone "$REPO" "$CODE"; fi
elif [ ! -d "$CODE" ]; then
  echo "!! No repo URL given and $CODE doesn't exist."
  echo "   rsync the code up first, e.g. from your Mac:"
  echo "     rsync -avz --exclude-from=.gitignore ./ root@<DROPLET_IP>:/opt/kintara/"
  exit 1
fi

echo "==> python venv + deps"
python3 -m venv "$CODE/venv"
"$CODE/venv/bin/pip" install --upgrade pip
"$CODE/venv/bin/pip" install -r "$CODE/requirements.txt"

echo "==> permissions"
chown -R kintara:kintara "$CODE" "$DATA"

echo "==> installing systemd service"
cp "$CODE/deploy/kintara.service" /etc/systemd/system/kintara.service
systemctl daemon-reload
systemctl enable --now kintara

# open the firewall (8765 = app for now/verification; 80+443 = Caddy/HTTPS once you add a domain)
if command -v ufw >/dev/null && ufw status | grep -q "Status: active"; then
  ufw allow 8765/tcp || true; ufw allow 80/tcp || true; ufw allow 443/tcp || true
fi

sleep 2
systemctl --no-pager --full status kintara | head -n 6 || true
IP="$(curl -s ifconfig.me || echo YOUR_DROPLET_IP)"
echo
echo "==> Done. Verify it works now at:  http://${IP}:8765/"
echo "    Add your domain + HTTPS next:  bash /opt/kintara/deploy/setup-domain.sh <yourdomain.com>"
echo "    Logs:     journalctl -u kintara -f"
echo "    Update:   bash /opt/kintara/deploy/deploy.sh"
