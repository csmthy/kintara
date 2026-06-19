#!/usr/bin/env bash
# Update KinScan on the Droplet to the latest code, then restart.
# Run as root on the Droplet:   bash /opt/kintara/deploy/deploy.sh
# (If you push code with rsync instead of git, just rsync over /opt/kintara/ first,
#  then run this — it'll skip the git pull and reinstall deps + restart.)
set -euo pipefail
CODE=/opt/kintara

if [ -d "$CODE/.git" ]; then
  echo "==> git pull"
  git config --global --add safe.directory "$CODE" || true
  git -C "$CODE" pull
fi

echo "==> updating deps"
"$CODE/venv/bin/pip" install -q -r "$CODE/requirements.txt"

echo "==> fixing ownership + restarting"
chown -R kintara:kintara "$CODE" /opt/kintara-data
systemctl restart kintara
sleep 2
systemctl --no-pager --full status kintara | head -n 5
echo "==> Updated. Your data (kintara.db) was untouched."
