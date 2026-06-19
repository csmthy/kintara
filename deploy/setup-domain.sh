#!/usr/bin/env bash
# Add a domain + automatic HTTPS in front of KinScan, using Caddy.
# Run as root on the Droplet AFTER your domain's DNS A-records point at this Droplet:
#     bash /opt/kintara/deploy/setup-domain.sh yourdomain.com
# Caddy then listens on 80/443, auto-obtains a free Let's Encrypt certificate, and
# reverse-proxies to the app on localhost:8765. Re-run any time to change the domain.
set -euo pipefail

DOMAIN="${1:-}"
if [ -z "$DOMAIN" ]; then
  echo "usage: bash setup-domain.sh yourdomain.com"; exit 1
fi

if ! command -v caddy >/dev/null; then
  echo "==> installing Caddy"
  apt-get install -y debian-keyring debian-archive-keyring apt-transport-https curl gnupg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
    | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
    | tee /etc/apt/sources.list.d/caddy-stable.list >/dev/null
  apt-get update -y
  apt-get install -y caddy
fi

echo "==> writing Caddyfile for ${DOMAIN} (+ www)"
cat > /etc/caddy/Caddyfile <<EOF
${DOMAIN}, www.${DOMAIN} {
    reverse_proxy 127.0.0.1:8765
}
EOF

systemctl reload caddy || systemctl restart caddy
echo
echo "==> Caddy is configured for ${DOMAIN}."
echo "    As soon as DNS for ${DOMAIN} (and www) points at this Droplet's IP, Caddy will"
echo "    auto-issue an HTTPS certificate and your site will be live at:"
echo "        https://${DOMAIN}"
echo "    (first request after DNS resolves can take ~30s while the cert is issued)."
echo "    Watch it:  journalctl -u caddy -f"
