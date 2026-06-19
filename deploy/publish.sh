#!/usr/bin/env bash
# One-command Mac -> GitHub -> DigitalOcean publish for KinScan.
# Usage:
#   bash deploy/publish.sh "Describe the change"
#   bash deploy/publish.sh                 # ok when there are no local changes
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRANCH="${KINSCAN_DEPLOY_BRANCH:-main}"
DEPLOY_HOST="${KINSCAN_DEPLOY_HOST:-root@159.203.132.20}"
DEPLOY_CMD="${KINSCAN_DEPLOY_CMD:-bash /opt/kintara/deploy/deploy.sh}"
MSG="${1:-}"

cd "$ROOT"

CURRENT_BRANCH="$(git branch --show-current)"
if [ "$CURRENT_BRANCH" != "$BRANCH" ]; then
  echo "!! Refusing to publish from '$CURRENT_BRANCH'. Switch to '$BRANCH' first."
  echo "   Or set KINSCAN_DEPLOY_BRANCH=$CURRENT_BRANCH if you really mean it."
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  if [ -z "$MSG" ]; then
    echo "!! You have local changes. Give this deploy a commit message:"
    echo '   bash deploy/publish.sh "Describe the change"'
    exit 1
  fi
  echo "==> committing local changes"
  git add -A
  git commit -m "$MSG"
else
  echo "==> no local changes to commit"
fi

echo "==> pushing $BRANCH to GitHub"
git push origin "$BRANCH"

echo "==> deploying on $DEPLOY_HOST"
ssh "$DEPLOY_HOST" "$DEPLOY_CMD"

echo "==> Published."
