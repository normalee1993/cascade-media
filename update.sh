#!/bin/bash
# Pull the latest cascade-media and recreate the container.
#
# Runs as root on Unraid, so it refuses to blindly apply upstream changes to the
# files that decide how the container (or this script) runs: if docker-compose.yml
# or update.sh changed upstream, the diff is shown and you must re-run with
# --yes after reviewing it (S33). Only fast-forward updates are applied.
#
# Usage: ./update.sh [--yes]
set -euo pipefail

APP_DIR="${APP_DIR:-/mnt/user/appdata/media-automation}"
ASSUME_YES=false
[ "${1:-}" = "--yes" ] && ASSUME_YES=true

cd "$APP_DIR"

git fetch --quiet origin
UPSTREAM="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}')"

if [ "$(git rev-parse HEAD)" = "$(git rev-parse "$UPSTREAM")" ]; then
    echo "Repository already up to date."
else
    echo "Incoming commits:"
    git log --oneline "HEAD..$UPSTREAM"

    SENSITIVE="$(git diff --name-only HEAD "$UPSTREAM" -- docker-compose.yml update.sh)"
    if [ -n "$SENSITIVE" ] && [ "$ASSUME_YES" != true ]; then
        echo
        echo "These files change how the container (or this script) runs as root:"
        echo "$SENSITIVE"
        echo
        git --no-pager diff HEAD "$UPSTREAM" -- docker-compose.yml update.sh
        echo
        echo "Review the diff above, then re-run: $0 --yes"
        exit 1
    fi

    git merge --ff-only "$UPSTREAM"
fi

docker compose pull
docker compose up -d --force-recreate
docker image prune -f
