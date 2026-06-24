#!/bin/bash
set -e
cd /mnt/user/appdata/media-automation
git pull
docker compose pull
docker compose up -d --force-recreate
docker image prune -f
