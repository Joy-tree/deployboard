#!/usr/bin/env bash
set -euo pipefail
mkdir -p data/local_storage/excel data/local_storage/access
chmod -R 775 data
docker compose up --build -d
sleep 30
docker compose ps
