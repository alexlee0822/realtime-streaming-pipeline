#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "Starting realtime streaming stack..."
docker compose up -d --build

echo
echo "Services:"
echo "  Grafana  : http://localhost:3000  (admin / admin)"
echo "  Postgres : localhost:5432  (stream / stream / clickstream)"
echo "  Kafka UI : docker compose --profile ui up -d   → http://localhost:8080"
echo
echo "Kafka topics: clickstream.raw → clickstream.cleaned → clickstream.curated (+ clickstream.dlq)"
echo
docker compose ps
