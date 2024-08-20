#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "Starting realtime streaming stack..."
docker compose up -d --build

echo
echo "Services:"
echo "  Kafka UI : http://localhost:8080"
echo "  Grafana  : http://localhost:3000  (admin / admin)"
echo "  Spark UI : http://localhost:8081"
echo "  Postgres : localhost:5432  (stream / stream / clickstream)"
echo
echo "Kafka topics: clickstream.raw → clickstream.cleaned → clickstream.curated (+ clickstream.dlq)"
echo
docker compose ps
