# Realtime Clickstream Pipeline
<img width="1609" height="865" alt="image" src="https://github.com/user-attachments/assets/8c080832-00c8-494d-81a7-89322d5f3918" />

Kafka + Spark Structured Streaming pipeline that takes live clickstream events from **raw → cleaned → curated**, lands curated metrics in **Delta Lake** and **Postgres**, and visualises them in **Grafana**.

## Architecture

```text
Producer ──► clickstream.raw ──► Spark clean ──► clickstream.cleaned ──► Spark curate ──► Postgres + Delta
                     │                                    │                      │
                     └──► clickstream.dlq                 └──► Delta silver       └──► clickstream.curated
                                                                              Grafana ◄── Postgres
```

| Stage | Topic / sink | What happens |
|---|---|---|
| Raw | `clickstream.raw` | Synthetic page_view / click / add_to_cart / purchase events |
| Clean | `clickstream.cleaned`, Delta `silver/events_cleaned`, `clickstream.dlq` | Schema validation, null checks, normalisation |
| Curate | Postgres `curated.*`, Delta `gold/*`, `clickstream.curated` | 1-minute window metrics, funnel, device×country |

## Quick start

```bash
chmod +x scripts/*.sh
./scripts/start.sh
```

| UI | URL |
|---|---|
| Grafana | http://localhost:3000 (`admin` / `admin`) |
| Kafka UI | http://localhost:8080 |

Stop and wipe volumes:

```bash
./scripts/stop.sh
```

## Stack

- **Kafka** (KRaft) – three-stage topics + DLQ
- **Spark 3.5** Structured Streaming – clean + curate jobs
- **Delta Lake** – silver events + gold aggregates
- **Postgres 16** – queryable curated tables for Grafana
- **Grafana** – realtime clickstream dashboard

## Project layout

```text
producer/          clickstream event generator
spark/jobs/        clean_stream.py, curate_stream.py
postgres/          curated schema
grafana/           provisioned datasource + dashboard
scripts/           start / stop helpers
docker-compose.yml full local stack
```
