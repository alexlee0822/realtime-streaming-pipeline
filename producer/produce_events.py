#!/usr/bin/env python3
"""Emit synthetic e-commerce clickstream events into Kafka (clickstream.raw)."""

from __future__ import annotations

import json
import os
import random
import signal
import string
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("TOPIC", "clickstream.raw")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "20"))

PAGES = [
    "/",
    "/products",
    "/products/headphones",
    "/products/laptop",
    "/products/keyboard",
    "/cart",
    "/checkout",
    "/search",
    "/account",
    "/help",
]
EVENT_TYPES = [
    ("page_view", 0.55),
    ("click", 0.25),
    ("add_to_cart", 0.12),
    ("purchase", 0.05),
    ("search", 0.03),
]
DEVICES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["GB", "US", "DE", "FR", "IE", "NL", "CA", "AU"]
UTM_SOURCES = ["google", "bing", "newsletter", "direct", "twitter", "affiliate"]

_running = True


def _stop(*_args) -> None:
    global _running
    _running = False


def weighted_choice(pairs):
    values, weights = zip(*pairs)
    return random.choices(values, weights=weights, k=1)[0]


def session_id() -> str:
    return "sess_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


def maybe_corrupt(event: dict) -> dict:
    """Inject a small % of bad payloads so the clean job has work to do."""
    roll = random.random()
    if roll < 0.02:
        event.pop("event_type", None)
    elif roll < 0.035:
        event["user_id"] = None
    elif roll < 0.045:
        event["ts"] = "not-a-timestamp"
    return event


def build_event(active_sessions: dict[str, dict]) -> dict:
    # Reuse sessions to make funnel metrics look real
    if active_sessions and random.random() < 0.7:
        sid = random.choice(list(active_sessions))
        meta = active_sessions[sid]
    else:
        sid = session_id()
        meta = {
            "user_id": f"user_{random.randint(1000, 9999)}",
            "device": random.choice(DEVICES),
            "country": random.choice(COUNTRIES),
            "utm_source": random.choice(UTM_SOURCES),
        }
        active_sessions[sid] = meta
        if len(active_sessions) > 200:
            active_sessions.pop(next(iter(active_sessions)))

    event_type = weighted_choice(EVENT_TYPES)
    page = random.choice(PAGES)
    if event_type == "purchase":
        page = "/checkout"
    elif event_type == "add_to_cart":
        page = random.choice([p for p in PAGES if p.startswith("/products/")])

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        "user_id": meta["user_id"],
        "session_id": sid,
        "page": page,
        "referrer": random.choice(PAGES + ["https://google.com", "https://bing.com", ""]),
        "device": meta["device"],
        "country": meta["country"],
        "utm_source": meta["utm_source"],
        "latency_ms": max(10, int(random.gauss(120, 40))),
        "amount": round(random.uniform(19.0, 499.0), 2) if event_type == "purchase" else 0.0,
    }
    return maybe_corrupt(event)


def delivery_report(err, _msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "linger.ms": 20,
            "batch.num.messages": 100,
            "compression.type": "lz4",
        }
    )
    active_sessions: dict[str, dict] = {}
    interval = 1.0 / max(EVENTS_PER_SEC, 0.1)
    print(f"producing to {TOPIC} via {BOOTSTRAP} at ~{EVENTS_PER_SEC}/s")

    while _running:
        event = build_event(active_sessions)
        key = event.get("session_id") or "unknown"
        producer.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(interval)

    producer.flush()
    print("producer stopped")


if __name__ == "__main__":
    main()
