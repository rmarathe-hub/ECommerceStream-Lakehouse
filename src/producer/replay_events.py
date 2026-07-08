#!/usr/bin/env python3
"""Replay sampled e-commerce CSV events into Redpanda/Kafka as JSON."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

DEFAULT_INPUT = Path("data/raw/events_1m.csv")
DEFAULT_TOPIC = "ecommerce_events"
DEFAULT_BOOTSTRAP = "localhost:19092"
DEFAULT_BATCH_SIZE = 1000

LOG = logging.getLogger("replay_events")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay sampled CSV e-commerce events to a Kafka/Redpanda topic."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Sampled CSV input path (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC),
        help=f"Kafka topic name (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of events to send (default: all rows).",
    )
    parser.add_argument(
        "--rate-per-second",
        type=float,
        default=None,
        help="Throttle send rate, e.g. 1000 for ~1000 events/sec.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Flush to Kafka every N messages (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=10_000,
        help="Log progress every N events (default: 10000).",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)


def normalize_row(row: dict[str, str]) -> dict[str, Any]:
    """Convert CSV strings to JSON-friendly types where possible."""
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or value == "":
            payload[key] = None
            continue
        if key in {"product_id", "category_id", "user_id"}:
            try:
                payload[key] = int(value)
            except ValueError:
                payload[key] = value
            continue
        if key == "price":
            try:
                payload[key] = float(value)
            except ValueError:
                payload[key] = value
            continue
        payload[key] = value
    return payload


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=50,
            batch_size=32_768,
            buffer_memory=64 * 1024 * 1024,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            f"Could not connect to Kafka at {bootstrap_servers}. "
            "Is the local stack running? Try: make up"
        ) from exc


def wait_for_futures(futures: list) -> None:
    for future in futures:
        try:
            future.get(timeout=60)
        except KafkaError as exc:
            raise RuntimeError(f"Failed to deliver event batch: {exc}") from exc
    futures.clear()


def replay_events(
    producer: KafkaProducer,
    input_path: Path,
    topic: str,
    limit: int | None,
    rate_per_second: float | None,
    batch_size: int,
    log_every: int,
) -> int:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    min_interval = (1.0 / rate_per_second) if rate_per_second and rate_per_second > 0 else 0.0
    sent = 0
    started = time.perf_counter()
    last_send_at = 0.0
    pending_futures: list = []

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"Input file has no header: {input_path}")

        for row in reader:
            if limit is not None and sent >= limit:
                break

            payload = normalize_row(row)
            event_id = str(payload.get("event_id", f"row_{sent + 1}"))

            if min_interval > 0:
                now = time.perf_counter()
                elapsed = now - last_send_at
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)

            pending_futures.append(producer.send(topic, key=event_id, value=payload))
            sent += 1
            last_send_at = time.perf_counter()

            if sent % batch_size == 0:
                producer.flush()
                wait_for_futures(pending_futures)

            if sent == 1 or sent % log_every == 0:
                elapsed_total = max(time.perf_counter() - started, 1e-6)
                current_rate = sent / elapsed_total
                LOG.info(
                    "sent %s events to topic=%s (%.1f events/sec)",
                    f"{sent:,}",
                    topic,
                    current_rate,
                )

    if pending_futures:
        producer.flush()
        wait_for_futures(pending_futures)

    return sent


def main() -> int:
    configure_logging()
    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        LOG.error("--limit must be a positive integer")
        return 1
    if args.rate_per_second is not None and args.rate_per_second <= 0:
        LOG.error("--rate-per-second must be positive")
        return 1
    if args.batch_size <= 0:
        LOG.error("--batch-size must be a positive integer")
        return 1

    LOG.info(
        "Replaying events input=%s topic=%s bootstrap=%s limit=%s rate=%s batch=%s",
        args.input,
        args.topic,
        args.bootstrap_servers,
        args.limit if args.limit is not None else "all",
        args.rate_per_second if args.rate_per_second is not None else "unlimited",
        args.batch_size,
    )

    started = time.perf_counter()
    producer = None
    sent = 0
    try:
        producer = create_producer(args.bootstrap_servers)
        sent = replay_events(
            producer=producer,
            input_path=args.input,
            topic=args.topic,
            limit=args.limit,
            rate_per_second=args.rate_per_second,
            batch_size=args.batch_size,
            log_every=args.log_every,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        LOG.error("%s", exc)
        return 1
    finally:
        if producer is not None:
            producer.close()

    elapsed = max(time.perf_counter() - started, 1e-6)
    LOG.info(
        "Finished: sent %s events to %s in %.1fs (%.1f events/sec)",
        f"{sent:,}",
        args.topic,
        elapsed,
        sent / elapsed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
