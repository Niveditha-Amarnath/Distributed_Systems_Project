"""
producer.py
===========
Kafka producer that:
  1. Auto-creates the ``user-events`` topic (3 partitions, RF 1).
  2. Generates 1 000 (configurable) synthetic JSON events via
     event_generator.py.
  3. Routes every message to the correct partition using a custom
     content-type partitioner:
       login   → partition 0
       payment → partition 1
       order   → partition 2
  4. Persists per-partition counters and throughput samples to the
     metrics/ directory via MetricsManager.

Usage
-----
  python producer.py                                         # defaults
  python producer.py --events 500
  python producer.py --bootstrap localhost:9092 --events 2000
  python producer.py --topic my-topic --delay 0.01
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging setup (console + file)
# ---------------------------------------------------------------------------
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "producer.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("Producer")

# ---------------------------------------------------------------------------
# Third-party imports (confluent-kafka)
# ---------------------------------------------------------------------------
try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    log.error(
        "confluent-kafka is not installed. Run: pip install confluent-kafka"
    )
    sys.exit(1)

from event_generator import generate_events, get_type_distribution
from metrics_manager import MetricsManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC     = "user-events"
DEFAULT_EVENTS    = 1_000
DEFAULT_DELAY     = 0.005          # seconds between messages (≈ 200 msg/s)

# Partition routing map – the heart of the custom partitioner
PARTITION_MAP: Dict[str, int] = {
    "login"  : 0,
    "payment": 1,
    "order"  : 2,
}


# ---------------------------------------------------------------------------
# Custom partitioner
# ---------------------------------------------------------------------------

def content_type_partitioner(
    key              : bytes,
    all_partitions   : List[int],
    available_partitions: List[int],
) -> int:
    """
    Route a message to a partition based on the event type encoded in *key*.

    The confluent-kafka Producer calls this function for every message.
    If the key is unknown, fall back to the first available partition.

    Parameters
    ----------
    key                 : Message key as bytes (contains the event type string).
    all_partitions      : All partition IDs for this topic.
    available_partitions: Currently available (non-paused) partitions.

    Returns
    -------
    int – The target partition ID.
    """
    if key is None:
        return available_partitions[0]

    event_type = key.decode("utf-8")
    partition  = PARTITION_MAP.get(event_type)

    if partition is not None and partition in all_partitions:
        return partition

    # Fallback: use the first available partition
    log.warning("Unknown event type '%s'; routing to partition %d",
                event_type, available_partitions[0])
    return available_partitions[0]


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------

def ensure_topic(
    bootstrap_servers: str,
    topic            : str,
    num_partitions   : int = 3,
    replication      : int = 1,
) -> None:
    """
    Create the Kafka topic if it does not already exist.
    Safe to call even when the topic is already present.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Check existing topics
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        log.info("Topic '%s' already exists – skipping creation.", topic)
        return

    log.info(
        "Creating topic '%s' (partitions=%d, replication=%d) …",
        topic, num_partitions, replication,
    )
    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,
        replication_factor=replication,
    )
    futures = admin.create_topics([new_topic])
    for t, future in futures.items():
        try:
            future.result()
            log.info("Topic '%s' created successfully.", t)
        except Exception as exc:
            log.error("Failed to create topic '%s': %s", t, exc)
            raise


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

# Shared counter for delivery confirmations
_delivered : int = 0
_failed    : int = 0


def delivery_callback(err, msg) -> None:
    """Called by librdkafka for every produce() after broker ack/error."""
    global _delivered, _failed
    if err is not None:
        _failed += 1
        log.error("Delivery failed for event on partition %d: %s",
                  msg.partition() if msg else -1, err)
    else:
        _delivered += 1
        if _delivered % 100 == 0:
            log.info(
                "✓ Delivered %d messages (partition %d, offset %d)",
                _delivered, msg.partition(), msg.offset(),
            )


# ---------------------------------------------------------------------------
# Producer main
# ---------------------------------------------------------------------------

def run_producer(
    bootstrap_servers : str  = DEFAULT_BOOTSTRAP,
    topic             : str  = DEFAULT_TOPIC,
    num_events        : int  = DEFAULT_EVENTS,
    inter_message_delay: float = DEFAULT_DELAY,
    seed              : int  = 42,
) -> None:
    """
    End-to-end producer flow:
      1. Ensure topic exists.
      2. Generate events.
      3. Produce to Kafka with custom partitioner.
      4. Flush and report.
    """
    global _delivered, _failed
    _delivered = 0
    _failed    = 0

    mm = MetricsManager()
    mm.reset_all()
    log.info("Metrics reset for new producer run.")

    # ── Step 1: Topic ──────────────────────────────────────────────────────
    ensure_topic(bootstrap_servers, topic)

    # ── Step 2: Events ────────────────────────────────────────────────────
    log.info("Generating %d synthetic events …", num_events)
    events = generate_events(num_events, seed=seed)
    dist   = get_type_distribution(events)
    log.info("Event distribution: %s", dist)

    # ── Step 3: Producer configuration ───────────────────────────────────
    producer_conf = {
        "bootstrap.servers"        : bootstrap_servers,
       
        # Reliability: wait for leader + 1 replica acknowledgement
        "acks"                     : "all",
        # Retry on transient errors
        "retries"                  : 3,
        "retry.backoff.ms"         : 200,
        # Batching for throughput
        "linger.ms"                : 5,
        "batch.size"               : 32_768,
        # Message compression
        "compression.type"         : "gzip",
        # Message timeout
        "message.timeout.ms"       : 10_000,
    }

    producer = Producer(producer_conf)
    log.info("Producer connected to %s", bootstrap_servers)

    # ── Step 4: Produce ───────────────────────────────────────────────────
    # ── Step 4: Produce ───────────────────────────────────────────────────
    partition_counts: Dict[str, int] = {"0": 0, "1": 0, "2": 0}
    start_time = time.monotonic()
    last_sample = start_time
    last_count = 0

    log.info("Producing %d events to topic '%s' …", num_events, topic)

    partition_map = {
        "login": 0,
        "payment": 1,
        "order": 2
    }

    for idx, event in enumerate(events):
        event_type = event["type"]

        partition = partition_map.get(event_type, 0)

        producer.produce(
    topic=topic,
    key=str(event.get("event_id", idx)),
    value=json.dumps(event),
    partition=partition
)

        # Track partition counts
        partition_counts[str(partition)] += 1

        # Trigger delivery callbacks
        if idx % 10 == 0:
            producer.poll(0)

        # Record throughput every 50 messages
        if idx > 0 and idx % 50 == 0:
            now = time.monotonic()
            elapsed = now - last_sample

            delta_msgs = (idx + 1) - last_count
            mps = delta_msgs / elapsed if elapsed > 0 else 0.0

            mm.record_producer_sample(
                total_sent=idx + 1,
                mps=mps
            )

            log.debug(
                "Throughput: %.1f msg/s (total sent: %d)",
                mps,
                idx + 1
            )

            last_sample = now
            last_count = idx + 1

        # Optional throttling
        if inter_message_delay > 0:
            time.sleep(inter_message_delay)

    # ── Step 5: Flush ─────────────────────────────────────────────────────
    log.info("Flushing remaining messages …")
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        log.warning("%d messages were NOT delivered after flush timeout.", remaining)

    # ── Step 6: Persist final metrics ────────────────────────────────────
    for partition_str, count in partition_counts.items():
        mm.increment_partition_count(int(partition_str), count)

    total_time = time.monotonic() - start_time
    overall_mps = num_events / total_time if total_time > 0 else 0.0
    mm.record_producer_sample(total_sent=num_events, mps=overall_mps)

    # ── Step 7: Summary ───────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Producer run complete.")
    log.info("  Events generated  : %d", num_events)
    log.info("  Messages delivered: %d", _delivered)
    log.info("  Messages failed   : %d", _failed)
    log.info("  Total time        : %.2f s", total_time)
    log.info("  Overall throughput: %.1f msg/s", overall_mps)
    log.info("  Partition 0 (login)  : %s", partition_counts["0"])
    log.info("  Partition 1 (payment): %s", partition_counts["1"])
    log.info("  Partition 2 (order)  : %s", partition_counts["2"])
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Kafka producer with content-type custom partitioner."
    )
    p.add_argument(
        "--bootstrap", default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP})",
    )
    p.add_argument(
        "--topic", default=DEFAULT_TOPIC,
        help=f"Kafka topic name (default: {DEFAULT_TOPIC})",
    )
    p.add_argument(
        "--events", type=int, default=DEFAULT_EVENTS,
        help=f"Number of events to produce (default: {DEFAULT_EVENTS})",
    )
    p.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"Seconds between messages (default: {DEFAULT_DELAY})",
    )
    p.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for event generation (default: 42)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_producer(
        bootstrap_servers   = args.bootstrap,
        topic               = args.topic,
        num_events          = args.events,
        inter_message_delay = args.delay,
        seed                = args.seed,
    )
