"""
consumer.py
===========
Kafka consumer that:
  1. Joins the ``user-events-group`` consumer group.
  2. Subscribes to the ``user-events`` topic.
  3. Implements full rebalance callbacks:
       on_assign   – partition assignment received
       on_revoke   – partitions being taken away
  4. Monitors its own process with psutil (CPU, memory, PID, uptime).
  5. Calculates per-consumer throughput (msg/s) and writes samples.
  6. Persists all metrics via MetricsManager.

Run three instances in separate terminals:
  python consumer.py --name ConsumerA
  python consumer.py --name ConsumerB
  python consumer.py --name ConsumerC
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def _build_logger(name: str) -> logging.Logger:
    fmt = f"%(asctime)s [{name}] %(levelname)s – %(message)s"
    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setFormatter(logging.Formatter(fmt))
    handler_file = logging.FileHandler(
        LOG_DIR / f"{name}.log", encoding="utf-8"
    )
    handler_file.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler_console)
    logger.addHandler(handler_file)
    logger.propagate = False
    return logger


# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError:
    print("confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)

try:
    import psutil
except ImportError:
    print("psutil not installed. Run: pip install psutil")
    sys.exit(1)

from metrics_manager import MetricsManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BOOTSTRAP  = "localhost:9092"
DEFAULT_TOPIC      = "user-events"
DEFAULT_GROUP      = "user-events-group"
HEALTH_INTERVAL    = 3.0   # seconds between health updates
THROUGHPUT_WINDOW  = 5.0   # seconds for rolling throughput calculation


# ---------------------------------------------------------------------------
# Health monitor (background thread)
# ---------------------------------------------------------------------------

class HealthMonitor(threading.Thread):
    """
    Periodically captures process-level metrics via psutil and writes
    them to the metrics directory.
    """

    def __init__(
        self,
        consumer_name : str,
        metrics_manager: MetricsManager,
        interval      : float = HEALTH_INTERVAL,
    ) -> None:
        super().__init__(daemon=True, name=f"HealthMonitor-{consumer_name}")
        self.consumer_name   = consumer_name
        self.mm              = metrics_manager
        self.interval        = interval
        self._stop_event     = threading.Event()
        self._proc           = psutil.Process(os.getpid())
        self._start_time     = time.monotonic()

    def run(self) -> None:
        """Main loop – runs until stop() is called."""
        while not self._stop_event.wait(self.interval):
            try:
                self._capture()
            except Exception as exc:
                # Never crash the background thread
                logging.getLogger(self.consumer_name).warning(
                    "HealthMonitor error: %s", exc
                )

    def _capture(self) -> None:
        """Collect and persist a single health snapshot."""
        uptime  = time.monotonic() - self._start_time
        cpu_pct = self._proc.cpu_percent(interval=0.1)
        mem     = self._proc.memory_info()
        mem_mb  = mem.rss / 1_048_576  # bytes → MB

        health: Dict[str, Any] = {
            "consumer"      : self.consumer_name,
            "pid"           : self._proc.pid,
            "status"        : "running",
            "uptime_s"      : round(uptime, 1),
            "cpu_percent"   : round(cpu_pct, 1),
            "memory_mb"     : round(mem_mb, 2),
            "memory_percent": round(self._proc.memory_percent(), 2),
            "num_threads"   : self._proc.num_threads(),
        }
        self.mm.update_health(self.consumer_name, health)

    def mark_crashed(self, reason: str) -> None:
        """Write a 'crashed' status before the thread exits."""
        health = {
            "consumer" : self.consumer_name,
            "pid"      : os.getpid(),
            "status"   : "crashed",
            "reason"   : reason,
            "uptime_s" : round(time.monotonic() - self._start_time, 1),
            "cpu_percent" : 0.0,
            "memory_mb"   : 0.0,
        }
        self.mm.update_health(self.consumer_name, health)

    def mark_stopped(self) -> None:
        """Write a 'stopped' status on graceful shutdown."""
        health = {
            "consumer" : self.consumer_name,
            "pid"      : os.getpid(),
            "status"   : "stopped",
            "uptime_s" : round(time.monotonic() - self._start_time, 1),
            "cpu_percent" : 0.0,
            "memory_mb"   : 0.0,
        }
        self.mm.update_health(self.consumer_name, health)

    def stop(self) -> None:
        """Signal the monitor thread to exit."""
        self._stop_event.set()


# ---------------------------------------------------------------------------
# Rebalance callbacks
# ---------------------------------------------------------------------------

def make_on_assign(consumer_name: str, mm: MetricsManager, log: logging.Logger):
    """Factory returning the on_assign callback for this consumer instance."""

    def on_assign(consumer, partitions) -> None:
        """
        Called when the consumer group coordinator assigns partitions.
        This happens:
          - When the consumer first joins the group.
          - After a rebalance triggered by another consumer joining/leaving.
        """
        assigned = [p.partition for p in partitions]
        log.info("★ Partition ASSIGNED  → %s", assigned)

        # Persist to metrics
        mm.update_consumer_assignment(consumer_name, assigned)
        mm.append_rebalance_event(
            consumer_name = consumer_name,
            event_type    = "assigned",
            partitions    = assigned,
            detail        = f"Assigned {len(assigned)} partition(s) after rebalance",
        )

        # Commit the consumer assignment (resume from last committed offset)
        consumer.assign(partitions)

    return on_assign


def make_on_revoke(consumer_name: str, mm: MetricsManager, log: logging.Logger):
    """Factory returning the on_revoke callback for this consumer instance."""

    def on_revoke(consumer, partitions) -> None:
        """
        Called just before the broker takes partitions away.
        Use this to commit offsets to avoid re-processing.
        """
        revoked = [p.partition for p in partitions]
        log.info("✘ Partition REVOKED   → %s", revoked)

        # Commit offsets before giving up the partitions
        try:
            consumer.commit(asynchronous=False)
            log.info("Offsets committed before revocation.")
        except KafkaException as exc:
            log.warning("Could not commit offsets on revoke: %s", exc)

        mm.update_consumer_assignment(consumer_name, [])
        mm.append_rebalance_event(
            consumer_name = consumer_name,
            event_type    = "revoked",
            partitions    = revoked,
            detail        = f"Revoked {len(revoked)} partition(s)",
        )

    return on_revoke


# ---------------------------------------------------------------------------
# Consumer main
# ---------------------------------------------------------------------------

def run_consumer(
    consumer_name    : str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    topic            : str = DEFAULT_TOPIC,
    group_id         : str = DEFAULT_GROUP,
    max_messages     : Optional[int] = None,
    poll_timeout     : float = 1.0,
) -> None:
    """
    Start a Kafka consumer with the given name.

    Parameters
    ----------
    consumer_name       : Human-readable name (ConsumerA / B / C).
    bootstrap_servers   : Kafka broker address.
    topic               : Topic to subscribe to.
    group_id            : Consumer group ID (all three share the same one).
    max_messages        : Stop after consuming this many messages (None = run forever).
    poll_timeout        : Seconds to wait per poll() call.
    """
    log = _build_logger(consumer_name)
    mm  = MetricsManager()

    # ── Health monitor ────────────────────────────────────────────────────
    monitor = HealthMonitor(consumer_name, mm)
    monitor.start()
    log.info("Health monitor started (PID %d).", os.getpid())

    # ── Log consumer joined ───────────────────────────────────────────────
    mm.append_rebalance_event(
        consumer_name = consumer_name,
        event_type    = "joined",
        detail        = f"{consumer_name} joined group '{group_id}'",
    )
    log.info("► %s joined consumer group '%s'", consumer_name, group_id)

    # ── Consumer configuration ────────────────────────────────────────────
    consumer_conf = {
        "bootstrap.servers"            : bootstrap_servers,
        "group.id"                     : group_id,
        # Read from the beginning if no committed offset exists
        "auto.offset.reset"            : "earliest",
        # We commit manually (after on_revoke) for correctness
        "enable.auto.commit"           : False,
        # How often the consumer sends heartbeats
        "heartbeat.interval.ms"        : 3_000,
        # Max time between polls before the consumer is considered dead
        "max.poll.interval.ms"         : 300_000,
        # Session timeout for group coordinator
        "session.timeout.ms"           : 10_000,
        # Fetch at most N bytes per partition per request
        "fetch.max.bytes"              : 1_048_576,
        # Client identifier visible in Kafka broker logs
        "client.id"                    : consumer_name,
    }

    consumer = Consumer(consumer_conf)

    # ── Subscribe with rebalance callbacks ───────────────────────────────
    consumer.subscribe(
        [topic],
        on_assign=make_on_assign(consumer_name, mm, log),
        on_revoke=make_on_revoke(consumer_name, mm, log),
    )
    log.info("Subscribed to topic '%s'.", topic)

    # ── Graceful shutdown handler ─────────────────────────────────────────
    _running = [True]  # mutable container so inner function can modify it

    def _shutdown(signum, frame) -> None:
        log.info("Shutdown signal received – stopping consumer …")
        _running[0] = False

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Poll loop ─────────────────────────────────────────────────────────
    total_consumed    = 0
    window_start      = time.monotonic()
    window_count      = 0
    auto_commit_every = 100          # commit every N messages

    try:
        while _running[0]:
            msg = consumer.poll(timeout=poll_timeout)

            if msg is None:
                # No message in this poll window – nothing to do
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    # Reached end of a partition – normal, not an error
                    log.debug(
                        "End of partition %d at offset %d",
                        msg.partition(), msg.offset(),
                    )
                else:
                    log.error("Consumer error: %s", err)
                continue

            # ── Process the message ───────────────────────────────────────
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("Could not decode message at offset %d: %s",
                             msg.offset(), exc)
                continue

            total_consumed += 1
            window_count   += 1

            log.debug(
                "[P%d | off:%d] event_id=%s type=%s",
                msg.partition(), msg.offset(),
                payload.get("event_id"), payload.get("type"),
            )

            # Periodic commit
            if total_consumed % auto_commit_every == 0:
                consumer.commit(asynchronous=True)
                log.debug("Async commit at message %d", total_consumed)

            # Throughput sample every THROUGHPUT_WINDOW seconds
            elapsed = time.monotonic() - window_start
            if elapsed >= THROUGHPUT_WINDOW:
                mps = window_count / elapsed
                mm.record_throughput(consumer_name, mps)
                log.info(
                    "Throughput: %.1f msg/s | Total consumed: %d",
                    mps, total_consumed,
                )
                window_start = time.monotonic()
                window_count = 0

            # Optional: stop after N messages (useful for testing)
            if max_messages and total_consumed >= max_messages:
                log.info("Reached max_messages=%d – stopping.", max_messages)
                break

    except Exception as exc:
        # ── Crash handling ────────────────────────────────────────────────
        log.exception("Unhandled exception in poll loop: %s", exc)
        mm.append_rebalance_event(
            consumer_name = consumer_name,
            event_type    = "crashed",
            detail        = str(exc),
        )
        monitor.mark_crashed(str(exc))
        raise

    finally:
        # ── Graceful shutdown ─────────────────────────────────────────────
        log.info("Committing final offsets …")
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            log.warning("Final commit failed: %s", exc)

        consumer.close()
        log.info("Consumer closed.")

        mm.mark_consumer_offline(consumer_name)
        monitor.mark_stopped()
        monitor.stop()

        log.info(
            "─── %s shut down. Total consumed: %d ───",
            consumer_name, total_consumed,
        )


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Kafka consumer with rebalance callbacks and health monitoring."
    )
    p.add_argument(
        "--name", required=True,
        help="Consumer name, e.g. ConsumerA",
    )
    p.add_argument(
        "--bootstrap", default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP})",
    )
    p.add_argument(
        "--topic", default=DEFAULT_TOPIC,
        help=f"Kafka topic (default: {DEFAULT_TOPIC})",
    )
    p.add_argument(
        "--group", default=DEFAULT_GROUP,
        help=f"Consumer group ID (default: {DEFAULT_GROUP})",
    )
    p.add_argument(
        "--max-messages", type=int, default=None,
        help="Stop after consuming this many messages (default: run forever)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_consumer(
        consumer_name     = args.name,
        bootstrap_servers = args.bootstrap,
        topic             = args.topic,
        group_id          = args.group,
        max_messages      = args.max_messages,
    )
