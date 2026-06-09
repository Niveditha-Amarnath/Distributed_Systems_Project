"""
metrics_manager.py
==================
Thread-safe JSON metrics persistence layer.

All metric files live under the ``metrics/`` directory (created if absent).
Every public method acquires a per-file lock so that multiple consumer
processes can safely write concurrently.

Files managed
-------------
partition_counts.json       – running message count per partition
consumer_assignments.json   – partition → consumer name mapping + timestamp
rebalance_history.json      – ordered list of rebalance events
throughput.json             – per-consumer msg/s time-series samples
{name}_health.json          – CPU / memory / PID snapshot (one per consumer)
producer_stats.json         – producer throughput samples
live_events.json            – real-time event stream
event_type_stats.json       – event type counters (login/payment/order)
consumer_control.json       – consumer start/stop control flags
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

METRICS_DIR = Path(__file__).parent / "metrics"

# One threading lock per file path — lazily created
_file_locks: Dict[str, threading.Lock] = {}
_lock_registry = threading.Lock()


def _get_lock(path: str) -> threading.Lock:
    """Return (or create) the lock for *path*."""
    with _lock_registry:
        if path not in _file_locks:
            _file_locks[path] = threading.Lock()
        return _file_locks[path]


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None

    for _ in range(5):
        try:
            with open(path, "r", encoding="utf-8-sig") as fh:
                return json.load(fh)

        except PermissionError:
            time.sleep(0.1)

        except (json.JSONDecodeError, UnicodeDecodeError):
            print(f"[MetricsManager] Corrupted JSON: {path}")

            if "rebalance" in path.name:
                return []

            return {}

        except Exception as e:
            print(f"[MetricsManager] Read error {path}: {e}")

            if "rebalance" in path.name:
                return []

            return {}

    # Could not read after retries
    if "rebalance" in path.name:
        return []

    return {}


def _write_json(path: Path, data: Any) -> None:
    """
    Atomic write with unique temp file.
    Prevents collisions between multiple consumers.
    """

    tmp = path.with_suffix(f".{os.getpid()}.tmp")

    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(
            data,
            fh,
            indent=2,
            ensure_ascii=False
        )

    # retry if Windows still holds lock
    for _ in range(5):
        try:
            tmp.replace(path)
            return
        except PermissionError:
            time.sleep(0.1)

    print(f"[MetricsManager] Failed writing {path}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# MetricsManager
# ---------------------------------------------------------------------------

class MetricsManager:
    """
    Central interface for all metric read / write operations.

    Usage (from any process):
        mm = MetricsManager()
        mm.increment_partition_count(partition=1)
        mm.record_throughput("ConsumerA", msgs_per_second=12.4)
    """

    def __init__(self, metrics_dir: Optional[Path] = None) -> None:
        self.metrics_dir = metrics_dir or METRICS_DIR
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # Pre-define file paths for convenience
        self.partition_counts_path    = self.metrics_dir / "partition_counts.json"
        self.consumer_assignments_path = self.metrics_dir / "consumer_assignments.json"
        self.rebalance_history_path   = self.metrics_dir / "rebalance_history.json"
        self.throughput_path          = self.metrics_dir / "throughput.json"
        self.producer_stats_path      = self.metrics_dir / "producer_stats.json"
        self.live_events_path         = self.metrics_dir / "live_events.json"
        self.event_type_stats_path    = self.metrics_dir / "event_type_stats.json"
        self.consumer_control_path    = self.metrics_dir / "consumer_control.json"

        # Ensure base structures exist on first use
        self._init_files()

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_files(self) -> None:
        """Create empty metric files if they don't yet exist."""
        defaults: Dict[Path, Any] = {
            self.partition_counts_path    : {"0": 0, "1": 0, "2": 0},
            self.consumer_assignments_path: {},
            self.rebalance_history_path   : [],
            self.throughput_path          : {},
            self.producer_stats_path      : {"samples": [], "total_sent": 0},
            self.live_events_path         : [],
            self.event_type_stats_path    : {"login": 0, "payment": 0, "order": 0},
            self.consumer_control_path    : {
                "ConsumerA": True,
                "ConsumerB": True,
                "ConsumerC": True
            },
        }
        for path, default in defaults.items():
            if not path.exists():
                _write_json(path, default)

    # ------------------------------------------------------------------
    # Partition counts
    # ------------------------------------------------------------------

    def increment_partition_count(self, partition: int, amount: int = 1) -> None:
        """Atomically increment the message counter for *partition*."""
        path = self.partition_counts_path
        lock = _get_lock(str(path))
        with lock:
            data = _read_json(path) or {"0": 0, "1": 0, "2": 0}
            key  = str(partition)
            data[key] = data.get(key, 0) + amount
            _write_json(path, data)

    def get_partition_counts(self) -> Dict[str, int]:
        data = _read_json(self.partition_counts_path)
        return data or {"0": 0, "1": 0, "2": 0}

    def reset_partition_counts(self) -> None:
        lock = _get_lock(str(self.partition_counts_path))
        with lock:
            _write_json(self.partition_counts_path, {"0": 0, "1": 0, "2": 0})

    # ------------------------------------------------------------------
    # Consumer assignments
    # ------------------------------------------------------------------

    def update_consumer_assignment(
        self,
        consumer_name: str,
        partitions: List[int],
    ) -> None:
        """
        Record which partitions are assigned to *consumer_name*.

        The JSON structure is:
            {
              "ConsumerA": {
                "partitions": [0],
                "assigned_at": "2024-..."
              },
              ...
            }
        """
        path = self.consumer_assignments_path
        lock = _get_lock(str(path))
        with lock:
            data = _read_json(path) or {}
            data[consumer_name] = {
                "partitions" : partitions,
                "assigned_at": _now_iso(),
                "status"     : "active",
            }
            _write_json(path, data)

    def mark_consumer_offline(self, consumer_name: str) -> None:
        path = self.consumer_assignments_path
        lock = _get_lock(str(path))
        with lock:
            data = _read_json(path) or {}
            if consumer_name in data:
                data[consumer_name]["status"]      = "offline"
                data[consumer_name]["offline_at"]  = _now_iso()
                data[consumer_name]["partitions"]  = []
                _write_json(path, data)

    def get_consumer_assignments(self) -> Dict[str, Any]:
        return _read_json(self.consumer_assignments_path) or {}

    # ------------------------------------------------------------------
    # Rebalance history
    # ------------------------------------------------------------------

    def append_rebalance_event(
        self,
        consumer_name : str,
        event_type    : str,   # joined | assigned | revoked | crashed
        partitions    : Optional[List[int]] = None,
        detail        : Optional[str]       = None,
    ) -> None:
        """Append one rebalance event to the ordered history log."""
        path = self.rebalance_history_path
        lock = _get_lock(str(path))
        with lock:
            history: List[Dict[str, Any]] = _read_json(path) or []
            entry: Dict[str, Any] = {
                "ts"           : _now_iso(),
                "consumer"     : consumer_name,
                "event"        : event_type,
                "partitions"   : partitions or [],
                "detail"       : detail or "",
            }
            history.append(entry)
            # Keep the last 500 events to avoid unbounded growth
            if len(history) > 500:
                history = history[-500:]
            _write_json(path, history)

    def get_rebalance_history(self) -> List[Dict[str, Any]]:
        return _read_json(self.rebalance_history_path) or []

    def clear_rebalance_history(self) -> None:
        lock = _get_lock(str(self.rebalance_history_path))
        with lock:
            _write_json(self.rebalance_history_path, [])

    # ------------------------------------------------------------------
    # Throughput
    # ------------------------------------------------------------------

    def record_throughput(self, consumer_name: str, msgs_per_second: float) -> None:
        """
        Append a throughput sample for *consumer_name*.

        Structure:
            {
              "ConsumerA": [
                {"ts": "...", "mps": 12.4},
                ...
              ],
              ...
            }
        Keeps the last 120 samples per consumer (≈ 4 minutes at 2-second granularity).
        """
        path = self.throughput_path
        lock = _get_lock(str(path))
        with lock:
            data = _read_json(path) or {}
            series = data.get(consumer_name, [])
            series.append({"ts": _now_iso(), "mps": round(msgs_per_second, 2)})
            if len(series) > 120:
                series = series[-120:]
            data[consumer_name] = series
            _write_json(path, data)

    def get_throughput(self) -> Dict[str, List[Dict[str, Any]]]:
        return _read_json(self.throughput_path) or {}

    # ------------------------------------------------------------------
    # Consumer health (psutil stats)
    # ------------------------------------------------------------------

    def update_health(self, consumer_name: str, health_data: Dict[str, Any]) -> None:
        """Write a full health snapshot for *consumer_name*."""
        path = self.metrics_dir / f"{consumer_name}_health.json"
        lock = _get_lock(str(path))
        with lock:
            health_data["last_updated"] = _now_iso()
            _write_json(path, health_data)

    def get_health(self, consumer_name):
        path = self.metrics_dir / f"{consumer_name}_health.json"

        try:
            return _read_json(path) or {}
        except Exception:
            return {}

    def get_all_health(self, consumer_names: List[str]) -> Dict[str, Any]:
        return {name: self.get_health(name) for name in consumer_names}

    # ------------------------------------------------------------------
    # Producer stats
    # ------------------------------------------------------------------

    def record_producer_sample(self, total_sent: int, mps: float) -> None:
        path = self.producer_stats_path
        lock = _get_lock(str(path))
        with lock:
            data = _read_json(path) or {"samples": [], "total_sent": 0}
            data["total_sent"] = total_sent
            data["samples"].append({"ts": _now_iso(), "mps": round(mps, 2)})
            if len(data["samples"]) > 120:
                data["samples"] = data["samples"][-120:]
            _write_json(path, data)

    def get_producer_stats(self) -> Dict[str, Any]:
        return _read_json(self.producer_stats_path) or {"samples": [], "total_sent": 0}

    # ------------------------------------------------------------------
    # Live Event Stream
    # ------------------------------------------------------------------

    def add_live_event(
        self,
        event_type: str,
        partition: int,
        consumer_name: str,
        message_key: str = ""
    ) -> None:

        path = self.live_events_path
        lock = _get_lock(str(path))

        with lock:
            events = _read_json(path) or []

            events.append(
                {
                    "ts": _now_iso(),
                    "event_type": event_type,
                    "partition": partition,
                    "consumer": consumer_name,
                    "key": message_key,
                }
            )

            if len(events) > 50:
                events = events[-50:]

            _write_json(path, events)

    def get_live_events(self):
        return _read_json(self.live_events_path) or []

    def clear_live_events(self):
        lock = _get_lock(str(self.live_events_path))

        with lock:
            _write_json(self.live_events_path, [])

    # ------------------------------------------------------------------
    # Event Type Analytics
    # ------------------------------------------------------------------

    def increment_event_type(self, event_type: str) -> None:
        """
        Increment the counter for a specific event type.
        
        Parameters
        ----------
        event_type : str
            Type of event (login, payment, order)
        """
        path = self.event_type_stats_path
        lock = _get_lock(str(path))
        
        with lock:
            data = _read_json(path) or {"login": 0, "payment": 0, "order": 0}
            
            # Normalize event type
            event_type = str(event_type).lower()
            
            # Initialize if not exists
            if event_type not in data:
                data[event_type] = 0
            
            data[event_type] += 1
            _write_json(path, data)
    
    def get_event_type_stats(self) -> Dict[str, int]:
        """
        Get current event type statistics.
        
        Returns
        -------
        Dictionary with event types as keys and counts as values
        """
        return _read_json(self.event_type_stats_path) or {"login": 0, "payment": 0, "order": 0}
    
    def reset_event_type_stats(self) -> None:
        """Reset event type statistics to zero."""
        path = self.event_type_stats_path
        lock = _get_lock(str(path))
        with lock:
            _write_json(path, {"login": 0, "payment": 0, "order": 0})

    # ------------------------------------------------------------------
    # Consumer Control (Failure Simulation)
    # ------------------------------------------------------------------

    def set_consumer_enabled(self, consumer_name: str, enabled: bool) -> None:
        """
        Set whether a consumer should be running or stopped.
        
        Parameters
        ----------
        consumer_name : str
            Name of the consumer (ConsumerA, ConsumerB, ConsumerC)
        enabled : bool
            True to run, False to stop
        """
        path = self.consumer_control_path
        lock = _get_lock(str(path))
        
        with lock:
            data = _read_json(path) or {
                "ConsumerA": True,
                "ConsumerB": True,
                "ConsumerC": True
            }
            data[consumer_name] = enabled
            _write_json(path, data)
    
    def is_consumer_enabled(self, consumer_name: str) -> bool:
        """
        Check if a consumer should be running.
        
        Parameters
        ----------
        consumer_name : str
            Name of the consumer
            
        Returns
        -------
        True if consumer should run, False otherwise
        """
        path = self.consumer_control_path
        data = _read_json(path) or {
            "ConsumerA": True,
            "ConsumerB": True,
            "ConsumerC": True
        }
        return data.get(consumer_name, True)
    
    def get_all_consumer_controls(self) -> Dict[str, bool]:
        """Get the enabled/disabled status for all consumers."""
        return _read_json(self.consumer_control_path) or {
            "ConsumerA": True,
            "ConsumerB": True,
            "ConsumerC": True
        }

    # ------------------------------------------------------------------
    # Global reset
    # ------------------------------------------------------------------

    def reset_all(self) -> None:
        """Wipe all metric files back to empty defaults (useful for re-runs)."""
        self.reset_partition_counts()
        self.clear_rebalance_history()

        # Clear assignments
        lock = _get_lock(str(self.consumer_assignments_path))
        with lock:
            _write_json(self.consumer_assignments_path, {})

        # Clear throughput
        lock = _get_lock(str(self.throughput_path))
        with lock:
            _write_json(self.throughput_path, {})

        # Clear producer stats
        lock = _get_lock(str(self.producer_stats_path))
        with lock:
            _write_json(self.producer_stats_path, {"samples": [], "total_sent": 0})

        # Clear live events
        lock = _get_lock(str(self.live_events_path))
        with lock:
            _write_json(self.live_events_path, [])
        
        # Reset event type stats
        lock = _get_lock(str(self.event_type_stats_path))
        with lock:
            _write_json(self.event_type_stats_path, {"login": 0, "payment": 0, "order": 0})
        
        # Reset consumer controls (all enabled)
        lock = _get_lock(str(self.consumer_control_path))
        with lock:
            _write_json(self.consumer_control_path, {
                "ConsumerA": True,
                "ConsumerB": True,
                "ConsumerC": True
            })

        # Remove health files
        for fname in self.metrics_dir.glob("*_health.json"):
            fname.unlink(missing_ok=True)

        print("[MetricsManager] All metrics reset.")


# ---------------------------------------------------------------------------
# CLI helper
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mm = MetricsManager()
    mm.reset_all()
    print("Metrics directory:", mm.metrics_dir)