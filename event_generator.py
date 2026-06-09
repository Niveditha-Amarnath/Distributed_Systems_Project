"""
event_generator.py
==================
Generates N synthetic JSON events with a realistic distribution across
three content types: login, payment, and order.

Distribution (approximate):
  - login   : 40 %
  - payment : 35 %
  - order   : 25 %

Each event has the schema:
  {
      "event_id" : <int>            unique sequential ID
      "type"     : <str>            one of login | payment | order
      "timestamp": <str>            ISO-8601 UTC timestamp
      "user_id"  : <int>            simulated user (1-200)
      "session_id": <str>           UUID-like session reference
      "metadata" : <dict>           type-specific extra fields
  }
"""

import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_TYPES: List[str] = ["login", "payment", "order"]

# Weighted distribution — mirrors real-world traffic patterns
_WEIGHTS: List[float] = [0.40, 0.35, 0.25]

# Simulated devices / regions / currencies for richer metadata
_DEVICES    = ["mobile", "desktop", "tablet"]
_REGIONS    = ["us-east", "eu-west", "ap-south", "us-west", "eu-central"]
_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY"]
_STATUSES   = ["success", "pending", "failed"]
_CATEGORIES = ["electronics", "clothing", "books", "food", "travel"]


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

def _login_metadata(rng: random.Random) -> Dict[str, Any]:
    """Extra fields relevant to a login event."""
    return {
        "device"   : rng.choice(_DEVICES),
        "region"   : rng.choice(_REGIONS),
        "mfa"      : rng.choice([True, False]),
        "ip_prefix": f"192.168.{rng.randint(0, 255)}.x",
    }


def _payment_metadata(rng: random.Random) -> Dict[str, Any]:
    """Extra fields relevant to a payment event."""
    return {
        "amount"  : round(rng.uniform(1.0, 999.99), 2),
        "currency": rng.choice(_CURRENCIES),
        "status"  : rng.choice(_STATUSES),
        "gateway" : rng.choice(["stripe", "paypal", "razorpay", "adyen"]),
    }


def _order_metadata(rng: random.Random) -> Dict[str, Any]:
    """Extra fields relevant to an order event."""
    return {
        "items"    : rng.randint(1, 8),
        "category" : rng.choice(_CATEGORIES),
        "discount" : round(rng.uniform(0, 0.30), 2),
        "warehouse": rng.choice(["WH-1", "WH-2", "WH-3"]),
    }


_METADATA_BUILDERS = {
    "login"  : _login_metadata,
    "payment": _payment_metadata,
    "order"  : _order_metadata,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_events(
    n: int = 1_000,
    seed: int = 42,
    spread_minutes: int = 60,
) -> List[Dict[str, Any]]:
    """
    Generate *n* synthetic events.

    Parameters
    ----------
    n               : Total number of events to create.
    seed            : Random seed for reproducibility.
    spread_minutes  : Events are spread across this many minutes in the past.

    Returns
    -------
    A list of event dicts sorted by timestamp (oldest first).
    """
    rng    = random.Random(seed)
    now    = datetime.now(timezone.utc)
    start  = now - timedelta(minutes=spread_minutes)
    span   = spread_minutes * 60  # total seconds

    events: List[Dict[str, Any]] = []

    for event_id in range(1, n + 1):
        etype   = rng.choices(EVENT_TYPES, weights=_WEIGHTS, k=1)[0]
        user_id = rng.randint(1, 200)

        # Random timestamp within the spread window
        offset    = rng.uniform(0, span)
        ts        = start + timedelta(seconds=offset)
        ts_string = ts.isoformat()

        event: Dict[str, Any] = {
            "event_id"  : event_id,
            "type"      : etype,
            "timestamp" : ts_string,
            "user_id"   : user_id,
            "session_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "metadata"  : _METADATA_BUILDERS[etype](rng),
        }
        events.append(event)

    # Sort chronologically so the producer sends them in time order
    events.sort(key=lambda e: e["timestamp"])

    # Re-assign sequential IDs after sort so they match display order
    for idx, event in enumerate(events, start=1):
        event["event_id"] = idx

    return events


def get_type_distribution(events: List[Dict[str, Any]]) -> Dict[str, int]:
    """Return a count dict keyed by event type."""
    dist: Dict[str, int] = {t: 0 for t in EVENT_TYPES}
    for e in events:
        dist[e["type"]] += 1
    return dist


# ---------------------------------------------------------------------------
# Quick sanity-check when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    events = generate_events(1_000)
    dist   = get_type_distribution(events)

    print(f"Generated {len(events)} events")
    print("Distribution:", json.dumps(dist, indent=2))
    print("First event :", json.dumps(events[0], indent=2))
    print("Last  event :", json.dumps(events[-1], indent=2))
