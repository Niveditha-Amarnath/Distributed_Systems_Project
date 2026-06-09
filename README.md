# ⚡ Kafka Topics & Partitions – Custom Partitioner by Content Type

**Hackathon Problem H.17** — End-to-end Kafka demo with custom content-type
partitioning, three consumers, rebalance callbacks, psutil health monitoring,
and a live Streamlit dashboard.

---

## 🗂️ Project Structure

```
kafka-partitioner/
├── producer.py           # Kafka producer with custom content-type partitioner
├── consumer.py           # Kafka consumer (run 3 named instances)
├── dashboard.py          # Streamlit live dashboard (dark, Plotly charts)
├── event_generator.py    # Generates N synthetic JSON events
├── metrics_manager.py    # Thread-safe JSON metrics persistence
├── requirements.txt      # Python dependencies
├── docker-compose.yml    # Single-node Kafka (KRaft, no ZooKeeper)
├── setup_topic.sh        # Linux/macOS topic creation helper
├── setup_topic.bat       # Windows topic creation helper
├── logs/                 # Per-process log files (auto-created)
│   ├── producer.log
│   ├── ConsumerA.log
│   ├── ConsumerB.log
│   └── ConsumerC.log
├── metrics/              # JSON metric files (auto-created)
│   ├── partition_counts.json
│   ├── consumer_assignments.json
│   ├── rebalance_history.json
│   ├── throughput.json
│   ├── producer_stats.json
│   ├── ConsumerA_health.json
│   ├── ConsumerB_health.json
│   └── ConsumerC_health.json
└── screenshots/          # Add your own screenshots here
    ├── dashboard_overview.png
    ├── partition_chart.png
    ├── consumer_health.png
    └── rebalance_log.png
```

---

## 🚀 Prerequisites

| Requirement | Version  | Notes                            |
|-------------|----------|----------------------------------|
| Python      | 3.11+    | Any 3.10+ should work            |
| Apache Kafka| 3.x      | Or use Docker Compose (easiest)  |
| Java (JDK)  | 11+      | Required by Kafka                |
| Docker      | 24+      | Optional – for Docker Compose    |

---

## ⚙️ Kafka Startup Commands

### Option A – Docker Compose ✅ Recommended (easiest)

```bash
# Start Kafka broker in background
docker-compose up -d

# Follow broker logs
docker-compose logs -f kafka

# Stop and remove containers
docker-compose down
```

### Option B – KRaft mode (Kafka 3.x, no ZooKeeper)

```bash
# 1. Download & extract Kafka
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0

# 2. Generate a cluster UUID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# 3. Format the storage directory
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# 4. Start the broker
bin/kafka-server-start.sh config/kraft/server.properties
```

### Option C – ZooKeeper mode (Kafka 2.x / 3.x)

```bash
# Terminal 1 – Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 – Start Kafka broker
bin/kafka-server-start.sh config/server.properties
```

---

## 📦 Kafka Topic Creation

The producer **auto-creates** the topic on first run, but you can also create
it manually:

```bash
# ── Linux / macOS ─────────────────────────────────────────────────────────────
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1

# ── Windows ───────────────────────────────────────────────────────────────────
bin\windows\kafka-topics.bat ^
  --bootstrap-server localhost:9092 ^
  --create ^
  --topic user-events ^
  --partitions 3 ^
  --replication-factor 1

# ── Docker (exec into container) ──────────────────────────────────────────────
docker exec kafka-broker \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic user-events --partitions 3 --replication-factor 1

# ── Verify topic ──────────────────────────────────────────────────────────────
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic user-events

# ── List all topics ───────────────────────────────────────────────────────────
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Or use the convenience scripts:

```bash
chmod +x setup_topic.sh && ./setup_topic.sh          # Linux/macOS
setup_topic.bat                                       # Windows
```

---

## 🐍 Python Setup

```bash
# Create virtual environment (recommended)
python -m venv .venv

# Activate
# Windows:
.venv\Scripts\activate
# Linux / macOS:
source .venv/bin/activate

# Install all dependencies
pip install -r requirements.txt
```

### requirements.txt

```
confluent-kafka==2.3.0
streamlit==1.32.0
psutil==5.9.8
plotly==5.20.0
pandas==2.2.1
watchdog==4.0.0
colorlog==6.8.2
python-dateutil==2.9.0
```

---

## ▶️ Execution Steps

Open **5 terminals** (or use tmux / Windows Terminal):

### Terminal 1 — Dashboard

```bash
cd kafka-partitioner
streamlit run dashboard.py
# Opens automatically at http://localhost:8501
```

### Terminal 2 — ConsumerA

```bash
python consumer.py --name ConsumerA
```

### Terminal 3 — ConsumerB

```bash
python consumer.py --name ConsumerB
```

### Terminal 4 — ConsumerC

```bash
python consumer.py --name ConsumerC
```

### Terminal 5 — Producer

```bash
python producer.py --events 1000
```

> **Tip:** Start consumers **before** the producer so all three are assigned
> partitions before messages arrive. If you start them after, Kafka will
> rebalance and you will see the rebalance events in the dashboard.

---

## 🔀 Custom Partitioner Logic

```
event.type == "login"   → Partition 0
event.type == "payment" → Partition 1
event.type == "order"   → Partition 2
```

Implemented as a plain Python function passed via the `partitioner`
configuration key of the confluent-kafka `Producer`:

```python
PARTITION_MAP = {"login": 0, "payment": 1, "order": 2}

def content_type_partitioner(key, all_partitions, available_partitions):
    event_type = key.decode("utf-8")
    return PARTITION_MAP.get(event_type, available_partitions[0])
```

---

## 🔄 Rebalance Callbacks

Four rebalance events are tracked:

| Event      | When                                                      |
|------------|-----------------------------------------------------------|
| `joined`   | Consumer starts and subscribes to the topic               |
| `assigned` | Kafka hands partitions to the consumer                    |
| `revoked`  | Kafka takes partitions away (before reassignment)         |
| `crashed`  | Unhandled exception in the poll loop                      |

All events are persisted to `metrics/rebalance_history.json` and displayed in
the dashboard's colour-coded event log.

---

## 📊 Metrics Files

| File                      | Contents                                         |
|---------------------------|--------------------------------------------------|
| `partition_counts.json`   | Running message count per partition              |
| `consumer_assignments.json` | Consumer → partitions + status + timestamp     |
| `rebalance_history.json`  | Time-ordered rebalance event log (last 500)      |
| `throughput.json`         | Per-consumer msg/s time-series (last 120 samples)|
| `producer_stats.json`     | Producer throughput samples + total sent         |
| `{Name}_health.json`      | CPU%, memory MB, PID, uptime, status per consumer|

---

## 🖥️ Dashboard Features

| Panel                   | Description                                            |
|-------------------------|--------------------------------------------------------|
| Header KPIs (5 cards)   | Total events · online consumers · rebalance count · produced · msg/s |
| Partition mini-cards    | Per-partition count with event-type labels             |
| Bar chart               | Partition message distribution (Plotly, dark theme)    |
| Pie chart               | Event-type breakdown (login / payment / order)         |
| Health cards            | CPU% progress · memory MB · PID · uptime · partitions |
| Assignment table        | Consumer → partition mapping with timestamps           |
| Throughput chart        | Time-series msg/s for all three consumers              |
| Rebalance log           | Filterable, colour-coded, newest-first event stream    |
| Auto-refresh            | Configurable 1–10 s (default 2 s) via sidebar slider  |
| Reset Metrics button    | Wipes all JSON metrics for a clean re-run              |
| Raw JSON viewer         | Toggle in sidebar to inspect metric files              |

---

## 📋 Event Schema

```json
{
    "event_id"   : 42,
    "type"       : "payment",
    "timestamp"  : "2024-03-15T14:23:01.123456+00:00",
    "user_id"    : 117,
    "session_id" : "a3f8c2e1-...",
    "metadata"   : {
        "amount"  : 149.99,
        "currency": "USD",
        "status"  : "success",
        "gateway" : "stripe"
    }
}
```

---

## 🛠️ Advanced Options

```bash
# Custom broker / topic / event count
python producer.py --bootstrap localhost:9092 --topic user-events --events 5000 --delay 0
python consumer.py --name ConsumerA --bootstrap localhost:9092 --topic user-events

# Slower production (for visible rebalance effects)
python producer.py --events 1000 --delay 0.02

# Reset all metrics without restarting the dashboard
python -c "from metrics_manager import MetricsManager; MetricsManager().reset_all()"
# OR: click "Reset" in the dashboard sidebar
```

---

## 🪟 Windows Notes

- Use `python` instead of `python3`.
- Run terminals as **Administrator** if you encounter permission errors.
- The `snappy` compression codec requires `python-snappy`; the producer uses
  `gzip` by default which works everywhere.
- Use `setup_topic.bat` instead of `setup_topic.sh`.

---

## 🐧 Linux / macOS Notes

- `confluent-kafka` bundles `librdkafka` on most platforms. If you see build
  errors, install it first:
  ```bash
  # Ubuntu / Debian
  sudo apt-get install librdkafka-dev
  # RHEL / CentOS
  sudo yum install librdkafka-devel
  # macOS
  brew install librdkafka
  ```
- Make the helper scripts executable: `chmod +x setup_topic.sh`

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kafka Broker                                 │
│                                                                     │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐             │
│   │  Partition 0 │   │ Partition 1  │   │ Partition 2  │             │
│   │  (login)    │   │ (payment)   │   │ (order)     │             │
│   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘             │
└──────────┼─────────────────┼─────────────────┼───────────────────── ┘
           │                 │                 │
    ┌──────┴─────────────────┴─────────────────┴──────┐
    │          Consumer Group: user-events-group        │
    │                                                  │
    │   ┌──────────┐  ┌──────────┐  ┌──────────┐     │
    │   │ConsumerA │  │ConsumerB │  │ConsumerC │     │
    │   │(P-0)     │  │(P-1)     │  │(P-2)     │     │
    │   └──────────┘  └──────────┘  └──────────┘     │
    └──────────────────────────────────────────────────┘
           │
    ┌──────┴──────────────────────┐
    │      metrics/ (JSON files)   │
    └──────┬──────────────────────┘
           │
    ┌──────┴──────────────────────┐
    │   Streamlit Dashboard        │
    │   http://localhost:8501      │
    └─────────────────────────────┘
```

---

## 📸 Screenshots

_Add your own screenshots to the `screenshots/` folder._

```
screenshots/
├── dashboard_overview.png    # Full dashboard — all panels
├── partition_chart.png       # Bar + pie charts
├── consumer_health.png       # Health cards with CPU gauges
└── rebalance_log.png         # Colour-coded rebalance event stream
```

---

## 📄 License

MIT — use freely for learning and hackathon submissions.
