#!/usr/bin/env bash
# setup_topic.sh
# ──────────────────────────────────────────────────────────────────────────────
# Convenience script to create and verify the 'user-events' Kafka topic.
#
# Usage:
#   chmod +x setup_topic.sh
#   ./setup_topic.sh [KAFKA_HOME] [BOOTSTRAP_SERVER]
#
# Defaults:
#   KAFKA_HOME        = ./kafka_2.13-3.7.0
#   BOOTSTRAP_SERVER  = localhost:9092
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

KAFKA_HOME="${1:-./kafka_2.13-3.7.0}"
BOOTSTRAP="${2:-localhost:9092}"
TOPIC="user-events"
PARTITIONS=3
REPLICATION=1

echo "========================================================"
echo "  Kafka Topic Setup Script"
echo "========================================================"
echo "  Bootstrap server : $BOOTSTRAP"
echo "  Topic            : $TOPIC"
echo "  Partitions       : $PARTITIONS"
echo "  Replication      : $REPLICATION"
echo "========================================================"

# ── Wait for Kafka to be ready ────────────────────────────────────────────────
echo ""
echo "⏳ Waiting for Kafka to be available on $BOOTSTRAP …"
for i in $(seq 1 30); do
    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server "$BOOTSTRAP" &>/dev/null; then
        echo "✅ Kafka is ready."
        break
    fi
    echo "   Attempt $i/30 – retrying in 2 s …"
    sleep 2
    if [ "$i" -eq 30 ]; then
        echo "❌ Kafka did not become ready in time. Check your broker."
        exit 1
    fi
done

# ── Create topic ──────────────────────────────────────────────────────────────
echo ""
echo "📦 Creating topic '$TOPIC' …"
"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"

echo "✅ Topic '$TOPIC' created (or already existed)."

# ── Describe topic ────────────────────────────────────────────────────────────
echo ""
echo "📋 Topic details:"
"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"

# ── List all topics ───────────────────────────────────────────────────────────
echo ""
echo "📋 All topics on this broker:"
"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --list

echo ""
echo "========================================================"
echo "  Setup complete! You can now run:"
echo "    streamlit run dashboard.py"
echo "    python consumer.py --name ConsumerA"
echo "    python consumer.py --name ConsumerB"
echo "    python consumer.py --name ConsumerC"
echo "    python producer.py --events 1000"
echo "========================================================"
