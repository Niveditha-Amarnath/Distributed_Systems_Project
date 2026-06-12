"""
dashboard.py
============
Streamlit live dashboard for the Kafka custom-partitioner demo.

Features
--------
• Header KPI cards  – total events, per-partition counts, online consumers
• Bar chart          – partition message distribution (Plotly)
• Pie chart          – event-type breakdown (login / payment / order)
• Event Analytics    – real-time event type tracking with anomaly detection
• Consumer assignment table
• Health cards       – CPU%, memory MB, PID, uptime, status per consumer
• Throughput chart   – time-series msg/s for all three consumers
• Live Event Stream  – real-time event feed from Kafka
• Rebalance event log – filterable, colour-coded stream
• Partition Visualization Upgrade – balance score and percentages
• Consumer Failure Simulation – start/stop consumer buttons
• Auto-refresh       – configurable 1–10 s (default 2 s)
• Reset Metrics button in the sidebar

Run
---
  streamlit run dashboard.py
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from metrics_manager import MetricsManager

# ---------------------------------------------------------------------------
# Page config – must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Kafka Partitioner Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS for dark, polished UI
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* ── Global ── */
    [data-testid="stAppViewContainer"] {
        background: #0e1117;
        color: #e6edf3;
    }
    [data-testid="stSidebar"] {
        background: #161b22;
        border-right: 1px solid #30363d;
        min-width: 280px;
    }

    /* ── KPI Cards ── */
    .kpi-card {
        background: linear-gradient(135deg, #1c2333 0%, #21262d 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 16px 20px;
        text-align: center;
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 24px rgba(88, 166, 255, 0.15);
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        line-height: 1;
        margin-bottom: 6px;
        background: linear-gradient(90deg, #58a6ff, #bc8cff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .kpi-label {
        font-size: 0.75rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .kpi-sub {
        font-size: 0.7rem;
        color: #58a6ff;
        margin-top: 4px;
    }

    /* ── Partition mini-cards ── */
    .partition-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 12px 16px;
        text-align: center;
    }
    .partition-card.p0 { border-top: 3px solid #58a6ff; }
    .partition-card.p1 { border-top: 3px solid #3fb950; }
    .partition-card.p2 { border-top: 3px solid #bc8cff; }
    .partition-count {
        font-size: 1.6rem;
        font-weight: 700;
        color: #e6edf3;
    }
    .partition-label {
        font-size: 0.7rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }

    /* ── Section headers ── */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #e6edf3;
        margin: 0 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #30363d;
    }

    /* ── Health cards ── */
    .health-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 12px;
    }
    .health-name {
        font-size: 0.9rem;
        font-weight: 600;
        color: #e6edf3;
        margin-bottom: 8px;
    }
    .health-metric {
        display: flex;
        justify-content: space-between;
        font-size: 0.72rem;
        color: #8b949e;
        margin-bottom: 4px;
    }
    .health-metric span.val { color: #e6edf3; }
    .status-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 20px;
        font-size: 0.65rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .status-running  { background: #1b3a2d; color: #3fb950; border: 1px solid #3fb950; }
    .status-stopped  { background: #2d2115; color: #e3b341; border: 1px solid #e3b341; }
    .status-crashed  { background: #3d1b1b; color: #f85149; border: 1px solid #f85149; }
    .status-offline  { background: #21262d; color: #8b949e; border: 1px solid #8b949e; }
    .status-waiting  { background: #162032; color: #58a6ff; border: 1px solid #58a6ff; }

    /* ── Rebalance log ── */
    .rebal-event {
        padding: 6px 10px;
        margin-bottom: 4px;
        border-radius: 6px;
        font-size: 0.72rem;
        display: flex;
        gap: 8px;
        align-items: center;
    }
    .rebal-joined   { background: #162032; border-left: 3px solid #58a6ff; }
    .rebal-assigned { background: #1b3a2d; border-left: 3px solid #3fb950; }
    .rebal-revoked  { background: #2d2115; border-left: 3px solid #e3b341; }
    .rebal-crashed  { background: #3d1b1b; border-left: 3px solid #f85149; }
    .rebal-ts    { color: #8b949e; min-width: 75px; font-size: 0.62rem; }
    .rebal-cons  { color: #58a6ff; font-weight: 600; min-width: 80px; font-size: 0.68rem; }
    .rebal-badge {
        font-size: 0.6rem; font-weight: 700; text-transform: uppercase;
        padding: 1px 6px; border-radius: 10px; min-width: 55px; text-align: center;
    }
    .badge-joined   { background: #1b4a7a; color: #58a6ff; }
    .badge-assigned { background: #1b3a2d; color: #3fb950; }
    .badge-revoked  { background: #3d3015; color: #e3b341; }
    .badge-crashed  { background: #5a1e1e; color: #f85149; }
    .rebal-detail { color: #8b949e; font-size: 0.68rem; }

    /* ── Tables ── */
    [data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

    /* ── Sidebar ── */
    .sidebar-section {
        background: #21262d;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 10px;
        margin-bottom: 12px;
    }

    /* ── Progress bars ── */
    .stProgress > div > div > div { border-radius: 4px; }

    /* ── Live Event Stream styling ── */
    .live-event-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        margin-bottom: 6px;
        padding: 8px;
        transition: all 0.2s ease;
    }
    .live-event-card:hover {
        background: #1c2333;
        border-left: 3px solid #58a6ff;
    }
    .live-event-time {
        font-size: 0.65rem;
        color: #8b949e;
        font-family: monospace;
    }
    .live-event-type {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.65rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .type-login { background: #1b4a7a; color: #58a6ff; }
    .type-payment { background: #1b3a2d; color: #3fb950; }
    .type-order { background: #3d2a6e; color: #bc8cff; }
    .live-event-detail {
        font-size: 0.7rem;
        color: #e6edf3;
    }

    /* Hide Streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }
    
    /* Fix metric cards */
    [data-testid="column"] {
        padding: 0 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
METRICS_DIR = Path(__file__).parent / "metrics"
CONSUMER_NAMES = ["ConsumerA", "ConsumerB", "ConsumerC"]
PARTITION_COLORS = {
    "0": "#58a6ff",
    "1": "#3fb950",
    "2": "#bc8cff",
}

mm = MetricsManager()

# ---------------------------------------------------------------------------
# Helper functions for loading data
# ---------------------------------------------------------------------------

def load_partition_counts() -> Dict[str, int]:
    return mm.get_partition_counts()

def load_throughput() -> Dict[str, List[Dict[str, Any]]]:
    return mm.get_throughput()

def load_rebalance_history() -> List[Dict[str, Any]]:
    return mm.get_rebalance_history()

def load_live_events() -> List[Dict[str, Any]]:
    path = METRICS_DIR / "live_events.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return []

def load_consumer_assignments() -> Dict[str, Any]:
    return mm.get_consumer_assignments()

def load_consumer_health() -> Dict[str, Any]:
    return mm.get_all_health(CONSUMER_NAMES)

def load_producer_stats() -> Dict[str, Any]:
    return mm.get_producer_stats()

def load_event_type_stats() -> Dict[str, int]:
    path = METRICS_DIR / "event_type_stats.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"image": 0, "video": 0, "order": 0}

def calculate_balance_score(counts: Dict[str, int]) -> tuple[float, str]:
    values = list(counts.values())
    if not values or sum(values) == 0:
        return 100.0, "No Data"
    
    max_val = max(values)
    min_val = min(values)
    
    if max_val == 0:
        return 100.0, "Perfectly Balanced"
    
    balance_ratio = (min_val / max_val) * 100 if max_val > 0 else 0
    
    if balance_ratio >= 90:
        return balance_ratio, "Perfectly Balanced"
    elif balance_ratio >= 70:
        return balance_ratio, "Well Balanced"
    elif balance_ratio >= 50:
        return balance_ratio, "Moderately Balanced"
    elif balance_ratio >= 30:
        return balance_ratio, "Imbalanced"
    else:
        return balance_ratio, "Severely Imbalanced"

def detect_anomalies(
    throughput: Dict[str, List[Dict[str, Any]]],
    health: Dict[str, Any],
    counts: Dict[str, int]
) -> List[Dict[str, Any]]:
    anomalies = []
    
    for consumer, series in throughput.items():
        if series and len(series) > 0:
            latest_mps = series[-1].get("mps", 0)
            if latest_mps > 200:
                anomalies.append({
                    "type": "HIGH_TRAFFIC",
                    "severity": "warning",
                    "message": f"⚠️ High traffic detected on {consumer}: {latest_mps:.1f} msg/s"
                })
    
    for consumer, health_data in health.items():
        status = health_data.get("status", "waiting")
        if status == "crashed":
            anomalies.append({
                "type": "CONSUMER_FAILURE",
                "severity": "error",
                "message": f"🚨 Consumer failure: {consumer} has crashed!"
            })
        elif status == "offline":
            anomalies.append({
                "type": "CONSUMER_FAILURE",
                "severity": "warning",
                "message": f"⚠️ Consumer {consumer} is offline"
            })
    
    values = list(counts.values())
    if values and sum(values) > 0:
        max_count = max(values)
        min_count = min(values)
        if max_count > 0:
            imbalance_ratio = (max_count - min_count) / max_count * 100
            if imbalance_ratio > 50:
                anomalies.append({
                    "type": "PARTITION_IMBALANCE",
                    "severity": "warning",
                    "message": f"⚠️ Partition imbalance detected! Range: {min_count} to {max_count} messages"
                })
    
    return anomalies

def hex_to_rgba(hex_color, opacity=0.3):
    hex_color = hex_color.lstrip('#')
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {opacity})"
# ---------------------------------------------------------------------------
# Sidebar - Consumer Control with Better Feedback
# ---------------------------------------------------------------------------

with st.sidebar:
    # Header
    st.markdown("## ⚡ Kafka Partitioner")
    st.markdown("### LIVE DASHBOARD")
    st.markdown("---")
    
    # Settings
    st.markdown("### ⚙️ Settings")
    refresh_rate = st.slider("Refresh (seconds)", 1, 10, 2)
    show_raw = st.checkbox("Show Raw JSON")
    
    st.markdown("---")
    
    # Controls
    st.markdown("### 🎮 Controls")
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("🗑️ Reset", use_container_width=True):
            mm.reset_all()
            st.success("Reset complete!")
            time.sleep(0.5)
            st.rerun()
    with col_r2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    
    st.markdown("---")
    
    # Consumer Control
    st.markdown("### 🖥️ Consumer Control")
    st.caption("Click Start/Stop to control consumers")
    
    # Get current status for each consumer
    health = load_consumer_health()
    
    for consumer in CONSUMER_NAMES:
        # Get current status
        current_status = health.get(consumer, {}).get("status", "waiting")
        is_enabled = mm.is_consumer_enabled(consumer)
        
        # Create a container for each consumer
        with st.container():
            st.markdown(f"**{consumer}**")
            
            # Show current status with colored indicator
            status_color = {
                "running": "🟢",
                "stopped": "🟡", 
                "crashed": "🔴",
                "offline": "⚪",
                "waiting": "🔵"
            }.get(current_status, "⚪")
            
            st.markdown(f"Status: {status_color} `{current_status.upper()}`")
            
            # Start/Stop buttons
            col1, col2 = st.columns(2)
            with col1:
                if st.button(f"▶️ Start", key=f"start_{consumer}", use_container_width=True):
                    mm.set_consumer_enabled(consumer, True)
                    st.success(f"✅ {consumer} start signal sent")
                    st.info("Consumer will start within a few seconds")
                    time.sleep(0.5)
                    st.rerun()
            
            with col2:
                if st.button(f"⏹️ Stop", key=f"stop_{consumer}", use_container_width=True):
                    mm.set_consumer_enabled(consumer, False)
                    st.warning(f"⚠️ {consumer} stop signal sent")
                    st.info("Consumer will stop within a few seconds")
                    time.sleep(0.5)
                    st.rerun()
            
            st.markdown("---")
    
    # Partition Map
    st.markdown("### 📍 Partition Map")
    st.markdown("🔐 login → **Partition 0**")
    st.markdown("💳 payment → **Partition 1**")
    st.markdown("📦 order → **Partition 2**")
    
    st.markdown("---")
    
    # Consumer Group
    st.markdown("### 🔗 Consumer Group")
    st.markdown(f"**Group ID:** `user-events-group`")
    st.markdown(f"**Topic:** `user-events`")
    st.markdown(f"**Partitions:** 3")
    
    st.markdown("---")
    
    # System Info
    st.markdown(f"**Last updated:** {datetime.now().strftime('%H:%M:%S')}")

# ---------------------------------------------------------------------------
# Load all data
# ---------------------------------------------------------------------------

counts = load_partition_counts()
assignments = load_consumer_assignments()
rebalances = load_rebalance_history()
throughput = load_throughput()
health = load_consumer_health()
prod_stats = load_producer_stats()
live_events = load_live_events()
event_type_stats = load_event_type_stats()

# ---------------------------------------------------------------------------
# Derived values
# ---------------------------------------------------------------------------

total_messages = sum(int(v) for v in counts.values())
online_consumers = sum(1 for c in CONSUMER_NAMES if health.get(c, {}).get("status") == "running")
balance_score, balance_status = calculate_balance_score(counts)
anomalies = detect_anomalies(throughput, health, counts)

total_events = sum(event_type_stats.values())
image_pct = (event_type_stats.get("image", 0) / total_events * 100) if total_events > 0 else 0
video_pct = (event_type_stats.get("video", 0) / total_events * 100) if total_events > 0 else 0
order_pct = (event_type_stats.get("order", 0) / total_events * 100) if total_events > 0 else 0

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("⚡ Kafka Topics & Partitions")
st.caption("Custom Partitioner · Real-time Consumer Dashboard · H.17")

for anomaly in anomalies:
    if anomaly["severity"] == "error":
        st.error(anomaly["message"])
    elif anomaly["severity"] == "warning":
        st.warning(anomaly["message"])

# ---------------------------------------------------------------------------
# KPI Row
# ---------------------------------------------------------------------------

k1, k2, k3, k4, k5, k6 = st.columns(6)

with k1:
    st.metric("Total Messages", f"{total_messages:,}")
with k2:
    st.metric("Online Consumers", f"{online_consumers}/3")
with k3:
    st.metric("Rebalance Events", f"{len(rebalances)}")
with k4:
    total_sent = prod_stats.get("total_sent", 0)
    st.metric("Events Produced", f"{total_sent:,}")
with k5:
    samples = prod_stats.get("samples", [])
    latest_mps = samples[-1]["mps"] if samples else 0.0
    st.metric("Producer Throughput", f"{latest_mps:.1f} msg/s")
with k6:
    st.metric("Balance Score", f"{balance_score:.1f}%", balance_status)

# ---------------------------------------------------------------------------
# Partition Message Distribution
# ---------------------------------------------------------------------------

st.markdown("### 📦 Partition Message Distribution")

pc1, pc2, pc3 = st.columns(3)
partition_meta = [
    ("0", "login", "p0", "🔐"),
    ("1", "payment", "p1", "💳"),
    ("2", "order", "p2", "📦"),
]

for col, (pid, etype, css_cls, icon) in zip([pc1, pc2, pc3], partition_meta):
    count = int(counts.get(pid, 0))
    pct = (count / total_messages * 100) if total_messages > 0 else 0.0
    
    with col:
        st.markdown(f"##### {icon} Partition {pid} - {etype.title()}")
        st.markdown(f"## {count:,}")
        st.caption(f"{pct:.1f}% of total")
        st.progress(pct / 100 if pct > 0 else 0)

# ---------------------------------------------------------------------------
# Charts Row (Partition Distribution)
# ---------------------------------------------------------------------------

chart_col, pie_col = st.columns([3, 2])

with chart_col:
    st.markdown("### 📊 Partition Distribution")
    bar_data = pd.DataFrame({
        "Partition": ["Partition 0 (login)", "Partition 1 (payment)", "Partition 2 (order)"],
        "Messages": [int(counts.get("0", 0)), int(counts.get("1", 0)), int(counts.get("2", 0))],
    })
    
    fig_bar = px.bar(
        bar_data,
        x="Partition",
        y="Messages",
        color="Partition",
        color_discrete_sequence=["#58a6ff", "#3fb950", "#bc8cff"],
        text="Messages"
    )
    fig_bar.update_traces(
        textposition='outside',
        textfont=dict(color="#e6edf3", size=13),
        marker=dict(line=dict(color='white', width=1))
    )
    fig_bar.update_layout(
        title=dict(text="Partition Message Count", font=dict(color="#e6edf3", size=16), x=0.5),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="#e6edf3"),
        showlegend=False,
        height=400,
        xaxis=dict(
            title="Partition",
            tickfont=dict(color="#e6edf3"),
            gridcolor="#21262d",
            showgrid=True
        ),
        yaxis=dict(
            title="Messages",
            tickfont=dict(color="#e6edf3"),
            gridcolor="#21262d",
            showgrid=True,
            zeroline=True,
            zerolinecolor="#30363d"
        ),
        bargap=0.3
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with pie_col:
    st.markdown("### 🎨 Event-Type Breakdown")
    if event_type_stats and sum(event_type_stats.values()) > 0:
        display_stats = {
            "login": event_type_stats.get("image", 0),
            "payment": event_type_stats.get("video", 0),
            "order": event_type_stats.get("order", 0),
        }
        pie_data = pd.DataFrame({
            "Type": list(display_stats.keys()),
            "Count": list(display_stats.values())
        })
        
        fig_pie = px.pie(
            pie_data,
            names="Type",
            values="Count",
            color="Type",
            color_discrete_map={"login": "#58a6ff", "payment": "#3fb950", "order": "#bc8cff"},
            hole=0.5,
        )
        fig_pie.update_traces(
            textposition='outside',
            textinfo='label+percent',
            textfont=dict(color="#e6edf3", size=12),
            marker=dict(line=dict(color='#0e1117', width=2)),
            pull=[0.05, 0, 0]
        )
        fig_pie.update_layout(
            title=dict(text="Event Distribution", font=dict(color="#e6edf3", size=16), x=0.5),
            paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117",
            font=dict(color="#e6edf3"),
            height=400,
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.5,
                xanchor="left",
                x=1.05,
                bgcolor="rgba(0,0,0,0.5)",
                bordercolor="#30363d",
                borderwidth=1,
                font=dict(color="#e6edf3", size=11)
            ),
            annotations=[
                dict(
                    text=f"Total<br>{pie_data['Count'].sum():,}",
                    x=0.5, y=0.5, font_size=20, showarrow=False,
                    font=dict(color="#e6edf3")
                )
            ]
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No event data available yet. Start producer to see analytics.")

# ---------------------------------------------------------------------------
# Live Event Stream
# ---------------------------------------------------------------------------

st.markdown("### 📡 Live Event Stream")

le_col1, le_col2, le_col3 = st.columns([2, 2, 1])
with le_col1:
    le_filter_type = st.multiselect(
        "Event Type", options=["login", "payment", "order", "All"], default=["All"],
        key="le_type_filter", label_visibility="collapsed"
    )
with le_col2:
    le_filter_consumer = st.multiselect(
        "Consumer", options=CONSUMER_NAMES + ["All"], default=["All"],
        key="le_consumer_filter", label_visibility="collapsed"
    )
with le_col3:
    le_max_events = st.number_input("Max", min_value=10, max_value=200, value=50, step=10,
                                     key="le_max", label_visibility="collapsed")

filtered_events = list(reversed(live_events))
if "All" not in le_filter_type and le_filter_type:
    filtered_events = [e for e in filtered_events if e.get("event_type") in le_filter_type]
if "All" not in le_filter_consumer and le_filter_consumer:
    filtered_events = [e for e in filtered_events if e.get("consumer") in le_filter_consumer]
filtered_events = filtered_events[:le_max_events]

if filtered_events:
    event_rows = []
    for event in filtered_events:
        ts_raw = event.get("ts", "")
        ts_display = ts_raw[11:19] if len(ts_raw) >= 19 else ts_raw
        event_rows.append({
            "Time": ts_display,
            "Type": event.get("event_type", ""),
            "Partition": event.get("partition", ""),
            "Consumer": event.get("consumer", ""),
            "Event ID": event.get("key", "")[:20]
        })
    st.dataframe(pd.DataFrame(event_rows), use_container_width=True, height=350, hide_index=True)
else:
    st.info("No live events available. Start producer and consumers to see events.")

# ---------------------------------------------------------------------------
# Event Analytics Section - Fixed
# ---------------------------------------------------------------------------

st.subheader("📊 Event Analytics")

# Load event type stats
event_type_stats = load_event_type_stats()

# Map the event types correctly
# If your stats have 'login', 'payment', 'order' - map them to display names
if "login" in event_type_stats:
    # Case 1: Stats are stored as login/payment/order (from partition mapping)
    image_count = event_type_stats.get("login", 0)
    video_count = event_type_stats.get("payment", 0)
    order_count = event_type_stats.get("order", 0)
elif "image" in event_type_stats:
    # Case 2: Stats are stored as image/video/order (from consumer)
    image_count = event_type_stats.get("image", 0)
    video_count = event_type_stats.get("video", 0)
    order_count = event_type_stats.get("order", 0)
else:
    # Default fallback
    image_count = 0
    video_count = 0
    order_count = 0

total_events = image_count + video_count + order_count

# Calculate percentages
image_pct = (image_count / total_events * 100) if total_events > 0 else 0
video_pct = (video_count / total_events * 100) if total_events > 0 else 0
order_pct = (order_count / total_events * 100) if total_events > 0 else 0

# Display KPI Cards
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #1b4a7a 0%, #162032 100%); 
                border-radius: 12px; padding: 20px; text-align: center; 
                border: 1px solid #58a6ff;">
        <div style="font-size: 2rem;">📷</div>
        <div style="font-size: 1.8rem; font-weight: 700; color: #58a6ff;">{image_count:,}</div>
        <div style="font-size: 0.8rem; color: #8b949e;">Image Events (Login)</div>
        <div style="font-size: 1.2rem; font-weight: 600; color: #58a6ff;">{image_pct:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #1b3a2d 0%, #162032 100%); 
                border-radius: 12px; padding: 20px; text-align: center; 
                border: 1px solid #3fb950;">
        <div style="font-size: 2rem;">🎥</div>
        <div style="font-size: 1.8rem; font-weight: 700; color: #3fb950;">{video_count:,}</div>
        <div style="font-size: 0.8rem; color: #8b949e;">Video Events (Payment)</div>
        <div style="font-size: 1.2rem; font-weight: 600; color: #3fb950;">{video_pct:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #3d2a6e 0%, #162032 100%); 
                border-radius: 12px; padding: 20px; text-align: center; 
                border: 1px solid #bc8cff;">
        <div style="font-size: 2rem;">📦</div>
        <div style="font-size: 1.8rem; font-weight: 700; color: #bc8cff;">{order_count:,}</div>
        <div style="font-size: 0.8rem; color: #8b949e;">Order Events</div>
        <div style="font-size: 1.2rem; font-weight: 600; color: #bc8cff;">{order_pct:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

# Display charts if there are events
if total_events > 0:
    # Create dataframe for charts
    analytics_df = pd.DataFrame({
        "Event Type": ["Image (Login)", "Video (Payment)", "Order"],
        "Count": [image_count, video_count, order_count]
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📊 Event Distribution Bar Chart")
        fig_bar = px.bar(
            analytics_df,
            x="Event Type",
            y="Count",
            color="Event Type",
            color_discrete_map={
                "Image (Login)": "#58a6ff",
                "Video (Payment)": "#3fb950",
                "Order": "#bc8cff"
            },
            text="Count"
        )
        fig_bar.update_traces(
            textposition='outside',
            textfont=dict(color="#e6edf3", size=13)
        )
        fig_bar.update_layout(
            paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117",
            font=dict(color="#e6edf3"),
            showlegend=False,
            height=350
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with col2:
        st.markdown("#### 🎨 Event Distribution Pie Chart")
        fig_pie = px.pie(
            analytics_df,
            names="Event Type",
            values="Count",
            color="Event Type",
            color_discrete_map={
                "Image (Login)": "#58a6ff",
                "Video (Payment)": "#3fb950",
                "Order": "#bc8cff"
            },
            hole=0.4
        )
        fig_pie.update_traces(
            textposition='outside',
            textinfo='label+percent',
            textfont=dict(color="#e6edf3", size=12)
        )
        fig_pie.update_layout(
            paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117",
            font=dict(color="#e6edf3"),
            height=350,
            annotations=[
                dict(
                    text=f"Total<br>{total_events:,}",
                    x=0.5, y=0.5, font_size=16, showarrow=False,
                    font=dict(color="#e6edf3")
                )
            ]
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    # Summary
    st.success(f"""
    ### 📊 Event Distribution Summary
    
    | Event Type | Count | Percentage | Visual |
    |-----------|-------|------------|--------|
    | 🖼️ **Image (Login)** | {image_count:,} | {image_pct:.1f}% | {'█' * int(image_pct/2)}{'░' * (50 - int(image_pct/2))} |
    | 🎬 **Video (Payment)** | {video_count:,} | {video_pct:.1f}% | {'█' * int(video_pct/2)}{'░' * (50 - int(video_pct/2))} |
    | 📦 **Order** | {order_count:,} | {order_pct:.1f}% | {'█' * int(order_pct/2)}{'░' * (50 - int(order_pct/2))} |
    
    **Total Events:** {total_events:,}
    """)
    
else:
    st.info("ℹ️ No event data available yet. Run the producer to generate events!")
    st.caption("💡 **Tip:** Run `python producer.py --events 1000` to generate test events")

# Also display raw stats for debugging
with st.expander("🔍 Raw Event Statistics (Debug)"):
    st.json(event_type_stats)
    st.caption(f"Total events calculated: {total_events}")
# ---------------------------------------------------------------------------
# Consumer Health Cards
# ---------------------------------------------------------------------------

st.markdown("### 🖥️ Consumer Health Monitor")

hc1, hc2, hc3 = st.columns(3)

for col, name in zip([hc1, hc2, hc3], CONSUMER_NAMES):
    h = health.get(name, {})
    status = h.get("status", "waiting")
    partitions_assigned = assignments.get(name, {}).get("partitions", [])
    partitions_str = ", ".join(f"P-{p}" for p in partitions_assigned) if partitions_assigned else "none"
    
    uptime_s = h.get("uptime_s", 0)
    uptime_str = f"{int(uptime_s // 60)}m {int(uptime_s % 60)}s" if uptime_s else "—"
    
    with col:
        st.markdown(f"##### {name}")
        st.markdown(f"**Status:** `{status.upper()}`")
        st.markdown(f"**PID:** {h.get('pid', '—')}")
        st.markdown(f"**Uptime:** {uptime_str}")
        st.markdown(f"**CPU:** {h.get('cpu_percent', 0):.1f}%")
        st.progress(min(int(h.get('cpu_percent', 0)), 100))
        st.markdown(f"**Memory:** {h.get('memory_mb', 0):.1f} MB")
        st.markdown(f"**Partitions:** {partitions_str}")

# ---------------------------------------------------------------------------
# Consumer Assignment Table
# ---------------------------------------------------------------------------

st.markdown("### 📌 Consumer Assignment Table")

assign_rows = []
for cname in CONSUMER_NAMES:
    a = assignments.get(cname, {})
    partitions = a.get("partitions", [])
    for p in partitions:
        assign_rows.append({
            "Consumer": cname,
            "Partition": f"Partition {p}",
            "Event Type": ["login", "payment", "order"][p] if p < 3 else "unknown",
            "Status": a.get("status", "—").upper(),
            "Assigned At": a.get("assigned_at", "—")[:19].replace("T", " ") if a.get("assigned_at") else "—",
        })

if assign_rows:
    st.dataframe(pd.DataFrame(assign_rows), use_container_width=True, hide_index=True)
else:
    st.info("No active partition assignments. Start consumers to see assignments here.")
# ---------------------------------------------------------------------------
# Consumer Throughput Chart - Simplified & Beautiful (FIXED)
# ---------------------------------------------------------------------------

st.markdown("### 📈 Consumer Throughput (msg/s)")

# Prepare data for throughput chart
consumer_colors = {
    "ConsumerA": "#58a6ff",
    "ConsumerB": "#3fb950", 
    "ConsumerC": "#bc8cff",
}

# Collect all throughput data
throughput_data = {}
max_throughput = 0
for cname in CONSUMER_NAMES:
    series = throughput.get(cname, [])
    if series:
        throughput_data[cname] = {
            "time": [s["ts"][11:19] for s in series],  # HH:MM:SS
            "mps": [s["mps"] for s in series]
        }
        max_throughput = max(max_throughput, max([s["mps"] for s in series]) if series else 0)

# Create three columns for different views
view_option = st.radio(
    "Select View:",
    ["📊 Bar Chart (Easy to Compare)", "📈 Line Chart (See Trends)", "💚 Health Cards (Quick Status)"],
    horizontal=True,
    key="throughput_view"
)

if view_option == "📊 Bar Chart (Easy to Compare)":
    # Get latest throughput for each consumer
    latest_data = []
    for cname in CONSUMER_NAMES:
        series = throughput.get(cname, [])
        latest_mps = series[-1]["mps"] if series else 0
        latest_data.append({"Consumer": cname, "Throughput (msg/s)": latest_mps})
    
    df_latest = pd.DataFrame(latest_data)
    
    # Create a beautiful bar chart
    fig_bar = go.Figure()
    
    colors = ["#58a6ff", "#3fb950", "#bc8cff"]
    for i, row in df_latest.iterrows():
        fig_bar.add_trace(go.Bar(
            x=[row["Consumer"]],
            y=[row["Throughput (msg/s)"]],
            name=row["Consumer"],
            marker_color=colors[i],
            marker=dict(
                line=dict(color='white', width=2),
                cornerradius=10
            ),
            text=[f"{row['Throughput (msg/s)']:.1f} msg/s"],
            textposition='outside',
            textfont=dict(color='white', size=14),
            width=0.6,
            hovertemplate='<b>%{x}</b><br>Throughput: %{y:.1f} msg/s<extra></extra>'
        ))
    
    # Add threshold line
    max_y = max(df_latest["Throughput (msg/s)"]) if len(df_latest) > 0 else 100
    fig_bar.add_hline(y=200, line_dash="dash", line_color="#f85149", 
                      annotation_text="⚠️ High Traffic Alert (200 msg/s)",
                      annotation_position="top right",
                      annotation_font_size=11,
                      annotation_font_color="#f85149")
    
    fig_bar.update_layout(
        title=dict(
            text="Current Consumer Throughput",
            font=dict(color="#e6edf3", size=18),
            x=0.5
        ),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="#e6edf3", size=12),
        height=400,
        showlegend=False,
        xaxis=dict(
            title="Consumer",
            tickfont=dict(color="#e6edf3", size=12),
            gridcolor="#21262d",
            showgrid=False
        ),
        yaxis=dict(
            title="Throughput (messages per second)",
            tickfont=dict(color="#e6edf3", size=12),
            gridcolor="#21262d",
            showgrid=True,
            zeroline=True,
            zerolinecolor="#30363d",
            range=[0, max(max_y * 1.2, 250)]
        ),
        bargap=0.5
    )
    
    st.plotly_chart(fig_bar, use_container_width=True)
    
    # Add simple interpretation
    if len(df_latest) > 0:
        max_consumer = df_latest.loc[df_latest["Throughput (msg/s)"].idxmax(), "Consumer"]
        max_value = df_latest["Throughput (msg/s)"].max()
        st.info(f"💡 **Insight:** {max_consumer} has the highest throughput at {max_value:.1f} msg/s")

elif view_option == "📈 Line Chart (See Trends)":
    # Create line chart showing trends over time
    fig_line = go.Figure()
    
    for cname in CONSUMER_NAMES:
        data = throughput_data.get(cname, {})
        if data.get("time"):
            fig_line.add_trace(go.Scatter(
                x=data["time"],
                y=data["mps"],
                name=cname,
                mode='lines+markers',
                line=dict(width=3, color=consumer_colors[cname]),
                marker=dict(size=8, symbol='circle', color=consumer_colors[cname], 
                           line=dict(width=2, color='white')),
                hovertemplate='<b>%{fullData.name}</b><br>⏰ Time: %{x}<br>📊 Throughput: %{y:.1f} msg/s<extra></extra>'
            ))
    
    # Add threshold line
    fig_line.add_hline(y=200, line_dash="dash", line_color="#f85149", opacity=0.8,
                       annotation_text="⚠️ Alert: High Traffic (>200 msg/s)",
                       annotation_position="top right",
                       annotation_font_size=11,
                       annotation_font_color="#f85149")
    
    fig_line.update_layout(
        title=dict(
            text="Throughput Trends Over Time",
            font=dict(color="#e6edf3", size=18),
            x=0.5
        ),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="#e6edf3", size=12),
        height=400,
        xaxis=dict(
            title="Time (HH:MM:SS)",
            tickfont=dict(color="#e6edf3", size=10),
            gridcolor="#21262d",
            showgrid=True,
            tickangle=45
        ),
        yaxis=dict(
            title="Throughput (messages per second)",
            tickfont=dict(color="#e6edf3", size=12),
            gridcolor="#21262d",
            showgrid=True,
            zeroline=True,
            zerolinecolor="#30363d"
        ),
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(0,0,0,0.6)",
            bordercolor="#30363d",
            borderwidth=1,
            font=dict(color="#e6edf3", size=12)
        )
    )
    
    st.plotly_chart(fig_line, use_container_width=True)
    
    # Add trend interpretation
    if throughput_data:
        st.caption("📈 **How to read:** Upward trend = increasing load | Downward trend = decreasing load | Flat line = steady processing")

else:  # Health Cards view
    # Create metric cards for each consumer
    metric_cols = st.columns(3)
    
    for idx, cname in enumerate(CONSUMER_NAMES):
        series = throughput.get(cname, [])
        if series:
            mps_values = [s["mps"] for s in series]
            current_mps = mps_values[-1] if mps_values else 0
            avg_mps = sum(mps_values) / len(mps_values) if mps_values else 0
            max_mps = max(mps_values) if mps_values else 0
            
            # Determine status color and icon
            if current_mps > 200:
                status_color = "🔴"
                status_text = "High Load"
            elif current_mps > 100:
                status_color = "🟡"
                status_text = "Moderate"
            else:
                status_color = "🟢"
                status_text = "Normal"
            
            with metric_cols[idx]:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #1c2333 0%, #21262d 100%);
                            border: 1px solid {consumer_colors[cname]};
                            border-radius: 15px;
                            padding: 20px;
                            margin: 5px;
                            text-align: center;">
                    <div style="font-size: 1.2rem; font-weight: 600; color: {consumer_colors[cname]};">
                        {cname}
                    </div>
                    <div style="font-size: 2rem; font-weight: 700; color: #e6edf3; margin: 10px 0;">
                        {current_mps:.1f}
                    </div>
                    <div style="font-size: 0.8rem; color: #8b949e;">msg/s</div>
                    <div style="margin-top: 10px; padding: 5px; background: #0e1117; border-radius: 8px;">
                        <div style="display: flex; justify-content: space-between; font-size: 0.7rem;">
                            <span>📊 Avg: {avg_mps:.1f}</span>
                            <span>⚡ Peak: {max_mps:.1f}</span>
                        </div>
                    </div>
                    <div style="margin-top: 8px;">
                        <span style="font-size: 0.85rem;">{status_color} {status_text}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            with metric_cols[idx]:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #1c2333 0%, #21262d 100%);
                            border: 1px solid #30363d;
                            border-radius: 15px;
                            padding: 20px;
                            margin: 5px;
                            text-align: center;">
                    <div style="font-size: 1.2rem; font-weight: 600; color: {consumer_colors[cname]};">
                        {cname}
                    </div>
                    <div style="font-size: 2rem; font-weight: 700; color: #8b949e; margin: 10px 0;">
                        --
                    </div>
                    <div style="font-size: 0.8rem; color: #8b949e;">msg/s</div>
                    <div style="margin-top: 8px;">
                        <span style="font-size: 0.85rem;">⚪ No Data</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

# Add overall system status
if throughput_data:
    st.markdown("---")
    col_status1, col_status2, col_status3 = st.columns(3)
    
    # Calculate total system throughput
    total_current = 0
    total_avg = 0
    consumer_count = 0
    
    for cname in CONSUMER_NAMES:
        series = throughput.get(cname, [])
        if series:
            mps_values = [s["mps"] for s in series]
            total_current += mps_values[-1] if mps_values else 0
            total_avg += sum(mps_values) / len(mps_values) if mps_values else 0
            consumer_count += 1
    
    if consumer_count > 0:
        total_avg = total_avg / consumer_count
        
        with col_status1:
            st.metric("System Total Throughput", f"{total_current:.1f} msg/s", 
                     help="Combined throughput of all active consumers")
        
        with col_status2:
            st.metric("Average per Consumer", f"{total_avg:.1f} msg/s",
                     help="Average throughput across all consumers")
        
        with col_status3:
            if total_current > 300:
                st.warning("⚠️ High System Load")
            elif total_current > 150:
                st.info("📊 Moderate System Load")
            else:
                st.success("✅ System Load Normal")

# ---------------------------------------------------------------------------
# Rebalance Event Log - Clean & Modern Design (FIXED)
# ---------------------------------------------------------------------------

st.markdown("### 🔄 Rebalance Event Log")

# Create filters in a single row
col_filter1, col_filter2, col_filter3, col_filter4 = st.columns([2, 2, 1, 1])

with col_filter1:
    filter_consumer = st.multiselect(
        "Consumer", 
        options=CONSUMER_NAMES + ["All"], 
        default=["All"],
        key="rebal_consumer_filter"
    )

with col_filter2:
    filter_event = st.multiselect(
        "Event Type", 
        options=["assigned", "revoked", "joined", "stopped", "crashed", "All"], 
        default=["All"],
        key="rebal_event_filter"
    )

with col_filter3:
    max_events = st.selectbox(
        "Show",
        options=[10, 20, 30, 50, 100],
        index=1,
        key="rebal_max_events"
    )

with col_filter4:
    sort_order = st.radio(
        "Sort",
        options=["⬇️ Newest", "⬆️ Oldest"],
        horizontal=True,
        key="rebal_sort_order"
    )

# Apply filters
filtered = rebalances.copy()

if "All" not in filter_consumer and filter_consumer:
    filtered = [e for e in filtered if e.get("consumer") in filter_consumer]
if "All" not in filter_event and filter_event:
    filtered = [e for e in filtered if e.get("event") in filter_event]

# Sort based on user choice
if sort_order == "⬇️ Newest":
    filtered = list(reversed(filtered))
else:
    filtered = filtered

filtered = filtered[:max_events]

if filtered:
    # Display events using Streamlit's native components (no custom HTML)
    for ev in filtered:
        etype = ev.get("event", "")
        consumer = ev.get("consumer", "")
        ts_raw = ev.get("ts", "")
        
        # Format time
        if len(ts_raw) >= 19:
            ts_time = ts_raw[11:19]
        else:
            ts_time = ts_raw
        
        partitions = ev.get("partitions", [])
        detail = ev.get("detail", "")
        
        # Format partitions string
        if partitions:
            parts_str = f"Partitions: {', '.join(str(p) for p in partitions)}"
        else:
            parts_str = ""
        
        # Choose icon and color based on event type
        if etype == "assigned":
            icon = "✅"
            color = "#3fb950"
        elif etype == "revoked":
            icon = "❌"
            color = "#e3b341"
        elif etype == "joined":
            icon = "🔌"
            color = "#58a6ff"
        elif etype == "stopped":
            icon = "⏹️"
            color = "#8b949e"
        elif etype == "crashed":
            icon = "💥"
            color = "#f85149"
        else:
            icon = "📌"
            color = "#8b949e"
        
        # Create columns for the event row
        col_time, col_event, col_parts, col_detail = st.columns([1, 2, 2, 4])
        
        with col_time:
            st.markdown(f"<span style='color: #8b949e; font-family: monospace;'>🕐 {ts_time}</span>", unsafe_allow_html=True)
        
        with col_event:
            st.markdown(f"<span style='color: {color}; font-weight: 600;'>{icon} {consumer}</span> <span style='background: {color}20; color: {color}; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600;'>{etype.upper()}</span>", unsafe_allow_html=True)
        
        with col_parts:
            if parts_str:
                st.markdown(f"<span style='color: #58a6ff; font-family: monospace; font-size: 0.75rem;'>{parts_str}</span>", unsafe_allow_html=True)
            else:
                st.markdown("—")
        
        with col_detail:
            st.markdown(f"<span style='color: #b1bac4; font-size: 0.75rem;'>{detail}</span>", unsafe_allow_html=True)
        
        # Add a subtle separator
        st.markdown("<hr style='margin: 4px 0; border-color: #21262d;'>", unsafe_allow_html=True)
    
    # Summary Statistics
    st.markdown("---")
    st.markdown("#### 📊 Quick Stats")
    
    # Calculate stats
    event_counts = {}
    for ev in rebalances:
        etype = ev.get("event", "")
        event_counts[etype] = event_counts.get(etype, 0) + 1
    
    total_events = len(rebalances)
    assigned_count = event_counts.get("assigned", 0)
    revoked_count = event_counts.get("revoked", 0)
    changes_count = event_counts.get("joined", 0) + event_counts.get("stopped", 0) + event_counts.get("crashed", 0)
    
    # Create 4 compact metric cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Events", total_events)
    
    with col2:
        st.metric("✅ Assignments", assigned_count)
    
    with col3:
        st.metric("❌ Revocations", revoked_count)
    
    with col4:
        st.metric("🔄 Changes", changes_count)
    
    # Simple event distribution using columns
    if event_counts:
        st.markdown("#### 📈 Event Distribution")
        
        # Create a row of progress bars
        ordered_events = [
            ("assigned", "✅ Assigned", "#3fb950"),
            ("revoked", "❌ Revoked", "#e3b341"),
            ("joined", "🔌 Joined", "#58a6ff"),
            ("stopped", "⏹️ Stopped", "#8b949e"),
            ("crashed", "💥 Crashed", "#f85149")
        ]
        
        for event_key, event_label, color in ordered_events:
            count = event_counts.get(event_key, 0)
            if count > 0:
                percentage = (count / total_events) * 100
                
                # Use columns for better layout
                col_label, col_bar, col_count = st.columns([1.5, 3, 1])
                with col_label:
                    st.markdown(f"<span style='color: {color}; font-size: 0.8rem;'>{event_label}</span>", unsafe_allow_html=True)
                with col_bar:
                    st.progress(percentage / 100)
                with col_count:
                    st.markdown(f"<span style='color: #8b949e; font-size: 0.75rem;'>{count} ({percentage:.1f}%)</span>", unsafe_allow_html=True)
    
    # Export button
    st.markdown("---")
    col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
    with col_btn2:
        if st.button("📥 Export Event Log", use_container_width=True, key="export_btn"):
            import json
            from datetime import datetime
            export_data = {
                "export_time": datetime.now().isoformat(),
                "total_events": total_events,
                "events": rebalances
            }
            json_str = json.dumps(export_data, indent=2)
            st.download_button(
                label="✅ Click to Save",
                data=json_str,
                file_name=f"rebalance_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                key="download_btn",
                use_container_width=True
            )

else:
    st.info("ℹ️ No rebalance events match the current filters.")
    
    with st.expander("ℹ️ About Rebalance Events"):
        st.markdown("""
        **Rebalance events** occur when the consumer group changes:
        
        | Event | Meaning |
        |-------|---------|
        | ✅ **ASSIGNED** | Partitions assigned to a consumer |
        | ❌ **REVOKED** | Partitions taken away from a consumer |
        | 🔌 **JOINED** | A new consumer joined the group |
        | ⏹️ **STOPPED** | A consumer gracefully stopped |
        | 💥 **CRASHED** | A consumer stopped unexpectedly |
        
        💡 **Try this:** Use the Start/Stop buttons in the sidebar to trigger rebalance events!
        """)
        
# Auto-refresh
# ---------------------------------------------------------------------------

time.sleep(refresh_rate)
st.rerun()
