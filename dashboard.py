"""
dashboard.py
============
Streamlit live dashboard for the Kafka custom-partitioner demo.

Features
--------
• Header KPI cards  – total events, per-partition counts, online consumers
• Bar chart          – partition message distribution (Plotly)
• Pie chart          – event-type breakdown (login / payment / order)
• Consumer assignment table
• Health cards       – CPU%, memory MB, PID, uptime, status per consumer
• Throughput chart   – time-series msg/s for all three consumers
• Rebalance event log – filterable, colour-coded stream
• Auto-refresh       – configurable 1–10 s (default 2 s)
• Reset Metrics button in the sidebar

Run
---
  streamlit run dashboard.py
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
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
    }

    /* ── KPI Cards ── */
    .kpi-card {
        background: linear-gradient(135deg, #1c2333 0%, #21262d 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 24px rgba(88, 166, 255, 0.15);
    }
    .kpi-value {
        font-size: 2.4rem;
        font-weight: 700;
        line-height: 1;
        margin-bottom: 6px;
        background: linear-gradient(90deg, #58a6ff, #bc8cff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .kpi-label {
        font-size: 0.8rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .kpi-sub {
        font-size: 0.75rem;
        color: #58a6ff;
        margin-top: 4px;
    }

    /* ── Partition mini-cards ── */
    .partition-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 14px 18px;
        text-align: center;
    }
    .partition-card.p0 { border-top: 3px solid #58a6ff; }
    .partition-card.p1 { border-top: 3px solid #3fb950; }
    .partition-card.p2 { border-top: 3px solid #bc8cff; }
    .partition-count {
        font-size: 1.8rem;
        font-weight: 700;
        color: #e6edf3;
    }
    .partition-label {
        font-size: 0.72rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }

    /* ── Section headers ── */
    .section-header {
        font-size: 1.05rem;
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
        padding: 16px;
    }
    .health-name {
        font-size: 0.95rem;
        font-weight: 600;
        color: #e6edf3;
        margin-bottom: 10px;
    }
    .health-metric {
        display: flex;
        justify-content: space-between;
        font-size: 0.78rem;
        color: #8b949e;
        margin-bottom: 4px;
    }
    .health-metric span.val { color: #e6edf3; }
    .status-badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.7rem;
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
        padding: 7px 12px;
        margin-bottom: 4px;
        border-radius: 6px;
        font-size: 0.78rem;
        display: flex;
        gap: 10px;
        align-items: center;
    }
    .rebal-joined   { background: #162032; border-left: 3px solid #58a6ff; }
    .rebal-assigned { background: #1b3a2d; border-left: 3px solid #3fb950; }
    .rebal-revoked  { background: #2d2115; border-left: 3px solid #e3b341; }
    .rebal-crashed  { background: #3d1b1b; border-left: 3px solid #f85149; }
    .rebal-ts    { color: #8b949e; min-width: 85px; font-size: 0.68rem; }
    .rebal-cons  { color: #58a6ff; font-weight: 600; min-width: 90px; }
    .rebal-badge {
        font-size: 0.65rem; font-weight: 700; text-transform: uppercase;
        padding: 1px 7px; border-radius: 10px; min-width: 60px; text-align: center;
    }
    .badge-joined   { background: #1b4a7a; color: #58a6ff; }
    .badge-assigned { background: #1b3a2d; color: #3fb950; }
    .badge-revoked  { background: #3d3015; color: #e3b341; }
    .badge-crashed  { background: #5a1e1e; color: #f85149; }
    .rebal-detail { color: #8b949e; }

    /* ── Tables ── */
    [data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

    /* ── Sidebar ── */
    .sidebar-section {
        background: #21262d;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 12px;
        margin-bottom: 12px;
    }

    /* ── Progress bars ── */
    .stProgress > div > div > div { border-radius: 4px; }

    /* Hide Streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONSUMER_NAMES  = ["ConsumerA", "ConsumerB", "ConsumerC"]
PARTITION_COLORS = {
    "0": "#58a6ff",   # blue   – login
    "1": "#3fb950",   # green  – payment
    "2": "#bc8cff",   # purple – order
}
EVENT_TYPE_COLORS = {
    "login"  : "#58a6ff",
    "payment": "#3fb950",
    "order"  : "#bc8cff",
}
REBALANCE_EVENT_COLORS = {
    "joined"  : "rebal-joined",
    "assigned": "rebal-assigned",
    "revoked" : "rebal-revoked",
    "crashed" : "rebal-crashed",
}
BADGE_CLASSES = {
    "joined"  : "badge-joined",
    "assigned": "badge-assigned",
    "revoked" : "badge-revoked",
    "crashed" : "badge-crashed",
}

mm = MetricsManager()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center; padding: 10px 0 20px 0;">
            <div style="font-size: 2rem;">⚡</div>
            <div style="font-size: 1.1rem; font-weight:700; color:#e6edf3;">Kafka Partitioner</div>
            <div style="font-size: 0.7rem; color:#8b949e; letter-spacing:0.1em;">
                LIVE DASHBOARD
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### ⚙️ Settings")
    refresh_rate = st.slider(
        "Auto-refresh (seconds)", min_value=1, max_value=10, value=2, step=1
    )
    show_raw = st.checkbox("Show raw JSON metrics", value=False)

    st.markdown("---")
    st.markdown("### 🔧 Controls")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("🗑️ Reset", use_container_width=True, help="Reset all metrics files"):
            mm.reset_all()
            st.success("Metrics reset!")
            time.sleep(0.5)
            st.rerun()
    with col_r2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

    st.markdown("---")
    st.markdown("### 📖 Partition Map")
    st.markdown(
        """
        <div class="sidebar-section">
            <div style="font-size:0.78rem; color:#8b949e; margin-bottom:8px;">
                Custom Partitioner Routing
            </div>
            <div style="display:flex; justify-content:space-between; font-size:0.8rem; margin-bottom:5px;">
                <span style="color:#58a6ff;">● login</span>
                <span style="color:#e6edf3;">→ P-0</span>
            </div>
            <div style="display:flex; justify-content:space-between; font-size:0.8rem; margin-bottom:5px;">
                <span style="color:#3fb950;">● payment</span>
                <span style="color:#e6edf3;">→ P-1</span>
            </div>
            <div style="display:flex; justify-content:space-between; font-size:0.8rem;">
                <span style="color:#bc8cff;">● order</span>
                <span style="color:#e6edf3;">→ P-2</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### 🧩 Consumer Group")
    st.markdown(
        """
        <div class="sidebar-section" style="font-size:0.78rem; color:#8b949e;">
            Group ID: <span style="color:#58a6ff;">user-events-group</span><br>
            Topic: <span style="color:#3fb950;">user-events</span><br>
            Partitions: <span style="color:#bc8cff;">3</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style="font-size:0.65rem; color:#8b949e; margin-top:20px; text-align:center;">
            Last updated<br>{datetime.now().strftime("%H:%M:%S")}
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_data():
    counts      = mm.get_partition_counts()
    assignments = mm.get_consumer_assignments()
    rebalances  = mm.get_rebalance_history()
    throughput  = mm.get_throughput()
    health      = mm.get_all_health(CONSUMER_NAMES)
    prod_stats  = mm.get_producer_stats()
    return counts, assignments, rebalances, throughput, health, prod_stats


counts, assignments, rebalances, throughput, health, prod_stats = load_data()

# ---------------------------------------------------------------------------
# Derived values
# ---------------------------------------------------------------------------

total_messages = sum(int(v) for v in counts.values())
online_consumers = sum(
    1 for c in CONSUMER_NAMES
    if health.get(c, {}).get("status") == "running"
)

# Infer event-type distribution from partition counts
type_distribution = {
    "login"  : int(counts.get("0", 0)),
    "payment": int(counts.get("1", 0)),
    "order"  : int(counts.get("2", 0)),
}

# ---------------------------------------------------------------------------
# ── HEADER ──────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown(
    """
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:24px;">
        <div style="font-size:2rem;">⚡</div>
        <div>
            <div style="font-size:1.6rem; font-weight:700; color:#e6edf3; line-height:1.1;">
                Kafka Topics & Partitions
            </div>
            <div style="font-size:0.8rem; color:#8b949e;">
                Custom Partitioner · Real-time Consumer Dashboard · H.17
            </div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# ── KPI ROW ──────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-value">{total_messages:,}</div>
            <div class="kpi-label">Total Messages</div>
            <div class="kpi-sub">across all partitions</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k2:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-value">{online_consumers}</div>
            <div class="kpi-label">Online Consumers</div>
            <div class="kpi-sub">out of 3 in group</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k3:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-value">{len(rebalances)}</div>
            <div class="kpi-label">Rebalance Events</div>
            <div class="kpi-sub">in this session</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k4:
    total_sent = prod_stats.get("total_sent", 0)
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-value">{total_sent:,}</div>
            <div class="kpi-label">Events Produced</div>
            <div class="kpi-sub">by the producer</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k5:
    samples = prod_stats.get("samples", [])
    latest_mps = samples[-1]["mps"] if samples else 0.0
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-value">{latest_mps:.1f}</div>
            <div class="kpi-label">Producer msg/s</div>
            <div class="kpi-sub">latest throughput</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div style='margin:16px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── PARTITION MINI-CARDS ─────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">📦 Partition Message Distribution</p>', unsafe_allow_html=True)

pc1, pc2, pc3 = st.columns(3)
partition_meta = [
    ("0", "login",   "p0", "🔐"),
    ("1", "payment", "p1", "💳"),
    ("2", "order",   "p2", "📦"),
]
for col, (pid, etype, css_cls, icon) in zip([pc1, pc2, pc3], partition_meta):
    count = int(counts.get(pid, 0))
    pct   = (count / total_messages * 100) if total_messages > 0 else 0.0
    col.markdown(
        f"""
        <div class="partition-card {css_cls}">
            <div style="font-size:1.4rem; margin-bottom:4px;">{icon}</div>
            <div class="partition-count">{count:,}</div>
            <div class="partition-label">Partition {pid} · {etype}</div>
            <div style="font-size:0.7rem; color:#8b949e; margin-top:4px;">
                {pct:.1f}% of total
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div style='margin:20px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── CHARTS ROW (Bar + Pie) ────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

chart_col, pie_col = st.columns([3, 2], gap="large")

with chart_col:
    st.markdown('<p class="section-header">📊 Partition Bar Chart</p>', unsafe_allow_html=True)

    bar_data = pd.DataFrame({
        "Partition": ["Partition 0\n(login)", "Partition 1\n(payment)", "Partition 2\n(order)"],
        "Messages" : [int(counts.get("0", 0)), int(counts.get("1", 0)), int(counts.get("2", 0))],
        "Color"    : ["#58a6ff", "#3fb950", "#bc8cff"],
    })

    fig_bar = go.Figure(
        go.Bar(
            x           = bar_data["Partition"],
            y           = bar_data["Messages"],
            marker_color= bar_data["Color"],
            text        = bar_data["Messages"],
            textposition= "outside",
            textfont    = dict(color="#e6edf3", size=13),
        )
    )
    fig_bar.update_layout(
        paper_bgcolor="#161b22",
        plot_bgcolor ="#161b22",
        font         = dict(color="#8b949e", size=11),
        margin       = dict(l=0, r=0, t=10, b=0),
        height       = 260,
        xaxis        = dict(showgrid=False, color="#8b949e"),
        yaxis        = dict(
            showgrid  = True,
            gridcolor = "#21262d",
            color     = "#8b949e",
        ),
        showlegend   = False,
    )
    st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

with pie_col:
    st.markdown('<p class="section-header">🍕 Event-Type Breakdown</p>', unsafe_allow_html=True)

    pie_data = pd.DataFrame({
        "Type" : list(type_distribution.keys()),
        "Count": list(type_distribution.values()),
    })

    fig_pie = px.pie(
        pie_data,
        names  = "Type",
        values = "Count",
        color  = "Type",
        color_discrete_map = EVENT_TYPE_COLORS,
        hole   = 0.55,
    )
    fig_pie.update_traces(
        textinfo    = "percent+label",
        textfont    = dict(color="#e6edf3", size=11),
        marker      = dict(line=dict(color="#0e1117", width=2)),
    )
    fig_pie.update_layout(
        paper_bgcolor = "#161b22",
        plot_bgcolor  = "#161b22",
        font          = dict(color="#8b949e", size=11),
        margin        = dict(l=0, r=0, t=10, b=10),
        height        = 260,
        showlegend    = True,
        legend        = dict(
            font            = dict(color="#8b949e"),
            bgcolor         = "#21262d",
            bordercolor     = "#30363d",
            borderwidth     = 1,
            orientation     = "v",
            x=1.05, y=0.5,
        ),
    )
    # Donut center annotation
    total_pie = sum(type_distribution.values())
    fig_pie.add_annotation(
        text    = f"<b>{total_pie:,}</b><br><span style='font-size:10px'>total</span>",
        x=0.5, y=0.5,
        font    = dict(size=14, color="#e6edf3"),
        showarrow=False,
    )
    st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='margin:4px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── CONSUMER HEALTH CARDS ─────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">🖥️ Consumer Health Monitor</p>', unsafe_allow_html=True)

hc1, hc2, hc3 = st.columns(3)
status_colors = {
    "running": "#3fb950",
    "stopped": "#e3b341",
    "crashed": "#f85149",
    "offline": "#8b949e",
    "waiting": "#58a6ff",
}

for col, name in zip([hc1, hc2, hc3], CONSUMER_NAMES):
    h  = health.get(name, {})
    status = h.get("status", "waiting")
    sc     = status_colors.get(status, "#8b949e")
    partitions_assigned = assignments.get(name, {}).get("partitions", [])
    partitions_str = ", ".join(f"P-{p}" for p in partitions_assigned) if partitions_assigned else "none"

    uptime_s = h.get("uptime_s", 0)
    uptime_str = f"{int(uptime_s // 60)}m {int(uptime_s % 60)}s" if uptime_s else "—"

    cpu_pct = h.get("cpu_percent", 0)
    mem_mb  = h.get("memory_mb", 0)
    pid     = h.get("pid", "—")

    col.markdown(
        f"""
        <div class="health-card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
                <div class="health-name">{name}</div>
                <span class="status-badge status-{status}">{status}</span>
            </div>
            <div class="health-metric">
                <span>🔢 PID</span>
                <span class="val">{pid}</span>
            </div>
            <div class="health-metric">
                <span>⏱️ Uptime</span>
                <span class="val">{uptime_str}</span>
            </div>
            <div class="health-metric">
                <span>💻 CPU</span>
                <span class="val">{cpu_pct:.1f}%</span>
            </div>
            <div class="health-metric">
                <span>🧠 Memory</span>
                <span class="val">{mem_mb:.1f} MB</span>
            </div>
            <div class="health-metric">
                <span>📌 Partitions</span>
                <span class="val" style="color:#58a6ff;">{partitions_str}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    # CPU progress bar
    col.progress(min(int(cpu_pct), 100))

st.markdown("<div style='margin:20px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── CONSUMER ASSIGNMENT TABLE ─────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">📌 Consumer Partition Assignment Table</p>', unsafe_allow_html=True)

assign_rows = []
for cname in CONSUMER_NAMES:
    a = assignments.get(cname, {})
    partitions = a.get("partitions", [])
    for p in partitions:
        assign_rows.append({
            "Consumer"     : cname,
            "Partition"    : f"Partition {p}",
            "Event Type"   : ["login", "payment", "order"][p] if p < 3 else "unknown",
            "Status"       : a.get("status", "—").upper(),
            "Assigned At"  : a.get("assigned_at", "—")[:19].replace("T", " ") if a.get("assigned_at") else "—",
        })

if assign_rows:
    df_assign = pd.DataFrame(assign_rows)
    st.dataframe(
        df_assign,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Consumer"  : st.column_config.TextColumn("Consumer",   width="medium"),
            "Partition" : st.column_config.TextColumn("Partition",  width="medium"),
            "Event Type": st.column_config.TextColumn("Event Type", width="medium"),
            "Status"    : st.column_config.TextColumn("Status",     width="small"),
            "Assigned At":st.column_config.TextColumn("Assigned At",width="large"),
        },
    )
else:
    st.info("No active partition assignments. Start consumers to see assignments here.")

st.markdown("<div style='margin:8px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── THROUGHPUT CHART ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">📈 Consumer Throughput (msg/s)</p>', unsafe_allow_html=True)

consumer_colors_tp = {
    "ConsumerA": "#58a6ff",
    "ConsumerB": "#3fb950",
    "ConsumerC": "#bc8cff",
}

fig_tp = go.Figure()
has_tp_data = False

for cname in CONSUMER_NAMES:
    series = throughput.get(cname, [])
    if series:
        has_tp_data = True
        ts_vals  = [s["ts"][11:19] for s in series]     # HH:MM:SS
        mps_vals = [s["mps"] for s in series]
        fig_tp.add_trace(
            go.Scatter(
                x          = ts_vals,
                y          = mps_vals,
                mode       = "lines+markers",
                name       = cname,
                line       = dict(color=consumer_colors_tp[cname], width=2),
                marker     = dict(size=4),
                fill       = "tozeroy",
                fillcolor  = consumer_colors_tp[cname].replace("ff", "22"),
            )
        )

if not has_tp_data:
    fig_tp.add_annotation(
        text      = "No throughput data yet — start consumers and producer",
        x=0.5, y=0.5, xref="paper", yref="paper",
        font      = dict(color="#8b949e", size=13),
        showarrow = False,
    )

fig_tp.update_layout(
    paper_bgcolor = "#161b22",
    plot_bgcolor  = "#161b22",
    font          = dict(color="#8b949e", size=11),
    margin        = dict(l=0, r=0, t=10, b=0),
    height        = 220,
    xaxis         = dict(showgrid=False, color="#8b949e", title="Time"),
    yaxis         = dict(showgrid=True, gridcolor="#21262d", color="#8b949e", title="msg/s"),
    legend        = dict(
        font      = dict(color="#8b949e"),
        bgcolor   = "#21262d",
        bordercolor="#30363d",
        borderwidth=1,
        orientation="h",
        x=0, y=1.12,
    ),
    hovermode     = "x unified",
)
st.plotly_chart(fig_tp, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='margin:8px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── REBALANCE EVENT LOG ───────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">🔄 Rebalance Event Log</p>', unsafe_allow_html=True)

# Filters
rf_col1, rf_col2, rf_col3 = st.columns([2, 2, 1])
with rf_col1:
    filter_consumer = st.multiselect(
        "Filter by consumer",
        options=CONSUMER_NAMES + ["All"],
        default=["All"],
        label_visibility="collapsed",
    )
with rf_col2:
    filter_event = st.multiselect(
        "Filter by event type",
        options=["joined", "assigned", "revoked", "crashed", "All"],
        default=["All"],
        label_visibility="collapsed",
    )
with rf_col3:
    max_events = st.number_input("Max rows", min_value=5, max_value=100, value=20, step=5,
                                  label_visibility="collapsed")

# Apply filters
filtered = list(reversed(rebalances))  # newest first

if "All" not in filter_consumer and filter_consumer:
    filtered = [e for e in filtered if e.get("consumer") in filter_consumer]
if "All" not in filter_event and filter_event:
    filtered = [e for e in filtered if e.get("event") in filter_event]

filtered = filtered[:max_events]

if filtered:
    events_html = ""
    for ev in filtered:
        etype     = ev.get("event", "")
        consumer  = ev.get("consumer", "")
        ts_raw    = ev.get("ts", "")
        ts_display= ts_raw[11:19] if len(ts_raw) >= 19 else ts_raw
        partitions= ev.get("partitions", [])
        detail    = ev.get("detail", "")
        parts_str = f"[{', '.join(str(p) for p in partitions)}]" if partitions else ""

        row_class  = REBALANCE_EVENT_COLORS.get(etype, "rebal-joined")
        badge_class= BADGE_CLASSES.get(etype, "badge-joined")

        events_html += f"""
        <div class="rebal-event {row_class}">
            <span class="rebal-ts">{ts_display}</span>
            <span class="rebal-cons">{consumer}</span>
            <span class="rebal-badge {badge_class}">{etype}</span>
            <span class="rebal-detail">{parts_str} {detail}</span>
        </div>
        """
    st.markdown(
        f'<div style="max-height:320px; overflow-y:auto; padding:4px;">{events_html}</div>',
        unsafe_allow_html=True,
    )
else:
    st.info("No rebalance events match the current filters.")

st.markdown("<div style='margin:20px 0'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# ── RAW JSON ─────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

if show_raw:
    st.markdown('<p class="section-header">🔍 Raw Metrics JSON</p>', unsafe_allow_html=True)
    raw_tab1, raw_tab2, raw_tab3, raw_tab4 = st.tabs(
        ["Partition Counts", "Assignments", "Throughput", "Health"]
    )
    with raw_tab1:
        st.json(counts)
    with raw_tab2:
        st.json(assignments)
    with raw_tab3:
        st.json(throughput)
    with raw_tab4:
        st.json(health)

# ---------------------------------------------------------------------------
# ── FOOTER ────────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

st.markdown(
    """
    <div style="text-align:center; padding:24px 0 8px 0;
                font-size:0.72rem; color:#484f58;
                border-top: 1px solid #21262d; margin-top:24px;">
        ⚡ Kafka Partitioner Dashboard · H.17 · Built with Streamlit + Plotly + confluent-kafka
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# ── AUTO-REFRESH ──────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

time.sleep(refresh_rate)
st.rerun()
