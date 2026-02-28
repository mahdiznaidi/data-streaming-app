import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import time

from data_loader import load_tumbling, load_sliding
from ml_model import detect_anomalies, predict_prices

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Stream Dashboard",
    page_icon="📈",
    layout="wide"
)

# ── Dark trading terminal CSS ─────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@700;800&display=swap');

:root {
    --bg: #050810; --surface: #0d1117; --card: #111827;
    --border: #1f2937; --accent: #00d4aa; --accent2: #f59e0b;
    --danger: #ef4444; --green: #10b981; --muted: #6b7280; --text: #e2e8f0;
}
html, body, [data-testid="stAppViewContainer"] {
    background: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
h1,h2,h3 { font-family: 'Syne', sans-serif !important; }
.metric-card {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 8px; padding: 18px 22px; position: relative; overflow: hidden;
}
.metric-card::before {
    content:''; position:absolute; top:0; left:0; right:0; height:2px;
    background: linear-gradient(90deg, var(--accent), transparent);
}
.metric-label { font-size:11px; letter-spacing:2px; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.metric-value { font-size:26px; font-weight:700; color:var(--accent); line-height:1; }
.metric-sub   { font-size:11px; color:var(--muted); margin-top:4px; }
.status-dot   { display:inline-block; width:8px; height:8px; background:var(--accent);
                border-radius:50%; animation:pulse 2s infinite; margin-right:6px; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.section-title {
    font-size:12px; letter-spacing:3px; text-transform:uppercase; color:var(--muted);
    border-bottom:1px solid var(--border); padding-bottom:8px; margin-bottom:16px;
}
div[data-testid="metric-container"] {
    background: var(--card) !important; border:1px solid var(--border) !important; border-radius:8px !important;
}
.stTabs [data-baseweb="tab"] { background: transparent !important; color: var(--muted) !important; }
.stTabs [aria-selected="true"] { color: var(--accent) !important; border-bottom: 2px solid var(--accent) !important; }
</style>
""", unsafe_allow_html=True)

CHART_BG = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="JetBrains Mono, monospace", color="#94a3b8", size=11),
    xaxis=dict(gridcolor="#1f2937", showgrid=True, zeroline=False),
    yaxis=dict(gridcolor="#1f2937", showgrid=True, zeroline=False),
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#94a3b8", size=10)),
)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='display:flex;align-items:center;gap:10px;margin-bottom:4px'>
  <span style='font-family:Syne,sans-serif;font-size:26px;font-weight:800;color:#e2e8f0'>CRYPTO</span>
  <span style='font-family:Syne,sans-serif;font-size:26px;font-weight:800;color:#00d4aa'>STREAM</span>
  <span style='font-size:11px;color:#6b7280;margin-left:6px'>REAL-TIME ANALYTICS · ML POWERED</span>
</div>
<div style='font-size:11px;color:#6b7280;margin-bottom:16px'>
  <span class="status-dot"></span>Binance WebSocket → Kafka → Spark → ML Dashboard
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")
    refresh      = st.slider("Auto-refresh (sec)", 5, 60, 15)
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    st.markdown("---")
    if st.button("🔄 Refresh Now"):
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
tumbling_df = load_tumbling()
sliding_df  = load_sliding()

if tumbling_df.empty:
    st.markdown("""
    <div style='text-align:center;padding:80px 20px'>
        <div style='font-size:48px'>📡</div>
        <div style='font-family:Syne,sans-serif;font-size:22px;color:#e2e8f0;margin:12px 0'>Waiting for data...</div>
        <div style='color:#6b7280;font-size:12px'>Make sure Kafka + producer + Spark are running</div>
        <br/><code style='background:#0d1117;padding:8px 16px;border-radius:4px;color:#00d4aa'>
        python producer.py &nbsp;|&nbsp; SINK=parquet python -m spark.app
        </code>
    </div>""", unsafe_allow_html=True)
    if auto_refresh:
        time.sleep(5)
        st.rerun()
    st.stop()

# ── Symbol filter ─────────────────────────────────────────────────────────────
symbols      = sorted(tumbling_df["symbol"].unique().tolist()) if "symbol" in tumbling_df.columns else []
default_syms = symbols[:2] if len(symbols) >= 2 else symbols

with st.sidebar:
    st.markdown("---")
    st.markdown("### 🪙 Symbols")
    selected = st.multiselect("Filter", symbols, default=default_syms)

if selected:
    tumbling_df = tumbling_df[tumbling_df["symbol"].isin(selected)]
    if not sliding_df.empty and "symbol" in sliding_df.columns:
        sliding_df = sliding_df[sliding_df["symbol"].isin(selected)]

# ── Run ML ────────────────────────────────────────────────────────────────────
with st.spinner("Running ML models..."):
    tumbling_df = detect_anomalies(tumbling_df)
    tumbling_df = predict_prices(tumbling_df)

# ── KPI Row ───────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title"><span class="status-dot"></span>LIVE METRICS</div>', unsafe_allow_html=True)

col1, col2, col3, col4, col5 = st.columns(5)

total_trades   = int(tumbling_df["trade_count"].sum()) if "trade_count" in tumbling_df.columns else 0
anomaly_count  = int(tumbling_df["anomaly"].sum()) if "anomaly" in tumbling_df.columns else 0
latest         = tumbling_df.sort_values("window_start").iloc[-1] if not tumbling_df.empty else {}
latest_price   = float(latest.get("avg_price", 0)) if len(latest) > 0 else 0
pred_price     = float(latest.get("predicted_price", 0)) if len(latest) > 0 else 0
latest_sym     = str(latest.get("symbol", "")) if len(latest) > 0 else ""
total_volume   = float(tumbling_df["sum_value"].sum()) if "sum_value" in tumbling_df.columns else 0

with col1:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Latest Price</div><div class="metric-value">${latest_price:,.2f}</div><div class="metric-sub">{latest_sym}</div></div>', unsafe_allow_html=True)
with col2:
    direction = "📈" if pred_price >= latest_price else "📉"
    color = "#10b981" if pred_price >= latest_price else "#f43f5e"
    st.markdown(f'<div class="metric-card"><div class="metric-label">Next Prediction</div><div class="metric-value" style="color:{color}">${pred_price:,.2f}</div><div class="metric-sub">{direction} Linear Regression</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Total Trades</div><div class="metric-value" style="color:#6366f1">{total_trades:,}</div><div class="metric-sub">across all windows</div></div>', unsafe_allow_html=True)
with col4:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Total Volume</div><div class="metric-value" style="color:#10b981">${total_volume:,.0f}</div><div class="metric-sub">USDT</div></div>', unsafe_allow_html=True)
with col5:
    badge_color = "#ef4444" if anomaly_count > 0 else "#10b981"
    badge_text  = f"⚠ {anomaly_count} ANOMALIES" if anomaly_count > 0 else "✓ NORMAL"
    st.markdown(f'<div class="metric-card"><div class="metric-label">ML Status</div><div class="metric-value" style="color:{badge_color};font-size:18px">{badge_text}</div><div class="metric-sub">Isolation Forest</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 Aggregations", "🤖 ML Predictions", "🔍 Anomaly Detection", "📋 Raw Data"])

# ── Tab 1: Aggregations ───────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-title">TUMBLING 1-MIN WINDOWS</div>', unsafe_allow_html=True)

    if "avg_price" in tumbling_df.columns:
        fig = px.line(tumbling_df, x="window_start", y="avg_price", color="symbol",
                      title="Average Price per Symbol")
        fig.update_layout(**CHART_BG)
        fig.update_traces(line_width=2)
        st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        if "trade_count" in tumbling_df.columns:
            fig2 = px.bar(tumbling_df, x="window_start", y="trade_count", color="symbol",
                          title="Trade Count per Window")
            fig2.update_layout(**CHART_BG)
            st.plotly_chart(fig2, use_container_width=True)
    with col_b:
        if "sum_value" in tumbling_df.columns:
            fig3 = px.area(tumbling_df, x="window_start", y="sum_value", color="symbol",
                           title="Trade Volume USDT per Window")
            fig3.update_layout(**CHART_BG)
            st.plotly_chart(fig3, use_container_width=True)

    # Spike count from Spark
    if "spike_count" in tumbling_df.columns:
        st.markdown('<div class="section-title">PRICE SPIKES (FROM PRODUCER)</div>', unsafe_allow_html=True)
        fig_spike = px.bar(tumbling_df, x="window_start", y="spike_count", color="symbol",
                           title="Price Spikes Detected per Window",
                           color_discrete_sequence=["#ef4444", "#f59e0b"])
        fig_spike.update_layout(**CHART_BG)
        st.plotly_chart(fig_spike, use_container_width=True)

    # Sliding window
    st.markdown('<div class="section-title">SLIDING 5-MIN WINDOWS</div>', unsafe_allow_html=True)
    needed = {"window_start", "min_price", "max_price", "symbol"}
    if not sliding_df.empty and needed.issubset(set(sliding_df.columns)):
        fig4 = go.Figure()
        colors = {"BTCUSDT": "#00d4aa", "ETHUSDT": "#6366f1"}
        for sym in (selected or symbols):
            df_sym = sliding_df[sliding_df["symbol"] == sym].sort_values("window_start")
            c = colors.get(sym, "#f59e0b")
            fig4.add_trace(go.Scatter(
                x=pd.concat([df_sym["window_start"], df_sym["window_start"][::-1]]),
                y=pd.concat([df_sym["max_price"], df_sym["min_price"][::-1]]),
                fill="toself", fillcolor=f"rgba({int(c[1:3],16)},{int(c[3:5],16)},{int(c[5:7],16)},0.1)",
                line=dict(color="rgba(0,0,0,0)"), name=f"{sym} range"
            ))
            fig4.add_trace(go.Scatter(
                x=df_sym["window_start"], y=df_sym["avg_price"],
                name=f"{sym} avg", line=dict(color=c, width=2)
            ))
        fig4.update_layout(**CHART_BG, title="Price Range — Sliding 5-min Window")
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No sliding window data yet.")

# ── Tab 2: ML Predictions ─────────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-title">LINEAR REGRESSION — NEXT WINDOW PRICE PREDICTION</div>', unsafe_allow_html=True)
    st.caption("Model trained on rolling means, volatility, momentum, and volume features from Spark aggregations")

    if "predicted_price" in tumbling_df.columns and "avg_price" in tumbling_df.columns:
        for sym in (selected or symbols):
            df_sym = tumbling_df[tumbling_df["symbol"] == sym].sort_values("window_start")
            if df_sym.empty:
                continue
            fig_pred = go.Figure()
            fig_pred.add_trace(go.Scatter(
                x=df_sym["window_start"], y=df_sym["avg_price"],
                name="Actual Price", line=dict(color="#00d4aa", width=2),
                fill="tozeroy", fillcolor="rgba(0,212,170,0.05)"
            ))
            fig_pred.add_trace(go.Scatter(
                x=df_sym["window_start"], y=df_sym["predicted_price"],
                name="Predicted Price", line=dict(color="#f59e0b", width=2, dash="dot")
            ))
            fig_pred.update_layout(**CHART_BG,
                title=f"{sym} — Actual vs Predicted Price",
                yaxis_title="Price (USDT)")
            st.plotly_chart(fig_pred, use_container_width=True)

        # Prediction error
        tumbling_df["pred_error"] = tumbling_df["predicted_price"] - tumbling_df["avg_price"]
        fig_err = px.bar(tumbling_df, x="window_start", y="pred_error", color="symbol",
                         title="Prediction Error (Predicted − Actual)")
        fig_err.update_layout(**CHART_BG)
        st.plotly_chart(fig_err, use_container_width=True)
    else:
        st.info("Need at least 15 data points to train the price predictor.")

# ── Tab 3: Anomaly Detection ──────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-title">ISOLATION FOREST — ANOMALY DETECTION</div>', unsafe_allow_html=True)
    st.caption("Anomalies = unusual windows detected from price, volume, trade count, spike rate features")

    if {"anomaly", "avg_price", "window_start"}.issubset(set(tumbling_df.columns)):
        for sym in (selected or symbols):
            df_sym   = tumbling_df[tumbling_df["symbol"] == sym].sort_values("window_start")
            normals  = df_sym[~df_sym["anomaly"]]
            anomalies = df_sym[df_sym["anomaly"]]

            fig5 = go.Figure()
            fig5.add_trace(go.Scatter(
                x=normals["window_start"], y=normals["avg_price"],
                mode="markers+lines", name="Normal",
                marker=dict(color="#10b981", size=4),
                line=dict(color="#10b981", width=1.5)
            ))
            if not anomalies.empty:
                fig5.add_trace(go.Scatter(
                    x=anomalies["window_start"], y=anomalies["avg_price"],
                    mode="markers", name="🚨 Anomaly",
                    marker=dict(color="#ef4444", size=14, symbol="x",
                                line=dict(color="#ef4444", width=2))
                ))
            fig5.update_layout(**CHART_BG, title=f"{sym} — Anomaly Detection")
            st.plotly_chart(fig5, use_container_width=True)

        # Anomaly score over time
        if "anomaly_score" in tumbling_df.columns:
            fig_score = px.line(tumbling_df, x="window_start", y="anomaly_score", color="symbol",
                                title="Anomaly Score Over Time (lower = more anomalous)")
            fig_score.update_layout(**CHART_BG)
            st.plotly_chart(fig_score, use_container_width=True)

        # Anomaly table
        all_anomalies = tumbling_df[tumbling_df["anomaly"] == True]
        if not all_anomalies.empty:
            st.error(f"⚠️ {len(all_anomalies)} anomalies detected across all symbols!")
            cols = [c for c in ["symbol", "window_start", "avg_price", "trade_count",
                                "sum_value", "spike_count", "anomaly_score"] if c in all_anomalies.columns]
            st.dataframe(all_anomalies[cols].style.format({
                "avg_price": "${:,.2f}", "sum_value": "${:,.2f}", "anomaly_score": "{:.4f}"
            }), use_container_width=True)
        else:
            st.success("✅ No anomalies detected in current data")
    else:
        st.info("Anomaly columns not available yet.")

# ── Tab 4: Raw Data ───────────────────────────────────────────────────────────
with tab4:
    st.subheader("Tumbling 1-min Window Data")
    st.dataframe(tumbling_df, use_container_width=True)
    st.subheader("Sliding 5-min Window Data")
    st.dataframe(sliding_df, use_container_width=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh)
    st.rerun()
