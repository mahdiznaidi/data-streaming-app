import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import time

from data_loader import load_tumbling, load_sliding
from ml_model import detect_anomalies

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Stream Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Crypto Stream Dashboard")
st.caption("Powered by Binance WebSocket → Kafka → Spark → ML")

# ── Controls ────────────────────────────────────────────────
refresh = st.sidebar.slider("Auto-refresh interval (sec)", 5, 60, 10)
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)

# ── Load data ────────────────────────────────────────────────
tumbling_df = load_tumbling()
sliding_df = load_sliding()

if tumbling_df.empty:
    st.warning("⏳ No data yet. Make sure Spark is running with SINK=parquet")
    st.stop()

# Safety: ensure window_start exists
if "window_start" not in tumbling_df.columns:
    st.error("Missing column: window_start. Check your parquet schema / loader.")
    st.stop()

# ── Symbol filter ────────────────────────────────────────────
symbols = sorted(tumbling_df["symbol"].unique().tolist()) if "symbol" in tumbling_df.columns else []
default_syms = symbols[:3] if len(symbols) >= 3 else symbols

selected = st.sidebar.multiselect("Symbols", symbols, default=default_syms)

if selected:
    tumbling_df = tumbling_df[tumbling_df["symbol"].isin(selected)]
    if not sliding_df.empty and "symbol" in sliding_df.columns:
        sliding_df = sliding_df[sliding_df["symbol"].isin(selected)]

# ── Run ML ───────────────────────────────────────────────────
tumbling_df = detect_anomalies(tumbling_df)

# ── KPI Row ──────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_trades = int(tumbling_df["trade_count"].sum()) if "trade_count" in tumbling_df.columns else 0
col1.metric("Total Trades", total_trades)

col2.metric("Symbols tracked", len(symbols))

anomaly_count = int(tumbling_df["anomaly"].sum()) if "anomaly" in tumbling_df.columns else 0
col3.metric("Anomalies detected", anomaly_count)

if "avg_price" in tumbling_df.columns and not tumbling_df.empty:
    latest = tumbling_df.sort_values("window_start").iloc[-1]
    col4.metric("Latest avg price", f"${float(latest['avg_price']):.4f}", str(latest.get("symbol", "")))
else:
    col4.metric("Latest avg price", "N/A")

st.divider()

# ── Tabs ─────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Aggregations", "🔍 Anomaly Detection", "📋 Raw Data"])

with tab1:
    st.subheader("Price Over Time (Tumbling 1-min windows)")
    if "avg_price" in tumbling_df.columns and "symbol" in tumbling_df.columns:
        fig = px.line(
            tumbling_df,
            x="window_start",
            y="avg_price",
            color="symbol",
            title="Average Price per Symbol"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Missing columns for price plot (need: window_start, avg_price, symbol).")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Trade Count (1-min windows)")
        if "trade_count" in tumbling_df.columns and "symbol" in tumbling_df.columns:
            fig2 = px.bar(
                tumbling_df,
                x="window_start",
                y="trade_count",
                color="symbol",
                title="Trade Count"
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Missing columns for trade count plot (need: window_start, trade_count, symbol).")

    with col_b:
        st.subheader("Trade Value USD (1-min windows)")
        if "sum_value" in tumbling_df.columns and "symbol" in tumbling_df.columns:
            fig3 = px.area(
                tumbling_df,
                x="window_start",
                y="sum_value",
                color="symbol",
                title="Trade Value (sum_value)"
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Missing columns for sum_value plot (need: window_start, sum_value, symbol).")

    st.subheader("Price Range (Sliding 5-min windows)")
    needed = {"window_start", "min_price", "max_price", "symbol"}
    if not sliding_df.empty and needed.issubset(set(sliding_df.columns)):
        fig4 = go.Figure()
        for sym in selected:
            df_sym = sliding_df[sliding_df["symbol"] == sym].sort_values("window_start")
            fig4.add_trace(go.Scatter(
                x=df_sym["window_start"], y=df_sym["max_price"],
                name=f"{sym} max", mode="lines"
            ))
            fig4.add_trace(go.Scatter(
                x=df_sym["window_start"], y=df_sym["min_price"],
                name=f"{sym} min", mode="lines", fill="tonexty"
            ))
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No sliding data yet (or missing columns: window_start, min_price, max_price, symbol).")

with tab2:
    st.subheader("🚨 Anomaly Detection — Isolation Forest")
    st.caption("Anomalies = unusual patterns detected by ML on tumbling-window aggregations")

    if {"anomaly", "avg_price", "window_start"}.issubset(set(tumbling_df.columns)):
        anomalies = tumbling_df[tumbling_df["anomaly"] == True]
        normals = tumbling_df[tumbling_df["anomaly"] == False]

        fig5 = go.Figure()
        fig5.add_trace(go.Scatter(
            x=normals["window_start"], y=normals["avg_price"],
            mode="markers", name="Normal"
        ))
        fig5.add_trace(go.Scatter(
            x=anomalies["window_start"], y=anomalies["avg_price"],
            mode="markers", name="Anomaly", marker_symbol="x", marker_size=12
        ))
        fig5.update_layout(title="Price Anomalies Over Time")
        st.plotly_chart(fig5, use_container_width=True)

        if not anomalies.empty:
            st.error(f"⚠️ {len(anomalies)} anomalies detected!")
            cols = [c for c in ["symbol", "window_start", "avg_price", "trade_count", "sum_value", "anomaly_score"] if c in anomalies.columns]
            st.dataframe(anomalies[cols], use_container_width=True)
        else:
            st.success("✅ No anomalies detected in current window")
    else:
        st.info("Missing columns for anomaly view (need: anomaly, avg_price, window_start).")

with tab3:
    st.subheader("Raw Tumbling Window Data")
    st.dataframe(tumbling_df, use_container_width=True)

    st.subheader("Raw Sliding Window Data")
    st.dataframe(sliding_df, use_container_width=True)

# ── Auto-refresh ─────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh)
    st.rerun()