import os
import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Dividend Anomaly Dashboard",
    page_icon="📈",
    layout="wide",
)

# --- DB ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        dbname=os.getenv("DB_NAME"),
    )

@st.cache_data(ttl=15)  # refresh every 15 seconds to match ingestion cadence
def load_data():
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM predictions
        ORDER BY created_at DESC
        LIMIT 1000
        """,
        conn,
    )
    return df

# --- Layout ---
st.title("📈 Dividend Anomaly Dashboard")
st.caption("Auto-refreshes every 15 seconds · Z-score anomaly detection")

df = load_data()

if df.empty:
    st.warning("No data yet — make sure the ingestion and processing scripts are running.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
tickers = sorted(df["ticker"].unique().tolist())
selected_tickers = st.sidebar.multiselect("Tickers", tickers, default=tickers[:10])
show_anomalies_only = st.sidebar.toggle("Anomalies only", value=False)

filtered = df[df["ticker"].isin(selected_tickers)]
if show_anomalies_only:
    filtered = filtered[filtered["is_anomaly"] == True]

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
total     = len(filtered)
anomalies = filtered["is_anomaly"].sum() if "is_anomaly" in filtered.columns else 0
pct       = round((anomalies / total * 100), 1) if total > 0 else 0
avg_z     = round(filtered["z_score"].dropna().mean(), 2) if "z_score" in filtered.columns else "—"

col1.metric("Records loaded",    total)
col2.metric("Anomalies flagged", int(anomalies))
col3.metric("Anomaly rate",      f"{pct}%")
col4.metric("Avg z-score",       avg_z)

st.divider()

# --- Chart 1: Cash amount over time, coloured by anomaly ---
st.subheader("Cash amount over time")
if "is_anomaly" in filtered.columns:
    fig1 = px.scatter(
        filtered.sort_values("created_at"),
        x="created_at",
        y="cash_amount",
        color="is_anomaly",
        color_discrete_map={True: "#E24B4A", False: "#1D9E75"},
        hover_data=["ticker", "z_score", "dividend_type"],
        labels={
            "created_at":  "Time",
            "cash_amount": "Cash amount",
            "is_anomaly":  "Anomaly",
        },
    )
    fig1.update_traces(marker=dict(size=7, opacity=0.8))
    fig1.update_layout(legend_title_text="Anomaly")
    st.plotly_chart(fig1, use_container_width=True)

# --- Chart 2: Z-score distribution + Anomaly count by ticker ---
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Z-score distribution")
    z_data = filtered["z_score"].dropna()
    if not z_data.empty:
        fig2 = px.histogram(
            z_data,
            nbins=40,
            labels={"value": "Z-score", "count": "Count"},
            color_discrete_sequence=["#378ADD"],
        )
        fig2.add_vline(
            x=2.0, line_dash="dash", line_color="#E24B4A",
            annotation_text="Threshold (2σ)", annotation_position="top right"
        )
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Z-scores not yet available — need more history per ticker.")

with col_b:
    st.subheader("Anomaly count by ticker")
    anomaly_counts = (
        filtered[filtered["is_anomaly"] == True]
        .groupby("ticker")
        .size()
        .reset_index(name="anomaly_count")
        .sort_values("anomaly_count", ascending=False)
        .head(15)
    )
    if not anomaly_counts.empty:
        fig3 = px.bar(
            anomaly_counts,
            x="ticker",
            y="anomaly_count",
            color_discrete_sequence=["#E24B4A"],
            labels={"ticker": "Ticker", "anomaly_count": "Anomalies"},
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No anomalies detected yet.")

# --- Chart 3: Average cash amount per ticker ---
st.subheader("Average cash amount by ticker")
avg_by_ticker = (
    filtered.groupby("ticker")["cash_amount"]
    .mean()
    .reset_index()
    .sort_values("cash_amount", ascending=False)
    .head(20)
)
fig4 = px.bar(
    avg_by_ticker,
    x="ticker",
    y="cash_amount",
    color_discrete_sequence=["#1D9E75"],
    labels={"ticker": "Ticker", "cash_amount": "Avg cash amount"},
)
st.plotly_chart(fig4, use_container_width=True)

# --- Raw data table ---
st.subheader("Recent records")
st.dataframe(
    filtered[[
        "created_at", "ticker", "cash_amount", "currency",
        "dividend_type", "frequency", "z_score", "is_anomaly"
    ]].head(200),
    use_container_width=True,
    hide_index=True,
)

# Auto-rerun to keep data fresh
import time
time.sleep(15)
st.rerun()