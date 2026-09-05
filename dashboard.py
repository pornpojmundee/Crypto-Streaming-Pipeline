"""
Phase 7 — Streamlit dashboard for the crypto streaming pipeline.

Two views, matching the mockups built earlier in the project:
  - Engineer view: pipeline health (time since last Iceberg checkpoint)
  - Business view: recent anomalies and the large trade feed

Install dependencies:
    pip install streamlit streamlit-autorefresh boto3 pandas

Run:
    streamlit run dashboard.py

Opens in your browser automatically (usually http://localhost:8501).
"""

import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

AWS_REGION = "ap-southeast-1"
ATHENA_DATABASE = "crypto_curated"
ATHENA_OUTPUT_LOCATION = "s3://pornpoj-crypto-lakehouse-2026/athena-results/"
STALE_THRESHOLD_SECONDS = 180

st.set_page_config(page_title="Crypto Pipeline Dashboard", layout="wide")
athena = boto3.client("athena", region_name=AWS_REGION)


def run_athena_query(query: str) -> pd.DataFrame:
    exec_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown error")
        raise RuntimeError(f"Athena query failed: {reason}")

    rows = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=exec_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([col.get("VarCharValue") for col in row["Data"]])

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows[1:], columns=rows[0])


@st.cache_data(ttl=20)
def get_last_checkpoint_age():
    df = run_athena_query('SELECT MAX(committed_at) AS last_commit FROM "large_trades$snapshots"')
    if df.empty or df["last_commit"][0] is None:
        return None
    cleaned = df["last_commit"][0].replace(" UTC", "")
    last_commit = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last_commit).total_seconds()


@st.cache_data(ttl=20)
def get_recent_large_trades(limit=10):
    return run_athena_query(f"SELECT * FROM large_trades ORDER BY notional_usd DESC LIMIT {limit}")


@st.cache_data(ttl=20)
def get_recent_anomalies(limit=10):
    return run_athena_query(
        f"SELECT * FROM anomalies ORDER BY trade_time DESC LIMIT {limit}"
    )


@st.cache_data(ttl=20)
def get_summary_stats():
    df = run_athena_query(
        "SELECT COUNT(*) AS trade_count, SUM(notional_usd) AS total_notional FROM large_trades"
    )
    return df.iloc[0] if not df.empty else None


st_autorefresh(interval=30_000, key="refresh")

st.title("Crypto Streaming Pipeline")
st.caption("Binance -> Kinesis -> Flink -> Iceberg (S3/Glue) -> Athena")

col_engineer, col_business = st.columns(2)

with col_engineer:
    st.subheader(":bell: Engineer view")
    age = get_last_checkpoint_age()
    if age is None:
        st.warning("No checkpoint data yet.")
    elif age > STALE_THRESHOLD_SECONDS:
        st.error(f"Pipeline looks stuck — last checkpoint {age:.0f}s ago (expected ~30s)")
    else:
        st.success(f"Pipeline healthy — last checkpoint {age:.0f}s ago")

    st.metric("Checkpoint interval", "30s")

with col_business:
    st.subheader(":bar_chart: Business view")
    stats = get_summary_stats()
    if stats is not None:
        c1, c2 = st.columns(2)
        c1.metric("Large trades captured", stats["trade_count"])
        c2.metric("Total notional", f"${float(stats['total_notional'] or 0):,.0f}")

st.divider()

col_anomalies, col_trades = st.columns(2)

with col_anomalies:
    st.markdown("**Recent anomalies (z-score)**")
    anomalies_df = get_recent_anomalies()
    if anomalies_df.empty:
        st.caption("No anomalies recorded yet.")
    else:
        st.dataframe(anomalies_df, hide_index=True, use_container_width=True)

with col_trades:
    st.markdown("**Largest trades captured**")
    trades_df = get_recent_large_trades()
    if trades_df.empty:
        st.caption("No large trades recorded yet.")
    else:
        st.dataframe(trades_df, hide_index=True, use_container_width=True)
