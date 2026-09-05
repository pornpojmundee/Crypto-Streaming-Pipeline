# Real-Time Crypto Market Data Pipeline

End-to-end streaming data engineering project: ingests live trade data from
the Binance public API, processes it in real time to detect market
anomalies and large trades, lands curated data in an Apache Iceberg
lakehouse, and serves two alerting/dashboard views — one for engineers
(pipeline health) and one for the business (market anomalies).

This is the streaming companion to
[AWS-DataEngineer](https://github.com/pornpojmundee/AWS-DataEngineer),
which covers batch ETL on a fintech dataset.

## Architecture

```
Binance WebSocket (public market data, no auth)
        │
        ▼
Kinesis Data Streams (crypto-trades)
        │
        ▼
PyFlink 1.20 (run locally for this project)
  - rolling volume/price stats, anomaly detection (log-z-score)
  - large trade ("whale") detection
        │
        ▼
S3 + Apache Iceberg (via Glue Data Catalog)
  - large_trades table
  - anomalies table
        │
        ├──► pipeline_health_watcher.py → Slack: engineer alerts
        │      (fires when no new checkpoint in 3+ minutes)
        │
        ├──► anomaly_alert_watcher.py → Slack: business alerts
        │      (fires on new rows in the anomalies table)
        │
        └──► Athena → dashboard.py (Streamlit): live dashboard,
               engineer + business views
```

## Status — everything below is implemented and verified working

| Component | Status |
|---|---|
| Binance WebSocket producer (with auto-reconnect) | ✅ Done |
| S3 bucket + folder structure | ✅ Done |
| AWS Budget alert + account upgraded to Paid plan | ✅ Done |
| Kinesis Data Stream | ✅ Done |
| Producer → Kinesis | ✅ Done |
| PyFlink stream processing (local, Flink 1.20.5) | ✅ Done |
| Large trade feed (event-level filter) | ✅ Done |
| Anomaly detection (rolling z-score, log-transformed) | ✅ Done |
| Iceberg sink — `large_trades` table | ✅ Done |
| Iceberg sink — `anomalies` table (single job, StatementSet) | ✅ Done |
| Engineer alerting (pipeline health → Slack) | ✅ Done |
| Business alerting (anomaly detected → Slack) | ✅ Done |
| Streamlit dashboard (engineer + business views) | ✅ Done |

## Tech stack

Python, AWS Kinesis Data Streams, Apache Flink 1.20 (PyFlink), S3, Apache
Iceberg 1.11, AWS Glue Data Catalog, Athena, Slack Incoming Webhooks,
Streamlit.

## Data source

[Binance public market data API](https://data-stream.binance.vision) —
no API key required. Streams individual trade events (`<symbol>@trade`)
for BTC/USDT, ETH/USDT, SOL/USDT.

## Files

| File | Purpose |
|---|---|
| `binance_kinesis_producer.py` | WebSocket → Kinesis producer, auto-reconnects on disconnect |
| `flink_kinesis_test.py` | Verifies PyFlink can read from Kinesis |
| `flink_large_trade_feed.py` | Standalone large-trade filter (console output) |
| `flink_anomaly_detection.py` | Standalone anomaly detection (console output) |
| `flink_iceberg_sink.py` | Single-table Iceberg sink (large_trades only) |
| `flink_iceberg_sink_v2.py` | Two-table Iceberg sink (large_trades + anomalies), current version |
| `anomaly_alert_watcher.py` | Polls Athena, posts new anomalies to Slack |
| `pipeline_health_watcher.py` | Polls Iceberg checkpoint freshness, alerts Slack if the pipeline stalls |
| `dashboard.py` | Streamlit dashboard (engineer + business views) |

## Notable engineering decisions

- **z-score on log-transformed quantity, not raw quantity** — trade sizes
  in crypto markets are heavy-tailed/log-normal, not normally distributed;
  a raw z-score flagged far too many false positives during testing.
- **Minimum sample-count guard (≥30) before evaluating anomalies** — avoids
  cold-start false positives when a rolling window has too few data points.
- **PyFlink pinned to 1.20.5** — the Kinesis SQL connector has no released
  version compatible with Flink 2.x yet.
- **Java 17 required at runtime** — Java 21+ removed legacy Security
  Manager APIs that Hadoop's `UserGroupInformation` (a transitive Iceberg
  dependency) still relies on.
- **Checkpointing enabled via `StreamExecutionEnvironment` directly** —
  setting it through `TableConfig` did not reliably take effect for the
  embedded local mini-cluster; without checkpointing, the Iceberg sink
  never commits files to S3.
- **Dashboard runs in its own virtualenv** — Streamlit's protobuf version
  conflicts with apache-beam/PyFlink's, so it's kept isolated rather than
  risking breaking the Flink environment.

## Local setup

```bash
# Flink environment (separate venv — PyFlink pins many dependency versions)
python -m venv flink_env
flink_env\Scripts\activate
pip install apache-flink==1.20.5 boto3 websocket-client

# Kinesis + Iceberg + Hadoop connector jars go into:
#   flink_env\Lib\site-packages\pyflink\lib\

# Requires Java 17 active in the shell (Java 21+ breaks Hadoop's
# UserGroupInformation):
set JAVA_HOME=<path to a JDK 17 install>
set PATH=%JAVA_HOME%\bin;%PATH%

# Run (each in its own terminal, all activated in flink_env):
python binance_kinesis_producer.py
python flink_iceberg_sink_v2.py
python anomaly_alert_watcher.py
python pipeline_health_watcher.py
```

```bash
# Dashboard environment (separate venv)
python -m venv dashboard_env
dashboard_env\Scripts\activate
pip install streamlit streamlit-autorefresh boto3 pandas
streamlit run dashboard.py
```
