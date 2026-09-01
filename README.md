# Real-Time Crypto Market Data Pipeline

End-to-end streaming data engineering project: ingests live trade data from
the Binance public API, processes it in real time to detect market
anomalies and large trades, and serves two dashboard views — one for
engineers (pipeline health) and one for the business (market anomalies).

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
Amazon Managed Service for Apache Flink
  - rolling volume/price stats, anomaly detection (z-score)
  - large trade ("whale") detection
        │
        ▼
S3 + Apache Iceberg (via Glue Data Catalog)
        │
        ├──► SNS → Slack: engineer alerts (pipeline lag, schema drift)
        └──► Dashboard: business view (anomalies, large trade feed)
```

## Status

| Component | Status |
|---|---|
| Binance WebSocket producer (local) | ✅ Done |
| S3 bucket + folder structure | ✅ Done |
| AWS Budget alert | ✅ Done |
| Kinesis Data Stream | ✅ Done |
| Producer → Kinesis | ✅ Done |
| Managed Flink processing job | ⏳ In progress |
| Iceberg curated tables | ⏳ Planned |
| Engineer alerting (SNS/Slack) | ⏳ Planned |
| Business dashboard | ⏳ Planned |

## Tech stack

Python, AWS Kinesis Data Streams, Amazon Managed Service for Apache Flink,
S3, Apache Iceberg, AWS Glue Data Catalog, Athena, SNS.

## Data source

[Binance public market data API](https://data-stream.binance.vision) —
no API key required. Streams individual trade events (`<symbol>@trade`)
for BTC/USDT, ETH/USDT, SOL/USDT.

## Local setup

```bash
pip install websocket-client boto3
python binance_kinesis_producer.py
```

Requires AWS credentials configured (`aws configure`) with permissions
for `kinesis:PutRecord` on the `crypto-trades` stream.
