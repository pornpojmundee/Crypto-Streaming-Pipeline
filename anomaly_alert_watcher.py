"""
Phase 6 — engineer alerting: polls the `anomalies` Iceberg table via
Athena for new rows and pushes each one to Slack via Incoming Webhook.

Install dependencies:
    pip install boto3 requests

Run (separate window, alongside the producer and flink_iceberg_sink_v2.py):
    set SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
    python anomaly_alert_watcher.py

On startup, only alerts newer than the start time are sent (so it doesn't
immediately flood Slack with every historical test row already in the
table). Runs forever, polling every POLL_INTERVAL_SECONDS.
"""

import os
import time
from datetime import datetime, timezone

import boto3
import requests

AWS_REGION = "ap-southeast-1"
ATHENA_DATABASE = "crypto_curated"
ATHENA_OUTPUT_LOCATION = "s3://pornpoj-crypto-lakehouse-2026/athena-results/"
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
POLL_INTERVAL_SECONDS = 60

athena = boto3.client("athena", region_name=AWS_REGION)


def run_athena_query(query: str):
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
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown error")
        raise RuntimeError(f"Athena query failed: {reason}")

    rows = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=exec_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([col.get("VarCharValue") for col in row["Data"]])
    return rows  # first row is the header


def send_slack_alert(symbol, price, quantity, notional_usd, z_score):
    message = {
        "text": (
            f":rotating_light: *Anomaly detected: {symbol}*\n"
            f"z-score: `{float(z_score):.2f}`  |  "
            f"notional: `${float(notional_usd):,.2f}`  |  "
            f"price: `{price}`  |  qty: `{quantity}`"
        )
    }
    resp = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
    if resp.status_code != 200:
        print(f"Slack post failed ({resp.status_code}): {resp.text}")


if __name__ == "__main__":
    # Only alert on trades newer than the moment this script started.
    last_seen_trade_time = int(time.time() * 1000)
    print(f"Watching for new anomalies (polling every {POLL_INTERVAL_SECONDS}s) ...")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            query = f"""
                SELECT symbol, price, quantity, notional_usd, z_score, trade_time
                FROM anomalies
                WHERE trade_time > {last_seen_trade_time}
                ORDER BY trade_time ASC
            """
            rows = run_athena_query(query)

            for row in rows[1:]:  # skip header row
                symbol, price, quantity, notional_usd, z_score, trade_time = row
                send_slack_alert(symbol, price, quantity, notional_usd, z_score)
                last_seen_trade_time = max(last_seen_trade_time, int(trade_time))
                print(f"Alerted: {symbol} z={z_score} at {datetime.now(timezone.utc).isoformat()}")
                time.sleep(1.2)  # stay under Slack's ~1 msg/sec webhook rate limit

        except Exception as e:
            print(f"Error during poll: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)
