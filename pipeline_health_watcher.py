"""
Phase 6b — engineer alerting (pipeline health), as distinct from the
business-facing anomaly_alert_watcher.py.

This checks how long it's been since the last Iceberg checkpoint commit
on the `large_trades` table (Flink commits a snapshot every 30s per the
checkpoint interval, even when no rows matched a filter that period —
so "no new snapshot in a while" is a real signal that Kinesis stopped
delivering data or the Flink job died/stalled), and alerts Slack if the
pipeline looks stuck.

Install dependencies:
    pip install boto3 requests

Run (separate window, alongside the other pipeline components):
    set SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
    python pipeline_health_watcher.py
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

CHECK_INTERVAL_SECONDS = 60
STALE_THRESHOLD_SECONDS = 180  # 3 minutes with no new checkpoint = alert

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


def get_last_commit_time():
    rows = run_athena_query(
        'SELECT MAX(committed_at) FROM "large_trades$snapshots"'
    )
    value = rows[1][0]  # skip header
    # Athena returns e.g. '2026-09-05 12:08:24.319 UTC'
    cleaned = value.replace(" UTC", "")
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)


def send_engineer_alert(seconds_since_commit):
    message = {
        "text": (
            f":warning: *Pipeline health alert*\n"
            f"No new Iceberg checkpoint in {int(seconds_since_commit)}s "
            f"(expected every ~30s). Kinesis ingestion or the Flink job "
            f"may have stopped — check `binance_kinesis_producer.py` and "
            f"`flink_iceberg_sink_v2.py`."
        )
    }
    resp = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
    if resp.status_code != 200:
        print(f"Slack post failed ({resp.status_code}): {resp.text}")


if __name__ == "__main__":
    print(f"Watching pipeline health (checking every {CHECK_INTERVAL_SECONDS}s) ...")
    print("Press Ctrl+C to stop.\n")

    already_alerted = False

    while True:
        try:
            last_commit = get_last_commit_time()
            age_seconds = (datetime.now(timezone.utc) - last_commit).total_seconds()
            print(f"Last checkpoint: {age_seconds:.0f}s ago")

            if age_seconds > STALE_THRESHOLD_SECONDS:
                if not already_alerted:
                    send_engineer_alert(age_seconds)
                    already_alerted = True
                    print("-> Sent engineer alert (pipeline looks stuck)")
            else:
                already_alerted = False  # reset once healthy again

        except Exception as e:
            print(f"Error during health check: {e}")

        time.sleep(CHECK_INTERVAL_SECONDS)
