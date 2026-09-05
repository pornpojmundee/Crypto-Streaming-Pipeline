"""
Phase 2 producer — streams Binance trades into Kinesis Data Streams.

Install dependencies:
    pip install websocket-client boto3

Requires AWS credentials configured (aws configure) for a user/role with
kinesis:PutRecord on the target stream.

Run:
    python binance_kinesis_producer.py

Auto-reconnects on disconnect (Binance closes connections after 24h as a
matter of policy, and network hiccups happen) so this can be left running
for long stretches without manual restarts.
"""

import json
import time

import boto3
import websocket

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
STREAMS = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL = f"wss://data-stream.binance.vision/stream?streams={STREAMS}"

KINESIS_STREAM_NAME = "crypto-trades"
AWS_REGION = "ap-southeast-1"

kinesis = boto3.client("kinesis", region_name=AWS_REGION)


def on_message(ws, message):
    payload = json.loads(message)
    data = payload.get("data", {})
    symbol = data.get("s")
    if not symbol:
        return

    record = {
        "symbol": symbol,
        "price": float(data.get("p", 0)),
        "quantity": float(data.get("q", 0)),
        "trade_time": data.get("T"),
        "is_buyer_maker": data.get("m"),
        "ingested_at": int(time.time() * 1000),
    }

    try:
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=symbol,
        )
        print(
            f"Sent to Kinesis: {symbol} "
            f"price={record['price']} qty={record['quantity']}"
        )
    except Exception as e:
        print(f"Failed to send record: {e}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed: {close_status_code} {close_msg}")


def on_open(ws):
    global reconnect_delay
    reconnect_delay = 2
    print(f"Connected. Streaming {STREAMS} -> Kinesis stream '{KINESIS_STREAM_NAME}'")
    print("Press Ctrl+C to stop.\n")


if __name__ == "__main__":
    reconnect_delay = 2  # seconds, doubles on repeated failures up to a cap
    max_reconnect_delay = 60

    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print("Stopped by user.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")

        print(f"Disconnected. Reconnecting in {reconnect_delay}s ...")
        time.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
