"""
Phase 1 test script — Binance public market data WebSocket
No API key required (market data only). Run this first to confirm
the data source works before wiring it into Kinesis.

Install dependency:
    pip install websocket-client

Run:
    python binance_ws_test.py
"""

import json
import websocket

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
STREAMS = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL = f"wss://data-stream.binance.vision/stream?streams={STREAMS}"


def on_message(ws, message):
    payload = json.loads(message)
    data = payload.get("data", {})
    symbol = data.get("s")
    price = float(data.get("p", 0))
    qty = float(data.get("q", 0))
    notional = price * qty
    side = "SELL" if data.get("m") else "BUY"
    print(
        f"{symbol:10s} {side:4s} price={price:>12.4f} "
        f"qty={qty:>10.4f} notional=${notional:>12,.2f}"
    )


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed: {close_status_code} {close_msg}")


def on_open(ws):
    print(f"Connected. Subscribed to: {STREAMS}")
    print("Press Ctrl+C to stop.\n")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)
