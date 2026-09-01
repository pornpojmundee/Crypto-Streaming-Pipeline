"""
Phase 3 (step 2a) — large trade ("whale") feed.

Filters trades where notional value (price * quantity) exceeds a threshold
and prints them live, same idea as the "large trade feed" panel in the
business dashboard mockup.

Run alongside binance_kinesis_producer.py (must be running in another
window, sending live trades into the crypto-trades stream).

    python flink_large_trade_feed.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

AWS_REGION = "ap-southeast-1"
STREAM_ARN = "arn:aws:kinesis:ap-southeast-1:746931977088:stream/crypto-trades"

# Notional value (price * quantity) above which a trade counts as "large".
# BTC/ETH naturally trade in larger notional sizes than SOL, so a single
# fixed threshold is a simplification — a dynamic per-symbol percentile
# threshold is a good next iteration once this baseline works.
LARGE_TRADE_THRESHOLD_USD = 5000

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql(f"""
    CREATE TABLE crypto_trades (
        symbol STRING,
        price DOUBLE,
        quantity DOUBLE,
        trade_time BIGINT,
        is_buyer_maker BOOLEAN,
        ingested_at BIGINT
    ) WITH (
        'connector' = 'kinesis',
        'stream.arn' = '{STREAM_ARN}',
        'aws.region' = '{AWS_REGION}',
        'source.init.position' = 'LATEST',
        'format' = 'json'
    )
""")

print(f"Watching for trades with notional value > ${LARGE_TRADE_THRESHOLD_USD:,}")
print("Press Ctrl+C to stop.\n")

table_env.sql_query(f"""
    SELECT
        symbol,
        CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS side,
        price,
        quantity,
        CAST(price * quantity AS DECIMAL(18, 2)) AS notional_usd
    FROM crypto_trades
    WHERE price * quantity > {LARGE_TRADE_THRESHOLD_USD}
""").execute().print()
