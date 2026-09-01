"""
Phase 3 (step 2b) — rolling anomaly detection.

Computes a rolling mean/stddev of LOG(trade quantity) per symbol over a
5-minute window and flags trades whose (log) quantity deviates more than
N standard deviations from that rolling mean (a z-score anomaly) — the
same idea as the "volume spike detected" anomaly card in the business
dashboard mockup. Trade sizes in crypto markets are heavy-tailed /
log-normal rather than normally distributed, so the z-score is computed
on the log of quantity rather than the raw value — this avoids flagging
almost every moderately-larger-than-usual trade as an anomaly.

Run alongside binance_kinesis_producer.py (must be running in another
window, sending live trades into the crypto-trades stream). For the
z-score condition to trigger meaningfully you generally need a few
minutes of data flowing first, so the rolling window has something to
compare against.

    python flink_anomaly_detection.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

AWS_REGION = "ap-southeast-1"
STREAM_ARN = "arn:aws:kinesis:ap-southeast-1:746931977088:stream/crypto-trades"
ZSCORE_THRESHOLD = 4.5

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql(f"""
    CREATE TABLE crypto_trades (
        symbol STRING,
        price DOUBLE,
        quantity DOUBLE,
        trade_time BIGINT,
        is_buyer_maker BOOLEAN,
        ingested_at BIGINT,
        proc_time AS PROCTIME()
    ) WITH (
        'connector' = 'kinesis',
        'stream.arn' = '{STREAM_ARN}',
        'aws.region' = '{AWS_REGION}',
        'source.init.position' = 'LATEST',
        'format' = 'json'
    )
""")

print(f"Watching for trade-size anomalies (|z-score| > {ZSCORE_THRESHOLD})")
print("Needs a few minutes of live data before the rolling window fills in.")
print("Press Ctrl+C to stop.\n")

table_env.sql_query(f"""
    SELECT symbol, price, quantity, notional_usd, z_score, sample_count
    FROM (
        SELECT
            symbol,
            price,
            quantity,
            CAST(price * quantity AS DECIMAL(18, 2)) AS notional_usd,
            COUNT(*) OVER w AS sample_count,
            AVG(LN(quantity)) OVER w AS avg_log_qty,
            STDDEV_POP(LN(quantity)) OVER w AS stddev_log_qty,
            (LN(quantity) - AVG(LN(quantity)) OVER w) / NULLIF(STDDEV_POP(LN(quantity)) OVER w, 0) AS z_score
        FROM crypto_trades
        WINDOW w AS (
            PARTITION BY symbol
            ORDER BY proc_time
            RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
        )
    )
    WHERE sample_count >= 30 AND ABS(z_score) > {ZSCORE_THRESHOLD}
""").execute().print()
