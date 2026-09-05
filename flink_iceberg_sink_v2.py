"""
Phase 5 — write BOTH large trades and z-score anomalies to Iceberg tables
on S3 (via Glue Data Catalog), in a single Flink job.

Combines Phase 3's anomaly detection logic (log-transformed rolling
z-score) with Phase 4's Iceberg sink. Two INSERT INTO statements can't
each call .wait() sequentially (the first would block forever), so this
uses a StatementSet to run both as one job.

Prerequisites: same jars as flink_iceberg_sink.py (Kinesis connector,
iceberg-flink-runtime, iceberg-aws-bundle, hadoop-client-api/runtime),
Java 17 active in the shell, IAM permissions on Kinesis/Glue/S3.

Run alongside binance_kinesis_producer.py (must be running in another
window, sending live trades into the crypto-trades stream).

    python flink_iceberg_sink_v2.py
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

AWS_REGION = "ap-southeast-1"
STREAM_ARN = "arn:aws:kinesis:ap-southeast-1:746931977088:stream/crypto-trades"
ICEBERG_WAREHOUSE = "s3://pornpoj-crypto-lakehouse-2026/curated-iceberg/"
LARGE_TRADE_THRESHOLD_USD = 5000
ZSCORE_THRESHOLD = 3.0  # chosen to surface anomalies more frequently; noisier than 4.5 but easier to demo
MIN_SAMPLE_COUNT = 30

# Iceberg's Flink sink only commits files to S3 at checkpoint boundaries.
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(30000)  # milliseconds
table_env = StreamTableEnvironment.create(env)

# --- Source: Kinesis ---
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

# --- Sink catalog: Iceberg on S3, cataloged via Glue ---
table_env.execute_sql(f"""
    CREATE CATALOG glue_catalog WITH (
        'type' = 'iceberg',
        'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        'warehouse' = '{ICEBERG_WAREHOUSE}',
        'client.region' = '{AWS_REGION}'
    )
""")

table_env.execute_sql("CREATE DATABASE IF NOT EXISTS glue_catalog.crypto_curated")

table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.crypto_curated.large_trades (
        symbol STRING,
        side STRING,
        price DOUBLE,
        quantity DOUBLE,
        notional_usd DECIMAL(18, 2)
    )
""")

table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.crypto_curated.anomalies (
        symbol STRING,
        price DOUBLE,
        quantity DOUBLE,
        notional_usd DECIMAL(18, 2),
        z_score DOUBLE,
        sample_count BIGINT,
        trade_time BIGINT
    )
""")

print("Writing large_trades and anomalies to Iceberg in one job ...")
print("Press Ctrl+C to stop.\n")

statement_set = table_env.create_statement_set()

statement_set.add_insert_sql(f"""
    INSERT INTO glue_catalog.crypto_curated.large_trades
    SELECT
        symbol,
        CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS side,
        price,
        quantity,
        CAST(price * quantity AS DECIMAL(18, 2)) AS notional_usd
    FROM crypto_trades
    WHERE price * quantity > {LARGE_TRADE_THRESHOLD_USD}
""")

statement_set.add_insert_sql(f"""
    INSERT INTO glue_catalog.crypto_curated.anomalies
    SELECT symbol, price, quantity, notional_usd, z_score, sample_count, trade_time
    FROM (
        SELECT
            symbol,
            price,
            quantity,
            trade_time,
            CAST(price * quantity AS DECIMAL(18, 2)) AS notional_usd,
            COUNT(*) OVER w AS sample_count,
            (LN(quantity) - AVG(LN(quantity)) OVER w) / NULLIF(STDDEV_POP(LN(quantity)) OVER w, 0) AS z_score
        FROM crypto_trades
        WINDOW w AS (
            PARTITION BY symbol
            ORDER BY proc_time
            RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
        )
    )
    WHERE sample_count >= {MIN_SAMPLE_COUNT} AND ABS(z_score) > {ZSCORE_THRESHOLD}
""")

statement_set.execute().wait()
