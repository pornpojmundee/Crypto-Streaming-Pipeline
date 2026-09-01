"""
Phase 4 — write large trades to an Apache Iceberg table on S3 (via Glue
Data Catalog), instead of just printing them to the console.

Prerequisites (in addition to Phase 3's Kinesis connector jar):
    - iceberg-flink-runtime-1.20-1.11.0.jar
    - iceberg-aws-bundle-1.11.0.jar
  both copied into:
    C:\\Users\\Asus\\flink_env\\Lib\\site-packages\\pyflink\\lib\\

  The IAM user (data-engineer-dev) needs Glue Data Catalog permissions
  (AWSGlueServiceRole or equivalent, already attached from the first
  project) plus S3 read/write on the crypto lakehouse bucket.

Run alongside binance_kinesis_producer.py (must be running in another
window, sending live trades into the crypto-trades stream).

    python flink_iceberg_sink.py
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

AWS_REGION = "ap-southeast-1"
STREAM_ARN = "arn:aws:kinesis:ap-southeast-1:746931977088:stream/crypto-trades"
ICEBERG_WAREHOUSE = "s3://pornpoj-crypto-lakehouse-2026/curated-iceberg/"
LARGE_TRADE_THRESHOLD_USD = 5000

# Iceberg's Flink sink only commits files to S3 at checkpoint boundaries,
# so checkpointing must be enabled directly on the StreamExecutionEnvironment
# or nothing ever gets written out. Setting it via table_env.get_config()
# does not reliably take effect for the embedded local mini-cluster.
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(30000)  # milliseconds
table_env = StreamTableEnvironment.create(env)

# --- Source: Kinesis (same as Phase 3) ---
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

# --- Sink: Iceberg table on S3, cataloged via Glue ---
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

print("Writing large trades to Iceberg table crypto_curated.large_trades ...")
print("Press Ctrl+C to stop.\n")

table_env.execute_sql(f"""
    INSERT INTO glue_catalog.crypto_curated.large_trades
    SELECT
        symbol,
        CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS side,
        price,
        quantity,
        CAST(price * quantity AS DECIMAL(18, 2)) AS notional_usd
    FROM default_catalog.default_database.crypto_trades
    WHERE price * quantity > {LARGE_TRADE_THRESHOLD_USD}
""").wait()
