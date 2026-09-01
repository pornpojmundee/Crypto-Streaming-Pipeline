"""
Phase 3 (step 1) — verify Flink can read from the Kinesis stream.

This uses PyFlink's Table API with the Kinesis SQL connector. Once this
prints incoming trades correctly, the next step adds windowed anomaly
detection (z-score) and large-trade filtering on top of this source table.

Setup:
    IMPORTANT: plain `pip install apache-flink` installs the latest 2.x
    line, which has no released Kinesis connector yet. Use the 1.20 line
    instead, which the connector fully supports:

        pip uninstall apache-flink apache-flink-libraries -y
        pip install apache-flink==1.20.5

    Verify: python -c "import pyflink; print(pyflink.version.__version__)"
    Should print 1.20.5.

    Then get the matching Kinesis connector jar from the official docs page
    (use the Download link there, don't guess the filename):
    https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kinesis/

    Place the downloaded jar somewhere local, e.g. C:\\flink-connectors\\
    and update CONNECTOR_JAR_PATH below to match the exact filename.

Run:
    python flink_kinesis_test.py

AWS credentials must be configured (aws configure) with kinesis:GetRecords /
kinesis:DescribeStream permissions on the crypto-trades stream.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

CONNECTOR_JAR_PATH = "file:///C:/flink-connectors/flink-sql-connector-kinesis-5.1.0-1.20.jar"
# Note: no longer needed here since the jar was copied into pyflink's own
# lib folder (C:\Users\Asus\flink_env\Lib\site-packages\pyflink\lib\),
# which PyFlink loads automatically at startup.
AWS_REGION = "ap-southeast-1"
STREAM_ARN = "arn:aws:kinesis:ap-southeast-1:746931977088:stream/crypto-trades"

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

print("Reading from Kinesis stream. Press Ctrl+C to stop.\n")
table_env.sql_query("SELECT * FROM crypto_trades").execute().print()
