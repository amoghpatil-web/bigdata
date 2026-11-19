import json
import time
import clickhouse_connect
from kafka import KafkaConsumer

# 1. Connect to ClickHouse and create table
client = None
while client is None:
    try:
        client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default")
        client.command("""
            CREATE TABLE IF NOT EXISTS default.live_metrics (
                event_time DateTime,
                city String,
                temperature Float64,
                orders UInt32
            ) ENGINE = MergeTree()
            ORDER BY event_time
        """)
        print("Consumer connected to ClickHouse & verified table!")
    except Exception as e:
        print("Waiting for ClickHouse...", e)
        time.sleep(3)

# 2. Connect to Kafka and stream into ClickHouse
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            "live_metrics",
            bootstrap_servers=["kafka:9092"],
            auto_offset_reset="earliest",
            group_id="clickhouse_writer",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        print("Consumer subscribed to Kafka topic 'live_metrics'!")
    except Exception as e:
        print("Waiting for Kafka...", e)
        time.sleep(3)

for message in consumer:
    data = message.value
    client.insert(
        "default.live_metrics",
        [[data["event_time"], data["city"], data["temperature"], data["orders"]]],
        column_names=["event_time", "city", "temperature", "orders"],
    )
    print("Inserted into ClickHouse:", data)
