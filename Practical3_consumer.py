//pip install kafka-python
//docker compose up -d
//docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic student-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3

from kafka import KafkaConsumer

consumer=KafkaConsumer(
    'student-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',

)
print("Listening for messages")

for message in consumer:
    print(f"Received:{message.value.decode('utf-8')}")

