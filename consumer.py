from kafka import KafkaConsumer

consumer=KafkaConsumer(
    'student-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',

)
print("Listening for messages")

for message in consumer:
    print(f"Received:{message.value.decode('utf-8')}")

