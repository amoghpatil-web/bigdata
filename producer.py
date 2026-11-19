from kafka import KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers=['localhost:29092'])

print("Starting to send messages")

for i in range(1,6):
    msg=f"Student message number{i}".encode('utf-8')
    producer.send('student-events',msg)
    print(f"Sent:{msg.decode('utf-8')}")
    time.sleep(1)

producer.flush()
print("All messages sent successfully")
