from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sr_client=SchemaRegistryClient({'url':'http://localhost:8081'})
avro_deserializer = AvroDeserializer(sr_client, from_dict=lambda obj,ctx: obj)

conf ={
    'bootstrap.servers':'localhost:9092',
    'group.id':'emp-group',
    'auto.offset.reset':'earliest',
    'value.deserializer':avro_deserializer
}

consumer =DeserializingConsumer(conf)
consumer.subscribe(['emp-topic'])

print("Listening Messages")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        print(f"Received :{msg.value()}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()