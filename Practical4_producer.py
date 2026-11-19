from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_v1 = """
{
  "type": "record",
  "name": "Employee",
  "fields":[
  {"name":"id","type":"int"},
  {"name":"name","type":"string"},
  {"name":"salary","type":"float"}
  ]}"""

schema_v2 = """
{
  "type": "record",
  "name": "Employee",
  "fields":[
  {"name":"id","type":"int"},
  {"name":"name","type":"string"},
  {"name":"salary","type":"float"},
  {"name":"dept","type":"string","default":"IT"}
  ]}"""

sr_client=SchemaRegistryClient({'url':'http://localhost:8081'})

v1_serializer =AvroSerializer(sr_client,schema_v1,to_dict=lambda obj,ctx: obj)
v2_serializer =AvroSerializer(sr_client,schema_v2,to_dict=lambda obj,ctx: obj)

prod_v1=SerializingProducer({'bootstrap.servers':'localhost:9092','value.serializer':v1_serializer})
prod_v2=SerializingProducer({'bootstrap.servers':'localhost:9092','value.serializer':v2_serializer})

prod_v1.produce(topic='emp-topic', value={"id": 1, "name": "Alice", "salary": 50000.0})
prod_v1.flush()
print("Sent V1 data(Alice)")

prod_v2.produce(topic='emp-topic', value={"id": 2, "name": "Bob", "salary": 45000.0, "dept": "HR"})
prod_v2.flush()
print("Sent V2 data(Bob)")

