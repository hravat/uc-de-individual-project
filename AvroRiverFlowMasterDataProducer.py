from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import requests
from datetime import datetime
import os 
from utils import get_schema_from_registry,get_all_json_response

schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Configure AvroProducer for first run
#with open("mongodb-schemas/riverflow_master.avsc") as f:
#    value_schema = f.read()

schema_id='11'
value_schema = get_schema_from_registry(schema_registry_conf['url'],schema_id)
avro_serializer = AvroSerializer(schema_registry_client, value_schema)

#print(value_schema)
print('Read in  schema')

producer_conf = {'bootstrap.servers': 'localhost:9092'}
avro_producer = Producer(producer_conf)

# Produce Avro messages
#msg_user_1 =  {'data':{'getObservations':{"locationId": "TestingRecord"}}}
#avro_producer.produce(topic='my_topic', value=user)
msg_user_1 = get_all_json_response('master-data.txt')
#print(msg_user)

for loc_id in msg_user_1['data']['getObservations']:
    loc_id['InsertTime']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    avro_producer.produce(
    topic="river-flow-master-data",
    value=avro_serializer(loc_id, SerializationContext("riverflow_master", MessageField.VALUE)),
	)



print('Message stage reached')

#avro_producer.produce(
#    topic="riverflow-avro",
#    value=avro_serializer(msg_user, SerializationContext("riverflow", MessageField.VALUE)),
#)

avro_producer.flush()

print('Message pushed succesfully')
