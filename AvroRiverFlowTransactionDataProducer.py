from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import requests
from datetime import datetime,timedelta
import os 
from utils import get_schema_from_registry,get_all_json_response

# Get the current timestamp

###########################


schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Configure AvroProducer for first run
#with open("mongodb-schemas/riverflow_transaction.avsc") as f:
#    value_schema = f.read()

schema_id='13'
value_schema = get_schema_from_registry(schema_registry_conf['url'],schema_id)
avro_serializer = AvroSerializer(schema_registry_client, value_schema)

#print(value_schema)
print('Read in  schema')

producer_conf = {'bootstrap.servers': 'localhost:9092'}
avro_producer = Producer(producer_conf)

# Produce Avro messages
msg_user_1 = get_all_json_response('transaction-data.txt')
#print(msg_user_1) 

for flow_data in msg_user_1['data']['getObservations']:
    loc_id = flow_data["locationId"]
    for observation in flow_data['observations']:
    	#print(observation)
    	msg_for_insert = dict()
    	msg_for_insert["locationId"] = loc_id
    	msg_for_insert["qualityCode"] = observation["qualityCode"]
    	msg_for_insert["ObservationTime"] = observation["timestamp"]
    	msg_for_insert["value"] = observation["value"]
    	msg_for_insert['InsertTime']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    	
    	#print(msg_for_insert)
    	
    	avro_producer.produce(
    	topic="river-flow-transaction-data",
    	value=avro_serializer(msg_for_insert, SerializationContext("riverflow-transaction", MessageField.VALUE)),
    		)



print('Message stage reached')

#msg_for_insert = dict()
#msg_for_insert["locationId"] = '-1'
#msg_for_insert["qualityCode"] = '-200'
#msg_for_insert["ObservationTime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#msg_for_insert["value"] = '-1'
#msg_for_insert['InsertTime']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#print(msg_for_insert) 

avro_producer.produce(
    	topic="river-flow-transaction-data",
    	value=avro_serializer(msg_for_insert, SerializationContext("riverflow-transaction", MessageField.VALUE)),
    		)

avro_producer.flush()

print('Message pushed succesfully')
