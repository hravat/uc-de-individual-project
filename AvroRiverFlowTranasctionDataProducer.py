from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import requests
from datetime import datetime,timedelta

# Get the current timestamp

def get_schema_from_registry(schema_registry_url, schema_id):
    response = requests.get(f"{schema_registry_url}/schemas/ids/{schema_id}")
    if response.status_code == 200:
        schema = response.json()["schema"]
        return schema
    else:
        raise Exception(f"Failed to fetch schema from Schema Registry. Status code: {response.status_code}")



###########################

current_time = datetime.now()
end_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
two_hours_prior = current_time - timedelta(hours=2)
start_time = two_hours_prior.strftime("%Y-%m-%d %H:%M:%S")
 

def get_all_json_response():
    query = """ 
    
query {
	getObservations {
		locationId
		observations(filter: { start: \""""+start_time+"""\", end: \""""+end_time+"""\" }) {
			qualityCode
			timestamp
			value
		}
	}
}
    
    """
    
    
    
    
    graphql_endpoint = 'https://apis.ecan.govt.nz/waterdata/observations/graphql'
    
    
    
    # Define the subscription key
    
    subscription_key = '00474accb6494ab2bdb1239058f18996'
    
    
    
    # Set the request headers, including the content type
    
    headers = {
    
        'Content-Type': 'application/json',
    
        'Ocp-Apim-Subscription-Key': subscription_key
    
    }
    
    
    
    # Create the request payload as a dictionary
    
    payload = {
    
        'query': query
    
    }
    
    
    
    
    
    # Send the POST request to the GraphQL endpoint
    
    response = requests.post(graphql_endpoint, headers=headers, json=payload)
    print(response)
    
    
    # Check if the request was successful (status code 200)
    
    if response.status_code == 200:
    
        # Parse the JSON response
    
        result = response.json()
    
 #       print(result)
        
        return result
    
    else:
    
        # If there was an error, print the error message
    
        print(f"Error: {response.status_code} - {response.text}")
        return None

############

schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Configure AvroProducer
#with open("riverflow_transaction.avsc") as f:
#    value_schema = f.read()
schema_id='13'
value_schema = get_schema_from_registry(schema_registry_conf['url'],schema_id)
avro_serializer = AvroSerializer(schema_registry_client, value_schema)

print(value_schema)
print('Read in  schema')

producer_conf = {'bootstrap.servers': 'localhost:9092'}
avro_producer = Producer(producer_conf)

# Produce Avro messages
msg_user_1 = get_all_json_response()

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
    	
    	avro_producer.produce(
    	topic="riverflow-transaction-data",
    	value=avro_serializer(msg_for_insert, SerializationContext("riverflow-transaction", MessageField.VALUE)),
    		)



print('Message stage reached')

#avro_producer.produce(
#    topic="riverflow-avro",
#    value=avro_serializer(msg_user, SerializationContext("riverflow", MessageField.VALUE)),
#)

avro_producer.flush()

print('Message pushed succesfully')
