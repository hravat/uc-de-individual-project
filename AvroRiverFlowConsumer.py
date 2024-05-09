from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from pymongo import MongoClient

######Serilization Error 
def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict

##Mongo DB Config
mongodb_uri = 'mongodb+srv://hravat:hravat@cluster0.7wqtwdz.mongodb.net/de-river-flow'
client = MongoClient(mongodb_uri)
db = client['de-river-flow']
collection = db['river-flow-avro']

try:
    # Attempt to connect to the MongoDB Atlas cluster
    client.server_info()
    print("Connection to MongoDB Atlas successful!")
except Exception as e:
    print("Connection to MongoDB Atlas failed:", e)
#finally:
    # Close the MongoDB client
#    client.close()

# Define Kafka broker and schema registry configuration
kafka_broker = 'localhost:9092'
schema_registry = 'http://localhost:8081'
schema_subject = 'your_schema_subject_name'

# Define Avro consumer configuration
avro_consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': schema_registry,
}




# Create AvroConsumer instance
consumer = AvroConsumer(avro_consumer_config)

# Subscribe to Kafka topic
consumer.subscribe(['riverflow-avro'])

# Start consuming messages
try:
    while True:
        msg = consumer.poll(1.0)  # Adjust the timeout as needed
        if msg is None:
            continue
        print("Received message: {}".format(msg.value()))
        message_value = msg.value() 
        document = {'message': message_value}
        query = {"message.locationId": message_value['locationId']}
       	update = {"$set": {"message.InsertTime": message_value['InsertTime']}}
       	collection.update_one(query, update, upsert=True)
#        collection.insert_one(document)
        consumer.commit()
except KeyboardInterrupt:
    client.close()
    consumer.close()
    

