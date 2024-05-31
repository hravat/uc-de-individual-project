import requests
import os 
from datetime import datetime,timedelta

def get_schema_from_registry(schema_registry_url, schema_id):
    response = requests.get(f"{schema_registry_url}/schemas/ids/{schema_id}")
    if response.status_code == 200:
        schema = response.json()["schema"]
        return schema
    else:
        raise Exception(f"Failed to fetch schema from Schema Registry. Status code: {response.status_code}")
    
def get_all_json_response(query_file):
    
    
    current_time = datetime.now()- timedelta(hours=24)
    end_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    two_hours_prior = current_time - timedelta(hours=2)
    start_time = two_hours_prior.strftime("%Y-%m-%d %H:%M:%S")
    name = "Alice"
    age = 30
    
    graphql_endpoint = 'https://apis.ecan.govt.nz/waterdata/observations/graphql'
    subscription_key = os.environ['ECAN_SUBSCRIPTION_KEY']
    
    
    query_path = os.environ['DE_ECAN_QUERY_PATH']+query_file
    
    
    
    try:
        with open(query_path, 'r') as file:
            query = file.read()
    except FileNotFoundError:
        print(f"The file at path {query_path} was not found.")
    except IOError:
        print(f"An error occurred while reading the file at path {query_path}.")

    if 'api-queries/transaction-data.txt' in query_path:
        query = query.replace("{start_time}", "\""+str(start_time)+"\"")
        query = query.replace("{end_time}", "\""+str(end_time)+"\"")

    headers = {    
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key': subscription_key
    
    }
    
    payload = {
        'query': query
    }

    response = requests.post(graphql_endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        result = response.json()    
#       print(result)
        return result
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None