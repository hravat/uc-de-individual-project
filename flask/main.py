from flask import Flask,jsonify,request
from flask_pymongo import PyMongo
from datetime import datetime  
import os 

app = Flask(__name__)
app.config["MONGO_URI"] = os.environ['MONGODB_ATLAS_URL']
mongo = PyMongo(app)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/test-connection")
def test_connection():
    try:
        # Try to connect to the MongoDB database
        mongo.db.command("ping")
        return "Connection successful"
    except Exception as e:
        return f"Connection failed: {e}"
    
@app.route("/get-document/river-flow-master-data-by-location")
def get_master_data_location():
    document_list = []
    try:
        # Retrieve the document by its ID
        locationID = request.args["locationId"]
        print(locationID)
        cursor = mongo.db['river-flow-master-data'].find({"locationId":locationID})
        print(cursor)
        if cursor:
            for document in cursor:
            	print(document)
            	document_list.append({"locationId":document["locationId"],
            			      "name":document["name"],
            			      "nztmx":document["nztmx"],
            			      "nztmy":document["nztmy"],
            			      "type":document["type"],
            			      "unit":document["unit"],	
            	})
            return jsonify(document_list)
        else:
            return "Document not found", 404
    except Exception as e:
        return f"Error: {e}", 500
        
@app.route("/get-document/river-flow-all-master-data")
def get_all_master_data():
    document_list = []
    try:
        cursor = mongo.db['river-flow-master-data'].find()
        print(cursor)
        if cursor:
            for document in cursor:
            	document_list.append({"locationId":document["locationId"],
            			      "name":document["name"],
            			      "nztmx":document["nztmx"],
            			      "nztmy":document["nztmy"],
            			      "type":document["type"],
            			      "unit":document["unit"],	
            	})
            return jsonify(document_list)
        else:
            return "Document not found", 404
    except Exception as e:
        return f"Error: {e}", 500
        
@app.route("/get-document/river-flow-transaction-data-all")
def get_all_transaction_data():
    
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    
    iso_start_time = datetime.fromisoformat(start_date) 
    iso_end_time =   datetime.fromisoformat(end_date)
    
    
    document_list = []
    try:
        cursor = mongo.db['river-flow-transaction-data'].find({
        "ObservationTime": {"$gte": iso_start_time,"$lte": iso_end_time}
          	})
          	
        if cursor:
            for document in cursor:
            	print(document)
            	document_list.append({"locationId":document["locationId"],
            			      "ObservationTime":document["ObservationTime"],
            			      "qualityCode":document["qualityCode"],
            			      "value":document["value"]
            	})
            return jsonify(document_list)
        else:
            return "Document not found", 404
    except Exception as e:
        return f"Error: {e}", 500        

@app.route("/get-document/river-flow-transaction-data-location")
def get_location_transaction_data():
    
    
    locationID = request.args['locationId']
    start_date = request.args['start_date']
    end_date = request.args['end_date']
    
    iso_start_time = datetime.fromisoformat(start_date) 
    iso_end_time =   datetime.fromisoformat(end_date)
   
    
    document_list = []
    try:
        cursor = mongo.db['river-flow-transaction-data'].find({
        		"ObservationTime": {"$gte": iso_start_time,"$lte": iso_end_time} ,
        		"locationId": locationID
          	})
 
        if cursor:
            for document in cursor:
 
            	document_list.append({"locationId":document["locationId"],
            			      "ObservationTime":document["ObservationTime"],
            			      "qualityCode":document["qualityCode"],
            			      "value":document["value"]
            	})
            return jsonify(document_list)
        else:
            return "Document not found", 404
    except Exception as e:
        return f"Error: {e}", 500                      
        
app.run(host='0.0.0.0', port=5000)       
        
