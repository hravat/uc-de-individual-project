from flask import Flask,jsonify,request
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb+srv://hravat:hravat@cluster0.7wqtwdz.mongodb.net/de-river-flow"
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
    
@app.route("/get-document/river-flow-avro")
def get_document():
    document_list = []
    try:
        # Retrieve the document by its ID
        locationID = request.args["locationId"]
        print(locationID)
        cursor = mongo.db['river-flow-avro'].find({"message.locationId":locationID})
        if cursor:
            for document in cursor:
            	print(document['message'])
            	document_list.append(document['message'])
            return jsonify(document_list)
        else:
            return "Document not found", 404
    except Exception as e:
        return f"Error: {e}", 500
        
app.run(debug=True)       
        
