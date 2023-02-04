from flask import Flask, request
from kafka import KafkaProducer
import json
from pymongo import MongoClient
import stream_pods_status 

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
client = MongoClient('localhost', 27017)  # default mongodb port is 27017


@app.route('/data', methods=['POST'])
def post_data():

    data = request.get_json()  # get the data from the API

    # convert the data to json format and send it to kafka topic 
    producer.send('topic-name', json.dumps(data).encode('utf-8'))

    # insert the data into mongodb as a json document 
    db = client.test_database  # database name 
    collection = db.test_collection  # collection name 

    collection.insert_one(data)

    return 'Data successfully posted!'


if __name__ == '__main__':
   app.run()