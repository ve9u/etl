from pymongo import MongoClient

# Connect to the mongodb server from Python
def connect_mongodb():
    mongodb = MongoClient('localhost', 27017)['test']
    return mongodb

# Load some document in mongodb collection
# collection: json(mongodb) <-> dict(python)
def load_data(mongodb, collection, document):
    object_id = mongodb[collection].insert_one(document)
    return object_id