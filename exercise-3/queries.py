from pprint import pprint 
from DbConnector import DbConnector
import time
import math
import json

class queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
   
    def queryExample(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        
    def show_coll(self):
        collections = self.client['activity_data'].list_collection_names()
        print(collections)

def main():
    program = None
    try:
        program = queries()
        program.queryExample("Users")


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
