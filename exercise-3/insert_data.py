from pprint import pprint 
from DbConnector import DbConnector
import time
import math
import json
from datetime import datetime

user_tables = []
user_activities = []
track_points = []
class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

        # Splits a list into chunks of a given size for batch insert
    def split_list_to_chunks(self, list, chunk_size):
        return [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]

    def load_data(self):
        with open('../output.json', 'r') as json_file:
            data = json.load(json_file)
            for user_id, activities in data.items():
                has_labels = any(activity['mode'] for activity in activities)
                user_tables.append({
                    'id': user_id,
                    'has_labels': has_labels,
                })
                for activity in activities:
                    user_activities.append({
                        'id': activity['file_name'].replace('.plt', '') + user_id,
                        'user_id': user_id,
                        'transportation_mode': activity['mode'],
                        'start_date_time': activity['start_time'],
                        'end_date_time': activity['end_time'],
                    })
                    for track_point in activity['data']:
                        point = track_point.split(',')
                        lat = float(point[0])
                        lon = float(point[1])
                        altitude = int(float(point[3]))
                        date_days = float(point[4])
                        date_time = point[5] + ' ' + point[6].strip()
                        track_points.append({
                            'activity_id': activity['file_name'].replace('.plt', '') + user_id,
                            'lat': lat,
                            'lon': lon,
                            'altitude': altitude,
                            'date_days': date_days,
                            'date_time': date_time,
                        })


    def insert_user_data(self):
        data_to_insert = [({"_id": item["id"], "has_labels": item["has_labels"]}) for item in user_tables]
        collection = self.db["Users"]
        collection.insert_many(data_to_insert)

    def insert_activity_data(self):
        collection = self.db["Activities"]
        data_to_insert = [
            {
                "_id": item["id"],
                "user_id": item["user_id"],
                "transportation_mode": item["transportation_mode"],
                "start_date_time": datetime.strptime(item["start_date_time"].strip(" \n"), "%Y-%m-%d %H:%M:%S"),
                "end_date_time": datetime.strptime(item["end_date_time"].strip(" \n"), "%Y-%m-%d %H:%M:%S")
            }
            for item in user_activities
        ]
        collection.insert_many(data_to_insert)

    def insert_track_point_data(self):
        collection = self.db["TrackPoints"]

        track_points_chunks = self.split_list_to_chunks(track_points, 30000)
        number_of_chunks = len(track_points_chunks)
        for chunk in track_points_chunks:
            print(f"Inserting chunk {track_points_chunks.index(chunk) + 1} of {number_of_chunks}")
            data_to_insert = [({"activity_id": item["activity_id"], "location": { "type": "Point", "coordinates": [item["lon"], item["lat"]] }, "altitude": item["altitude"], "date_days": item["date_days"], "date_time": datetime.strptime(item["date_time"], "%Y-%m-%d %H:%M:%S")}) for item in chunk]
            collection.insert_many(data_to_insert)
            print(f"Inserted chunk {track_points_chunks.index(chunk) + 1} of {number_of_chunks} successfully")


        

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['activity_data'].list_collection_names()
        print(collections)

    def initialize_collections(self):
        self.drop_coll('Users')
        self.drop_coll('Activities')
        self.drop_coll('TrackPoints')

        self.create_coll('Users')
        self.create_coll('Activities')
        self.create_coll('TrackPoints')
         


def main():
    program = None
    try:
        program = ExampleProgram()

        program.initialize_collections()

        print("Loading data...")
        start_time = time.time()
        program.load_data()
        print("Loaded data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Inserting user data...")
        start_time = time.time()
        program.insert_user_data()
        print("Inserted user data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Inserting activity data...")
        start_time = time.time()
        program.insert_activity_data()
        print("Inserted activity data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Inserting track point data...")
        start_time = time.time()
        program.insert_track_point_data()
        print("Inserted track point data in " + str(math.trunc(time.time() - start_time)) + " seconds")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
