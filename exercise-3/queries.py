from pprint import pprint 
from DbConnector import DbConnector
import time
import math
import json
from haversine import haversine, Unit

class queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
   
    def query2(self):
        userCollection = self.db["Users"]
        activitiesCollection = self.db['Activities']

        print(activitiesCollection.count_documents({}) / userCollection.count_documents({}))

    def query5(self):
        activitiesCollection = self.db['Activities']

        # Aggregation which filter out activities with no transportation mode and groups the rest by transportation mode.
        result = activitiesCollection.aggregate([
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "count": {"$sum": 1}
                }
            }
        ])
        for doc in result:
            pprint(doc)

    def query8(self):
        activitiesCollection = self.db['Activities']
        trackpointsCollection = self.db['TrackPoints']

        altitude_gain_by_user = []
        
        # For each user with an activity, find all trackpoints for the user's activities.
        for userID in activitiesCollection.distinct("user_id"):
            userActivities = list(activitiesCollection.find({"user_id": userID}, {"_id": 1}))
            userActivityIds = [activity["_id"] for activity in userActivities]

            trackpoints = list(trackpointsCollection.find({"activity_id": {"$in": userActivityIds}, "altitude": {"$ne": -777}}))

            altitude_gain = 0
            last_altitude = 0
            for trackpoint in trackpoints:
                if trackpoint['altitude'] > last_altitude:
                    altitude_gain += trackpoint['altitude'] - last_altitude
                last_altitude = trackpoint['altitude']
            
            # Convert altitude gain from feet to meters.
            altitude_gain = round(altitude_gain * 0.3048, 2)
            print("User: ", userID, " Altitude gain: ", altitude_gain)
            altitude_gain_by_user.append({"user_id": userID, "altitude_gain": altitude_gain})
        
        # Sort the list of users by altitude gain and print the top 20.
        altitude_gain_by_user.sort(key=lambda x: x['altitude_gain'], reverse=True)
        print("\n\n")
        print("Top 20 users with highest altitude gain:")
        for user in altitude_gain_by_user[:20]:
            print("User: ", user['user_id'], " Altitude gain: ", user['altitude_gain'])

    def query10(self):
        activitiesCollection = self.db['Activities']
        trackpointsCollection = self.db['TrackPoints']
        trackpointsCollection.create_index([("location", "2dsphere")])
        distance = 500

        trackpoints = trackpointsCollection.distinct("activity_id", {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [116.397, 39.916]
                    },
                    "$maxDistance": distance,
                    "$minDistance": 0
                }
            }
        })

        user_ids = activitiesCollection.distinct("user_id", 
            {"_id": 
             {"$in": trackpoints}
            })

        print(f"\n\nUsers that have tracked an activity within a radius of {distance}m of the forbidden city:")
        pprint(user_ids)


    def queryExample(self):
        collection = self.db['Activities']
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
        program.query10()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
