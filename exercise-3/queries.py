from collections import defaultdict
from pprint import pprint
import datetime
from DbConnector import DbConnector


class queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def query1(self):
        userCollection = self.db["Users"]
        activitiesCollection = self.db["Activities"]
        trackpointsCollection = self.db["TrackPoints"]

        print("Number of users:", userCollection.count_documents({}))
        print("Number of activities:", activitiesCollection.count_documents({}))
        print("Number of trackpoints:", trackpointsCollection.count_documents({}))

    def query2(self):
        userCollection = self.db["Users"]
        activitiesCollection = self.db['Activities']

        print(activitiesCollection.count_documents({}) / userCollection.count_documents({}))
            
    def query3(self):
        """
        Find the top 20 users with the highest number of activities.
        Group by user_id, count how many times this user_id appears in the collection.
        """
        collection = self.db['Activities']
        documents = collection.aggregate([
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ])
        for doc in documents:
            pprint(doc)

    def query4(self):
        activitiesCollection = self.db["Activities"]

        result = activitiesCollection.aggregate([
            {
                "$match": {
                    "transportation_mode": "taxi"
                }
            }, {
                "$group": {
                   "_id": "$user_id"
                }
            }
        ])

        print("Users who have taken a taxi:")
        for doc in result:
            pprint(doc)

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

    def query6(self):
        """
         a) Find the year with the most activities.
         b) Is this also the year with most recorded hours?
         Use MongoDB date operators to extract year from activity start_date_time.
         For each activity, calculate the difference between end_date_time and start_date_time  in hours.
         Group by year, count how many activities there are in each year and sum the hours.
        """
        collection = self.db['Activities']
        documents = collection.aggregate([
            {
                "$project": {
                    "year": {"$year": "$start_date_time"},
                    "hours": {
                        "$dateDiff": {
                            "startDate": "$start_date_time",
                            "endDate": "$end_date_time",
                            "unit": "hour"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$year",
                    "count": {"$sum": 1},
                    "hours": {"$sum": "$hours"}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ])
        for doc in documents:
            pprint(doc)

    def query7(self):
        activitiesCollection = self.db['Activities']
        trackpointsCollection = self.db['TrackPoints']
        userActivities = list(activitiesCollection.find({"user_id": "112", "transportation_mode": "walk"}, {"_id": 1}))

        start_date = datetime.datetime(2008, 1, 1)
        end_date = datetime.datetime(2009, 1, 1)
        
        distance = 0
        for activity in userActivities:
            trackpoints = list(trackpointsCollection.find({
                            "activity_id": activity["_id"],
                            "date_time": {
                                "$gte": start_date,
                                "$lt": end_date
                            }
                        }))

            last_pos = None
            for trackpoint in trackpoints:
                if last_pos is not None:
                    distance += hs.haversine((last_pos[1], last_pos[0]), (trackpoint['location']['coordinates'][1], trackpoint['location']['coordinates'][0]), unit=hs.Unit.KILOMETERS)
                last_pos = trackpoint['location']['coordinates']
        print("Total distance walked by user 112 in 2008: ", round(distance, 3), "km")

    def query8(self):
        activitiesCollection = self.db['Activities']
        trackpointsCollection = self.db['TrackPoints']

        altitude_gain_by_user = []

        # For each user with an activity, find all trackpoints for the user's activities.
        for userID in activitiesCollection.distinct("user_id"):
            userActivities = list(activitiesCollection.find({"user_id": userID}, {"_id": 1}))
            userActivityIds = [activity["_id"] for activity in userActivities]

            trackpoints = list(
                trackpointsCollection.find({"activity_id": {"$in": userActivityIds}, "altitude": {"$ne": -777}}))

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

    def query9(self):
        activitiesCollection = self.db['Activities']
        trackpointsCollection = self.db['TrackPoints']

        invalid_activities = defaultdict(int)
        for user_id in activitiesCollection.distinct("user_id"):
            # get all activities belonging to the user.
            user_activities = list(activitiesCollection.find({"user_id": user_id}, {"_id": 1}))
            user_activity_ids = [activity["_id"] for activity in user_activities]

            # get all trackpoints for the user's activities.
            user_trackpoints = trackpointsCollection.find({"activity_id": {"$in": user_activity_ids}},
                                                          {"activity_id": 1, "date_time": 1})
            # Create a dict for the activities and the trackpoints belonging to them.
            activity_dict = defaultdict(list)
            invalid_ids = set()
            for trackpoint in user_trackpoints:
                activity_id = trackpoint["activity_id"]
                activity_dict[activity_id].append(trackpoint)

            # Loop over every individual activity and look for trackpoints which deviate with 5 or more minutes.
            for activity_id, trackpoints in activity_dict.items():

                # If the activity has already been marked as invalid, skip it.
                if activity_id in invalid_ids:
                    print(f"Activity ID: {activity_id} has already been marked as invalid. Skipping...")
                    continue

                for i in range(1, len(trackpoints)):
                    current = trackpoints[i]["date_time"]
                    previous = trackpoints[i - 1]["date_time"]
                    time_diff = (current - previous).total_seconds() / 60
                    if time_diff >= 5:
                        invalid_activities[user_id] += 1
                        invalid_ids.add(activity_id)
                        break

        sorted_invalid_activities = sorted(invalid_activities.items(), key=lambda x: x[1], reverse=True)
        for user_id, count in sorted_invalid_activities:
            print(f"User ID: {user_id}, Invalid Activities: {count}")

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

    def query11(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode. First group by
        user_id and transportation_mode, count how many times each combination appears in the collection. Sort this in
        descending order, so that the most used transportation_mode for each user_id is the first element in the list.
        """
        collection = self.db['Activities']
        query = collection.aggregate([
            {
                "$lookup": {
                    "from": "Users",
                    "localField": "user_id",
                    "foreignField": "_id",
                    "as": "user"
                },
            },
            {"$unwind": "$user"},
            {"$match": {"user.has_labels": True, "transportation_mode": {"$ne": None}}},

            {"$group": {
                "_id": {
                    "user_id": "$user_id",
                    "transportation_mode": "$transportation_mode"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$group": {
                "_id": "$_id.user_id",
                "most_used_transportation_mode": {"$first": "$_id.transportation_mode"}
            }},
            {"$sort": {"_id": 1}}

        ])

        for doc in query:
            pprint(doc)

    def show_coll(self):
        collections = self.client['activity_data'].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = queries()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
