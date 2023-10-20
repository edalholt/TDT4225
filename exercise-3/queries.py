from collections import defaultdict
from pprint import pprint

from DbConnector import DbConnector


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


def query3(program):
    """
    Find the top 20 users with the highest number of activities.
    Group by user_id, count how many times this user_id appears in the collection.
    """
    collection = program.db['Activities']
    documents = collection.aggregate([
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20}
    ])
    for doc in documents:
        pprint(doc)


def query6(program):
    """
     a) Find the year with the most activities.
     b) Is this also the year with most recorded hours?
     Use MongoDB date operators to extract year from activity start_date_time.
     For each activity, calculate the difference between end_date_time and start_date_time  in hours.
     Group by year, count how many activities there are in each year and sum the hours.
    """
    collection = program.db['Activities']
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


def query9(program):
    activitiesCollection = program.db['Activities']
    trackpointsCollection = program.db['TrackPoints']

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
                    print(
                        f"Found invalid activity for user_id: {user_id}, activity_id: {activity_id}. User currently has {invalid_activities[user_id]} invalid activities.")
                    print(
                        f"First trackpoint: {previous}, Second trackpoint: {current}, Time difference: {time_diff} minutes.")
                    invalid_ids.add(activity_id)
                    break

    sorted_invalid_activities = sorted(invalid_activities.items(), key=lambda x: x[1], reverse=True)
    for user_id, count in sorted_invalid_activities:
        print(f"User ID: {user_id}, Invalid Activities: {count}")

    query = activitiesCollection.aggregate([
        # Join Activities with TrackPoints
        {
            "$lookup": {
                "from": "TrackPoints",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "track_points"
            }
        },
        {"$unwind": "$track_points"},
        {"$sort": {"_id": 1, "track_points.date_time": 1}},

        {
            "$group": {
                "_id": "$_id",
                "date_times": {"$push": "$track_points.date_time"},
                "user_id": {"$first": "$user_id"}
            }
        },

        {
            "$project": {
                "time_diffs": {
                    "$map": {
                        "input": {"$range": [0, {"$subtract": [{"$size": "$date_times"}, 1]}]},
                        "as": "index",
                        "in": {
                            "$dateDiff": {
                                "startDate": {"$arrayElemAt": ["$date_times", "$$index"]},
                                "endDate": {"$arrayElemAt": ["$date_times", {"$add": ["$$index", 1]}]},
                                "unit": "minute"
                            }
                        }
                    }
                },
                "user_id": 1
            }
        },

        {"$match": {"time_diffs": {"$elemMatch": {"$gte": 5}}}},

        {
            "$group": {
                "_id": "$user_id",
                "invalid_activities_count": {"$sum": 1}
            }
        },
        {"$sort": {"invalid_activities_count": -1}}
    ])

    for doc in query:
        pprint(doc)


def query11(program):
    """
    Find all users who have registered transportation_mode and their most used transportation_mode. First group by
    user_id and transportation_mode, count how many times each combination appears in the collection. Sort this in
    descending order, so that the most used transportation_mode for each user_id is the first element in the list.
    """
    collection = program.db['Activities']

    query = collection.aggregate([
        {"$match": {"transportation_mode": {"$exists": True, "$ne": None}}},
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

    queryv2 = collection.aggregate([
        {
            "$lookup": {
                "from": "User",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user"
            },
        },
        {"$unwind": "$user"},
        {"$match": {"has_labels": True}},
        {"$group": {
            "_id": {


    ])

    for doc in query:
        pprint(doc)


def main():
    program = None
    try:
        program = queries()
        # query3(program)
        # query6(program)
        # query9(program)
        query11(program)


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
