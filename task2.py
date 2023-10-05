import json
from collections import defaultdict

import haversine as hs
import pandas as pd
from haversine import haversine
from tabulate import tabulate

from DbConnector import DbConnector


class task2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names) + "\n")

    def execute_sql_query(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print("Data from query, tabulated: \n")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def execute_sql_query_raw(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(rows[0][0])
        return rows

    def execute_sql_no_print(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows


def query1(program):
    print("--- Query 1 ---\n")

    # Number of users
    print("Number of users:")
    program.execute_sql_query_raw("SELECT COUNT(id) FROM User")
    # Number of activities
    print("Number of activities:")
    program.execute_sql_query_raw("SELECT COUNT(id) FROM Activity")
    # Number of trackpoints
    print("Number of trackpoints:")
    program.execute_sql_query_raw("SELECT COUNT(id) FROM TrackPoint")


def query2(program):
    print("--- Query 2 ---\n")
    query = """
        WITH ActivityData AS (
            SELECT user_id, COUNT(activity_id) AS trackpoint_count
            FROM Activity
            INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            GROUP BY user_id, activity_id
            )
            SELECT user_id,
            AVG(trackpoint_count) AS avg_trackpoints_per_activity,
            MIN(trackpoint_count) AS min_trackpoints,
            MAX(trackpoint_count) AS max_trackpoints
            FROM ActivityData
            GROUP BY user_id
    """
    program.execute_sql_query(query)


def query3(program):
    print("--- Query 3 ---\n")

    program.execute_sql_query("""SELECT user_id, COUNT(user_id) AS 'Number of activities' 
    FROM Activity 
    GROUP BY user_id 
    ORDER BY COUNT(user_id) DESC 
    LIMIT 15""")


def query4(program):
    print("--- Query 4 ---\n")

    # Users with bus as transportation mode
    print("Users with bus as transportation mode:")
    program.execute_sql_query("SELECT DISTINCT user_id FROM Activity WHERE transportation_mode = 'bus'")


def query5(program):
    print("--- Query 5 ---\n")
    query5 = """
        SELECT user_id, COUNT(DISTINCT (transportation_mode)) AS unique_modes
        FROM Activity
        WHERE Transportation_mode IS NOT NULL
        GROUP BY user_id
        ORDER BY unique_modes DESC
        LIMIT 10
    """
    program.execute_sql_query(query5)


def query6(program):
    print("--- Query 6 ---\n")

    program.execute_sql_query("""SELECT user_id, start_date_time, end_date_time, COUNT(*) as count 
    FROM Activity 
    GROUP BY user_id, start_date_time, end_date_time 
    HAVING count > 1""")


def query7(program):
    print("--- Query 7 ---\n")

    # users that have started an activity in one day and ended the activity the next day
    print("Number of users that have started an activity in one day and ended the activity the next day:")
    program.execute_sql_query_raw(
        "SELECT COUNT(DISTINCT user_id) FROM Activity WHERE DATE(start_date_time) != DATE(end_date_time)")

    print(
        "Users that have started an activity in one day and ended the activity the next day, by transportation mode and descending duration:")
    program.execute_sql_query("""
                                        SELECT user_id, transportation_mode, TIMEDIFF(end_date_time, start_date_time) AS duration 
                                        FROM Activity WHERE DATE(start_date_time) != DATE(end_date_time) 
                                        AND transportation_mode IS NOT NULL 
                                        ORDER BY duration DESC""")


def query8(program):
    print("--- Query 8 ---\n")
    """
    Warning: This query takes a while to execute and to collect the results.
    """
    query = """
        SELECT  user_id as user, Activity.id as activity_id, start_date_time as activity_start_time,
                end_date_time as activity_end_time, TrackPoint.id as trackpoint_id, lat, lon, date_time
        FROM Activity
        INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        """

    print("Fetching data from database...")
    df = pd.read_sql_query(query, program.db_connection)
    print("Data fetched from database. \nCreating dict for lookups...")
    # Create dict for lookups.
    user_activities = {}
    for _, row in df.iterrows():
        user_id = row['user']
        activity_id = row['activity_id']
        start_time = row['activity_start_time']
        end_time = row['activity_end_time']
        if user_id not in user_activities:
            user_activities[user_id] = {}

        if activity_id not in user_activities[user_id]:
            user_activities[user_id][activity_id] = {
                'start_time': start_time,
                'end_time': end_time,
                'trackpoints': []
            }
        user_activities[user_id][activity_id]['trackpoints'].append({
            'lat': row['lat'],
            'lon': row['lon'],
            'date_time': row['date_time']
        })

    # Filter out non overlapping activities to reduce comparisons.
    overlapping_activity_pairs = []
    users = list(user_activities.keys())
    print("Filtering out non overlapping activities...")
    for index, cur_user in enumerate(users):
        for activity_id1, activity_data1 in user_activities[cur_user].items():
            start_time1 = activity_data1['start_time']
            end_time1 = activity_data1['end_time']
            for next_user in users[index + 1:]:
                for activity_id2, activity_data2 in user_activities[next_user].items():
                    # means that the activities in question is most likely from a bug.
                    filtered_activity1 = activity_id1[:-3]
                    filtered_activity2 = activity_id2[:-3]
                    # Same ID
                    if filtered_activity1 == filtered_activity2:
                        if len(activity_data1['trackpoints']) == len(activity_data2['trackpoints']):
                            continue
                    start_time2 = activity_data2['start_time']
                    end_time2 = activity_data2['end_time']
                    if (start_time1 <= end_time2) and (start_time2 <= end_time1):
                        overlap_start = pd.to_datetime(max(start_time1, start_time2))
                        overlap_end = pd.to_datetime(min(end_time1, end_time2))
                        overlapping_activity_pairs.append(
                            (cur_user, activity_id1, next_user, activity_id2, overlap_start, overlap_end))

    # Find users who have been close in both space and time.
    close_users = set()
    potential_duplicates = []
    total_pairs = len(overlapping_activity_pairs)
    for index, (cur_user, activity_id1, next_user, activity_id2, overlap_start, overlap_end) in enumerate(
            overlapping_activity_pairs):
        print(f"Processing pair {index + 1} of {total_pairs}...")
        # Early exit if the pair has already been found to be close.
        if tuple(sorted([cur_user, next_user])) in close_users:
            continue
        # Get trackpoints for the overlapping period, sorted for faster lookup.
        activity1_trackpoints = sorted([tp for tp in user_activities[cur_user][activity_id1]['trackpoints']
                                        if overlap_start <= pd.to_datetime(tp['date_time']) <= overlap_end],
                                       key=lambda tp: tp['date_time'])

        activity2_trackpoints = sorted([tp for tp in user_activities[next_user][activity_id2]['trackpoints']
                                        if overlap_start <= pd.to_datetime(tp['date_time']) <= overlap_end],
                                       key=lambda tp: tp['date_time'])

        found_close = False
        j = 0
        for trackpoint1 in activity1_trackpoints:
            if found_close:
                break
            while j < len(activity2_trackpoints):
                time_diff = abs(pd.to_datetime(trackpoint1['date_time']) - pd.to_datetime(
                    activity2_trackpoints[j]['date_time']))

                if time_diff.seconds > 30:
                    j += 1
                    continue

                cord1 = (trackpoint1['lat'], trackpoint1['lon'])
                cord2 = (activity2_trackpoints[j]['lat'], activity2_trackpoints[j]['lon'])
                distance = haversine(cord1, cord2, unit="m")

                if distance <= 50:
                    if distance == 0:
                        potential_duplicates.append(
                            [cur_user, activity_id1, trackpoint1['date_time'].strftime('%Y-%m-%d %H:%M:%S'), cord1,
                             next_user, activity_id2,
                             activity2_trackpoints[j]['date_time'].strftime('%Y-%m-%d %H:%M:%S'), cord2])
                    close_users.add(tuple(sorted([cur_user, next_user])))
                    print(
                        f"Closer users found! Match between {cur_user} (activity {activity_id1}) and {next_user} (activity {activity_id2}.)")
                    print(
                        f"User {cur_user} has t"
                        f"rackpoint {trackpoint1['date_time']} at {cord1}. User {next_user} has trackpoint {activity2_trackpoints[j]['date_time']} at {cord2}. Distance: {distance:.2f} meters.")
                    found_close = True
                    break
                j += 1
    with open("potential_duplicatesV2.txt", "w") as f:
        json.dump(potential_duplicates, f)

    #print(potential_duplicates)
    # Print results.
    close_users_dict = defaultdict(set)
    for user1, user2 in close_users:
        close_users_dict[user1].add(user2)
        close_users_dict[user2].add(user1)

    # Sort the users by their ID in ascending order
    sorted_users = sorted(close_users_dict.keys(), key=lambda x: int(x))

    for user in sorted_users:
        sorted_matches = sorted(close_users_dict[user],
                                key=lambda x: int(x))
        print(f"User {user} has been close to users: {', '.join(sorted_matches)}")


def query9(program):
    rows = program.execute_sql_no_print("""SELECT altitude, Activity.id AS activity_id, 
    TrackPoint.id AS tp_id, Activity.user_id AS user_id 
    FROM Activity 
    INNER JOIN TrackPoint ON Activity.id=TrackPoint.activity_id""")
    users_dict = {}
    for row in range(1, len(rows)):
        user_id = rows[row][-1]
        current_alt = rows[row][0]
        prev_alt = rows[row - 1][0]
        current_activity = rows[row][1]
        prev_activity = rows[row - 1][1]

        # Add userid to dictionary. All users start with 0 altitude meters
        if user_id not in users_dict:
            users_dict[user_id] = 0

        # If current trackpoint has a higher altitude value than the last trackpoint, 
        # and they belong to the same activity, add to user's total
        if (current_alt != -777 and prev_alt != -777 and current_alt > prev_alt and current_activity == prev_activity):
            users_dict[user_id] = users_dict[user_id] + (current_alt - prev_alt)

    # Find top 15 users who have gained the most altitude meters
    top_15 = []
    for i in range(15):
        most_alt_gained = (max(users_dict, key=users_dict.get), (max(users_dict.values()) / 3.281))
        top_15.append(most_alt_gained)
        del users_dict[max(users_dict, key=users_dict.get)]

    print(tabulate(top_15, headers=["user_id", "total altitude meters gained"]))


def query10(program):
    print("--- Query 10 ---\n")

    df = pd.read_sql_query(
        "SELECT TP.activity_id, TP.lat, TP.lon, A.transportation_mode, A.user_id FROM TrackPoint as TP JOIN (SELECT id, user_id, start_date_time, end_date_time, transportation_mode FROM Activity) as A ON A.id = TP.activity_id WHERE A.transportation_mode IS NOT NULL AND DATE(A.start_date_time) = DATE(A.end_date_time)",
        program.db_connection)
    # Group by activity_id for calculating total distance for each activity
    activity_coordinates = df.groupby('activity_id').agg(
        {'lat': list, 'lon': list, 'transportation_mode': list, 'user_id': list}).reset_index()

    # Initialize variables
    total_distances = []  # A dictionary to store total distances for each activity

    for index, row in activity_coordinates.iterrows():
        user_id = row['user_id'][0]
        label = row['transportation_mode'][0]
        latitudes = row['lat']
        longitudes = row['lon']

        last_lat = None
        last_lon = None
        total_distance = 0.0

        for i in range(len(latitudes)):
            if last_lat is not None and last_lon is not None:
                distance = hs.haversine((last_lat, last_lon), (latitudes[i], longitudes[i]))
                total_distance += distance

            last_lat = latitudes[i]
            last_lon = longitudes[i]

        total_distances.append([user_id, total_distance, label])

    # Now, total_distances is a dictionary where keys are activity_ids, and values are total distances for each activity
    max_distances = {}
    for data in total_distances:
        if data[2] not in max_distances:
            max_distances[data[2]] = data
        else:
            if data[1] > max_distances[data[2]][1]:
                max_distances[data[2]] = data

    print("Max distances for each transportation mode \n")
    for key in max_distances:
        print(
            f"User ID: {max_distances[key][0]}, Total Distance: {max_distances[key][1]:.2f} km, Transportation Mode: {max_distances[key][2]}")


def query11(program):
    print("--- Query 11 ---\n")
    query11 = """
           WITH UserTrackPoints AS (
               SELECT user_id, TrackPoint.id as track_point_id, date_time, activity_id
               FROM Activity
               INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
               )
               SELECT a.user_id,  COUNT(DISTINCT (a.activity_id)) AS invalid_activities
               FROM UserTrackPoints a
               JOIN UserTrackPoints b ON a.track_point_id = b.track_point_id + 1 AND a.user_id = b.user_id
               WHERE ABS(TIMESTAMPDIFF(MINUTE , a.date_time, b.date_time)) >= 5  
               GROUP BY a.user_id
               ORDER BY invalid_activities DESC
       """
    program.execute_sql_query(query11)


def query12(program):
    print("--- Query 12 ---\n")

    program.execute_sql_query("""WITH MostFrequent AS (
            SELECT
                User.id,
                transportation_mode,
                RANK() OVER (PARTITION BY User.id ORDER BY COUNT(*) DESC) AS most_frequent
            FROM User
            INNER JOIN Activity ON User.id = Activity.user_id
            WHERE transportation_mode IS NOT NULL
            GROUP BY User.id, transportation_mode
        )
        SELECT id, transportation_mode
        FROM MostFrequent
        WHERE most_frequent = 1
    """)


def main():
    program = None
    try:
        program = task2()
        query8(program)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
