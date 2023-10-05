import json
from collections import defaultdict

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


def query2(program):
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


def query5(program):
    query5 = """
        SELECT user_id, COUNT(DISTINCT (transportation_mode)) AS unique_modes
        FROM Activity
        WHERE Transportation_mode IS NOT NULL
        GROUP BY user_id
        ORDER BY unique_modes DESC
        LIMIT 10
    """
    program.execute_sql_query(query5)


def query11(program):
    query11 = """
        WITH UserTrackPoints AS (
            SELECT user_id, TrackPoint.id as track_point_id, date_time
            FROM Activity
            INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            )
            SELECT a.user_id,  COUNT(DISTINCT (a.track_point_id)) AS invalid_trackpoints
            FROM UserTrackPoints a
            JOIN UserTrackPoints b ON a.track_point_id = b.track_point_id + 1 AND a.user_id = b.user_id
            WHERE ABS(TIMESTAMPDIFF(MINUTE , a.date_time, b.date_time)) >= 5  
            GROUP BY a.user_id
            ORDER BY invalid_trackpoints DESC
    """
    program.execute_sql_query(query11)


def query8(program):
    query_entire = ("""
    WITH OverlappingActivities AS (
        SELECT  
            a.user_id as user1_id,
            b.user_id as user2_id,
            a.id AS activity1_id,
            b.id AS activity2_id,
            GREATEST(a.start_date_time, b.start_date_time) AS overlap_start,
            LEAST(a.end_date_time, b.end_date_time) AS overlap_end
        FROM 
            Activity a
        JOIN 
            Activity b ON a.user_id != b.user_id
                      AND (
                          (a.start_date_time <= b.end_date_time AND a.end_date_time >= b.start_date_time)
                       OR (b.start_date_time <= a.end_date_time AND b.end_date_time >= a.start_date_time)
                      )
        )
        SELECT 
            o.user1_id,
            o.activity1_id,
            t1.id AS trackpoint1_id,
            t1.lat AS trackpoint1_lat,
            t1.lon AS trackpoint1_lon,
            t1.date_time AS trackpoint1_date_time,
            o.user2_id,
            o.activity2_id,
            t2.id AS trackpoint2_id,
            t2.lat AS trackpoint2_lat,
            t2.lon AS trackpoint2_lon,
            t2.date_time AS trackpoint2_date_time
        FROM 
            OverlappingActivities o
        JOIN 
            TrackPoint t1 ON o.activity1_id = t1.activity_id
        JOIN 
            TrackPoint t2 ON o.activity2_id = t2.activity_id
        WHERE 
            t1.date_time BETWEEN o.overlap_start AND o.overlap_end
        AND 
            t2.date_time BETWEEN o.overlap_start AND o.overlap_end
        AND
            ABS(TIMESTAMPDIFF(SECOND, t1.date_time, t2.date_time)) <= 30
            
    """)
    # df = pd.read_sql_query(query_entire, program.db_connection)
    # df.to_csv("EDS.csv", index=False)

    query = """
    SELECT  user_id as user, Activity.id as activity_id, start_date_time as activity_start_time,
            end_date_time as activity_end_time, TrackPoint.id as trackpoint_id, lat, lon, date_time
    FROM Activity
    INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
    """

    # df = pd.read_sql_query(query, program.db_connection)
    # df.to_csv("stupid_solution.csv", index=False)

    try:
        with open('user_activities.json', 'r') as file:
            user_activities = json.load(file)
        if not user_activities:
            raise ValueError("The dictionary is empty.")
        print("Dictionary loaded from file.")

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
                        start_time2 = activity_data2['start_time']
                        end_time2 = activity_data2['end_time']
                        if (start_time1 <= end_time2) and (start_time2 <= end_time1):
                            overlap_start = pd.to_datetime(max(start_time1, start_time2))
                            overlap_end = pd.to_datetime(min(end_time1, end_time2))
                            overlapping_activity_pairs.append(
                                (cur_user, activity_id1, next_user, activity_id2, overlap_start, overlap_end))

        # Find users who have been close in both space and time.
        close_users = set()
        total_pairs = len(overlapping_activity_pairs)
        for index, (cur_user, activity_id1, next_user, activity_id2, overlap_start, overlap_end) in enumerate(
                overlapping_activity_pairs):
            print(f"Processing pair {index + 1} of {total_pairs}...")
            # Early exit if the pair has already been found to be close.
            if (cur_user, next_user) in close_users:
                continue
            # Get trackpoints for the overlapping period.
            activity1_trackpoints = [tp for tp in user_activities[cur_user][activity_id1]['trackpoints']
                                     if overlap_start <= pd.to_datetime(tp['date_time']) <= overlap_end]

            activity2_trackpoints = [tp for tp in user_activities[next_user][activity_id2]['trackpoints']
                                     if overlap_start <= pd.to_datetime(tp['date_time']) <= overlap_end]
            found_close = False
            for trackpoint1 in activity1_trackpoints:
                if found_close:
                    break
                for trackpoint2 in activity2_trackpoints:
                    time_diff = abs(pd.to_datetime(trackpoint1['date_time']) - pd.to_datetime(trackpoint2['date_time']))
                    if time_diff.seconds <= 30:
                        cord1 = (trackpoint1['lat'], trackpoint1['lon'])
                        cord2 = (trackpoint2['lat'], trackpoint2['lon'])
                        distance = haversine(cord1, cord2, unit="m")
                        if distance <= 50:
                            close_users.add(tuple(sorted([cur_user, next_user])))
                            print(f"Closer users found! Match between {cur_user} and {next_user}")
                            found_close = True
                            break

        close_users_dict = defaultdict(set)
        for user1, user2 in close_users:
            close_users_dict[user1].add(user2)
            close_users_dict[user2].add(user1)

        print("\nSummary of users and who they've been close to:")
        for user, close_to in close_users_dict.items():
            print(f"User {user} has been close to users: {', '.join(close_to)}")





    except (FileNotFoundError, ValueError, json.JSONDecodeError):
        # If there's any issue with the file or its contents, compute the data from scratch
        df = pd.read_csv("stupid_slution.csv")
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

        # Save the dict to a json file.
        with open('user_activities.json', 'w') as file:
            json.dump(user_activities, file)

    # processed = 0
    # total = len(df)
    # close_users = set()
    # for _, row in df.iterrows():
    #     user_pair = tuple(sorted([row["user1_id"], row["user2_id"]]))
    #     if user_pair in close_users:
    #         processed += 1
    #         continue
    #     coords1 = (row["trackpoint1_lat"], row["trackpoint1_lon"])
    #     coords2 = (row["trackpoint2_lat"], row["trackpoint2_lon"])
    #     distance = haversine(coords1, coords2, unit="m")
    #     if distance <= 50:
    #         close_users.add(user_pair)
    #         processed += 1
    #     print(f"Processed {processed}/{total} rows", end="\r")
    # print(close_users)


if __name__ == '__main__':
    program = None
    try:
        # program = task2()
        # query2(program)
        # query5(program)
        query8(program)
        # query11(program)
        # Load data

    except Exception as e:
        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()
