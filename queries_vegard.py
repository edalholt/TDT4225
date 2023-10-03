from multiprocessing import Pool, cpu_count

import numpy as np
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
        AND
            ST_DISTANCE_SPHERE(POINT(t1.lon,t1.lat), POINT(t2.lon, t2.lat)) <= 50
            LIMIT 100000
            
    """)
    df = pd.read_sql_query(query_entire, program.db_connection)

    #df = pd.read_csv("pls_work.csv.csv")

    processed = 0
    total = len(df)
    close_users = set()
    for _, row in df.iterrows():
        user_pair = tuple(sorted([row["user1_id"], row["user2_id"]]))
        if user_pair in close_users:
            processed += 1
            continue
        coords1 = (row["trackpoint1_lat"], row["trackpoint1_lon"])
        coords2 = (row["trackpoint2_lat"], row["trackpoint2_lon"])
        distance = haversine(coords1, coords2, unit="m")
        if distance <= 50:
            close_users.add(user_pair)
            processed += 1
        print(f"Processed {processed}/{total} rows", end="\r")
    print(close_users)




if __name__ == '__main__':
    program = None
    try:
        program = task2()
        #query2(program)
        #query5(program)
        query8(program)
        #query11(program)
        # Load data

    except Exception as e:
        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()
