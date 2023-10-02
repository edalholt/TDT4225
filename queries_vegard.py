from itertools import combinations

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


def main():
    program = None
    try:
        program = task2()
        program.show_tables()
        # Query 2
        # program.execute_sql_query(
        #     """
        #     SELECT user_id,
        #     AVG(trackpoint_count) AS avg_trackpoints_per_activity,
        #      MIN(trackpoint_count) AS min_trackpoints,
        #     MAX(trackpoint_count) AS max_trackpoints
        #
        #     FROM
        #         (
        #         SELECT user_id, COUNT(activity_id) AS trackpoint_count
        #         FROM Activity
        #         INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        #         GROUP BY user_id, activity_id
        #     ) AS ActivityData
        #     GROUP BY user_id
        #     """
        # )
        # Query 5
        # program.execute_sql_query(
        #     """
        #     SELECT user_id, COUNT(DISTINCT (transportation_mode)) AS unique_modes
        #     FROM Activity
        #     WHERE Transportation_mode IS NOT NULL
        #     GROUP BY user_id
        #     ORDER BY unique_modes DESC
        #     LIMIT 10
        #     """
        #
        # )
        # Query 8
        query = """
            SELECT 
                user_id,
                lat,
                lon,
                date_time
            FROM User
            JOIN Activity ON User.id = Activity.user_id
            JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            
        """

        # df = pd.read_sql_query(query, program.db_connection)
        # df.to_csv('data.csv', index=False)
        df = pd.read_csv('data.csv')
        df.groupby('user_id').agg({'lat': list, 'lon': list, 'date_time': list}).reset_index()

        close_users = []
        for (idx1, row1), (idx2, row2) in combinations(df.iterrows(), 2):
            time_diff = np.abs((row1['date_time'] - row2['date_time']).total_seconds())
            coord1 = (row1['lat'], row1['lon'])
            coord2 = (row2['lat'], row2['lon'])
            spatial_diff = haversine(coord1, coord2, unit='m')
            if time_diff <= 30 and spatial_diff <= 50:
                close_users.append((row1['user_id'], row2['user_id']))
        print(close_users)

    except Exception as e:

        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
