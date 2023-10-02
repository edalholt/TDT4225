import pandas as pd
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
        query2 = """
         SELECT user_id,
            AVG(trackpoint_count) AS avg_trackpoints_per_activity,
             MIN(trackpoint_count) AS min_trackpoints,
            MAX(trackpoint_count) AS max_trackpoints

            FROM
                (
                SELECT user_id, COUNT(activity_id) AS trackpoint_count
                FROM Activity
                INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                GROUP BY user_id, activity_id
            ) AS ActivityData
            GROUP BY user_id
        """
        # program.execute_sql_query(query2)
        query5 = """
            SELECT user_id, COUNT(DISTINCT (transportation_mode)) AS unique_modes
            FROM Activity
            WHERE Transportation_mode IS NOT NULL
            GROUP BY user_id
            ORDER BY unique_modes DESC
            LIMIT 10
        """
        # program.execute_sql_query(query5)
        create_temp_table = """
        CREATE TEMPORARY TABLE TempOverlappingActivities AS
        SELECT 
            a1.id AS activity_id1,
            a2.id AS activity_id2
        FROM 
            Activity AS a1
        JOIN 
            Activity AS a2 ON a1.user_id != a2.user_id
        WHERE 
            a2.start_date_time <= a1.end_date_time
            AND a2.end_date_time >= a1.start_date_time;
                """
        query8 = """
                SELECT 
            a1.user_id AS user1,
            a2.user_id AS user2,
            tp1.lat AS lat1,
            tp1.lon AS lon1,
            tp1.date_time AS date_time1,
            tp2.lat AS lat2,
            tp2.lon AS lon2,
            tp2.date_time AS date_time2
        FROM 
            TempOverlappingActivities oa
        JOIN 
            Activity a1 ON oa.activity_id1 = a1.id
        JOIN 
            Activity a2 ON oa.activity_id2 = a2.id
        JOIN 
            TrackPoint tp1 ON oa.activity_id1 = tp1.activity_id
        JOIN 
            TrackPoint tp2 ON oa.activity_id2 = tp2.activity_id;
        """
        results = program.execute_sql_query(create_temp_table)
        print(results)
        program.execute_sql_query(query8)











        df = pd.read_sql_query(query8, program.db_connection)
        df.to_csv('data2.csv', index=False)
        print("Saved to csv.")
        df = pd.read_csv('data.csv')
        df_reduced = df[0:100000]
        df_reduced.to_csv('data_reduced.csv', index=False)
        print("Read csv into memory")
        df['date_time'] = pd.to_datetime(df['date_time'])
        df = df.sort_values(by=['activity_id'])

        close_users = set()
        for _, row1 in df.iterrows():
            time_delta = pd.Timedelta(seconds=30)
        # Find users who have invalid activies (trackpoints deviate with 5 minutes) and count them
        query11 = """
          WITH UserTrackPoints AS (
            SELECT user_id, TrackPoint.id as track_point_id, date_time
            FROM Activity
            INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            )
            SELECT a.user_id,  COUNT(DISTINCT (a.track_point_id)) AS invalid_trackpoints
            FROM UserTrackPoints a
            JOIN UserTrackPoints b ON a.track_point_id = b.track_point_id - 1 AND a.user_id = b.user_id
            WHERE ABS(TIMESTAMPDIFF(MINUTE , a.date_time, b.date_time)) >= 5  
            GROUP BY a.user_id
        """
    # program.execute_sql_query(query11)

    except Exception as e:

        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
