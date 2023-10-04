from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import haversine as hs


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
        print(tabulate(rows, headers=self.cursor.column_names)+ "\n")

    def execute_sql_query_tabulated(self, query):
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


def query1(program):
        print("--- Query 1 ---\n")
        #Number of users
        print("Number of users:")
        program.execute_sql_query_raw("SELECT COUNT(id) FROM User")
        # Number of activities
        print("Number of activities:")
        program.execute_sql_query_raw("SELECT COUNT(id) FROM Activity")
        # Number of trackpoints
        print("Number of trackpoints:")
        program.execute_sql_query_raw("SELECT COUNT(id) FROM TrackPoint")

def query4(program):
        print("--- Query 4 ---\n")
        # Users with bus as transportation mode
        print("Users with bus as transportation mode:")
        program.execute_sql_query_tabulated("SELECT DISTINCT user_id FROM Activity WHERE transportation_mode = 'bus'")

def query7(program):
        print("--- Query 7 ---\n")
        # users that have started an activity in one day and ended the activity the next day
        print("Number of users that have started an activity in one day and ended the activity the next day:")
        program.execute_sql_query_raw("SELECT COUNT(DISTINCT user_id) FROM Activity WHERE DATE(start_date_time) != DATE(end_date_time)")
        
        print("Users that have started an activity in one day and ended the activity the next day, by transportation mode and descending duration:")
        program.execute_sql_query_tabulated("""
                                           SELECT user_id, transportation_mode, TIMEDIFF(end_date_time, start_date_time) AS duration 
                                            FROM Activity WHERE DATE(start_date_time) != DATE(end_date_time) 
                                            AND transportation_mode IS NOT NULL 
                                            ORDER BY duration DESC""")

def query10(program):
        # Get all activities using pandas
        print("--- Query 10 ---\n")
        df = pd.read_sql_query("SELECT TP.activity_id, TP.lat, TP.lon, A.transportation_mode, A.user_id FROM TrackPoint as TP JOIN (SELECT id, user_id, start_date_time, end_date_time, transportation_mode FROM Activity) as A ON A.id = TP.activity_id WHERE A.transportation_mode IS NOT NULL AND DATE(A.start_date_time) = DATE(A.end_date_time)", program.db_connection)
        activity_coordinates = df.groupby('activity_id').agg({'lat': list, 'lon': list, 'transportation_mode': list, 'user_id': list}).reset_index()

        # Initialize variables
        total_distances = []  # A dictionary to store total distances for each activity

        for index, row in activity_coordinates.iterrows():
            activity_id = row['activity_id']
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
            
            #print(f"Activity ID: {data[0]}, Total Distance: {data[1]:.2f} km, Transportation Mode: {data[2]}")

        print("Max distances for each transportation mode \n")
        for key in max_distances:
            print(f"User ID: {max_distances[key][0]}, Total Distance: {max_distances[key][1]:.2f} km, Transportation Mode: {max_distances[key][2]}")

       

def main():
    program = None
    try:
        program = task2()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
