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

    #Number of users
    print("Number of users:")
    program.execute_sql_query_raw("SELECT COUNT(id) FROM User")
    # Number of activities
    print("Number of activities:")
    program.execute_sql_query_raw("SELECT COUNT(id) FROM Activity")
    # Number of trackpoints
    print("Number of trackpoints:")
    program.execute_sql_query_raw("SELECT COUNT(id) FROM TrackPoint")

def query3(program):
    print("--- Query 3 ---\n")

    program.execute_sql_query("""SELECT user_id, COUNT(user_id) AS 'number_of_activities' 
    FROM Activity 
    GROUP BY user_id 
    ORDER BY COUNT(user_id) DESC 
    LIMIT 15""")

def query4(program):
    print("--- Query 4 ---\n")

    # Users with bus as transportation mode
    print("Users with bus as transportation mode:")
    program.execute_sql_query("SELECT DISTINCT user_id FROM Activity WHERE transportation_mode = 'bus'")

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
    program.execute_sql_query_raw("SELECT COUNT(DISTINCT user_id) FROM Activity WHERE DATE(start_date_time) != DATE(end_date_time)")
    
    print("Users that have started an activity in one day and ended the activity the next day, by transportation mode and descending duration:")
    program.execute_sql_query("""
                                        SELECT user_id, transportation_mode, TIMEDIFF(end_date_time, start_date_time) AS duration 
                                        FROM Activity WHERE DATE(start_date_time) != DATE(end_date_time) 
                                        AND transportation_mode IS NOT NULL 
                                        ORDER BY duration DESC""")

def query10(program):
    print("--- Query 10 ---\n")

    df = pd.read_sql_query("SELECT TP.activity_id, TP.lat, TP.lon, A.transportation_mode, A.user_id FROM TrackPoint as TP JOIN (SELECT id, user_id, start_date_time, end_date_time, transportation_mode FROM Activity) as A ON A.id = TP.activity_id WHERE A.transportation_mode IS NOT NULL AND DATE(A.start_date_time) = DATE(A.end_date_time)", program.db_connection)
    # Group by activity_id for calculating total distance for each activity
    activity_coordinates = df.groupby('activity_id').agg({'lat': list, 'lon': list, 'transportation_mode': list, 'user_id': list}).reset_index()

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
        print(f"User ID: {max_distances[key][0]}, Total Distance: {max_distances[key][1]:.2f} km, Transportation Mode: {max_distances[key][2]}")

def query9(program):
    rows = program.execute_sql_no_print("""SELECT altitude, Activity.id AS activity_id, 
    Activity.user_id AS user_id 
    FROM Activity 
    INNER JOIN TrackPoint ON Activity.id=TrackPoint.activity_id""")
    users_dict = {}
    for row in range (1, len(rows)):
        user_id = rows[row][-1]
        current_alt = rows[row][0]
        prev_alt = rows[row-1][0]
        current_activity = rows[row][1] 
        prev_activity = rows[row-1][1]

        # Add userid to dictionary. All users start with 0 altitude meters
        if user_id not in users_dict:
            users_dict[user_id] = 0

        # If current trackpoint has a higher altitude value than the last trackpoint, 
        # and they belong to the same activity, add to user's total
        if(current_alt != -777 and prev_alt != -777 and current_alt > prev_alt and current_activity == prev_activity):
            users_dict[user_id] = users_dict[user_id] + ((current_alt - prev_alt)/3.281)

    # Find top 15 users who have gained the most altitude meters
    top_15 = []
    for i in range(15):
        most_alt_gained = (max(users_dict, key=users_dict.get), max(users_dict.values()))
        top_15.append(most_alt_gained)
        del users_dict[max(users_dict, key=users_dict.get)]

    print(tabulate(top_15, headers=["user_id", "total altitude meters gained"]))

def query12(program):
    print("--- Query 12 ---\n")

    program.execute_sql_query("""WITH MostFrequent AS (
            SELECT
                User.id AS user_id,
                transportation_mode AS most_used_transportation_mode,
                RANK() OVER (PARTITION BY User.id ORDER BY COUNT(*) DESC) AS most_frequent
            FROM User
            INNER JOIN Activity ON User.id = Activity.user_id
            WHERE transportation_mode IS NOT NULL
            GROUP BY User.id, transportation_mode
        )
        SELECT user_id, most_used_transportation_mode
        FROM MostFrequent
        WHERE most_frequent = 1
    """)

def main():
    program = None
    try:
        program = task2()
        query3(program)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
