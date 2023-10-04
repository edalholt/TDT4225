from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd


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

    def execute_sql_no_print(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

def query3(program):
    program.execute_sql_query("""SELECT user_id, COUNT(user_id) AS 'Number of activities' 
    FROM Activity 
    GROUP BY user_id 
    ORDER BY COUNT(user_id) DESC 
    LIMIT 15""")

def query6(program):
    program.execute_sql_query("""SELECT user_id, start_date_time, end_date_time, COUNT(*) as count 
    FROM Activity 
    GROUP BY user_id, start_date_time, end_date_time 
    HAVING count > 1""")

def query9(program):
    rows = program.execute_sql_no_print("""SELECT altitude, Activity.id AS activity_id, 
    TrackPoint.id AS tp_id, Activity.user_id AS user_id 
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
            users_dict[user_id] = users_dict[user_id] + (current_alt - prev_alt)

    # Find top 15 users who have gained the most altitude meters
    top_15 = []
    for i in range(15):
        most_alt_gained = (max(users_dict, key=users_dict.get), (max(users_dict.values())/3.281))
        top_15.append(most_alt_gained)
        del users_dict[max(users_dict, key=users_dict.get)]

    print(tabulate(top_15, headers=["user_id", "total altitude meters gained"]))

def query12(program):
    rows = program.execute_sql_query("""WITH MostFrequent AS (
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
        WHERE most_frequent = 1;
    """)

def main():
    program = None
    try:
        program = task2()
        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
