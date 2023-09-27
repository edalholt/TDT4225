from DbConnector import DbConnector
from tabulate import tabulate


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

def main():
    program = None
    try:
        program = task2()
        program.show_tables()
        #program.execute_sql_query("SELECT user_id, COUNT(user_id) AS 'Number of activities' FROM Activity GROUP BY user_id ORDER BY COUNT(user_id) DESC LIMIT 15")
        #program.execute_sql_query("SELECT user_id, start_date_time, end_date_time, COUNT(*) as count FROM Activity GROUP BY user_id, start_date_time, end_date_time HAVING count > 1")
        #program.execute_sql_query("SELECT * FROM TrackPoint LIMIT 10")
        program.execute_sql_query("SELECT altitude, Activity.id, TrackPoint.id FROM Activity INNER JOIN TrackPoint ON Activity.id=TrackPoint.activity_id LIMIT 50")
        #program.execute_sql_query("SELECT altitude, TrackPoint.id, activity_id, user_id FROM TrackPoint INNER JOIN Activity ON TrackPoint.activity_id=Activity.id LIMIT 10")
        #program.execute_sql_query("SELECT COUNT(Activity.id) FROM Activity")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
