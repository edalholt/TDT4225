from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def reset_database(self):
        # Drop tables if they exist
        self.drop_table("TrackPoint")
        self.drop_table("Activity")
        self.drop_table("User")

    def create_user_schema(self):
        # Create new table if not exist for User
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(30) NOT NULL PRIMARY KEY)
                """
        self.cursor.execute(query % ("User"))

        # User has a ID (string), and "has_labels" (boolean)
        self.cursor.execute(
            "ALTER TABLE User ADD COLUMN has_labels BOOLEAN NOT NULL DEFAULT FALSE")
        
        self.db_connection.commit()
        print("User table created")
        
    def create_activity_schema(self):
        # Create new table if not exist for Activity
        query = """CREATE TABLE IF NOT EXISTS %s (
            id VARCHAR(30) NOT NULL PRIMARY KEY)
        """
        self.cursor.execute(query % ("Activity"))
        
        # Activity has a ID (string), foreign key to user, transportation_mode (string), start_date_time (datetime), end_date_time (datetime)
        self.cursor.execute(
            "ALTER TABLE Activity ADD COLUMN user_id VARCHAR(30) NOT NULL")
        # user_id has cascade on delete, meaning that if a user is deleted, all activities belonging to that user will also be deleted.
        self.cursor.execute(
            "ALTER TABLE Activity ADD FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE")
        
        self.cursor.execute(
            "ALTER TABLE Activity ADD COLUMN transportation_mode VARCHAR(30)")
        self.cursor.execute(
            "ALTER TABLE Activity ADD COLUMN start_date_time DATETIME")
        self.cursor.execute(
            "ALTER TABLE Activity ADD COLUMN end_date_time DATETIME")
        
        self.db_connection.commit()
        print("Activity table created")
        
    def create_trackpoint_schema(self):
        # Create new table if not exist for TrackPoint
        query = """CREATE TABLE IF NOT EXISTS %s (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY)
                """
        self.cursor.execute(query % ("TrackPoint"))
        
        # TrackPoint has a ID (string), activity_id (int), lat (float), lon (float), altitude (float), date_days (float), date_time (datetime)
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD COLUMN activity_id VARCHAR(30) NOT NULL")
        # activity_id has cascade on delete, meaning that if an activity is deleted, all trackpoints belonging to that activity will also be deleted.
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE")
        
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD COLUMN lat FLOAT")
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD COLUMN lon FLOAT")
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD COLUMN altitude INT")
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD COLUMN date_days FLOAT")
        self.cursor.execute(
            "ALTER TABLE TrackPoint ADD COLUMN date_time DATETIME")
        
        self.db_connection.commit()
        print("TrackPoint table created")

    def initialize_database(self):
        self.reset_database()
        self.create_user_schema()
        self.create_activity_schema()
        self.create_trackpoint_schema()

    def insert_data(self, table_name, data):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

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

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExampleProgram()
        program.initialize_database()
        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
