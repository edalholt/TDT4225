import json
from DbConnector import DbConnector
import time
import math

user_tables = []
user_activities = []
track_points = []


class InsertData:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # Splits a list into chunks of a given size for batch insert
    def split_list_to_chunks(list, chunk_size):
        return [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]

    def load_data(self):
        with open('output.json', 'r') as json_file:
            data = json.load(json_file)
            for user_id, activities in data.items():
                has_labels = any(activity['mode'] for activity in activities)
                user_tables.append({
                    'id': user_id,
                    'has_labels': has_labels,
                })
                for activity in activities:
                    user_activities.append({
                        'id': activity['file_name'].replace('.plt', '') + user_id,
                        'user_id': user_id,
                        'transportation_mode': activity['mode'],
                        'start_date_time': activity['start_time'],
                        'end_date_time': activity['end_time'],
                    })
                    for track_point in activity['data']:
                        point = track_point.split(',')
                        lat = float(point[0])
                        lon = float(point[1])
                        altitude = int(float(point[3]))
                        date_days = float(point[4])
                        date_time = point[5] + ' ' + point[6].strip()
                        track_points.append({
                            'activity_id': activity['file_name'].replace('.plt', '') + user_id,
                            'lat': lat,
                            'lon': lon,
                            'altitude': altitude,
                            'date_days': date_days,
                            'date_time': date_time,
                        })

    def insert_user_data(self):
        insert_query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
        data_to_insert = [(item["id"], item["has_labels"]) for item in user_tables]
        self.cursor.executemany(insert_query, data_to_insert)
        self.db_connection.commit()

    def insert_activity_data(self):
        insert_query = "INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)"
        data_to_insert = [(item["id"], item["user_id"], item["transportation_mode"], item["start_date_time"], item["end_date_time"]) for item in user_activities]
        self.cursor.executemany(insert_query, data_to_insert)
        self.db_connection.commit()

    def insert_track_point_data(self):
        insert_query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"

        # Split the list into chunks of 1000 for batch insert
        track_points_chunks = InsertData.split_list_to_chunks(track_points, 15000)
        number_of_chunks = len(track_points_chunks)
        for chunk in track_points_chunks:
            print(f"Inserting chunk {track_points_chunks.index(chunk) + 1} of {number_of_chunks}")
            data_to_insert = [(item["activity_id"], item["lat"], item["lon"], item["altitude"], item["date_days"], item["date_time"]) for item in chunk]
            self.cursor.executemany(insert_query, data_to_insert)
            self.db_connection.commit()
            print(f"Inserted chunk {track_points_chunks.index(chunk) + 1} of {number_of_chunks} successfully")

def main():
    program = None
    try:
        program = InsertData()
        print("Loading data...")
        start_time = time.time()
        program.load_data()
        print("Loaded data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Inserting user data...")
        start_time = time.time()
        program.insert_user_data()
        print("Inserted user data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Inserting activity data...")
        start_time = time.time()
        program.insert_activity_data()
        print("Inserted activity data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Inserting track point data...")
        start_time = time.time()
        program.insert_track_point_data()
        print("Inserted track point data in " + str(math.trunc(time.time() - start_time)) + " seconds")

        print("Done!")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
