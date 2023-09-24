import json
import DbConnector
user_tables = []
user_activities = []
track_points = []
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
                'start_time': activity['start_time'],
                'end_time': activity['end_time'],
            })
            for track_point in activity['data']:
                point = track_point.split(',')
                lat = float(point[0])
                lon = float(point[1])
                altitude = int(float(point[3]))
                date_days = float(point[4])
                date_time = point[5]
                i = 0
                track_points.append({
                    'id': i,
                    'activity_id': activity['file_name'].replace('.plt', '') + user_id,  # maps to id field in activity
                    'lat': lat,
                    'lon': lon,
                    'altitude': altitude,
                    'date_days': date_days,
                    'date_time': date_time,
                })
                i += 1



# SQL Statements her

conn = DbConnector.DbConnector()