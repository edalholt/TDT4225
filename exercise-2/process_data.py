import json
import os

DATA_PATH = 'dataset/Data/'
labels_txt = 'dataset/labeled_ids.txt'


def get_user_labeled_ids(path: str) -> list:
    """
    Returns a list of user ids that have labeled their data.
    :param path: path to the labeled_ids.txt file
    :return: List of users.
    """
    with open(path, 'r') as f:
        labels = f.readlines()
        return [x.strip() for x in labels]


def clean_data(filepath: str) -> dict:
    """
    Cleans the data by removing the first 6 lines of metadata and checking if the file is over 2500 lines.
    Args:
        filepath: Path to the file to the plt file to clean.

    Returns: Dictionary of the cleaned data, the start and end time of the file.
    """

    with open(filepath, 'r') as file:
        data = file.readlines()
        cleaned_data = data[6:]
        if len(cleaned_data) > 2500:
            return
        start_time = ' '.join(cleaned_data[0].split(',')[5:7]).replace("/", "-")
        end_time = ' '.join(cleaned_data[-1].split(',')[5:7]).replace("/", "-")

        return {
            'data': cleaned_data,
            'start_time': start_time,
            'end_time': end_time
        }


def convert_filename_to_datetime_format(filename: str) -> str:
    """
    Convert the filename format for matching.
    Example "20070804033032" --> "2007-08-04 03:30:32".
    """
    year = filename[:4]
    month = filename[4:6]
    day = filename[6:8]
    hour = filename[8:10]
    minute = filename[10:12]
    second = filename[12:14]
    return f"{year}-{month}-{day} {hour}:{minute}:{second}"


def match_labels(label_path: str, file_name: str, end_time: str):
    """
    Finds the correct transportation modes for the files that have labels.
    Args:
        label_path: Path to the labels.txt file which some users have.
        file_name:  The name of the file to match.
        end_time:  The end time of the file to match.

    Returns:

    """
    file_name = convert_filename_to_datetime_format(file_name.replace('.plt', ''))
    with open(label_path, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:
            parts = line.strip().split('\t')
            label_start_time = ' '.join(parts[0:1]).replace("/", "-")
            label_end_time = ' '.join(parts[1:2]).replace("/", "-")
            mode = parts[2:3][0]
            matches = []
            if label_start_time == file_name:
                if label_end_time == end_time:
                    print(f"Matched {file_name} with {label_start_time} and {label_end_time} with mode {mode}")
                    return mode

        return None


def get_data(path_to_labels: str, dataset_path: str) -> dict:
    """
    Args:
        path_to_labels: Path to the labels.txt file.
        dataset_path:  Path to the dataset.

    Returns: Dictionary of users and their activities.
    """
    users = get_user_labeled_ids(path_to_labels)
    files = {}

    for user_id in os.listdir(dataset_path):
        files[user_id] = []
        user_dir_path = os.path.join(dataset_path, user_id)
        trajectory_path = os.path.join(user_dir_path, 'Trajectory')
        i = 0
        for file in os.listdir(trajectory_path):
            file_path = os.path.join(trajectory_path, file)
            file_name = os.path.basename(file_path)
            if file_path.endswith('.plt'):
                result = clean_data(file_path)
                if result:
                    clean_file = result['data']
                    start_time = result['start_time']
                    end_time = result['end_time'].strip()

                    mode = None
                    if user_id in users:
                        label_path = os.path.join(user_dir_path, 'labels.txt')
                        mode = match_labels(label_path, file_name, end_time)

                    files[user_id].append({
                        'file_name': file_name,
                        'start_time': start_time,
                        'end_time': end_time,
                        'mode': mode,
                        'data': clean_file,
                    })
            i += 1
        print(f"Finished cleaning data for user {user_id}. Added {i} files")

    return files


def main():
    files = get_data(labels_txt, DATA_PATH)
    with open('output.json', 'w') as outfile:
        json.dump(files, outfile)


main()
