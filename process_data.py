import os

DATA_PATH = 'dataset/Data/'
labels_txt = 'dataset/labeled_ids.txt'


def get_user_labeled_ids(path):
    """
    Returns a list of user ids that have labeled their data.
    :param path: path to the labeled_ids.txt file
    :return: List of users.
    """
    with open(path, 'r') as f:
        labels = f.readlines()
        return [x.strip() for x in labels]


def clean_data(filepath):
    with open(filepath, 'r') as file:
        data = file.readlines()
        cleaned_data = data[6:]
        if len(cleaned_data) > 2500:
            return
        return cleaned_data[6:]


def get_data(path):
    users = get_user_labeled_ids(labels_txt)
    files = {}
    for user_id in os.listdir(DATA_PATH):
        files[user_id] = []
        user_dir_path = os.path.join(DATA_PATH, user_id)
        trajectory_path = os.path.join(user_dir_path, 'Trajectory')
        for file in os.listdir(trajectory_path):
            file_path = os.path.join(trajectory_path, file)
            if file_path.endswith('.plt'):
                clean_file = clean_data(file_path)
                if clean_file:
                    print(f"Length of clean file: {len(clean_file)}")
                    file_name = (file_path.split('\\')[-1])
                    files[user_id] = [file_name, clean_file]
    return files


def main():
    files = get_data(labels_txt)
    print(files)


main()
