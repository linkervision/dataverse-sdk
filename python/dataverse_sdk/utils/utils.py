from os import listdir
from os.path import isfile, join

import requests


def get_filepaths(path: str) -> list[str]:
    dirs: list[str] = listdir(path)
    all_files = []
    for dir_ in dirs:
        new_path = join(path, dir_)
        if isfile(new_path):
            all_files.append(new_path)
        else:
            all_files.extend(get_filepaths(new_path))
    return all_files


def download_file_from_response(response: requests.models.Response, save_path: str):
    with open(save_path, "wb") as file:
        for chunk in response.iter_content(1024 * 1024):
            if chunk:
                file.write(chunk)
                file.flush()
