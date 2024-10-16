from os import listdir
from os.path import isfile, join

import requests

IMAGE_SUPPORTED_FORMAT = {
    "jpeg",
    "jpg",
    "png",
    "bmp",
}
CUBOID_SUPPORTED_FORMAT = {"pcd"}


def get_filepaths(path: str) -> list[str]:
    dirs: list[str] = listdir(path)
    all_files = []
    for dir_ in dirs:
        new_path = join(path, dir_)
        if isfile(new_path):
            if new_path.split(".")[
                -1
            ] in IMAGE_SUPPORTED_FORMAT | CUBOID_SUPPORTED_FORMAT | {"txt", "json"}:
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
