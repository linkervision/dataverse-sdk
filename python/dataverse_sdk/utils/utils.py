from os import listdir
from os.path import isfile, join


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
