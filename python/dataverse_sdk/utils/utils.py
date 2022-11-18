from os import listdir
from os.path import isfile, join

    
def get_file_recursive(path) -> list[str]:
    dirs: list[str] = listdir(path)
    all_files = []
    for _dir in dirs:
        new_path = join(path, _dir)
        if isfile(new_path):
            all_files.append(new_path)
        else:
            all_files.extend(get_file_recursive(new_path))
    return all_files