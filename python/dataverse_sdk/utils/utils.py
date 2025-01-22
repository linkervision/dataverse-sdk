from os import listdir
from os.path import isfile, join

import requests
from tqdm import tqdm

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


def download_file_from_url(url: str, save_path: str):
    """Downloads a file from a URL and saves it to the specified path.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    save_path : str
        The local file path where the file will be saved.

    """
    try:
        # Send a HTTP request to the URL
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the request was successful

        # Get the total file size from headers (if available)
        total_size = int(response.headers.get("content-length", 0))

        # Initialize tqdm progress bar
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Downloading"
        ) as progress_bar:
            # Write the file in chunks to avoid using too much memory
            with open(save_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
                        progress_bar.update(len(chunk))  # Update progress bar

        print(f"\nFile downloaded successfully and saved to: {save_path}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
