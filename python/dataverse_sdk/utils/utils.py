from pathlib import Path
from typing import Set

import requests
from tqdm import tqdm

IMAGE_SUPPORTED_FORMAT = {
    "jpeg",
    "jpg",
    "png",
    "bmp",
}
CUBOID_SUPPORTED_FORMAT = {"pcd"}


def get_filepaths(directory: str) -> list[str]:
    SUPPORTED_FORMATS: Set[str] = (
        IMAGE_SUPPORTED_FORMAT | CUBOID_SUPPORTED_FORMAT | {"txt", "json"}
    )

    directory = Path(directory)
    all_files = []

    for file_path in directory.rglob("*"):
        if (
            file_path.is_file()
            and file_path.suffix.lower().lstrip(".") in SUPPORTED_FORMATS
        ):
            all_files.append(str(file_path))

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
        with open(save_path, "wb") as file, tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Downloading"
        ) as progress_bar:
            # Write the file in chunks to avoid using too much memory
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    file.write(chunk)
                    progress_bar.update(len(chunk))  # Update progress bar

        print(f"\nFile downloaded successfully and saved to: {save_path}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
