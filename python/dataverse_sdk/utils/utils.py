from pathlib import Path
from typing import Optional, Set

import requests
from tqdm import tqdm

from dataverse_sdk.schemas.common import (
    AnnotationFormat,
    DatasetType,
    OntologyImageType,
    OntologyPcdType,
    ProjectCreateDatasetConfig,
)

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
        with (
            open(save_path, "wb") as file,
            tqdm(
                total=total_size, unit="B", unit_scale=True, desc="Downloading"
            ) as progress_bar,
        ):
            # Write the file in chunks to avoid using too much memory
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    file.write(chunk)
                    progress_bar.update(len(chunk))  # Update progress bar

        print(f"\nFile downloaded successfully and saved to: {save_path}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")


def chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def float_in_range(min_value: float, max_value: float):
    """
    Returns a validator function for argparse to validate floats within a range.
    """
    import argparse

    def validator(value):
        f_value = float(value)
        if not (min_value <= f_value <= max_value):
            raise argparse.ArgumentTypeError(
                f"value {f_value} not in range [{min_value}, {max_value}]"
            )
        return f_value

    return validator


class CreateDatasetValidator:
    @staticmethod
    def is_vision_ai_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        VisionAI format is supported for almost all cases except VQA
        """
        if config.image_type == OntologyImageType.VQA:
            return False
        return True

    @staticmethod
    def is_kitti_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        KITTI format requirements:
        - Exactly 1 LIDAR sensor
        - 0 or 1 camera (raw: 0 cameras, annotated: 0-1 cameras)
        - Non-sequential only
        - If annotated: cuboid PCD type required
        - If annotated with camera: bbox image type required, no attributes
        """
        if config.sensor_counts.lidar != 1:
            return False

        camera_count = config.sensor_counts.camera
        if camera_count > 1 or (
            config.dataset_type == DatasetType.RAW_DATA and camera_count > 0
        ):
            return False

        if config.is_sequential:
            return False

        if config.dataset_type == DatasetType.ANNOTATED_DATA:
            if config.pcd_type != OntologyPcdType.CUBOID:
                return False
            if (
                camera_count == 1
                and config.image_type != OntologyImageType._2D_BOUNDING_BOX
            ):
                return False
            if config.has_attribute:
                return False

        return True

    @staticmethod
    def is_coco_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        COCO format requirements:
        - Annotated data only
        - Exactly 1 camera, 0 LIDARs
        - Bounding box image type
        - No attributes
        - Non-sequential only
        """
        if config.dataset_type == DatasetType.RAW_DATA:
            return False

        if config.sensor_counts.camera != 1 or config.sensor_counts.lidar != 0:
            return False

        if config.image_type != OntologyImageType._2D_BOUNDING_BOX:
            return False

        if config.has_attribute:
            return False

        if config.is_sequential:
            return False

        return True

    @staticmethod
    def is_image_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        Image format requirements:
        - Raw data only
        - Exactly 1 camera, 0 LIDARs
        - Non-sequential only
        """
        if config.dataset_type == "annotation":
            return False

        if config.sensor_counts.camera != 1 or config.sensor_counts.lidar != 0:
            return False

        if config.is_sequential:
            return False

        return True

    @staticmethod
    def is_yolo_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        YOLO format requirements:
        - Annotated data only
        - Exactly 1 camera, 0 LIDARs
        - Bounding box image type
        - No attributes
        - Non-sequential only
        """
        if config.dataset_type == DatasetType.RAW_DATA:
            return False

        if config.sensor_counts.camera != 1 or config.sensor_counts.lidar != 0:
            return False

        if config.image_type != OntologyImageType._2D_BOUNDING_BOX:
            return False

        if config.has_attribute:
            return False

        if config.is_sequential:
            return False

        return True

    @staticmethod
    def is_video_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        Video format requirements:
        - Raw data only
        - Exactly 1 camera, 0 LIDARs
        - Sequential only
        """
        if config.dataset_type != DatasetType.RAW_DATA:
            return False

        if config.sensor_counts.camera != 1 or config.sensor_counts.lidar != 0:
            return False

        if not config.is_sequential:
            return False

        return True

    @staticmethod
    def is_vlm_format_supported(
        config: ProjectCreateDatasetConfig,
    ) -> bool:
        """
        VLM format requirements:
        - Annotated data only
        - Exactly 1 camera, 0 LIDARs
        - VQA image type
        """
        if config.dataset_type == DatasetType.RAW_DATA:
            return False

        if config.sensor_counts.camera != 1 or config.sensor_counts.lidar != 0:
            return False

        if config.image_type != OntologyImageType.VQA:
            return False

        return True

    @classmethod
    def get_supported_formats(
        cls, config: ProjectCreateDatasetConfig
    ) -> list[AnnotationFormat]:
        supported_formats = []

        if cls.is_vision_ai_format_supported(config):
            supported_formats.append(AnnotationFormat.VISION_AI)

        if cls.is_kitti_format_supported(config):
            supported_formats.append(AnnotationFormat.KITTI)

        if cls.is_coco_format_supported(config):
            supported_formats.append(AnnotationFormat.COCO)

        if cls.is_image_format_supported(config):
            supported_formats.append(AnnotationFormat.IMAGE)

        if cls.is_yolo_format_supported(config):
            supported_formats.append(AnnotationFormat.YOLO)

        if cls.is_video_format_supported(config):
            supported_formats.append(AnnotationFormat.VIDEO)

        if cls.is_vlm_format_supported(config):
            supported_formats.append(AnnotationFormat.VLM)

        return supported_formats


def validate_before_create_dataset(
    config: ProjectCreateDatasetConfig,
) -> tuple[bool, Optional[str]]:
    supported_formats = CreateDatasetValidator.get_supported_formats(config)

    try:
        requested_format = AnnotationFormat(config.annotation_format)
    except ValueError:
        return False, f"Unknown annotation format: {config.annotation_format}"

    if requested_format in supported_formats:
        return True, None

    error_lines = [
        "",
        "❌ The input arguments in create_dataset are not compatible with the project:",
        f"  • Dataset type: {config.dataset_type}",
        f"  • Sensors: {config.sensor_counts.camera} camera(s), {config.sensor_counts.lidar} lidar(s)",
        f"  • Annotation format: {config.annotation_format}",
        f"  • Image type: {config.image_type or 'N/A'}",
        f"  • PCD type: {config.pcd_type or 'N/A'}",
        f"  • Has attributes: {config.has_attribute}",
        f"  • Sequential: {config.is_sequential}",
        "",
        "Please adjust your input arguments and try again.",
    ]

    return False, "\n".join(error_lines)
