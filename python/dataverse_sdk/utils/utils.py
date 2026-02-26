from pathlib import Path
from typing import Optional, Set

import requests
from tqdm import tqdm

from dataverse_sdk.schemas.common import (
    AnnotationFormat,
    DatasetType,
    ImportDataSetDataStructureConditions,
    OntologyImageType,
    OntologyPcdType,
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


class AnnotationFormatValidator:
    @staticmethod
    def is_vision_ai_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        VisionAI format is supported for almost all cases except VQA
        """
        if conditions.image_type == OntologyImageType.VQA:
            return False
        return True

    @staticmethod
    def is_kitti_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        KITTI format requirements:
        - Exactly 1 LIDAR sensor
        - 0 or 1 camera (raw: 0 cameras, annotated: 0-1 cameras)
        - Non-sequential only
        - If annotated: cuboid PCD type required
        - If annotated with camera: bbox image type required, no attributes
        """
        if conditions.sensor_counts.lidar != 1:
            return False

        camera_count = conditions.sensor_counts.camera
        if camera_count > 1 or (
            conditions.dataset_type == DatasetType.RAW_DATA and camera_count > 0
        ):
            return False

        if conditions.is_sequential:
            return False

        if conditions.dataset_type == DatasetType.ANNOTATED_DATA:
            if conditions.pcd_type != OntologyPcdType.CUBOID:
                return False
            if (
                camera_count == 1
                and conditions.image_type != OntologyImageType._2D_BOUNDING_BOX
            ):
                return False
            if conditions.has_attribute:
                return False

        return True

    @staticmethod
    def is_coco_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        COCO format requirements:
        - Annotated data only
        - Exactly 1 camera, 0 LIDARs
        - Bounding box image type
        - No attributes
        - Non-sequential only
        """
        if conditions.dataset_type == DatasetType.RAW_DATA:
            return False

        if conditions.sensor_counts.camera != 1 or conditions.sensor_counts.lidar != 0:
            return False

        if conditions.image_type != OntologyImageType._2D_BOUNDING_BOX:
            return False

        if conditions.has_attribute:
            return False

        if conditions.is_sequential:
            return False

        return True

    @staticmethod
    def is_image_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        Image format requirements:
        - Raw data only
        - Exactly 1 camera, 0 LIDARs
        - Non-sequential only
        """
        if conditions.dataset_type == "annotation":
            return False

        if conditions.sensor_counts.camera != 1 or conditions.sensor_counts.lidar != 0:
            return False

        if conditions.is_sequential:
            return False

        return True

    @staticmethod
    def is_yolo_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        YOLO format requirements:
        - Annotated data only
        - Exactly 1 camera, 0 LIDARs
        - Bounding box image type
        - No attributes
        - Non-sequential only
        """
        if conditions.dataset_type == DatasetType.RAW_DATA:
            return False

        if conditions.sensor_counts.camera != 1 or conditions.sensor_counts.lidar != 0:
            return False

        if conditions.image_type != OntologyImageType._2D_BOUNDING_BOX:
            return False

        if conditions.has_attribute:
            return False

        if conditions.is_sequential:
            return False

        return True

    @staticmethod
    def is_video_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        Video format requirements:
        - Raw data only
        - Exactly 1 camera, 0 LIDARs
        - Sequential only
        """
        if conditions.dataset_type != DatasetType.RAW_DATA:
            return False

        if conditions.sensor_counts.camera != 1 or conditions.sensor_counts.lidar != 0:
            return False

        if not conditions.is_sequential:
            return False

        return True

    @staticmethod
    def is_vlm_format_supported(
        conditions: ImportDataSetDataStructureConditions,
    ) -> bool:
        """
        VLM format requirements:
        - Annotated data only
        - Exactly 1 camera, 0 LIDARs
        - VQA image type
        """
        if conditions.dataset_type == DatasetType.RAW_DATA:
            return False

        if conditions.sensor_counts.camera != 1 or conditions.sensor_counts.lidar != 0:
            return False

        if conditions.image_type != OntologyImageType.VQA:
            return False

        return True

    @classmethod
    def get_supported_formats(
        cls, conditions: ImportDataSetDataStructureConditions
    ) -> list[AnnotationFormat]:
        supported_formats = []

        if cls.is_vision_ai_format_supported(conditions):
            supported_formats.append(AnnotationFormat.VISION_AI)

        if cls.is_kitti_format_supported(conditions):
            supported_formats.append(AnnotationFormat.KITTI)

        if cls.is_coco_format_supported(conditions):
            supported_formats.append(AnnotationFormat.COCO)

        if cls.is_image_format_supported(conditions):
            supported_formats.append(AnnotationFormat.IMAGE)

        if cls.is_yolo_format_supported(conditions):
            supported_formats.append(AnnotationFormat.YOLO)

        if cls.is_video_format_supported(conditions):
            supported_formats.append(AnnotationFormat.VIDEO)

        if cls.is_vlm_format_supported(conditions):
            supported_formats.append(AnnotationFormat.VLM)

        return supported_formats


def validate_annotation_format(
    annotation_format: AnnotationFormat,
    conditions: ImportDataSetDataStructureConditions,
) -> tuple[bool, Optional[str]]:
    supported_formats = AnnotationFormatValidator.get_supported_formats(conditions)

    try:
        requested_format = AnnotationFormat(annotation_format)
    except ValueError:
        return False, f"Unknown annotation format: {annotation_format}"

    if requested_format in supported_formats:
        return True, None

    supported_names = [fmt.value for fmt in supported_formats]

    error_lines = [
        "",
        f"Annotation format '{annotation_format}' is not compatible with your input arguments.",
        "",
        f"💡Supported format(s) with current arguments: {', '.join(supported_names) if supported_names else 'none'}",
        "",
        "Current arguments:",
        f"  • Type: {conditions.dataset_type}",
        f"  • Sensors: {conditions.sensor_counts.camera} camera(s), {conditions.sensor_counts.lidar} lidar(s)",
        f"  • Sequential: {conditions.is_sequential}",
        f"  • Image type: {conditions.image_type or 'N/A'}",
        f"  • PCD type: {conditions.pcd_type or 'N/A'}",
        f"  • Has attributes: {conditions.has_attribute}",
        "",
        "Please adjust your input arguments or choose a different annotation_format and try again.",
    ]

    return False, "\n".join(error_lines)
