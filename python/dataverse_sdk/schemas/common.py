from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ..constants import BaseEnumMeta


class AttributeType(str, Enum, metaclass=BaseEnumMeta):
    BOOLEAN = "boolean"
    OPTION = "option"
    NUMBER = "number"
    TEXT = "text"


class OntologyImageType(str, Enum, metaclass=BaseEnumMeta):
    _2D_BOUNDING_BOX = "2d_bounding_box"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"
    INSTANCE_SEGMENTATION = "instance_segmentation"
    CLASSIFICATION = "classification"
    POINT = "point"
    POLYGON = "polygon"
    POLYLINE = "polyline"
    VQA = "vqa"


class OntologyPcdType(str, Enum, metaclass=BaseEnumMeta):
    CUBOID = "cuboid"


class SensorType(str, Enum, metaclass=BaseEnumMeta):
    CAMERA = "camera"
    LIDAR = "lidar"


class AnnotationFormat(str, Enum, metaclass=BaseEnumMeta):
    VISION_AI = "vision_ai"
    COCO = "coco"
    BDDP = "bddp"
    IMAGE = "image"
    KITTI = "kitti"
    YOLO = "yolo"
    VIDEO = "video"
    VLM = "vlm"


class DatasetType(str, Enum, metaclass=BaseEnumMeta):
    ANNOTATED_DATA = "annotated_data"
    RAW_DATA = "raw_data"


class DatasetStatus(str, Enum, metaclass=BaseEnumMeta):
    PROCESSING = "processing"
    FAIL = "fail"
    READY = "ready"


class DataSource(str, Enum, metaclass=BaseEnumMeta):
    AWS = "aws"
    LOCAL = "local"
    SDK = "sdk"
    EXISTING_DATASETS = "existing_datasets"
    EXISTING_DATASLICES = "existing_dataslices"
    DATA_GENERATION = "data_generation"
    PRE_IMPORT = "pre_import"


class ConvertModelFileType(str, Enum, metaclass=BaseEnumMeta):
    """Which stored artifact of a convert record to download."""

    TRITON = "triton"
    MODEL = "model"
    RAW_ONNX = "raw_onnx"
    CALIB_CACHE = "calib_cache"


# Fallback local filename per artifact, used when no save_path is given. The MODEL
# and RAW_ONNX artifacts vary with the convert format (onnx vs trt), so these are
# convenience defaults rather than a promise about the file's contents.
CONVERT_MODEL_FILE_DEFAULT_SAVE_PATHS: dict[ConvertModelFileType, str] = {
    ConvertModelFileType.TRITON: "./triton.zip",
    ConvertModelFileType.MODEL: "./model.onnx",
    ConvertModelFileType.RAW_ONNX: "./raw.onnx",
    ConvertModelFileType.CALIB_CACHE: "./trt_int8_calib.cache",
}


class ModelStructure(str, Enum, metaclass=BaseEnumMeta):
    """Architecture to build uploaded custom model weights into."""

    YOLOV9_C = "yolov9-c"
    YOLOV9_E = "yolov9-e"
    YOLOV9_S = "yolov9-s"
    DFINE_N = "dfine-n"
    DFINE_S = "dfine-s"
    DFINE_M = "dfine-m"
    DFINE_L = "dfine-l"
    DFINE_X = "dfine-x"


@dataclass
class SensorCounts:
    camera: int = 0
    lidar: int = 0


@dataclass
class ProjectCreateDatasetConfig:
    annotation_format: AnnotationFormat
    dataset_type: DatasetType
    sensor_counts: SensorCounts
    is_sequential: bool
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None
    has_attribute: bool = False
