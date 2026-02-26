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
    Azure = "azure"
    AWS = "aws"
    LOCAL = "local"
    SDK = "sdk"


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
