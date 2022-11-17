from enum import Enum


class AttributeType(str, Enum):
    BOOLEAN = "boolean"
    OPTION = "option"
    NUMBER = "number"
    TEXT = "text"


class OntologyImageType(str, Enum):
    _2D_BOUNDING_BOX = "2d_bounding_box"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"
    CLASSIFICATION = "classification"
    POINT = "point"
    POLYGON = "polygon"
    POLYLINE = "polyline"


class OntologyPcdType(str, Enum):
    CUBOID = "cuboid"


class SensorType(str, Enum):
    CAMERA = "camera"
    LIDAR = "lidar"


class AnnotationFormat(str, Enum):
    VISION_AI = "vision_ai"


class DatasetType(str, Enum):
    ANNOTATED_DATA = "annotated_data"
    RAW_DATA = "raw_data"


class DatasetStatus(str, Enum):
    PROCESSING = "processing"
    FAIL = "fail"
    READY = "ready"
