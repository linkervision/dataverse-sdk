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
