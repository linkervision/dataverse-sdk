from enum import Enum
from typing import Optional

from pydantic import BaseModel


class SensorType(str, Enum):
    CAMERA = "camera"
    LIDAR = "lidar"


class OntologyImageType(str, Enum):
    _2D_BOUNDING_BOX = "2d_bounding_box"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"
    CLASSIFICATION = "classification"
    POINT = "point"
    POLYGON = "polygon"
    POLYLINE = "polyline"


class OntologyPcdType(str, Enum):
    CUBOID = "cuboid"


class Sensor(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType


class Ontology(BaseModel):
    id: Optional[int] = None
    name: str
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None


class Project(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology: Ontology
    sensors: list[Sensor]


# TODO: TBD
