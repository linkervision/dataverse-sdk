import re
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, validator


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


class BaseOntologySchema(BaseModel):
    id: Optional[int] = None
    name: str
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None


class BaseProjectSchema(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None


class BaseSensorSchema(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType


class BaseOntologyClassSchema(BaseModel):
    id: Optional[int] = None
    name: str
    color: str
    rank: int

    @validator("color", each_item=True)
    def color_validator(cls, value):
        if not value.startswith("#") or not re.search(
            r"\b[a-zA-Z0-9]{6}\b", value.lstrip("#")
        ):
            raise ValueError(
                f"Color field needs starts with `#` and has 6 digits behind it, get : {value}"
            )
        return value


class BaseAttributeOptionSchema(BaseModel):
    id: Optional[int] = None
    value: Union[str, float, int, bool]


class BaseAttributeSchema(BaseModel):
    id: Optional[int] = None
    name: str
