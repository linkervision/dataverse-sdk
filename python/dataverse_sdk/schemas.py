from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, validator
import re

# ======================Attribute Related Start======================
class AttributeType(str, Enum):
    BOOLEAN = "boolean"
    OPTION = "option"
    NUMBER = "number"
    TEXT = "text"


class AttributeOption(BaseModel):
    id: Optional[int] = None
    value: Union[str, float, int, bool]


class Attribute(BaseModel):
    id: Optional[int] = None
    name: str
    option_data: Optional[list[AttributeOption]] = None
    type: AttributeType

    @validator("type")
    def option_data_validator(cls, value, values, **kwargs):
        if value == AttributeType.OPTION and not values.get("option_data"):
            raise ValueError(
                "Need to assign value for `option_data` "
                + "if the Attribute type is option"
            )
        return value


# ======================Attribute Related End======================


# ======================Ontology Related Start======================


class OntologyImageType(str, Enum):
    _2D_BOUNDING_BOX = "2d_bounding_box"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"
    CLASSIFICATION = "classification"
    POINT = "point"
    POLYGON = "polygon"
    POLYLINE = "polyline"


class OntologyPcdType(str, Enum):
    CUBOID = "cuboid"


class OntologyClass(BaseModel):
    id: Optional[int] = None
    name: str
    color: str
    rank: int
    attribute_data: Optional[list[Attribute]] = None

    @validator("color", each_item=True)
    def color_validator(cls, value):
        if not value.startswith("#") \
            or not re.search("^[a-zA-Z0-9]+$",value.lstrip("#")):
            raise ValueError(
                f"Color field needs starts with `#` and has 6 digits behind it, get : {value}"
            )
        return value


class Ontology(BaseModel):
    id: Optional[int] = None
    name: str
    image_type: OntologyImageType
    pcd_type: Optional[OntologyPcdType] = None
    ontology_classes_data: Optional[list[OntologyClass]] = None


# ======================Ontology Related End======================


# ======================Sensor Related Start======================


class SensorType(str, Enum):
    CAMERA = "camera"
    LIDAR = "lidar"


class Sensor(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType


# ======================Sensor Related End======================


# ======================Project Related Start======================
class Project(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology_data: Ontology
    sensor_data: list[Sensor]


# ======================Project Related End======================
