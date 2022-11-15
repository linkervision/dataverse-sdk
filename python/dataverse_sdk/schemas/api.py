import re
from typing import Optional, Union

from pydantic import BaseModel, validator

from .common import AttributeType, OntologyImageType, OntologyPcdType, SensorType


class AttributeOptionAPISchema(BaseModel):
    id: Optional[int] = None
    value: Union[str, float, int, bool]


class AttributeAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    option_data: Optional[list[Union[str, bool, int, float]]] = None
    type: AttributeType

    class Config:
        use_enum_values = True

    @validator("type")
    def option_data_validator(cls, value, values, **kwargs):
        if value == AttributeType.OPTION and not values.get("option_data"):
            raise ValueError(
                "Need to assign value for `option_data` "
                + "if the Attribute type is option"
            )
        return value


class SensorAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType

    class Config:
        use_enum_values = True


class OntologyClassAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    color: str
    rank: int
    attribute_data: Optional[list[AttributeAPISchema]] = None

    @validator("color", each_item=True)
    def color_validator(cls, value):
        if not value.startswith("#") or not re.search(
            r"\b[a-zA-Z0-9]{6}\b", value.lstrip("#")
        ):
            raise ValueError(
                f"Color field needs starts with `#` and has 6 digits behind it, get : {value}"
            )
        return value


class OntologyAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None
    ontology_classes_data: Optional[list[OntologyClassAPISchema]] = None

    class Config:
        use_enum_values = True


class ProjectAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology_data: OntologyAPISchema
    sensor_data: list[SensorAPISchema]