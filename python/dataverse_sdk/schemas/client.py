import re
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, validator

from .common import AttributeType, OntologyImageType, OntologyPcdType, SensorType


class AttributeOption(BaseModel):
    id: Optional[int] = None
    value: Union[str, float, int, bool]


class Attribute(BaseModel):
    id: Optional[int] = None
    name: str
    options: Optional[list[AttributeOption]] = None
    type: AttributeType

    class Config:
        use_enum_values = True

    @validator("type")
    def option_data_validator(cls, value, values, **kwargs):
        if value == AttributeType.OPTION and not values.get("options"):
            raise ValueError(
                "Need to assign value for `options` "
                + "if the Attribute type is option"
            )
        return value


class Sensor(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType

    class Config:
        use_enum_values = True


class OntologyClass(BaseModel):
    id: Optional[int] = None
    name: str
    color: str
    rank: int
    attributes: Optional[list[Attribute]] = None

    @validator("color", each_item=True)
    def color_validator(cls, value):
        if not value.startswith("#") or not re.search(
            r"\b[a-zA-Z0-9]{6}\b", value.lstrip("#")
        ):
            raise ValueError(
                f"Color field needs starts with `#` and has 6 digits behind it, get : {value}"
            )
        return value


class Ontology(BaseModel):
    id: Optional[int] = None
    name: str
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None
    classes: Optional[list[OntologyClass]] = None

    class Config:
        use_enum_values = True


class DataSource(str, Enum):

    Azure = "azure"
    AWS = "aws"
    LOCAL = "local"


class Project(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology: Ontology
    sensors: list[Sensor]
    client: Optional[object] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_dataset(self, name: str, source: DataSource, dataset: dict):
        if self.client is not None:
            dataset_output = self.client.create_dataset(
                name=name, source=source, project=self, dataset=dataset
            )
            return dataset_output
        else:
            raise NotImplementedError("ClientServer is not defined")


class Dataset(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    data_source: DataSource
    project: Optional[dict] = None
    type: str
    image_count: Optional[int] = None
    pcd_count: Optional[int] = None
