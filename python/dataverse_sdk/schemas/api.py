import re
from typing import Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator

from .client import AnnotationFormat, DatasetType, DataSource, QuestionClass
from .common import AttributeType, OntologyImageType, OntologyPcdType, SensorType


class AttributeOptionAPISchema(BaseModel):
    id: Optional[int] = None
    value: Union[str, float, int, bool]


class AttributeAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    option_data: Optional[list[Union[str, bool, int, float]]] = None
    type: AttributeType

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("type")
    def option_data_validator(cls, value, info):
        if value == AttributeType.OPTION and not info.data.get("option_data"):
            raise ValueError(
                "Need to assign value for `option_data` "
                + "if the Attribute type is option"
            )
        return value


class ProjectTagAPISchema(BaseModel):
    attribute_data: Optional[list[AttributeAPISchema]] = None

    model_config = ConfigDict(use_enum_values=True)


class SensorAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType

    model_config = ConfigDict(use_enum_values=True)


class OntologyClassAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    color: str
    rank: int
    attribute_data: Optional[list[AttributeAPISchema]] = None

    @field_validator("color")
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

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("ontology_classes_data", mode="before")
    def ontology_classes_data_validator(cls, value):
        if len({v["rank"] for v in value}) != len(value):
            raise ValueError("Duplicated classes rank value")
        return value


class ProjectAPISchema(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology_data: OntologyAPISchema
    sensor_data: list[SensorAPISchema]
    project_tag_data: ProjectTagAPISchema


class VQAProjectAPISchema(BaseModel):
    name: str
    sensor_name: str
    ontology_name: str
    question_answer: list[QuestionClass]
    description: Optional[str] = None

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("question_answer", mode="before")
    def question_answer_validator(cls, value):
        if len({v.rank for v in value}) != len(value):
            raise ValueError("The question rank id of is duplicated.")
        return value


class UpdateQuestionAPISchema(BaseModel):
    extended_class_id: Optional[int] = None
    question: Optional[str] = None
    attribute_id: Optional[int] = None
    options: Optional[list] = None


class DatasetAPISchema(BaseModel):
    name: str
    project_id: int
    data_source: DataSource
    type: DatasetType
    annotation_format: AnnotationFormat
    storage_url: str
    data_folder: str
    container_name: Optional[str] = None
    sas_token: Optional[str] = None
    sequential: bool = False
    generate_metadata: bool = False
    render_pcd: Optional[bool] = None
    description: Optional[str] = None
    calibration_folder: Optional[str] = None
    annotation_file: Optional[str] = None
    annotation_folder: Optional[str] = None
    lidar_folder: Optional[str] = None
    annotations: Optional[list[str]] = []
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None


class CreateCustomModelAPISchema(BaseModel):
    project_id: int
    name: str
    input_classes: list[str]
    resolution_width: int
    resolution_height: int
    model_structure: Literal["yolov9-c", "yolov9-e", "yolov9-s"]
    weight_url: str
