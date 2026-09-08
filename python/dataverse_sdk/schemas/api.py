import re
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .client import AnnotationFormat, DatasetType, DataSource, QuestionClass
from .common import (
    CONVERT_RESOLUTIONS,
    AttributeType,
    ConvertFormat,
    ConvertPrecision,
    ModelStructure,
    OntologyImageType,
    OntologyPcdType,
    QuantizationMethod,
    SensorType,
)


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
    data_folder: str
    storage_url: Optional[str] = None
    container_name: Optional[str] = None
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
    model_structure: ModelStructure
    weight_url: str


class ConvertConfigurationAPISchema(BaseModel):
    format: ConvertFormat
    precision: ConvertPrecision
    confidence_threshold: int = Field(ge=10, le=90)
    iou: int = Field(ge=1, le=99)
    topk: int = Field(ge=50, le=300)
    main_obj_low: int = Field(ge=0)
    main_obj_high: int = Field(ge=0)
    resolution_width: int
    resolution_height: int
    # `nms_threshold` and `nms_class_agnostic`: required by yolov9, rejected by D-FINE.
    nms_threshold: Optional[int] = Field(default=None, ge=10, le=90)
    nms_class_agnostic: Optional[bool] = None
    machine_type: Optional[str] = None

    model_config = ConfigDict(use_enum_values=True)

    @model_validator(mode="after")
    def resolution_is_supported(self):
        if (self.resolution_width, self.resolution_height) not in CONVERT_RESOLUTIONS:
            supported = ", ".join(f"{w}x{h}" for w, h in sorted(CONVERT_RESOLUTIONS))
            raise ValueError(
                f"unsupported resolution {self.resolution_width}x"
                f"{self.resolution_height}, only support {supported}"
            )
        return self


class ConvertModelAPISchema(BaseModel):
    name: str
    source_model: int
    target_dataslice: int
    configuration: ConvertConfigurationAPISchema
    quantize_dataslice: Optional[int] = None
    quantizations: Optional[list[QuantizationMethod]] = None

    model_config = ConfigDict(use_enum_values=True)

    @model_validator(mode="after")
    def quantization_needs_a_calibration_dataslice(self):
        if bool(self.quantizations) is not (self.quantize_dataslice is not None):
            raise ValueError(
                "quantizations and quantize_dataslice_id have to be given together"
            )
        if self.quantizations and len(self.quantizations) > 1:
            raise ValueError("only one quantization method is accepted for now")
        return self
