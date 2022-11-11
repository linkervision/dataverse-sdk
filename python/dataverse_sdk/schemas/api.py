from typing import Optional, Union

from pydantic import validator

from .base import (
    AttributeType,
    BaseAttributeOptionSchema,
    BaseAttributeSchema,
    BaseOntologyClassSchema,
    BaseOntologySchema,
    BaseProjectSchema,
    BaseSensorSchema,
)


class AttributeOptionAPISchema(BaseAttributeOptionSchema):
    pass


class AttributeAPISchema(BaseAttributeSchema):
    option_data: Optional[list[Union[str, bool, int, float]]] = None
    type: AttributeType

    @validator("type")
    def option_data_validator(cls, value, values, **kwargs):
        if value == AttributeType.OPTION and not values.get("option_data"):
            raise ValueError(
                "Need to assign value for `option_data` "
                + "if the Attribute type is option"
            )
        return value


class SensorAPISchema(BaseSensorSchema):
    pass


class OntologyClassAPISchema(BaseOntologyClassSchema):
    attribute_data: Optional[list[AttributeAPISchema]] = None


class OntologyAPISchema(BaseOntologySchema):
    ontology_classes_data: Optional[list[OntologyClassAPISchema]] = None


class ProjectAPISchema(BaseProjectSchema):
    ontology_data: OntologyAPISchema
    sensor_data: list[SensorAPISchema]
