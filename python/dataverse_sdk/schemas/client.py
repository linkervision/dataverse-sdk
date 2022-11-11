from typing import Optional

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


class AttributeOption(BaseAttributeOptionSchema):
    pass


class Attribute(BaseAttributeSchema):
    options: Optional[list[AttributeOption]] = None
    type: AttributeType

    @validator("type")
    def option_data_validator(cls, value, values, **kwargs):
        if value == AttributeType.OPTION and not values.get("options"):
            raise ValueError(
                "Need to assign value for `options` "
                + "if the Attribute type is option"
            )
        return value


class Sensor(BaseSensorSchema):
    pass


class OntologyClass(BaseOntologyClassSchema):
    attributes: Optional[list[BaseAttributeSchema]] = None


class Ontology(BaseOntologySchema):
    classes: Optional[list[OntologyClass]] = None


class Project(BaseProjectSchema):
    ontology: Ontology
    sensors: list[Sensor]
