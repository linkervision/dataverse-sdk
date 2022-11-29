import re
from typing import Optional, Union

from pydantic import BaseModel, validator

from .common import (
    AnnotationFormat,
    AttributeType,
    DatasetStatus,
    DatasetType,
    DataSource,
    OntologyImageType,
    OntologyPcdType,
    SensorType,
)


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

    @classmethod
    def create(cls, sensor_data: dict) -> "Sensor":
        return cls(**sensor_data)


class OntologyClass(BaseModel):
    id: Optional[int] = None
    name: str
    color: str
    rank: Optional[int] = None
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

    @classmethod
    def create(cls, ontology_data: dict) -> "Ontology":
        classes = [
            OntologyClass(
                id=cls_["id"],
                name=cls_["name"],
                color=cls_["color"],
                rank=cls_["rank"],
                # TODO: attributes
            )
            for cls_ in ontology_data["classes"]
        ]
        return cls(
            id=ontology_data["id"],
            name=ontology_data["name"],
            image_type=ontology_data["image_type"],
            pcd_type=ontology_data["pcd_type"],
            classes=classes,
        )


class DataConfig(BaseModel):
    storage_url: str
    container_name: Optional[str]
    sas_token: Optional[str]
    data_folder: str
    sequential: bool
    generate_metadata: bool
    description: Optional[str]
    type: str
    annotation_format: str

    class Config:
        extra = "allow"


class Project(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology: Ontology
    sensors: list[Sensor]

    @classmethod
    def create(cls, project_data: dict) -> "Project":
        ontology = Ontology.create(project_data["ontology"])
        sensors = [
            Sensor.create(sensor_data) for sensor_data in project_data["sensors"]
        ]
        return cls(
            id=project_data["id"],
            name=project_data["name"],
            description=project_data["description"],
            ego_car=project_data["ego_car"],
            ontology=ontology,
            sensors=sensors,
        )

    def create_dataset(
        self,
        name: str,
        data_source: DataSource,
        sensors: list[Sensor],
        type: DatasetType,
        annotation_format: AnnotationFormat,
        storage_url: str,
        data_folder: str,
        container_name: Optional[str] = None,
        sas_token: Optional[str] = None,
        sequential: bool = False,
        generate_metadata: bool = False,
        render_pcd: bool = False,
        description: Optional[str] = None,
        **kwargs,
    ):
        """Create Dataset From project itself

        Parameters
        ----------
        name : str
            name of dataset
        data_source : DataSource
            the DataSource basemodel of the given dataset
        sensors : list[Sensor]
            list of Sensor basemodel
        type : DatasetType
            datasettype (annotation or raw)
        annotation_format : AnnotationFormat
            format type of annotation
        storage_url : str
            storage url for cloud
        data_folder : str
            data folder of the storage
        container_name : Optional[str], optional
            container name for Azure, by default None
        sas_token : Optional[str], optional
            SAStoken for Azure, by default None
        sequential : bool, optional
            sequential or not., by default False
        generate_metadata : bool, optional
            generate meta data or not, by default False
        render_pcd : bool, optional
            render pcd preview image or not, be default False
        description : Optional[str], optional
            description of the dataset, by default None

        Returns
        -------
        Dataset
            Dataset Basemodel

        Raises
        ------
        ClientConnectionError
            raise error if client is not exist
        """
        from ..client import DataverseClient

        dataset_output = DataverseClient.create_dataset(
            name=name,
            data_source=data_source,
            project=self,
            sensors=sensors,
            type=type,
            annotation_format=annotation_format,
            storage_url=storage_url,
            container_name=container_name,
            data_folder=data_folder,
            sas_token=sas_token,
            sequential=sequential,
            generate_metadata=generate_metadata,
            render_pcd=render_pcd,
            description=description,
            **kwargs,
        )
        return dataset_output


class Dataset(BaseModel):
    id: Optional[int] = None
    project: Project
    sensors: list[Sensor]
    name: str
    type: DatasetType
    data_source: DataSource
    annotation_format: AnnotationFormat
    status: DatasetStatus
    sequential: bool = False
    generate_metadata: bool = False
    description: Optional[str] = None
    file_count: Optional[int] = None
    image_count: Optional[int] = None
    pcd_count: Optional[int] = None
    created_by: Optional[int] = None
    container_name: Optional[str] = None
    storage_url: Optional[str] = None

    class Config:
        extra = "allow"
