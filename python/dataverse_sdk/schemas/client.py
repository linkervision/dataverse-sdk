import re
from typing import Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from .common import (
    AnnotationFormat,
    AttributeType,
    ConvertFormat,
    ConvertModelFileType,
    ConvertPrecision,
    DatasetStatus,
    DatasetType,
    DataSource,
    ModelStructure,
    OntologyImageType,
    OntologyPcdType,
    ProjectCreateDatasetConfig,
    SensorCounts,
    SensorType,
)


class AttributeOption(BaseModel):
    id: Optional[int] = None
    value: Union[str, float, int, bool]
    aliases: Optional[list] = None


class Attribute(BaseModel):
    id: Optional[int] = None
    name: str
    options: Optional[list[AttributeOption]] = None
    type: AttributeType
    aliases: Optional[list] = None

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("type")
    def option_data_validator(cls, value, info):
        if value == AttributeType.OPTION and not info.data.get("options"):
            raise ValueError(
                "Need to assign value for `options` "
                + "if the Attribute type is option"
            )
        return value


class ProjectTag(BaseModel):
    attributes: Optional[list[Attribute]] = None

    model_config = ConfigDict(use_enum_values=True)

    @classmethod
    def create(cls, project_tag_data: dict) -> "ProjectTag":
        return cls(**project_tag_data)


class Sensor(BaseModel):
    id: Optional[int] = None
    name: str
    type: SensorType

    model_config = ConfigDict(use_enum_values=True)

    @classmethod
    def create(cls, sensor_data: dict) -> "Sensor":
        return cls(**sensor_data)


class OntologyClass(BaseModel):
    id: Optional[int] = None
    name: str
    color: Optional[str] = "#cc39f4"
    rank: Optional[int] = None
    attributes: Optional[list[Attribute]] = None
    aliases: Optional[list] = None
    extended_class: Optional[dict] = None

    model_config = ConfigDict(validate_assignment=True)

    @field_validator("color", mode="before")
    def color_validator(cls, value):
        if not value:
            value = "#cc39f4"
        if not value.startswith("#") or not re.search(
            r"\b[a-zA-Z0-9]{6}\b", value.lstrip("#")
        ):
            raise ValueError(
                f"Color field needs starts with `#` and has 6 digits behind it, get : {value}"
            )
        return value


class QuestionClass(BaseModel):
    id: Optional[int] = None
    class_name: str
    question: str
    color: Optional[str] = "#cc39f4"
    rank: int
    answer_name: Optional[str] = "answer"
    answer_options: Optional[list] = None
    answer_type: AttributeType

    model_config = ConfigDict(validate_assignment=True)

    @field_validator("color", mode="before")
    def color_validator(cls, value):
        if not value:
            value = "#cc39f4"
        if not value.startswith("#") or not re.search(
            r"\b[a-zA-Z0-9]{6}\b", value.lstrip("#")
        ):
            raise ValueError(
                f"Color field needs starts with `#` and has 6 digits behind it, get : {value}"
            )
        return value

    @field_validator("answer_type")
    def answer_type_validator(cls, value, values: ValidationInfo, **kwargs):
        if value == AttributeType.OPTION and not values.data.get("answer_options"):
            raise ValueError(
                f"* {values.data} Need to assign value for `answer_options` "
                + "if the Answer type is option"
            )
        return value


class UpdateQuestionClass(BaseModel):
    rank: int
    question: Optional[str] = None
    options: Optional[list] = None


class Ontology(BaseModel):
    id: Optional[int] = None
    name: str
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None
    classes: Optional[list[OntologyClass]] = None

    model_config = ConfigDict(use_enum_values=True)

    @classmethod
    def create(cls, ontology_data: dict) -> "Ontology":
        classes = [
            OntologyClass(
                id=cls_["id"],
                name=cls_["name"],
                color=cls_.get("color"),
                rank=cls_.get("rank"),
                attributes=cls_.get("attributes"),
                aliases=cls_.get("aliases"),
                extended_class=cls_.get("extended_class"),
            )
            for cls_ in ontology_data["classes"]
        ]

        return cls(
            id=ontology_data["id"],
            name=ontology_data.get("name", ""),
            image_type=ontology_data["image_type"],
            pcd_type=ontology_data["pcd_type"],
            classes=classes,
        )


class Project(BaseModel):
    id: int
    name: str
    client_alias: str
    description: Optional[str] = None
    ego_car: Optional[str] = None
    ontology: Ontology
    sensors: Optional[list[Sensor]] = None
    project_tag: Optional[ProjectTag] = None

    @classmethod
    def create(cls, project_data: dict, client_alias: str) -> "Project":
        ontology = Ontology.create(project_data["ontology"])
        # TODO modify the condition if list projects results with list fields
        if project_data.get("sensors") is None:
            sensors = None
        else:
            sensors = [
                Sensor.create(sensor_data) for sensor_data in project_data["sensors"]
            ]
        if project_data.get("project_tag") is None:
            project_data["project_tag"] = {}
        project_tag = ProjectTag.create(project_data["project_tag"])
        return cls(
            id=project_data["id"],
            name=project_data["name"],
            description=project_data["description"],
            ego_car=project_data.get("ego_car"),
            ontology=ontology,
            sensors=sensors,
            project_tag=project_tag,
            client_alias=client_alias,
        )

    def add_project_tag(self, project_tag: ProjectTag):
        from ..client import DataverseClient

        project = DataverseClient.add_project_tag(
            project_tag=project_tag,
            project=self,
            project_id=self.id,
            client_alias=self.client_alias,
        )
        return project

    def edit_project_tag(self, project_tag: ProjectTag):
        from ..client import DataverseClient

        project = DataverseClient.edit_project_tag(
            project_tag=project_tag,
            project=self,
            project_id=self.id,
            client_alias=self.client_alias,
        )
        return project

    def add_ontology_classes(self, ontology_classes: list[OntologyClass]):
        from ..client import DataverseClient

        project = DataverseClient.add_ontology_classes(
            ontology_classes=ontology_classes,
            project=self,
            project_id=self.id,
            client_alias=self.client_alias,
        )
        return project

    def edit_ontology_classes(self, ontology_classes: list[OntologyClass]):
        from ..client import DataverseClient

        project = DataverseClient.edit_ontology_classes(
            ontology_classes=ontology_classes,
            project=self,
            project_id=self.id,
            client_alias=self.client_alias,
        )
        return project

    def edit_vqa_ontology(
        self,
        ontology_name: str = "",
        create: Optional[list[QuestionClass]] = None,
        update: Optional[list] = None,
    ):
        from ..client import DataverseClient

        project = DataverseClient.edit_vqa_ontology(
            ontology_name=ontology_name,
            create=create,
            update=update,
            project_id=self.id,
            project=self,
            client_alias=self.client_alias,
        )
        return project

    def list_datasets(self) -> list:
        from ..client import DataverseClient

        dataset_list: list = DataverseClient.list_datasets(
            project_id=self.id, client_alias=self.client_alias
        )
        return dataset_list

    def list_dataslices(self) -> list:
        from ..client import DataverseClient

        dataslice_list: list = DataverseClient.list_dataslices(
            project_id=self.id, client_alias=self.client_alias
        )
        return dataslice_list

    def list_models(
        self,
        type: Optional[
            Union[
                Literal["trained", "byom", "uploaded"],
                list[Literal["trained", "byom", "uploaded"]],
            ]
        ] = ["trained", "byom"],
    ) -> list:
        from ..client import DataverseClient

        model_list: list = DataverseClient.list_models(
            project_id=self.id, project=self, client_alias=self.client_alias, type=type
        )
        return model_list

    def get_model(self, model_id: int):
        from ..client import DataverseClient

        model_data = DataverseClient.get_model(
            model_id=model_id, project=self, client_alias=self.client_alias
        )
        return model_data

    def get_convert_record(self, convert_record_id: int):
        from ..client import DataverseClient

        convert_record_data = DataverseClient.get_convert_record(
            convert_record_id=convert_record_id,
            client_alias=self.client_alias,
        )
        return convert_record_data

    def list_convert_records(
        self,
        model_id: Optional[int] = None,
        name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list["ConvertRecord"]:
        from ..client import DataverseClient

        return DataverseClient.list_convert_records(
            project_id=self.id,
            model_id=model_id,
            name=name,
            status=status,
            client_alias=self.client_alias,
        )

    def get_dataslice_by_name(self, dataslice_name: str) -> "Dataslice":
        from ..client import DataverseClient

        return DataverseClient.get_client(self.client_alias).get_dataslice_by_name(
            project_id=self.id, dataslice_name=dataslice_name
        )

    def create_dataslice_from_dataset(
        self,
        dataset_id: int,
        dataslice_name: str,
        description: Optional[str] = None,
    ) -> "Dataslice":
        from ..client import DataverseClient

        return DataverseClient.get_client(
            self.client_alias
        ).create_dataslice_from_dataset(
            project_id=self.id,
            dataset_id=dataset_id,
            dataslice_name=dataslice_name,
            description=description,
        )

    def _validate_before_create_dataset(
        self,
        annotation_format: AnnotationFormat,
        dataset_type: DatasetType,
        sequential: bool,
    ) -> None:
        from dataverse_sdk.utils.utils import validate_before_create_dataset

        if self.sensors is None:
            from ..client import DataverseClient

            project = DataverseClient.get_client_project(
                project_id=self.id, client_alias=self.client_alias
            )
            self.sensors = project.sensors or []

        sensor_counts = SensorCounts(camera=0, lidar=0)
        for sensor in self.sensors:
            sensor_type: SensorType = getattr(sensor, "type", "")
            if sensor_type == SensorType.CAMERA:
                sensor_counts.camera += 1
            elif sensor_type == SensorType.LIDAR:
                sensor_counts.lidar += 1

        image_type = None
        pcd_type = None
        has_attribute = False

        if self.ontology:
            if self.ontology.image_type:
                image_type = self.ontology.image_type

            if self.ontology.pcd_type:
                pcd_type = self.ontology.pcd_type

            if self.ontology.classes:
                for cls in self.ontology.classes:
                    if cls.attributes:
                        has_attribute = True
                        break

        config = ProjectCreateDatasetConfig(
            annotation_format=annotation_format,
            dataset_type=dataset_type,
            sensor_counts=sensor_counts,
            is_sequential=sequential,
            image_type=image_type,
            pcd_type=pcd_type,
            has_attribute=has_attribute,
        )

        is_valid, error_message = validate_before_create_dataset(config)

        if not is_valid:
            raise ValueError(error_message)

    def create_dataset(
        self,
        name: str,
        data_source: DataSource,
        type: DatasetType,
        annotation_format: AnnotationFormat,
        data_folder: str,
        storage_url: Optional[str] = None,
        container_name: Optional[str] = None,
        annotations: Optional[list] = None,
        sequential: bool = False,
        generate_metadata: bool = False,
        render_pcd: bool = False,
        description: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        reupload_dataset_uuid: Optional[str] = None,
        **kwargs,
    ):
        """Create Dataset From project itself

        Parameters
        ----------
        name : str
            name of dataset
        data_source : DataSource
            the DataSource basemodel of the given dataset
        type : DatasetType
            datasettype (annotation or raw)
        annotation_format : AnnotationFormat
            format type of annotation
        data_folder : str
            data folder of the storage
        storage_url : Optional[str], optional
            storage url for cloud storage (e.g. AWS), by default None
        container_name : Optional[str], optional
            MinIO bucket name, by default None
        annotations: list, optional
            list of annotation folder name (should be groundtruth or $model_name)
        sequential : bool, optional
            sequential or not., by default False
        generate_metadata : bool, optional
            generate meta data or not, by default False
        render_pcd : bool, optional
            render pcd preview image or not, be default False
        description : Optional[str], optional
            description of the dataset, by default None
        access_key_id : Optional[str], optional
            access key id for AWS s3 bucket, by default None
        secret_access_key : Optional[str], optional
            secret access key for AWS s3 bucket, by default None
        reupload_dataset_uuid: Optional[str], optional
            dataset UUID of a previously failed local dataset import. If provided, the files that failed to upload
            (as recorded in `failed_upload.json`) will be re-uploaded, by default None

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

        self._validate_before_create_dataset(annotation_format, type, sequential)

        if annotations is None:
            annotations = []

        dataset_output = DataverseClient.create_dataset(
            name=name,
            data_source=data_source,
            project=self,
            type=type,
            annotation_format=annotation_format,
            storage_url=storage_url,
            container_name=container_name,
            data_folder=data_folder,
            annotations=annotations,
            sequential=sequential,
            generate_metadata=generate_metadata,
            render_pcd=render_pcd,
            description=description,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            client_alias=self.client_alias,
            reupload_dataset_uuid=reupload_dataset_uuid,
            **kwargs,
        )
        return dataset_output


class Dataset(BaseModel):
    id: Optional[int] = None
    project: Project
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
    client_alias: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class Dataslice(BaseModel):
    id: Optional[int] = None
    project: Project
    name: str
    status: str
    annotation_type: str
    type: str
    file_count: Optional[int] = None
    export_records: Optional[list] = None
    # Datarow counts per type: {"image": int | None, "pcd": ..., "frame": ..., "sequence": ...}
    metadata: Optional[dict] = None

    model_config = ConfigDict(extra="allow")

    @property
    def image_count(self) -> Optional[int]:
        """Image datarow count, `None` when the payload carried no `metadata`."""
        return (self.metadata or {}).get("image")

    @property
    def pcd_count(self) -> Optional[int]:
        """PCD datarow count, `None` when the payload carried no `metadata`."""
        return (self.metadata or {}).get("pcd")


class ConvertRecord(BaseModel):
    id: Optional[int] = None
    name: str
    client_alias: str
    configuration: dict
    status: str
    trait: dict

    # 0.0 while converting; None only when the backend omitted the field.
    f1_score: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    map_5_95: Optional[float] = None

    model_config = ConfigDict(extra="allow")

    @property
    def model_type(self) -> Optional[str]:
        """Format converted into, named after `convert_model`'s own argument."""
        return self.configuration.get("format")

    @property
    def data_type(self) -> Optional[str]:
        """Precision converted at, which `precision` is not -- that one is the metric."""
        return self.configuration.get("precision")

    @classmethod
    def create(cls, record_data: dict, client_alias: str) -> "ConvertRecord":
        """Build from a backend payload; `extra="allow"` carries unlisted fields through."""
        return cls(
            **{
                **record_data,
                "configuration": record_data.get("configuration") or {},
                "trait": record_data.get("trait") or {},
                "client_alias": client_alias,
            }
        )

    def get_label_file(
        self, save_path: str = "./labels.txt", timeout: int = 3000
    ) -> tuple[bool, str]:
        from ..client import DataverseClient

        return DataverseClient.get_label_file(
            convert_record_id=self.id,
            save_path=save_path,
            timeout=timeout,
            client_alias=self.client_alias,
        )

    def get_onnx_model_file(
        self, save_path: str = "./model.onnx", timeout: int = 3000
    ) -> tuple[bool, str]:
        if self.model_type != ConvertFormat.ONNX:
            raise ValueError("The converted model format is not onnx")
        from ..client import DataverseClient

        return DataverseClient.get_onnx_model_file(
            convert_record_id=self.id,
            save_path=save_path,
            timeout=timeout,
            client_alias=self.client_alias,
        )

    def get_convert_model_file(
        self,
        file_type: ConvertModelFileType = ConvertModelFileType.TRITON,
        save_path: Optional[str] = None,
        timeout: int = 3000,
        permission: str = "",
    ) -> tuple[bool, str]:
        from ..client import DataverseClient

        return DataverseClient.get_convert_model_file(
            convert_record_id=self.id,
            save_path=save_path,
            file_type=file_type,
            timeout=timeout,
            permission=permission,
            client_alias=self.client_alias,
        )


class MLModel(BaseModel):
    id: Optional[int] = None
    name: str
    client_alias: str
    updated_at: str
    project: Project
    classes: list
    operation_records: list = []
    triton_model_name: Optional[str] = None
    description: Optional[str] = None
    configuration: dict = {}
    # Typed as str, not MLModelStatus: an unknown status must not fail the whole parse.
    status: Optional[str] = None
    # Typed as str, not ModelStructure: an unknown value must not fail the whole list_models parse.
    # Comparing against ModelStructure still works, since that enum subclasses str.
    model_structure: Optional[str] = None

    model_config = ConfigDict(extra="allow")

    @classmethod
    def create(cls, model_data: dict, client_alias: str) -> "MLModel":
        if model_data["classes"] and isinstance(model_data["classes"][0], dict):
            target_class_id = {
                ontology_class["id"] for ontology_class in model_data["classes"]
            }
        else:
            target_class_id = set(model_data.get("classes") or [])

        project = model_data["project"]
        # get classes used in the model
        classes = [
            ontology_class
            for ontology_class in project.ontology.classes
            if ontology_class.id in target_class_id
        ]
        return cls(
            **{
                **model_data,
                "project": project,
                "classes": classes,
                "configuration": model_data.get("configuration") or {},
                "operation_records": model_data.get("model_records", []),
                "client_alias": client_alias,
            }
        )

    def get_convert_record(self, convert_record_id: int) -> ConvertRecord:
        from ..client import DataverseClient

        return DataverseClient.get_convert_record(
            convert_record_id=convert_record_id, client_alias=self.client_alias
        )

    def list_convert_records(
        self, name: Optional[str] = None, status: Optional[str] = None
    ) -> list[ConvertRecord]:
        from ..client import DataverseClient

        return DataverseClient.list_convert_records(
            model_id=self.id,
            name=name,
            status=status,
            client_alias=self.client_alias,
        )

    def get_convert_record_by_name(self, convert_name: str) -> ConvertRecord:
        from ..client import DataverseClient

        return DataverseClient.get_convert_record_by_name(
            model_id=self.id,
            convert_name=convert_name,
            client_alias=self.client_alias,
        )

    def convert(
        self,
        name: str,
        target_dataslice_id: int,
        model_type: Union[ConvertFormat, str],
        data_type: Union[ConvertPrecision, str],
        **kwargs,
    ) -> list[int]:
        """Convert this model. See DataverseClient.convert_model for every argument."""
        from ..client import DataverseClient

        if self.model_structure:
            kwargs.setdefault("model_structure", self.model_structure)
        for field in ("resolution_width", "resolution_height"):
            if self.configuration.get(field) is not None:
                kwargs.setdefault(field, self.configuration[field])
        return DataverseClient.convert_model(
            model_id=self.id,
            name=name,
            target_dataslice_id=target_dataslice_id,
            model_type=model_type,
            data_type=data_type,
            client_alias=self.client_alias,
            **kwargs,
        )

    def create_custom_model(
        self,
        project: Project,
        name: str,
        input_classes: list[str],
        resolution_width: int,
        resolution_height: int,
        model_structure: ModelStructure,
        weight_url: str,
        permission: str = "",
    ):
        from ..client import DataverseClient

        return DataverseClient.create_custom_model(
            project=project,
            name=name,
            input_classes=input_classes,
            resolution_width=resolution_width,
            resolution_height=resolution_height,
            model_structure=model_structure,
            weight_url=weight_url,
            client_alias=self.client_alias,
            permission=permission,
        )
