import asyncio
import logging
from collections import deque
from typing import Optional

from httpx import AsyncClient, AsyncHTTPTransport, Response, Timeout
from pydantic import ValidationError

from .apis.backend import BackendAPI
from .connections import add_connection, get_connection
from .constants import DataverseHost
from .exceptions.client import (
    AsyncThirdPartyAPIException,
    ClientConnectionError,
    DataverseExceptionBase,
)
from .schemas.api import (
    AttributeAPISchema,
    DatasetAPISchema,
    OntologyAPISchema,
    ProjectAPISchema,
    ProjectTagAPISchema,
)
from .schemas.client import (
    Dataset,
    DataSource,
    MLModel,
    Ontology,
    OntologyClass,
    Project,
    ProjectTag,
    Sensor,
)
from .schemas.common import AnnotationFormat, DatasetType, OntologyImageType, SensorType
from .utils.utils import download_file_from_response, get_filepaths


def parse_attribute(attr_list: list) -> list:
    new_attribute_list: list[AttributeAPISchema] = []
    for attr in attr_list:
        attr.pop("id", None)
        if attr["type"] != "option":
            new_attribute_list.append(attr)
            continue
        attr["option_data"] = [
            opt_data["value"] for opt_data in attr.pop("options", [])
        ]
        new_attribute_list.append(attr)
    return new_attribute_list


class DataverseClient:
    def __init__(
        self,
        host: DataverseHost,
        email: str,
        password: str,
        service_id: str,
        alias: str = "default",
        force: bool = False,
        access_token: str = "",
    ) -> None:
        """
        Instantiate a Dataverse client.

        Parameters
        ----------
        host : DataverseHost
        email : str
        password : str
        service_id : str
        alias: str
        force: bool, whether replace the connection if alias exists, default is False
        access_token: str, optional, will try to use access_token to do authentication

        Raises
        ------
        ValueError
        """
        if host not in DataverseHost:
            raise ValueError("Invalid dataverse host, is the host available?")
        self.host = host
        self._api_client = None
        self.alias = alias
        self._init_api_client(
            email=email,
            password=password,
            service_id=service_id,
            access_token=access_token,
        )
        add_connection(alias=alias, conn=self, force=force)

    def _init_api_client(
        self,
        email: str,
        password: str,
        service_id: str,
        access_token: str = "",
    ) -> None:
        try:
            self._api_client = BackendAPI(
                host=self.host,
                email=email,
                password=password,
                service_id=service_id,
                access_token=access_token,
            )
        except DataverseExceptionBase:
            logging.exception("Initial Client Error")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to initialize the api client: {e}")

    @staticmethod
    def _get_api_client(
        client: Optional["DataverseClient"] = None, client_alias: Optional[str] = None
    ) -> tuple[BackendAPI, str]:
        if client is None:
            if client_alias is None:
                raise ValueError(
                    "Please provide the DataverseClient or the connection alias!"
                )
            client = DataverseClient.get_client(client_alias)
        else:
            client_alias = client.alias
        api = client._api_client
        return api, client_alias

    @staticmethod
    def get_client(alias: str = "default") -> "DataverseClient":
        try:
            return get_connection(alias)
        except KeyError:
            raise

    def get_user(self):
        return self._api_client.get_user()

    def create_project(
        self,
        name: str,
        ontology: Ontology,
        sensors: list[Sensor],
        project_tag: Optional[ProjectTag] = None,
        description: Optional[str] = None,
    ) -> Project:
        """Creates project from client data
        Parameters
        ----------
        name : str
            name of current project
        ontology : Ontology
            the Ontology basemodel data of current project
        sensors : list[Sensor]
            the list of Sensor basemodel data of current project
        project_tag : Optional[ProjectTag]
            your project tags
        description : Optional[str]
            your project description

        Returns
        -------
        project : Project
            Project basemodel from host response for client usage

        Raises
        ------
        ValidationError
            raise exception if there is any error occurs when composing the request body.
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """

        raw_ontology_data: dict = ontology.dict(exclude_none=True)
        classes_data_list: list[dict] = []
        rank = 1
        # remove `id` field in OntologyClass and Attribute
        for cls_ in raw_ontology_data.pop("classes", []):
            cls_.pop("id", None)
            if rank not in cls_:
                cls_["rank"] = rank
                rank += 1
            if not (obj_attrs := cls_.pop("attributes", None)):
                classes_data_list.append(cls_)
                continue
            cls_["attribute_data"] = parse_attribute(obj_attrs)
            classes_data_list.append(cls_)
        raw_ontology_data["ontology_classes_data"] = classes_data_list
        if project_tag is not None:
            raw_project_tag_data: dict = project_tag.dict(exclude_none=True)
            if tag_attrs := raw_project_tag_data.pop("attributes", None):
                raw_project_tag_data["attribute_data"] = parse_attribute(tag_attrs)
        else:
            raw_project_tag_data = {}

        ontology_data = OntologyAPISchema(**raw_ontology_data).dict(exclude_none=True)
        project_tag_data = ProjectTagAPISchema(**raw_project_tag_data).dict()
        sensor_data = [sensor.dict(exclude_none=True) for sensor in sensors]

        try:
            raw_project_data = ProjectAPISchema(
                name=name,
                ontology_data=ontology_data,
                sensor_data=sensor_data,
                project_tag_data=project_tag_data,
                description=description,
            ).dict(exclude_none=True)
        except ValidationError as e:
            raise ValidationError(
                f"Something wrong when composing the final project data: {e}"
            )

        try:
            project_data: dict = self._api_client.create_project(**raw_project_data)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")
        return Project.create(project_data=project_data, client_alias=self.alias)

    def list_projects(
        self,
        current_user: bool = True,
        exclude_sensor_type: Optional[SensorType] = None,
        image_type: Optional[OntologyImageType] = None,
    ) -> list[Project]:
        """list projects in dataverse (with given filter query params)

        Parameters
        ----------
        current_user : bool, optional
            only show the projects of current user, by default True
        exclude_sensor_type : Optional[SensorType], optional
            exclude the projects with the given sensor type, by default None
        image_type : Optional[OntologyImageType], optional
            only include the projects with the given image type, by default None

        Returns
        -------
        list[Projects]
            list of project items

        Raises
        ------
        ClientConnectionError
            raise error if there is any error occurs when calling backend APIs.
        """

        try:
            project_list: list = self._api_client.list_projects(
                current_user=current_user,
                exclude_sensor_type=exclude_sensor_type,
                image_type=image_type,
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the projects: {e}")
        output_project_list = []
        for project in project_list:
            output_project_list.append(
                Project.create(project_data=project, client_alias=self.alias)
            )
        return output_project_list

    @classmethod
    def get_client_project(
        cls,
        project_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ):
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            project_data: dict = api.get_project(project_id=project_id)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the project: {e}")
        return Project.create(project_data, client_alias=client_alias)

    def get_project(self, project_id: int, client_alias: Optional[str] = None):
        """Get project detail by project-id

        Parameters
        ----------
        project_id : int
            project-id in db
        client_alias: Optional[str], by default None (will reset to self.alias if it's not provided)

        Returns
        -------
        Project
            Project basemodel from host response for client usage

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        if client_alias is None:
            client_alias = self.alias
        return self.get_client_project(project_id=project_id, client_alias=client_alias)

    @staticmethod
    def add_project_tag(
        project_id: int,
        project_tag: ProjectTag,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> dict:
        """Add New Project Tag

        Parameters
        ----------
        project_id : int
        project_tag : ProjectTag
        client : Optional["DataverseClient"], optional
            clientclass, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)
        project : Optional["Project"], optional
            project basemodel, by default None
        Returns
        -------
        dict
            dictionary with project id and project name info

        Raises
        ------
        ClientConnectionError
            API error when creating new project tag
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        if project is None:
            project = DataverseClient.get_client_project(
                project_id=project_id, client_alias=client_alias
            )

        raw_project_tag: dict = project_tag.dict(exclude_none=True)
        # new project tag attributes to be creaeted
        new_attribute_data: list = parse_attribute(
            raw_project_tag.get("attributes", [])
        )
        project_tag_data = {"new_attribute_data": new_attribute_data}
        try:
            project_data: dict = api.edit_project(
                project_id=project_id, project_tag_data=project_tag_data
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(
                f"Failed to add project tag, please check your data: {e}"
            )
        return project_data

    @staticmethod
    def edit_project_tag(
        project_id: int,
        project_tag: ProjectTag,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> dict:
        """Edit existing project tag

        Parameters
        ----------
        project_id : int
        project_tag : ProjectTag
        client : Optional["DataverseClient"], optional
            clientclass, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)
        project : Optional["Project"], optional
            project basemodel, by default None

        Returns
        -------
        dict
            dictionary with project id and project name info

        Raises
        ------
        ClientConnectionError
            API error when editing project tag
        """

        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        if project is None:
            project = DataverseClient.get_client_project(
                project_id=project_id, client=client, client_alias=client_alias
            )

        raw_project_tag: dict = project_tag.dict(exclude_none=True)
        # old project tag attributes to be extended
        patched_attribute_data: list = parse_attribute(
            raw_project_tag.get("attributes", [])
        )
        project_tag_data = {"patched_attribute_data": patched_attribute_data}
        try:
            project_data: dict = api.edit_project(
                project_id=project_id, project_tag_data=project_tag_data
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(
                f"Failed to edit project tag, please check your data: {e}"
            )
        return project_data

    @staticmethod
    def add_ontology_classes(
        project_id: int,
        ontology_classes: list[OntologyClass],
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> dict:
        """Add new ontology classes

        Parameters
        ----------
        project_id : int
        ontology_classes : list[OntologyClass]
        client : Optional["DataverseClient"], optional
            clientclass, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)
        project : Optional["Project"], optional
            project basemodel, by default None

        Returns
        -------
        dict
            dictionary with project id and project name info

        Raises
        ------
        ClientConnectionError
            API error when creating new ontology class
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        if project is None:
            project = DataverseClient.get_client_project(
                project_id=project_id, client=client, client_alias=client_alias
            )
        # new ontology classes to be creaeted
        new_classes_data = []
        for ontology_class in ontology_classes:
            raw_ontology_class: dict = ontology_class.dict(exclude_none=True)
            attribute_data: list = parse_attribute(
                raw_ontology_class.get("attributes", [])
            )
            new_classes_data.append(
                {
                    "name": ontology_class.name,
                    "color": ontology_class.color,
                    "attribute_data": attribute_data,
                }
            )
        ontology_data = {"new_classes_data": new_classes_data}
        try:
            project_data: dict = api.edit_project(
                project_id=project_id, ontology_data=ontology_data
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(
                f"Failed to add ontology classes, please check your data: {e}"
            )
        return project_data

    @staticmethod
    def edit_ontology_classes(
        project_id: int,
        ontology_classes: list[OntologyClass],
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> dict:
        """Edit ontology classes

        Parameters
        ----------
        project_id : int
        ontology_classes : list[OntologyClass]
        client : Optional["DataverseClient"], optional
            clientclass, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)
        project : Optional["Project"], optional
            project basemodel, by default None

        Returns
        -------
        dict
            dictionary with project id and project name info

        Raises
        ------
        ClientConnectionError
            API error when editing ontology classes
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        if project is None:
            project = DataverseClient.get_client_project(
                project_id=project_id, client=client, client_alias=client_alias
            )
        # ontology classes to be edited
        patched_classes_data = []
        for ontology_class in ontology_classes:
            raw_ontology_class: dict = ontology_class.dict(exclude_none=True)
            attribute_data: list = parse_attribute(
                raw_ontology_class.get("attributes", [])
            )
            patched_classes_data.append(
                {"name": ontology_class.name, "attribute_data": attribute_data}
            )
        ontology_data = {"patched_classes_data": patched_classes_data}
        try:
            project_data: dict = api.edit_project(
                project_id=project_id, ontology_data=ontology_data
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(
                f"Failed to edit ontology classes, please check your data: {e}"
            )
        return project_data

    @staticmethod
    def list_models(
        project_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> list[MLModel]:
        """Get the model list by project id

        Parameters
        ----------
        project_id : int
        client : Optional["DataverseClient"], optional
            clientclass, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)
        project: Optional["Project"]
            project basemodel, by default None

        Returns
        -------
        list
            list of model items

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            model_list: list = api.list_ml_models(project_id=project_id)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the models: {e}")
        if project is None:
            project = DataverseClient.get_client_project(
                project_id=project_id, client=client, client_alias=client_alias
            )
        output_model_list = []
        for model_data in model_list:
            model_config = model_data["configuration"]
            model_data.update(
                {
                    "project": project,
                    "triton_model_name": model_config.get("triton_model_name"),
                }
            )
            ml_model = MLModel.create(model_data, client_alias=client_alias)
            output_model_list.append(ml_model)
        return output_model_list

    @staticmethod
    def get_model(
        model_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> MLModel:
        """get the model detail by model id

        Parameters
        ----------
        model_id : int
        client : Optional["DataverseClient"], optional
            client class, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)
        project : Optional["Project"], optional
            the project class, by default None

        Returns
        -------
        MLModel
            BaseModel for ml_model that store model information

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            model_data: dict = api.get_ml_model(model_id=model_id)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the model: {e}")

        if project is None:
            project = DataverseClient.get_client_project(
                project_id=model_data["project"]["id"],
                client=client,
                client_alias=client_alias,
            )
        model_data.update({"id": model_id, "project": project})
        return MLModel.create(model_data, client_alias=client_alias)

    @staticmethod
    def get_label_file(
        model_id: int,
        save_path: str = "./labels.txt",
        timeout: int = 3000,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Download the model label file (which is a string txt file)

        Parameters
        ----------
        model_id : int
        save_path : str, optional
            local path for saving the label_file, by default './labels.txt'
        timeout : int, optional
            maximum timeout of the request, by default 3000
        client : Optional[&quot;DataverseClient&quot;], optional
            client class, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)

        Returns
        -------
        (status, save_path): tuple[bool, str]
            the first item means whether the download success or not
            the second item shows the save_path
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            resp = api.get_ml_model_labels(model_id=model_id, timeout=timeout)
            download_file_from_response(response=resp, save_path=save_path)
            return True, save_path
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception:
            logging.exception("Failed to get model label file")
            return False, save_path

    @staticmethod
    def get_triton_model_file(
        model_id: int,
        save_path: str = "./model.zip",
        timeout: int = 3000,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Download the triton model file (which is a zip file)

        Parameters
        ----------
        model_id : int
        save_path : str, optional
            local path for saving the triton model file, by default './model.zip'
        timeout : int, optional
            maximum timeout of the request, by default 3000
        client : Optional[&quot;DataverseClient&quot;], optional
            client class, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)

        Returns
        -------
        (status, save_path): tuple[bool, str]
            the first item means whether the download success or not
            the second item shows the save_path
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            resp = api.get_ml_model_file(model_id=model_id, timeout=timeout)
            download_file_from_response(response=resp, save_path=save_path)
            return True, save_path
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception:
            logging.exception("Failed to get triton model file")
            return False, save_path

    @staticmethod
    def get_onnx_model_file(
        model_id: int,
        save_path: str = "./model.onnx",
        timeout: int = 3000,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Download the onnx model file

        Parameters
        ----------
        model_id : int
        save_path : str, optional
            local path for saving the onnx model file, by default './model.onnx'
        timeout : int, optional
            maximum timeout of the request, by default 3000
        client : Optional['DataverseClient'], optional
            client class, by default None
        client_alias: Optional[str], by default None (should be provided if client is None)

        Returns
        -------
        (status, save_path): tuple[bool, str]
            the first item means whether the download success or not
            the second item shows the save_path
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            resp = api.get_ml_model_file(
                model_id=model_id, timeout=timeout, model_format="onnx"
            )
            download_file_from_response(response=resp, save_path=save_path)
            return True, save_path
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception:
            logging.exception("Failed to get onnx model file")
            return False, save_path

    def get_dataset(self, dataset_id: int, client_alias: Optional[str] = None):
        """Get dataset detail and status by id

        Parameters
        ----------
        dataset_id : int
            dataset-id in db

        Returns
        -------
        Dataset
            dataset basemodel from host response for client usage
        client_alias: Optional[str], by default None (will reset to self.alias if it's not provided)

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        if client_alias is None:
            client_alias = self.alias
        api, client_alias = DataverseClient._get_api_client(client_alias=client_alias)
        try:
            dataset_data: dict = api.get_dataset(dataset_id=dataset_id)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the dataset: {e}")

        project = self.get_project(dataset_data["project"]["id"])
        sensors = [
            Sensor.create(sensor_data) for sensor_data in dataset_data["sensors"]
        ]
        dataset_data.update({"project": project, "sensors": sensors})
        return Dataset(**dataset_data, client_alias=client_alias)

    # TODO: required arguments for different DataSource
    @staticmethod
    def create_dataset(
        name: str,
        data_source: DataSource,
        project: Project,
        sensors: list[Sensor],
        type: DatasetType,
        annotation_format: AnnotationFormat,
        storage_url: str,
        data_folder: str,
        container_name: Optional[str] = None,
        sas_token: Optional[str] = None,
        annotations: Optional[list] = None,
        sequential: bool = False,
        generate_metadata: bool = False,
        auto_tagging: Optional[list] = None,
        render_pcd: bool = False,
        description: Optional[str] = None,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        **kwargs,
    ) -> Dataset:
        """Create Dataset

        Parameters
        ----------
        name : str
            name of dataset
        data_source : DataSource
            the DataSource basemodel of the given dataset
        project : Project
            Project basemodel
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
        annotations: list, optional
            list of annotation folder name (should be groundtruth or $model_name)
        sequential : bool, optional
            sequential or not., by default False
        generate_metadata : bool, optional
            generate meta data or not, by default False
        auto_tagging: list
            generate auto_tagging with target models (weather/scene/timeofday)
        description : Optional[str], optional
            description of the dataset, by default None
        render_pcd : bool, optional
            render pcd preview image or not, be default False
        access_key_id : Optional[str], optional
            access key id for AWS s3 bucket, by default None
        secret_access_key : Optional[str], optional
            secret access key for AWS s3 bucket, by default None
        client : Optional[DataverseClient]
            the client to be used to create the dataset, will use the default client if it's None

        Returns
        -------
        Dataset
            Dataset Basemodel

        Raises
        ------
        ValueError
        ValidationError
            raise exception if there is any error occurs when composing request body.
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        if annotations is None:
            annotations = []
        if auto_tagging is None:
            auto_tagging = []

        if type == DatasetType.ANNOTATED_DATA and len(annotations) == 0:
            raise ValueError(
                "Annotated data should provide at least one annotation folder name (groundtruth or model_name)"
            )
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        sensor_ids = [sensor.id for sensor in sensors]
        project_id = project.id
        try:
            raw_dataset_data: dict = DatasetAPISchema(
                name=name,
                project_id=project_id,
                sensor_ids=sensor_ids,
                data_source=data_source,
                type=type,
                annotation_format=annotation_format,
                annotations=annotations,
                storage_url=storage_url,
                container_name=container_name,
                data_folder=data_folder,
                sas_token=sas_token,
                sequential=sequential,
                generate_metadata=generate_metadata,
                auto_tagging=auto_tagging,
                render_pcd=render_pcd,
                description=description,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                **kwargs,
            ).dict(exclude_none=True)
        except ValidationError as e:
            raise ValidationError(
                f"Something wrong when composing the final dataset data: {e}"
            )

        if data_source == DataSource.LOCAL:
            create_dataset_uuid = DataverseClient.upload_files_from_local(
                api, raw_dataset_data
            )
            raw_dataset_data["create_dataset_uuid"] = create_dataset_uuid
        dataset_data = api.create_dataset(**raw_dataset_data)
        dataset_data.update(
            {
                "project": project,
                "sensors": sensors,
                "sequential": sequential,
                "generate_metadata": generate_metadata,
                "auto_tagging": auto_tagging,
                "annotations": annotations,
            }
        )
        return Dataset(**dataset_data, client_alias=client_alias)

    @staticmethod
    def upload_files_from_local(api: BackendAPI, raw_dataset_data: dict) -> dict:
        loop = asyncio.get_event_loop()
        data_folder = raw_dataset_data["data_folder"]
        # TODO: support more format
        # Current only support scanning data_folder
        # meaning , support Visionai format
        # raw_dataset_data.get("annotation_folder"),
        # raw_dataset_data.get("calibration_folder"),
        # raw_dataset_data.get("lidar_folder"),
        file_paths = DataverseClient._find_all_paths(data_folder)
        upload_task_queue, create_dataset_uuid, failed_urls = loop.run_until_complete(
            DataverseClient.run_generate_presigned_urls(
                file_paths=file_paths, api=api, data_folder=data_folder
            )
        )
        if failed_urls:
            raise ClientConnectionError(f"unable to generate urls for: {failed_urls}")

        if not create_dataset_uuid:
            raise ClientConnectionError(
                "something went wrong, missing create dataset uuid"
            )

        failed_urls = loop.run_until_complete(
            DataverseClient.run_upload_tasks(upload_task_queue)
        )
        if failed_urls:
            raise ClientConnectionError(f"failed to upload urls: {failed_urls}")
        return create_dataset_uuid

    @staticmethod
    async def run_generate_presigned_urls(
        file_paths: list, api: BackendAPI, data_folder: str
    ) -> tuple[deque, str, list[str]]:
        max_retry_count, batch_size = 3, 50

        failed_urls = []
        upload_task_queue = deque()

        # TODO: convert the following code to async tasks loop
        generate_url_queue = deque()
        for i in range(0, len(file_paths), batch_size):
            generate_url_queue.append((file_paths[i : i + batch_size], 0))

        create_dataset_uuid: str = None
        while len(generate_url_queue) != 0:
            batched_file_paths, retry_count = generate_url_queue.popleft()
            if retry_count >= max_retry_count:
                failed_urls.extend(batched_file_paths)

            # NOTE: This is extremely slow to do it over here
            # this replaces the full file path to relative file path
            # i.e <long data folder path>/data/image.jpg -> /data/image.jpg
            filtered_paths = [
                path.replace(data_folder, "") for path in batched_file_paths
            ]
            try:
                resp = api.generate_presigned_url(
                    file_paths=filtered_paths,
                    create_dataset_uuid=create_dataset_uuid,
                    data_source=DataSource.LOCAL,
                )
                url_infos: list[dict] = resp["url_info"]
                create_dataset_uuid = resp["dataset_info"]["create_dataset_uuid"]

                upload_task_queue.append((batched_file_paths, url_infos))

            except KeyError:
                logging.exception("Is api schema changed?")
                raise
            except DataverseExceptionBase:
                logging.exception("Got api error from Dataverse")
                raise
            except Exception:
                generate_url_queue.append((batched_file_paths, retry_count + 1))
        return upload_task_queue, create_dataset_uuid, failed_urls

    @staticmethod
    async def run_upload_tasks(upload_task_queue: deque) -> list[str]:
        tasks = []
        client = AsyncThirdPartyAPI()
        for batched_file_paths, upload_file_infos in upload_task_queue:

            async def f(
                paths: list[str],
                upload_infos: list[dict],
                async_client: AsyncThirdPartyAPI,
            ) -> list[str]:
                failed_urls = []
                for path, info in zip(paths, upload_infos):
                    try:
                        with open(path, "rb") as file:
                            await async_client.upload_file(
                                method=info["method"],
                                target_url=info["url"],
                                file=file.read(),
                                content_type=info["content_type"],
                            )
                    except Exception as e:
                        logging.exception(e)
                        failed_urls.append(path)

                return failed_urls

            tasks.append(
                f(
                    paths=batched_file_paths,
                    upload_infos=upload_file_infos,
                    async_client=client,
                )
            )

        failed_urls = []
        for results in await asyncio.gather(*tasks):
            failed_urls.extend(results)
        return failed_urls

    @staticmethod
    def _find_all_paths(*paths) -> list[str]:
        all_filepaths: list[str] = []
        for path in paths:
            all_filepaths.extend(get_filepaths(path))
        return all_filepaths


class AsyncThirdPartyAPI:
    transport = AsyncHTTPTransport(
        retries=5,
    )

    def __init__(self):
        self.client = AsyncClient(transport=self.transport, timeout=Timeout(5))

    async def async_send_request(self, url: str, method: str, **kwargs) -> Response:
        try:
            resp: Response = await self.client.request(method=method, url=url, **kwargs)

        except Exception:
            logging.exception("async send request error")

        if not 200 <= resp.status_code <= 299:
            raise AsyncThirdPartyAPIException(
                status_code=resp.status_code, detail=resp.content
            )

        return resp

    async def upload_file(
        self, method: str, target_url: str, file: bytes, content_type: str
    ):
        await self.async_send_request(
            method=method,
            url=target_url,
            content=file,
            headers={"Content-Type": content_type},
        )
