from io import BytesIO
from os.path import isfile
from typing import Optional

from pydantic import ValidationError

from .apis.backend import BackendAPI
from .connections import add_connection, get_connection
from .constants import DataverseHost
from .exceptions.client import ClientConnectionError
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
    Project,
    ProjectTag,
    Sensor,
)
from .schemas.common import AnnotationFormat, DatasetType, OntologyImageType, SensorType
from .utils.utils import get_filepaths


class DataverseClient:
    def __init__(
        self,
        host: DataverseHost,
        email: Optional[str] = None,
        password: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        alias: str = "default",
    ) -> None:
        """
        Instantiate a Dataverse client.
        Parameters
        ----------
        host : DataverseHost
        email : Optional[str], optional
        password : Optional[str], optional
        access_token : Optional[str], optional
        refresh_token : Optional[str], optional
        Raises
        ------
        ValueError
        """
        if host not in DataverseHost:
            raise ValueError("Invalid dataverse host, if the host is available?")
        self.host = host
        self._api_client = None
        self._init_api_client(
            email=email,
            password=password,
            access_token=access_token,
            refresh_token=refresh_token,
        )
        add_connection(alias=alias, conn=self)

    def _init_api_client(
        self,
        email: Optional[str] = None,
        password: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ) -> None:
        try:
            self._api_client = BackendAPI(
                host=self.host,
                email=email,
                password=password,
                access_token=access_token,
                refresh_token=refresh_token,
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to initialize the api client: {e}")

    @staticmethod
    def get_client(alias: str = "default") -> "DataverseClient":
        try:
            return get_connection(alias)
        except KeyError:
            raise

    @staticmethod
    def create_project(
        name: str,
        ontology: Ontology,
        sensors: list[Sensor],
        project_tag: Optional[ProjectTag] = None,
        description: Optional[str] = None,
        client: Optional["DataverseClient"] = None,
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
        client : Optional[DataverseClient]
            the client to be used to create the project, will use the default client if it's None

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

        if client is None:
            client = DataverseClient.get_client()

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
            project_data: dict = client._api_client.create_project(**raw_project_data)
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")
        return Project.create(project_data)

    def list_projects(
        self,
        current_user: bool = True,
        exclude_sensor_type: Optional[SensorType] = None,
        image_type: Optional[OntologyImageType] = None,
    ) -> list:
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
        list
            list of projects [{'id': 5, 'name': 'Kitti Sequential Project'}, {'id': 6, 'name': 'project2'}]

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
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the projects: {e}")
        return project_list

    def get_project(self, project_id: int):
        """Get project detail by project-id

        Parameters
        ----------
        project_id : int
            project-id in db

        Returns
        -------
        Project
            Project basemodel from host response for client usage

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """

        try:
            project_data: dict = self._api_client.get_project(project_id=project_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the project: {e}")
        return Project.create(project_data)

    @staticmethod
    def list_models(
        project_id: int,
        client: Optional["DataverseClient"] = None,
    ) -> list:
        """Get the model list by project id

        Parameters
        ----------
        project_id : int
        client : Optional["DataverseClient"], optional
            clientclass, by default None

        Returns
        -------
        list
            model list from api response

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        if client is None:
            client = DataverseClient.get_client()
        api = client._api_client
        try:
            model_list: list = api.list_ml_models(project_id=project_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the models: {e}")
        return model_list

    @staticmethod
    def get_model(
        model_id: int,
        client: Optional["DataverseClient"] = None,
        project: Optional["Project"] = None,
    ) -> MLModel:
        """get the model detail by model id

        Parameters
        ----------
        model_id : int
        client : Optional["DataverseClient&quot"], optional
            client class, by default None
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
        if client is None:
            client = DataverseClient.get_client()
        api = client._api_client
        try:
            model_data: dict = api.get_ml_model(model_id=model_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the dataset: {e}")

        target_class_id = {
            ontology_class["id"] for ontology_class in model_data["classes"]
        }
        if project is None:
            project = client.get_project(project_id=model_data["project"]["id"])
        # get classes used in the model
        classes = [
            ontology_class
            for ontology_class in project.ontology.classes
            if ontology_class.id in target_class_id
        ]
        model_data.update({"id": model_id, "project": project, "classes": classes})
        return MLModel(**model_data)

    @staticmethod
    def get_label_file(
        model_id: int, client: Optional["DataverseClient"] = None
    ) -> Optional[BytesIO]:
        if client is None:
            client = DataverseClient.get_client()
        api = client._api_client
        try:
            labels: BytesIO = api.get_ml_model_labels(model_id=model_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get model labels: {e}")
        if labels:
            return labels

    @staticmethod
    def get_triton_model_file(
        model_id: int, client: Optional["DataverseClient"] = None
    ) -> Optional[BytesIO]:
        if client is None:
            client = DataverseClient.get_client()
        api = client._api_client
        try:
            model_file: BytesIO = api.get_ml_model_file(model_id=model_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get triton model file: {e}")
        if model_file:
            return model_file

    def get_dataset(self, dataset_id: int):
        """Get dataset detail and status by id

        Parameters
        ----------
        dataset_id : int
            dataset-id in db

        Returns
        -------
        Dataset
            dataset basemodel from host response for client usage

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """

        try:
            dataset_data: dict = self._api_client.get_dataset(dataset_id=dataset_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the dataset: {e}")

        project = self.get_project(dataset_data["project"]["id"])
        sensors = [
            Sensor.create(sensor_data) for sensor_data in dataset_data["sensors"]
        ]
        dataset_data.update({"project": project, "sensors": sensors})
        return Dataset(**dataset_data)

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
        if data_source not in DataSource:
            raise ValueError(f"Data source ({data_source}) is not supported currently.")

        if annotations is None:
            annotations = []
        if auto_tagging is None:
            auto_tagging = []

        if type == DatasetType.ANNOTATED_DATA and len(annotations) == 0:
            raise ValueError(
                "Annoted data should provide at least one annotation folder name (groundtruth or model_name)"
            )
        if client is None:
            client = DataverseClient.get_client()

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
                **kwargs,
            ).dict(exclude_none=True)
        except ValidationError as e:
            raise ValidationError(
                f"Something wrong when composing the final dataset data: {e}"
            )

        api = client._api_client

        try:
            dataset_data = api.create_dataset(**raw_dataset_data)
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the dataset: {e}")

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

        if data_source in {DataSource.Azure, DataSource.AWS}:
            return Dataset(**dataset_data)

        # start uploading from local
        folder_paths: list[Optional[str]] = [
            raw_dataset_data.get("data_folder"),
            raw_dataset_data.get("annotation_folder"),
            raw_dataset_data.get("calibration_folder"),
            raw_dataset_data.get("lidar_folder"),
        ]
        annotation_file = raw_dataset_data.get("annotation_file")

        all_filepaths: list[str] = []
        for path in folder_paths:
            if path is not None:
                all_filepaths.extend(get_filepaths(path))

        if annotation_file is not None:
            if not isfile(annotation_file):
                raise ValueError("annotation_file expects a file destination")

            all_filepaths.append(annotation_file)

        # TODO: find a better way to get client_container_name
        # instead of request backend again (generated while create dataset)
        container_name: dict = api.get_dataset(dataset_data["id"])[
            "client_container_name"
        ]

        try:
            batch_size = 5
            for i in range(0, len(all_filepaths), batch_size):
                file_dict: dict[str, bytes] = {
                    fpath: open(fpath, "rb").read()
                    for fpath in all_filepaths[i : i + batch_size]
                }

                api.upload_files(
                    dataset_id=dataset_data["id"],
                    container_name=container_name,
                    file_dict=file_dict,
                    is_finished=False,
                )

            # request finished status to backend
            api.upload_files(
                dataset_id=dataset_data["id"],
                container_name=container_name,
                is_finished=True,
                file_dict=dict(),
            )
        except Exception as e:
            raise ClientConnectionError(f"failed to upload files: {e}")

        return Dataset(**dataset_data)
