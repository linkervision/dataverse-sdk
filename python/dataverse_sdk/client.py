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
)
from .schemas.client import Dataset, DataSource, Ontology, Project, Sensor
from .schemas.common import AnnotationFormat, DatasetType
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
            if not (cur_attrs := cls_.pop("attributes", None)):
                classes_data_list.append(cls_)
                continue
            new_attribute_list: list[AttributeAPISchema] = []
            for attr in cur_attrs:
                attr.pop("id", None)
                if attr["type"] != "option":
                    new_attribute_list.append(attr)
                    continue
                attr["option_data"] = [
                    opt_data["value"] for opt_data in attr.pop("options", [])
                ]
                new_attribute_list.append(attr)
            cls_["attribute_data"] = new_attribute_list
            classes_data_list.append(cls_)
        raw_ontology_data["ontology_classes_data"] = classes_data_list
        ontology_data = OntologyAPISchema(**raw_ontology_data).dict(exclude_none=True)
        sensor_data = [sensor.dict(exclude_none=True) for sensor in sensors]

        try:
            raw_project_data = ProjectAPISchema(
                name=name,
                ontology_data=ontology_data,
                sensor_data=sensor_data,
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
        sequential: bool = False,
        generate_metadata: bool = False,
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
        sequential : bool, optional
            sequential or not., by default False
        generate_metadata : bool, optional
            generate meta data or not, by default False
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
                storage_url=storage_url,
                container_name=container_name,
                data_folder=data_folder,
                sas_token=sas_token,
                sequential=sequential,
                generate_metadata=generate_metadata,
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
