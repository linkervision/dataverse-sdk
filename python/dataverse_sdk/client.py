from typing import Optional

from .apis.backend import BackendAPI
from .constants import DataverseHost
from .exceptions.client import ClientConnectionError
from .schemas.api import AttributeAPISchema, DatasetAPISchema, OntologyAPISchema
from .schemas.client import Dataset, DataSource, Ontology, Project, Sensor
from .schemas.common import AnnotationFormat, DatasetType


class DataverseClient:

    __client = None

    def __init__(
        self,
        host: DataverseHost,
        email: Optional[str] = None,
        password: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
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
        if DataverseClient.__client is not None:
            raise Exception("This class is a singleton class !")
        else:
            DataverseClient.__client = self._api_client
            import config

            config._client = self

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
    def get_current_client():
        if DataverseClient.__client is None:
            raise ClientConnectionError("Failed to get client info!")
        else:
            return DataverseClient.__client

    def create_project(
        self, name: str, ontology: Ontology, sensors: list[Sensor]
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
        Returns
        -------
        project : Project
            Project basemodel from host response for client usage

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs
        """
        raw_ontology_data: dict = ontology.dict(exclude_none=True)
        classes_data_list: list[dict] = []
        # remove `id` field in OntologyClass and Attribute
        for cls_ in raw_ontology_data.pop("classes", []):
            cls_.pop("id", None)
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
        # TODO: projectAPIschema
        apiclient = self.get_current_client()
        try:
            project_data: dict = apiclient.create_project(
                name=name,
                ontology_data=ontology_data,
                sensor_data=sensor_data,
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")
        return Project(**project_data)

    def get_project(self, project_id: int):
        """Get project detail by project-id
        Args:
            project_id (int): project-id in db
        Raises:
            ClientConnectionError: raise error if there is any error occurs
        Returns:
            project:  Project
                Project basemodel from host response for client usage
        """
        try:
            project_data: dict = self._api_client.get_project(project_id=project_id)
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the project: {e}")
        return Project(**project_data)

    def create_dataset(
        self,
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
        description: Optional[str] = None,
    ) -> Dataset:
        """Creates dataset

        Args:
            name (str): name of dataset
            data_source (DataSource): the DataSource basemodel of the given dataset
            project (Project): Project basemodel from host response for client usage
            sensors (list[Sensor]): list of Sensor basemodel
            type (DatasetType): datasettype (annotation or raw)
            annotation_format (AnnotationFormat): annotation format
            storage_url (str): dataset storage url
            data_folder (str): dataset storage folder
            container_name (Optional[str], optional): container name for Azure, Defaults to None.
            sas_token (Optional[str], optional): sas token for Azure, Defaults to None.
            sequential (bool, optional): sequential or not. Defaults to False.
            generate_metadata (bool, optional): generate metadata or not. Defaults to False.
            description (Optional[str], optional): description of dataset. Defaults to None.

        Raises:
            NotImplementedError: raise error if datasource is not supported
            ClientConnectionError: raise exception if there is any error occurs when creating dataset

        Returns:
            Dataset: Dataset Basemodel
        """

        sensor_ids = [sensor.id for sensor in sensors]
        project_id = project.id
        datasetapi_data = DatasetAPISchema(
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
            description=description,
        ).dict(exclude_none=True)

        if data_source not in {DataSource.Azure, DataSource.AWS}:
            raise NotImplementedError
        # TODO: add local upload here
        apiclient = self.get_current_client()
        try:
            dataset_data: dict = apiclient.create_dataset(**datasetapi_data)
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the dataset: {e}")
        dataset_data.pop("project")
        dataset_data.pop("sensors")

        return Dataset(project=project, sensors=sensors, **dataset_data)
