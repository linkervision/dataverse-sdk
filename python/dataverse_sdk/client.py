from typing import Optional

from .apis.backend import BackendAPI
from .constants import DataverseHost
from .exceptions.client import ClientConnectionError
from .schemas.api import AttributeAPISchema, OntologyAPISchema
from .schemas.client import DataConfig, Dataset, DataSource, Ontology, Project, Sensor


class DataverseClient:
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
        try:
            project_data: dict = self._api_client.create_project(
                name=name,
                ontology_data=ontology_data,
                sensor_data=sensor_data,
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")
        return Project(client=self, **project_data)

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
        return Project(client=self, **project_data)

    def create_dataset(
        self, name: str, source: DataSource, project: Project, dataset: DataConfig
    ) -> Dataset:
        """Creates dataset
        Parameters
        ----------
        name : str
            name of dataset
        source : DataSource
            the DataSource basemodel of the given dataset
        project_info: Project
            Project basemodel from host response for client usage
        dataset: dict
            Dataset infomation from config or user setting
        Returns
        -------
        project : Project
            Project basemodel from host response for client usage
        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs when creating dataset
        NotImplementedError
            raise error if datasource is not supported
        """
        if source not in {DataSource.Azure, DataSource.AWS}:
            raise NotImplementedError
        # TODO: add local upload here
        try:
            dataset_data: dict = self._api_client.create_dataset(
                name=name,
                source=source,
                project=project.dict(),
                dataset=dataset.dict(),
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the dataset: {e}")

        return Dataset(client=self, **dataset_data)
