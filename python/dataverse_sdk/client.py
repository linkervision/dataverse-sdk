from typing import Optional

from .apis.backend import BackendAPI
from .constants import DataverseHost
from .exceptions.client import ClientConnectionError
from .schemas.api import AttributeAPISchema, OntologyAPISchema, OntologyClassAPISchema
from .schemas.client import Ontology, Project, Sensor


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
            the Ontology BaseModel data from client
        sensors : list[ClientSensor]
            the list of Sensor BaseModel data from client

        Returns
        -------
        Project
            return Project BaseModel

        Raises
        ------
        ClientConnectionError
            raise exception if there is any error occurs
        """

        ontology_data: dict = ontology.dict(exclude_none=True)
        classes_data_list: list[OntologyClassAPISchema] = []
        project_data: Project = {}
        try:
            # remove `id` field in OntologyClass and Attribute
            for cls_ in ontology_data["classes"]:
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

            ontology_data = OntologyAPISchema(
                **{
                    "name": ontology_data["name"],
                    "image_type": ontology_data["image_type"],
                    "ontology_classes_data": classes_data_list,
                }
            ).dict(exclude_none=True)

            project_data: dict = self._api_client.create_project(
                name=name,
                ontology_data=ontology_data,
                sensor_data=[
                    {"name": sensor.name, "type": sensor.type} for sensor in sensors
                ],
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")
        return Project(**project_data)
