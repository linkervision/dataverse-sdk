from typing import Optional

from .apis.backend import BackendAPI
from .constants import DataverseHost
from .exceptions.client import ClientConnectionError
from .schemas import (
    Attribute,
    AttributeOption,
    Ontology,
    OntologyClass,
    Project,
    Sensor,
)


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

        ontology_dict: dict = ontology.dict(exclude_none=True)
        classes_data_list: list[OntologyClass] = []

        try:
            # remove `id` field in OntologyClass, Attribute, and AttributeOption
            for cls_ in ontology_dict["ontology_classes_data"]:
                cls_.pop("id", None)
                if not (cur_attrs := cls_.get("attribute_data")):
                    classes_data_list.append(cls_)
                    continue
                new_attribute_list = []
                for attr in cur_attrs:
                    attr.pop("id", None)
                    new_opt_list = []
                    if attr["type"] != "option":
                        new_attribute_list.append(attr)
                        continue
                    for opt_data in attr.get("option_data", []):
                        opt_data.pop("id", None)
                        new_opt_list.append(opt_data["value"])
                    attr["option_data"] = new_opt_list
                    new_attribute_list.append(attr)
                cls_["attribute"] = new_attribute_list
                classes_data_list.append(cls_)
            project_data: dict = self._api_client.create_project(
                name=name,
                ontology_data={
                    "name": ontology_dict["name"],
                    "image_type": ontology_dict["image_type"],
                    "ontology_classes_data": classes_data_list,
                },
                sensor_data=[
                    {"name": sensor.name, "type": sensor.type} for sensor in sensors
                ],
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")

        ontology_data: dict = project_data["ontology"]
        sensor_data: list[dict] = project_data["sensors"]

        resp_ontology_data: list[dict] = ontology_data["classes"]
        new_ontology_classes_data = []
        for classes_data in resp_ontology_data:

            cls_attrs: list[dict] = classes_data.get("attributes", [])
            new_attribute_list: list[Attribute] = []
            for attr in cls_attrs:
                attr_opts = attr.get("options", [])
                new_opts: list[AttributeOption] = []
                for opt in attr_opts:
                    new_opts.append(AttributeOption(id=opt["id"], value=opt["value"]))
                new_attribute_list.append(
                    Attribute(
                        id=attr["id"],
                        name=attr["name"],
                        type=attr["type"],
                        option_data=new_opts if new_opts else None,
                    )
                )

            new_ontology_classes_data.append(
                OntologyClass(
                    name=classes_data.get("name"),
                    id=classes_data.get("id"),
                    rank=classes_data.get("rank"),
                    color=classes_data.get("color"),
                    attribute_data=new_attribute_list,
                )
            )

        return Project(
            id=project_data["id"],
            name=project_data["name"],
            ontology_data=Ontology(
                id=ontology_data["id"],
                name=ontology_data["name"],
                image_type=ontology_data["image_type"],
                pcd_type=ontology_data["pcd_type"],
                ontology_classes_data=new_ontology_classes_data,
            ),
            sensor_data=[
                Sensor(id=sensor["id"], type=sensor["type"], name=sensor["name"])
                for sensor in sensor_data
            ],
        )
