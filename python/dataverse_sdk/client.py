from typing import Optional

from .apis.backend import BackendAPI
from .constants import DataverseHost
from .exceptions.client import ClientConnectionError, InvalidParameters
from .schemas.api import AttributeAPISchema, DatasetAPISchema, OntologyAPISchema
from .schemas.client import Dataset, DataSource, Ontology, Project, Sensor
from .schemas.common import AnnotationFormat, DatasetType
from os.path import isfile
from .utils.utils import get_file_recursive

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
        api = self.get_current_client()
        try:
            project_data: dict = api.create_project(
                name=name,
                ontology_data=ontology_data,
                sensor_data=sensor_data,
            )
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the project: {e}")
        return Project(**project_data)

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
            raise error if there is any error occurs
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

        Returns
        -------
        Dataset
            Dataset Basemodel

        Raises
        ------
        NotImplementedError
            raise error if datasource is not supported
        ClientConnectionError
             raise exception if there is any error occurs when creating dataset
        """

        sensor_ids = [sensor.id for sensor in sensors]
        project_id = project.id
        try:
            dataset: dict = DatasetAPISchema(
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
                **kwargs
            ).dict(exclude_none=True)
        except Exception as e:
            raise InvalidParameters(f"Invalid parameters: {e}")

        api = self.get_current_client()
        resp = self.__create_dataset(dataset, api)
        if data_source in {DataSource.Azure, DataSource.AWS}:
            resp.pop("project")
            resp.pop("sensors")
            return Dataset(project=project, sensors=sensors, **resp)

        ## start uploading from local
        folder_paths: list[Optional[str]] = [
            dataset.get("data_folder"),
            dataset.get("annotation_folder"),
            dataset.get("calibration_folder"),
            dataset.get("lidar_folder")
        ]
        annotation_file = dataset.get("annotation_file")

        # find folders recursively
        all_filepaths: list[str] = []
        for path in folder_paths:
            if path is not None:
                all_filepaths.extend(get_file_recursive(path))

        if annotation_file is not None:
            if not isfile(annotation_file):
                raise ValueError("annotation_file expects a file destination")

            all_filepaths.append(annotation_file)


        # TODO: find a better way to get client_container_name
        # instead of request backend again
        # this client_container_name is generated by backend, its not truly from client
        container_name: dict = api.get_dataset(resp["id"])["client_container_name"]

        try:
            batch_size = 5
            for i in range(0, len(all_filepaths), batch_size):
                file_dict: dict[str, bytes] = {fpath: open(fpath, "rb").read() 
                                for fpath in all_filepaths[i: i+batch_size]
                            }

                api.upload_files(
                    dataset_id=resp["id"],
                    container_name=container_name,
                    file_dict=file_dict,
                    is_finished=False
                )

            ## request finished status to backend
            api.upload_files(
                dataset_id=resp["id"],
                container_name=container_name,
                is_finished=True,
                file_dict=dict()
            )
        except Exception as e:
            api.update_dataset(dataset_id=resp["id"], status="fail")
            raise ClientConnectionError(f"failed to upload files: {e}")


    def __create_dataset(self, dataset: dict, api) -> dict:
        try:
            dataset_data: dict = api.create_dataset(**dataset)
        except Exception as e:
            raise ClientConnectionError(f"Failed to create the dataset: {e}")
        return dataset_data
