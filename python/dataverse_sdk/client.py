import asyncio
import json
import logging
import os
import platform
from asyncio import AbstractEventLoop, Semaphore
from collections import deque
from pathlib import Path
from typing import Optional, Union
from uuid import uuid4

from aiofiles import open as aio_open
from httpx import AsyncClient, AsyncHTTPTransport, Response, Timeout
from pydantic import ValidationError
from tqdm.asyncio import tqdm_asyncio

from .apis.backend import AsyncBackendAPI, BackendAPI
from .connections import add_connection, get_connection
from .constants import DataverseHost
from .exceptions.client import (
    APIValidationError,
    AsyncThirdPartyAPIException,
    ClientConnectionError,
    DataverseExceptionBase,
    InvalidProcessError,
)
from .schemas.api import (
    AttributeAPISchema,
    CreateCustomModelAPISchema,
    DatasetAPISchema,
    OntologyAPISchema,
    ProjectAPISchema,
    ProjectTagAPISchema,
    UpdateQuestionAPISchema,
    VQAProjectAPISchema,
)
from .schemas.client import (
    AttributeType,
    ConvertRecord,
    Dataset,
    Dataslice,
    DataSource,
    MLModel,
    Ontology,
    OntologyClass,
    Project,
    ProjectTag,
    QuestionClass,
    Sensor,
    UpdateQuestionClass,
)
from .schemas.common import AnnotationFormat, DatasetType, OntologyImageType, SensorType
from .utils.utils import (
    download_file_from_response,
    download_file_from_url,
    get_filepaths,
)


def is_macOS():
    return platform.system() == "Darwin"


# to avoid the `Too many open files` error in macOS
MAX_CONCURRENT_FILES = 70 if is_macOS() else 100


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
        self.host = host
        self._api_client = None
        self._async_api_client = None
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
            self._async_api_client = AsyncBackendAPI(
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
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        is_async: Optional[bool] = False,
    ) -> tuple[Union[BackendAPI, AsyncBackendAPI], str]:
        if client is None:
            if client_alias is None:
                raise ValueError(
                    "Please provide the DataverseClient or the connection alias!"
                )
            client = DataverseClient.get_client(client_alias)
        else:
            client_alias = client.alias

        if is_async:
            api = client._async_api_client
        else:
            api = client._api_client
        return api, client_alias

    @staticmethod
    def get_client(alias: str = "default") -> "DataverseClient":
        try:
            return get_connection(alias)
        except KeyError:
            raise

    def get_host(self):
        return self.host

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
        APIValidationError
            raise exception if there is any error occurs when composing the request body.
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        if ontology.image_type == OntologyImageType.VQA:
            raise InvalidProcessError(
                "Could not create VQA project by this function, please use create_vqa_project"
            )
        raw_ontology_data: dict = ontology.model_dump(exclude_none=True)
        classes_data_list: list[dict] = []
        rank = 1
        # remove `id` field in OntologyClass and Attribute
        for cls_ in raw_ontology_data.pop("classes", []):
            cls_.pop("id", None)
            if "rank" not in cls_:
                cls_["rank"] = rank
                rank += 1
            if not (obj_attrs := cls_.pop("attributes", None)):
                classes_data_list.append(cls_)
                continue
            cls_["attribute_data"] = parse_attribute(obj_attrs)
            classes_data_list.append(cls_)
        raw_ontology_data["ontology_classes_data"] = classes_data_list
        if project_tag is not None:
            raw_project_tag_data: dict = project_tag.model_dump(exclude_none=True)
            if tag_attrs := raw_project_tag_data.pop("attributes", None):
                raw_project_tag_data["attribute_data"] = parse_attribute(tag_attrs)
        else:
            raw_project_tag_data = {}

        ontology_data = OntologyAPISchema(**raw_ontology_data).model_dump(
            exclude_none=True
        )
        project_tag_data = ProjectTagAPISchema(**raw_project_tag_data).model_dump(
            exclude_none=True
        )
        sensor_data = [sensor.model_dump(exclude_none=True) for sensor in sensors]

        try:
            raw_project_data = ProjectAPISchema(
                name=name,
                ontology_data=ontology_data,
                sensor_data=sensor_data,
                project_tag_data=project_tag_data,
                description=description,
            ).model_dump(exclude_none=True)
        except ValidationError as e:
            raise APIValidationError(
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

    def create_vqa_project(
        self,
        name: str,
        sensor_name: str,
        ontology_name: str,
        question_answer: list[QuestionClass],
        description: Optional[str] = None,
    ) -> dict:
        """Create VQA project

        Parameters
        ----------
        name : str
            project name
        sensor_name : str
            camera sensor name
        ontology_name : str
        question_answer : list[QuestionClass]
            list of QuestionClass for specifying question and answer_type
        description : Optional[str], optional

        Returns
        -------
        dict
            {"id": project_id}

        Raises
        ------
        APIValidationError
            the parameters does not meet the requirements
        ClientConnectionError

        """
        try:
            vqa_project_data = VQAProjectAPISchema(
                name=name,
                sensor_name=sensor_name,
                ontology_name=ontology_name,
                question_answer=question_answer,
                description=description,
            ).model_dump(exclude_none=True)
        except ValidationError as e:
            raise APIValidationError(
                f"Something wrong when composing the vqa project data: {e}"
            )
        try:
            vqa_project = self._api_client.create_vqa_project(**vqa_project_data)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the projects: {e}")
        return vqa_project

    @staticmethod
    def _validate_edit_vqa_ontology(
        project: Project,
        create: Optional[list[QuestionClass]] = None,
        update: Optional[list[dict]] = None,
    ):
        if project.ontology.image_type != OntologyImageType.VQA:
            raise InvalidProcessError("The project type is not VQA!")
        if create:
            current_question_rank = {
                question.rank for question in project.ontology.classes
            }
            for new_question in create:
                if new_question.rank in current_question_rank:
                    raise APIValidationError(
                        f"The question rank id of {new_question} is duplicated."
                    )
        if update:
            current_question_classes = {q.rank: q for q in project.ontology.classes}
            for update_question in update:
                update_question = UpdateQuestionClass(**update_question)
                if update_question.rank not in current_question_classes:
                    raise APIValidationError(
                        f"The question rank of {update_question} is not in current vqa project"
                    )
                if not update_question.question and not update_question.options:
                    continue
                if update_question.options:
                    if (
                        current_question_classes[update_question.rank]
                        .attributes[0]
                        .type
                        != AttributeType.OPTION
                    ):
                        raise APIValidationError(
                            f"The answer type for Question{update_question.rank}  is not option"
                        )
                    current_option_set = {
                        op.value
                        for op in current_question_classes[update_question.rank]
                        .attributes[0]
                        .options
                    }
                    for option in update_question.options:
                        if option in current_option_set:
                            raise APIValidationError(
                                f"The option {option} is already existing in Question{update_question.rank}"
                            )

    @staticmethod
    def edit_vqa_ontology(
        project_id: int,
        ontology_name: str = "",
        create: Optional[list[QuestionClass]] = None,
        update: Optional[list[dict]] = None,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        project: Optional["Project"] = None,
    ) -> dict:
        """Edit VQA ontology

        Parameters
        ----------
        project_id : int
        ontology_name : str, optional
        create : list[QuestionClass], optional
        update : list[dict], optional
        client : Optional[&quot;DataverseClient&quot;], optional
        client_alias : Optional[str], optional
        project : Optional[&quot;Project&quot;], optional

        Returns
        -------
        dict
            {"id": project_id}

        Raises
        ------
        InvalidProcessError
            The project is not VQA image type
        APIValidationError
            The parameters is not meet the api requirements
        ClientConnectionError
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        if project is None:
            project = DataverseClient.get_client_project(
                project_id=project_id, client_alias=client_alias
            )
        # validating the edit vqa data
        DataverseClient._validate_edit_vqa_ontology(
            project=project, create=create, update=update
        )
        # prepare the edit vqa data
        edit_vqa_data = {}
        if ontology_name:
            edit_vqa_data["ontology_name"] = ontology_name
        if create:
            edit_vqa_data["create"] = [q.model_dump(exclude_none=True) for q in create]
        if update:
            question_table = {
                q.rank: {
                    "extended_class_id": q.extended_class["id"],
                    "attribute_id": q.attributes[0].id,
                }
                for q in project.ontology.classes
            }
            update_questions = []
            for update_question in update:
                update_question = UpdateQuestionClass(**update_question)
                if not update_question.question and not update_question.options:
                    continue
                update_question_data = {}
                # edit question string contents
                if update_question.question:
                    update_question_data["extended_class_id"] = question_table[
                        update_question.rank
                    ]["extended_class_id"]
                    update_question_data["question"] = update_question.question
                # add question options
                if update_question.options:
                    update_question_data["attribute_id"] = question_table[
                        update_question.rank
                    ]["attribute_id"]
                    update_question_data["options"] = update_question.options
                update_questions.append(
                    UpdateQuestionAPISchema(**update_question_data).model_dump(
                        exclude_none=True
                    )
                )
            edit_vqa_data["update"] = update_questions
        if not edit_vqa_data:
            raise APIValidationError(
                "Please specify at least one item for editing vqa ontology"
            )
        try:
            vqa_project = api.edit_vqa_ontology(
                project_id=project_id, edit_vqa_data=edit_vqa_data
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to edit the VQA project: {e}")
        return vqa_project

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

    def get_project(
        self, project_id: int, client_alias: Optional[str] = None
    ) -> Project:
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

    def get_dataslice(
        self,
        dataslice_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> Dataslice:
        if client_alias is None:
            client_alias = self.alias

        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            dataslice_data: dict = api.get_dataslice(dataslice_id=dataslice_id)
        except DataverseExceptionBase as e:
            logging.exception(f"Got api error from Dataverse: {e}")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the dataslice: {e}")
        return Dataslice(
            id=dataslice_data["id"],
            name=dataslice_data["name"],
            type=dataslice_data["type"],
            annotation_type=dataslice_data["annotation_type"],
            status=dataslice_data["status"],
            project=Project(**dataslice_data["project"], client_alias=self.alias),
            export_records=dataslice_data["export_records"],
        )

    def export_dataslice(
        self,
        dataslice_id: int,
        annotation_name: str = "",
        export_format: str = "",
        is_sequential: bool = False,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> dict:
        if client_alias is None:
            client_alias = self.alias

        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        dataslice_data = self.get_dataslice(dataslice_id=dataslice_id)
        is_vqa = dataslice_data.annotation_type == "vqa"
        if not annotation_name:
            # use default groundtruth name if annotation name is not provided
            annotation_name = "groundtruth" if is_vqa else "(gt)"
        if not export_format:
            # only allow export vlm format for vqa project and export visionai for other projects
            export_format = "vlm" if is_vqa else "visionai"
        logging.info(
            f"Export Dataslice-{dataslice_id} with annotaiton_name:{annotation_name} in {export_format} format"
        )
        try:
            export_record: dict = api.export_dataslice(
                dataslice_id=dataslice_id,
                annotation_name=annotation_name,
                export_format=export_format,
                is_sequential=is_sequential,
            )
        except DataverseExceptionBase as e:
            logging.exception(f"Got api error from Dataverse: {e}")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the dataslice: {e}")
        return export_record

    def download_export_dataslice_data(
        self, dataslice_id: int, export_record_id: int, save_path: str = "./export.zip"
    ) -> bool:
        export_record_exist = False
        dataslice_data = self.get_dataslice(dataslice_id=dataslice_id)
        for record in dataslice_data.export_records:
            if record["id"] == export_record_id:
                export_record_exist = True
                if (
                    record["destination"] == "direct_download"
                    and record["status"] == "complete"
                ):
                    download_file_from_url(url=record["url"], save_path=save_path)
                    return True
                else:
                    if record["status"] == "fail":
                        raise ValueError(
                            f"export fail for dataslice-{dataslice_id} with export_record_id {export_record_id}"
                        )
                break
        if not export_record_exist:
            raise ValueError(
                f"Can not find dataslice-{dataslice_id} with export_record_id {export_record_id}"
            )
        return False

    def get_question_list(
        self,
        project_id: int,
        output_file_path: str = "questions.json",
    ) -> list:
        """Get question list for VQA project

        Parameters
        ----------
        project_id : int
        output_file_path : str, optional
            the json file path, by default "questions.json"

        Returns
        -------
        list
            the output question list

        Raises
        ------
        InvalidProcessError

        """
        file_extension = os.path.splitext(output_file_path)[1]
        if file_extension != ".json":
            raise InvalidProcessError(
                f"Invalid path: {output_file_path}! Should provide file path with .json extension"
            )
        project = self.get_project(project_id=project_id)
        if project.ontology.image_type != OntologyImageType.VQA:
            raise InvalidProcessError("The project type is not VQA!")
        output_list = []
        for question in project.ontology.classes:
            answer = question.attributes[0]
            option_list = [opt.value for opt in answer.options]
            output_list.append(
                {
                    "question_id": question.rank,
                    "question": question.extended_class["question"],
                    "type": answer.type,
                    "options": option_list,
                }
            )
        import json

        with open(output_file_path, "w", newline="") as jsonfile:
            json.dump(output_list, jsonfile)
        return output_list

    def generate_alias_map(
        self, project_id: int, alias_file_path: str = "./alias.csv"
    ) -> str:
        """Generate alias map

        Parameters
        ----------
        project_id : int
        alias_file_path : str, optional
            file_path for saving alias.csv, by default "./alias.csv"

        Returns
        -------
        alias_file_path: str
        """

        file_extension = os.path.splitext(alias_file_path)[1]
        if file_extension != ".csv":
            raise InvalidProcessError(
                f"Invalid path: {alias_file_path}! Should provide file path with .csv extension"
            )

        project = self.get_project(project_id=project_id)

        alias_mapping = []
        for ontology_class in project.ontology.classes:
            class_alias = (
                ontology_class.aliases[0]["name"] if ontology_class.aliases else ""
            )
            alias_mapping.append(
                [
                    ontology_class.id,
                    "ontology_class",
                    ontology_class.name,
                    class_alias,
                ]
            )
            if ontology_class.attributes:
                for attr in ontology_class.attributes:
                    attr_alias = attr.aliases[0]["name"] if attr.aliases else ""
                    alias_mapping.append(
                        [
                            attr.id,
                            "attribute",
                            f"{ontology_class.name}--{attr.name}",
                            attr_alias,
                        ]
                    )
                    if attr.options:
                        for option in attr.options:
                            option_alias = (
                                option.aliases[0]["name"] if option.aliases else ""
                            )
                            alias_mapping.append(
                                [
                                    option.id,
                                    "option",
                                    f"{ontology_class.name}--{attr.name}--{option.value}",
                                    option_alias,
                                ]
                            )

        # add project tags attributes/option to alias map
        for attr in project.project_tag.attributes:
            attr_alias = attr.aliases[0]["name"] if attr.aliases else ""
            alias_mapping.append(
                [
                    attr.id,
                    "attribute",
                    f"**tagging--{attr.name}",
                    attr_alias,
                ]
            )
            if attr.options:
                for option in attr.options:
                    option_alias = option.aliases[0]["name"] if option.aliases else ""
                    alias_mapping.append(
                        [
                            option.id,
                            "option",
                            f"**tagging--{attr.name}--{option.value}",
                            option_alias,
                        ]
                    )

        # output alias mapping to csv
        import csv

        # field names
        fields = ["ID", "type", "class--attribute--option", "alias"]

        with open(alias_file_path, "w") as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)

            write.writerow(fields)
            write.writerows(alias_mapping)
        logging.info(f"Alias file has been saved as {alias_file_path}")
        return alias_file_path

    def update_alias(self, project_id: int, alias_file_path: str):
        file_extension = os.path.splitext(alias_file_path)[1]
        if file_extension != ".csv":
            raise InvalidProcessError(
                f"Invalid path: {alias_file_path}! Should provide file path with .csv extension"
            )
        project = self.get_project(project_id=project_id)
        project_ontology_ids = {
            "ontology_class": {},
            "attribute": {},
            "option": {},
        }
        for ontology_class in project.ontology.classes:
            project_ontology_ids["ontology_class"][ontology_class.id] = (
                ontology_class.aliases
            )
            for attr in ontology_class.attributes:
                project_ontology_ids["attribute"][attr.id] = attr.aliases
                for option in attr.options:
                    project_ontology_ids["option"][option.id] = option.aliases
        for attr in project.project_tag.attributes:
            project_ontology_ids["attribute"][attr.id] = attr.aliases
            for option in attr.options:
                project_ontology_ids["option"][option.id] = option.aliases

        import csv

        alias_list = []
        try:
            with open(alias_file_path, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if int(row["ID"]) in project_ontology_ids[row["type"]]:
                        if (
                            not project_ontology_ids[row["type"]][int(row["ID"])]
                            and not row["alias"]
                        ):
                            # ignore alias for both before-update and after-update are empty
                            continue
                        if (
                            row["alias"]
                            in project_ontology_ids[row["type"]][int(row["ID"])]
                        ):
                            # ignore alias is same as current setting
                            continue
                        alias_list.append(
                            {row["type"]: int(row["ID"]), "name": row["alias"]}
                        )
                        project_ontology_ids[row["type"]].pop(int(row["ID"]))
                    else:
                        print(
                            f"The ID {int(row['ID'])}, {row['alias']}, is not belong to {row['type']} \
of this project OR has been added before"
                        )
        except FileNotFoundError as file_not_found:
            raise InvalidProcessError(f"File Not Found: {file_not_found}")

        if not alias_list:
            raise InvalidProcessError("No valid alias for updating")

        try:
            resp = self._api_client.update_alias(
                project_id=project_id, alias_list=alias_list
            )
            logging.info("Alias is updated.")
        except DataverseExceptionBase as api_error:
            logging.exception(
                f"Got [{api_error.status_code}] api error from Dataverse: {api_error.error}"
            )
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to edit the project alias: {e}")
        return resp.json()

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
        if project.ontology.image_type == OntologyImageType.VQA:
            raise InvalidProcessError("Could not add project_tag for VQA project")

        raw_project_tag: dict = project_tag.model_dump(exclude_none=True)
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
        if project.ontology.image_type == OntologyImageType.VQA:
            raise InvalidProcessError("Could not edit project_tag for VQA project")

        raw_project_tag: dict = project_tag.model_dump(exclude_none=True)
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
        if project.ontology.image_type == OntologyImageType.VQA:
            raise InvalidProcessError("Could not add ontology_classes for VQA project")
        project_classes_rank_set = {r.rank for r in project.ontology.classes}

        # new ontology classes to be created
        new_classes_data = []
        for ontology_class in ontology_classes:
            raw_ontology_class: dict = ontology_class.model_dump(exclude_none=True)
            attribute_data: list = parse_attribute(
                raw_ontology_class.get("attributes", [])
            )
            if ontology_class.rank in project_classes_rank_set:
                raise InvalidProcessError(
                    f"Class rank of {ontology_class} is duplicated to current classes."
                )
            new_classes_data.append(
                {
                    "name": ontology_class.name,
                    "color": ontology_class.color,
                    "attribute_data": attribute_data,
                    "rank": ontology_class.rank,
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
        if project.ontology.image_type == OntologyImageType.VQA:
            raise InvalidProcessError("Could not edit ontology_classes for VQA project")
        # ontology classes to be edited
        patched_classes_data = []
        for ontology_class in ontology_classes:
            raw_ontology_class: dict = ontology_class.model_dump(exclude_none=True)
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
    def list_dataslices(
        project_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> list:
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            dataslice_list: list = api.list_dataslices(project_id=project_id)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the models: {e}")
        return dataslice_list

    @staticmethod
    def list_datasets(
        project_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> list:
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            dataset_list: list = api.list_datasets(project_id=project_id)
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the models: {e}")
        return dataset_list

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
    def get_convert_record(
        convert_record_id: int,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> ConvertRecord:
        """Get model convert record

        Parameters
        ----------
        convert_record_id : int
        client : Optional[&quot;DataverseClient&quot;], optional
        client_alias : Optional[str], optional

        Returns
        -------
        ConvertRecord

        Raises
        ------
        ClientConnectionError
        """
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            convert_record: dict = api.get_convert_record(
                convert_record_id=convert_record_id
            )
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            raise ClientConnectionError(f"Failed to get the model: {e}")
        return ConvertRecord(
            id=convert_record_id,
            name=convert_record["name"],
            configuration=convert_record.get("configuration", {}),
            status=convert_record["status"],
            trait=convert_record.get("trait", {}),
            client_alias=client_alias,
        )

    @staticmethod
    def get_label_file(
        convert_record_id: int,
        save_path: str = "./labels.txt",
        timeout: int = 3000,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Download the model label file (which is a string txt file)

        Parameters
        ----------
        convert_record_id : int
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
            resp = api.get_convert_model_labels(
                convert_record_id=convert_record_id, timeout=timeout
            )
            download_file_from_response(response=resp, save_path=save_path)
            return True, save_path
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception:
            logging.exception("Failed to get model label file")
            return False, save_path

    @staticmethod
    def get_onnx_model_file(
        convert_record_id: int,
        save_path: str = "./model.onnx",
        timeout: int = 3000,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> tuple[bool, str]:
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias
        )
        try:
            resp = api.get_convert_onnx_model(
                convert_record_id=convert_record_id,
                timeout=timeout,
            )
            download_file_from_response(response=resp, save_path=save_path)
            return True, save_path
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception:
            logging.exception("Failed to get the onnx model file")
            return False, save_path

    @staticmethod
    def get_convert_model_file(
        convert_record_id: int,
        save_path: str = "./triton.zip",
        triton_format: bool = True,
        timeout: int = 3000,
        permission: str = "",
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Download convert model file

        Parameters
        ----------
        convert_record_id : int
        save_path : str, optional
            local path for saving the model file, by default './triton.zip'
        triton_format: bool, default=True
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
            resp = api.get_convert_model_file(
                convert_record_id=convert_record_id,
                triton_format=triton_format,
                timeout=timeout,
                permission=permission,
            )
            download_file_from_response(response=resp, save_path=save_path)
            return True, save_path
        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception:
            logging.exception("Failed to get the convert model file")
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
        dataset_data.update({"project": project})
        return Dataset(**dataset_data, client_alias=client_alias)

    # TODO: required arguments for different DataSource
    @staticmethod
    def create_dataset(
        name: str,
        data_source: DataSource,
        project: Project,
        type: DatasetType,
        annotation_format: AnnotationFormat,
        storage_url: str,
        data_folder: str,
        container_name: Optional[str] = None,
        sas_token: Optional[str] = None,
        annotations: Optional[list] = None,
        sequential: bool = False,
        generate_metadata: bool = False,
        render_pcd: bool = False,
        description: Optional[str] = None,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        reupload_dataset_uuid: Optional[str] = None,
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
        APIValidationError
            raise exception if there is any error occurs when composing request body.
        ClientConnectionError
            raise exception if there is any error occurs when calling backend APIs.
        """
        if annotations is None:
            annotations = []

        if type == DatasetType.ANNOTATED_DATA and len(annotations) == 0:
            raise ValueError(
                "Annotated data should provide at least one annotation folder name (groundtruth or model_name)"
            )
        api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias, is_async=False
        )
        async_api, client_alias = DataverseClient._get_api_client(
            client=client, client_alias=client_alias, is_async=True
        )

        host = api.get_host()
        if data_source != DataSource.LOCAL:
            if host not in DataverseHost:
                raise ValueError(
                    "Import data source must be LOCAL if host is not in DataverseHost."
                )

        project_id = project.id
        try:
            raw_dataset_data: dict = DatasetAPISchema(
                name=name,
                project_id=project_id,
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
                render_pcd=render_pcd,
                description=description,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                **kwargs,
            ).model_dump(exclude_none=True)
        except ValidationError as e:
            raise APIValidationError(
                f"Something wrong when composing the final dataset data: {e}"
            )

        if data_source == DataSource.LOCAL:
            create_dataset_uuid = DataverseClient.upload_files_from_local(
                async_api, api, raw_dataset_data, reupload_dataset_uuid
            )
            raw_dataset_data["create_dataset_uuid"] = create_dataset_uuid

        dataset_data = api.create_dataset(**raw_dataset_data)
        dataset_data.update(
            {
                "project": project,
                "sequential": sequential,
                "generate_metadata": generate_metadata,
                "annotations": annotations,
            }
        )
        return Dataset(**dataset_data, client_alias=client_alias)

    @staticmethod
    def upload_files_from_local(
        async_api: AsyncBackendAPI,
        api: BackendAPI,
        raw_dataset_data: dict,
        reupload_dataset_uuid: Optional[str] = None,
    ) -> str:
        def run_new_upload_tasks(
            data_folder: str,
            dataset_type: DatasetType,
            async_api_client: AsyncBackendAPI,
            event_loop: AbstractEventLoop,
        ):
            print(f"Uploading new dataset from [{data_folder}]...")

            # check folder structure
            required_data = DataverseClient._get_format_folders(
                annotation_format=raw_dataset_data["annotation_format"],
                dataset_type=dataset_type,
                project_id=raw_dataset_data["project_id"],
                api=api,
            )
            if required_data:
                for required_folder_or_file in required_data:
                    path = os.path.join(data_folder, required_folder_or_file)
                    if not os.path.exists(path):
                        raise DataverseExceptionBase(
                            type="",
                            detail=f"Require the file or folder: {path} for {raw_dataset_data['annotation_format']}",
                        )

            file_paths = DataverseClient._find_all_paths(data_folder)
            (
                upload_task_queue,
                create_dataset_uuid,
                failed_urls,
            ) = asyncio.run(
                DataverseClient.run_generate_presigned_urls(
                    file_paths=file_paths, api=async_api_client, data_folder=data_folder
                )
            )
            if failed_urls:
                raise ClientConnectionError(
                    f"unable to generate urls for: {failed_urls}"
                )

            if not create_dataset_uuid:
                raise ClientConnectionError(
                    "something went wrong, missing create dataset uuid"
                )

            failed_file_info_batches = asyncio.run(
                DataverseClient.run_upload_tasks(upload_task_queue)
            )

            return create_dataset_uuid, failed_file_info_batches

        def run_reupload_tasks(
            reupload_dataset_uuid: str,
            provided_data_folder: str,
            event_loop: AbstractEventLoop,
        ):
            print(f"Reuploading dataset from [{provided_data_folder}]...")

            prev_failed_report_path = (
                Path.cwd() / "report" / reupload_dataset_uuid / "failed_upload.json"
            )

            if not prev_failed_report_path.exists():
                raise DataverseExceptionBase(
                    detail=(
                        f"Failed upload report not found at [{prev_failed_report_path}]; "
                        f"cannot proceed with reuploading dataset [{reupload_dataset_uuid}]."
                    )
                )

            with open(prev_failed_report_path) as f:
                failed_report = json.load(f)

            if provided_data_folder != (
                reupload_local_dataset_folder := failed_report.get(
                    "local_dataset_folder"
                )
            ):
                raise DataverseExceptionBase(
                    detail=(
                        f"The local dataset folder [{reupload_local_dataset_folder}] for the reupload does not match "
                        f"the currently provided '--folder' [{provided_data_folder}].\n"
                        f"To reupload dataset [{reupload_dataset_uuid}], "
                        f"please set '--folder' to [{reupload_local_dataset_folder}]."
                    )
                )

            failed_file_info_list = failed_report["failed_file_info_list"]
            upload_task_queue = deque(failed_file_info_list)

            failed_file_info_batches = asyncio.run(
                DataverseClient.run_upload_tasks(upload_task_queue)
            )
            if not failed_file_info_batches:
                prev_failed_report_path.unlink(missing_ok=True)

            return reupload_dataset_uuid, failed_file_info_batches

        data_folder = raw_dataset_data["data_folder"]
        loop = asyncio.get_event_loop()

        create_dataset_uuid, failed_file_info_batches = (
            run_reupload_tasks(reupload_dataset_uuid, data_folder, loop)
            if reupload_dataset_uuid
            else run_new_upload_tasks(
                data_folder, raw_dataset_data["type"], async_api, loop
            )
        )

        if failed_file_info_batches:
            failed_report_path = (
                Path.cwd() / "report" / create_dataset_uuid / "failed_upload.json"
            )
            failed_report_path.parent.mkdir(parents=True, exist_ok=True)
            report = {
                "dataset_uuid": create_dataset_uuid,
                "local_dataset_folder": data_folder,
                "failed_file_info_list": failed_file_info_batches,
            }

            with open(failed_report_path, "w") as f:
                json.dump(report, f)

            raise ClientConnectionError(
                f"Failed to upload dataset.\n"
                f"A detailed failure report has been saved at: {failed_report_path}\n"
                f"To retry, import the dataset with the 'reupload_dataset_id' parameter set to [{create_dataset_uuid}]."
            )
        return create_dataset_uuid

    @staticmethod
    def create_custom_model(
        project: Project,
        name: str,
        input_classes: list[str],
        resolution_width: int,
        resolution_height: int,
        model_structure: str,
        weight_url: str,
        client: Optional["DataverseClient"] = None,
        client_alias: Optional[str] = None,
        permission: str = "",
    ):
        try:
            payload = CreateCustomModelAPISchema(
                project_id=project.id,
                name=name,
                input_classes=input_classes,
                resolution_width=resolution_width,
                resolution_height=resolution_height,
                model_structure=model_structure,
                weight_url=weight_url,
            ).model_dump()
        except ValidationError as e:
            raise APIValidationError(f"Something wrong when creating custom model: {e}")

        api, _ = DataverseClient._get_api_client(
            client=client, client_alias=client_alias, is_async=False
        )

        api.create_custom_model(**payload, permission=permission)

    @staticmethod
    async def run_generate_presigned_urls(
        file_paths: list, api: AsyncBackendAPI, data_folder: str
    ) -> tuple[deque[tuple[list[str], list[dict]]], str, list[str]]:
        max_retry_count, batch_size, max_concurrent_api_calls = 5, 500, 10
        semaphore = asyncio.Semaphore(max_concurrent_api_calls)

        failed_urls: list[str] = []
        upload_task_queue: deque[tuple[list[str], list[dict]]] = deque()

        data_folder_path = Path(data_folder).resolve()
        create_dataset_uuid: str = str(uuid4())

        async def generate_presigned_url_task(
            batched_file_paths: list[str], retry_count: int = 0
        ):
            nonlocal create_dataset_uuid

            if retry_count >= max_retry_count:
                failed_urls.extend(batched_file_paths)
                return

            # Convert absolute file paths to relative paths
            # i.e <long data folder path>/data/image.jpg -> /data/image.jpg
            filtered_paths = [
                str(Path(path).relative_to(data_folder_path)).replace("\\", "/")
                for path in batched_file_paths
            ]
            async with semaphore:
                try:
                    resp = await api.generate_presigned_url(
                        file_paths=filtered_paths,
                        create_dataset_uuid=create_dataset_uuid,
                        data_source=DataSource.LOCAL,
                    )
                    url_infos: list[dict] = resp["url_info"]
                    create_dataset_uuid = resp["dataset_info"]["create_dataset_uuid"]
                    upload_task_queue.append((batched_file_paths, url_infos))
                except KeyError:
                    logging.exception("API schema changed?")
                    raise
                except DataverseExceptionBase:
                    logging.exception("Dataverse API error")
                    raise
                except Exception as e:
                    logging.warning(f"Retrying batch due to error: {e}")
                    await asyncio.sleep(retry_count**2)
                    await generate_presigned_url_task(
                        batched_file_paths, retry_count + 1
                    )

        tasks = [
            generate_presigned_url_task(file_paths[i : i + batch_size], 0)
            for i in range(0, len(file_paths), batch_size)
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        return upload_task_queue, create_dataset_uuid, failed_urls

    @staticmethod
    async def run_upload_tasks(upload_task_queue: deque[tuple[list[str], list[dict]]]):
        async def upload_batch(
            paths: list[str],
            upload_infos: list[dict],
            async_client: AsyncThirdPartyAPI,
            semaphore: Semaphore,
            max_retry_count: int,
            progress_bar: tqdm_asyncio,
        ) -> tuple[list[str], list[dict[str, str]]] | None:
            async def upload_file(path: str, info: dict):
                async with semaphore:
                    try:
                        async with aio_open(path, "rb") as file:
                            file_content = await file.read()
                            await async_client.upload_file(
                                method="PUT",
                                target_url=info["url"],
                                file=file_content,
                                content_type="application/octet-stream",
                            )
                            progress_bar.update(1)
                    except Exception as e:
                        logging.exception(e)
                        return (path, info)

            remaining_files = (file for file in zip(paths, upload_infos, strict=True))
            attempt_count = 1

            while attempt_count <= max_retry_count:
                print(f"🔁 Upload file batch ({attempt_count}/{max_retry_count}) ...")

                upload_tasks = (
                    upload_file(path, info) for path, info in remaining_files
                )
                failed_files = await asyncio.gather(*upload_tasks)
                if not any(failed_files):
                    print(
                        f"✅ Upload file batch successful on attempt ({attempt_count}/{max_retry_count})"
                    )
                    return None

                remaining_files = (file for file in failed_files if file)
                print(
                    f"❌ Upload file batch failed on attempt ({attempt_count}/{max_retry_count})"
                )

                await asyncio.sleep(attempt_count**2)
                attempt_count += 1

            failed_files = list(remaining_files)
            failed_paths = [path for path, _ in failed_files]
            failed_remote_urls = [{"url": info["url"]} for _, info in failed_files]

            return (failed_paths, failed_remote_urls)

        tasks = []
        client = AsyncThirdPartyAPI()
        semaphore = Semaphore(MAX_CONCURRENT_FILES)
        max_retry_count = 3
        total_files = sum(len(paths) for paths, _ in upload_task_queue)
        progress_bar = tqdm_asyncio(
            total=total_files, desc="Uploading files", unit="file"
        )

        for batched_file_paths, upload_file_infos in upload_task_queue:
            tasks.append(
                upload_batch(
                    batched_file_paths,
                    upload_file_infos,
                    client,
                    semaphore,
                    max_retry_count,
                    progress_bar,
                )
            )

        failed_file_info_list: list[tuple[list[str], list[dict[str, str]]]] = []
        for results in await tqdm_asyncio.gather(*tasks):
            if results:
                failed_file_info_list.append(results)

        progress_bar.close()
        return failed_file_info_list

    @staticmethod
    def _find_all_paths(*paths) -> list[str]:
        all_filepaths: list[str] = []
        for path in paths:
            all_filepaths.extend(get_filepaths(path))
        return all_filepaths

    @staticmethod
    def _get_format_folders(
        annotation_format: AnnotationFormat,
        dataset_type: DatasetType,
        project_id: int,
        api: BackendAPI,
    ) -> list[str]:
        if annotation_format == AnnotationFormat.KITTI:
            project = api.get_project(project_id=project_id)
            sensors = project["sensors"]
            if dataset_type == DatasetType.RAW_DATA:
                return []
            elif len(sensors) == 1:
                if sensors[0]["type"] == SensorType.LIDAR:  # one-lidar case
                    return ["label_2", "velodyne"]
                else:
                    raise DataverseExceptionBase(
                        detail=f"single camera with the {annotation_format} format is not supported for local upload"
                    )
            else:
                return ["calib", "image_2", "label_2", "velodyne"]

        elif annotation_format == AnnotationFormat.COCO:
            return ["images/", "annotations/labels.json"]
        elif annotation_format == AnnotationFormat.YOLO:
            return ["images/", "labels/", "classes.txt"]
        elif annotation_format == AnnotationFormat.VLM:
            return ["images/", "annotations/"]
        elif annotation_format in (
            AnnotationFormat.VISION_AI,
            AnnotationFormat.BDDP,
            AnnotationFormat.IMAGE,
        ):
            return []
        else:
            raise DataverseExceptionBase(
                detail=f"the format {annotation_format} is not supported for local upload"
            )

    async def upload_videos_create_session(
        self,
        name: str,
        video_folder: str,
        video_curation: bool = False,
        curation_config: Optional[dict] = None,
    ) -> dict:
        video_path = Path(video_folder)
        if not video_path.exists() or not video_path.is_dir():
            raise ValueError(f"Video folder does not exist: {video_folder}")

        video_extensions = {".mp4", ".avi", ".mov", ".mpeg", ".flv"}
        video_paths = [
            path
            for path in video_path.iterdir()
            if path.is_file() and path.suffix.lower() in video_extensions
        ]
        if not video_paths:
            raise ValueError(f"No video files found in {video_folder}")

        filenames = [video.name for video in video_paths]
        logging.info(f"Found {len(filenames)} videos to upload")

        try:
            # Step 1: Get presigned URLs
            logging.info("Getting presigned URLs...")
            presigned_data = (
                await self._async_api_client.generate_session_task_presigned_urls(
                    filenames=filenames
                )
            )
            data_folder = presigned_data["data_folder"]
            url_info = presigned_data["url_info"]

            # Step 2: Upload videos concurrently with progress bar
            logging.info("Uploading videos...")
            upload_task_queue = deque([(video_paths, url_info)])
            failed_file_info_batches = await DataverseClient.run_upload_tasks(
                upload_task_queue
            )
            if failed_file_info_batches:
                raise ClientConnectionError(
                    f"Failed uploads: {failed_file_info_batches}"
                )

            # Step 3: Create session task
            logging.info("Creating session task...")
            session_task_data = await self._async_api_client.create_session_task(
                name=name,
                data_folder=data_folder,
                video_curation=video_curation,
                curation_config=curation_config,
            )
            logging.info(f"✅ Session task '{name}' created successfully!")

            return session_task_data

        except DataverseExceptionBase:
            logging.exception("Got api error from Dataverse")
            raise
        except Exception as e:
            try:
                error_data = json.loads(
                    getattr(getattr(e, "response", None), "text", str(e))
                )
                error_message = next(iter(error_data.get("error", {}).values()))[0]
            except Exception:
                error_message = str(e)

            raise ClientConnectionError(
                f"Failed to create session task: {error_message}"
            )


class AsyncThirdPartyAPI:
    transport = AsyncHTTPTransport(
        retries=5,
    )

    def __init__(self):
        self.client = AsyncClient(transport=self.transport, timeout=Timeout(30))

    async def async_send_request(self, url: str, method: str, **kwargs) -> Response:
        try:
            resp: Response = await self.client.request(method=method, url=url, **kwargs)
        except Exception as e:
            logging.exception("async send request error")
            raise AsyncThirdPartyAPIException(detail="async send request error") from e

        if not 200 <= resp.status_code <= 299:
            raise AsyncThirdPartyAPIException(
                status_code=resp.status_code, detail=resp.text
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
