import inspect
import json
import logging
from typing import Optional, Union
from urllib.parse import urlencode

import requests
from requests import sessions
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)


class BackendAPI:
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=10, backoff_factor=15, status_forcelist=[500, 502, 503, 504]
        )
    )

    def __init__(
        self,
        host: str,
        email: str,
        password: str,
    ):
        # TODO: Support api versioning
        self.host = host
        self.headers = {"Content-Type": "application/json"}
        self.access_token = None
        self.email = email
        self.password = password
        self.login(
            email=email,
            password=password,
        )

    def send_request(
        self,
        url: str,
        method: str,
        attempts: int = 1,
        max_attempts: int = 5,
        data: Optional[Union[str, dict]] = None,
        timeout: int = 3000,
        **kwargs,
    ):
        if attempts > max_attempts:
            msg = "Exceeds max attempts."
            logger.error(msg)
            raise Exception(msg)

        if (
            isinstance(data, dict)
            and kwargs.get("headers", {}).get("Content-Type") == "application/json"
        ):
            data = json.dumps(data)

        parent_func = inspect.stack()[2][3]
        try:
            with sessions.Session() as session:
                session.mount("http://", self.adapter)
                session.mount("https://", self.adapter)
                resp = session.request(
                    method=method, url=url, data=data, timeout=timeout, **kwargs
                )
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout: {method} {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"Invalid http: {repr(e)}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {repr(e)}")
            raise
        except (requests.exceptions.RequestException, Exception) as e:
            logger.error(f"Unexpected exception, err: {repr(e)}")
            raise

        if resp.status_code == 401 or resp.status_code == 403:
            logger.info(f"[{parent_func}] request forbidden.")
            logger.info(f"[{parent_func}] start to refresh access token.")
            self.login(email=self.email, password=self.password)
            logger.info(f"[{parent_func}] access token refreshed.")
            return self.send_request(
                url=url,
                method=method,
                attempts=attempts + 1,
                max_attempts=max_attempts,
                **kwargs,
            )

        if not 200 <= resp.status_code <= 299:
            raise Exception(
                f"[{parent_func}] request failed (kwargs: {kwargs})"
                f", status code: {resp.status_code}, response detail: {resp.__dict__}"
            )

        return resp

    def login(self, email: str, password: str):
        if email is None:
            raise ValueError("Can't login with null email.")
        if password is None:
            raise ValueError("Can't login with null password.")

        resp = self.send_request(
            url=f"{self.host}/auth/users/jwt/",
            method="post",
            headers={"Content-Type": "application/json"},
            data={"email": email, "password": password},
        )
        json_data = resp.json()
        self.set_auth(access_token=json_data["access_token"])

    def set_auth(self, access_token: str) -> None:
        self.access_token = access_token
        self.headers["Authorization"] = f"Bearer {access_token}"

    def create_project(
        self,
        name: str,
        ontology_data: dict,
        sensor_data: list[dict],
        project_tag_data: Optional[dict] = None,
        description: Optional[str] = None,
    ) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/projects/",
            method="post",
            headers=self.headers,
            data={
                "name": name,
                "ontology_data": ontology_data,
                "sensor_data": sensor_data,
                "project_tag_data": project_tag_data,
                "description": description,
            },
        )
        return resp.json()

    def edit_project(
        self,
        project_id: int,
        name: Optional[str] = None,
        ontology_data: Optional[dict] = None,
        project_tag_data: Optional[dict] = None,
        description: Optional[str] = None,
    ) -> dict:
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if ontology_data is not None:
            data["ontology_data"] = ontology_data
        if project_tag_data is not None:
            data["project_tag_data"] = project_tag_data
        resp = self.send_request(
            url=f"{self.host}/api/projects/{project_id}/",
            method="patch",
            headers=self.headers,
            data=data,
        )
        return resp.json()

    def get_project(self, project_id) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/projects/{project_id}",
            method="get",
            headers=self.headers,
        )
        return resp.json()

    def list_projects(
        self,
        current_user: Optional[bool] = True,
        exclude_sensor_type: Optional[str] = None,
        image_type: Optional[str] = None,
        **kwargs,
    ) -> list:
        if current_user:
            kwargs["current_user"] = current_user
        if exclude_sensor_type is not None:
            kwargs["exclude_sensor_type"] = exclude_sensor_type.value
        if image_type is not None:
            kwargs["ontology__image_type"] = image_type.value
        resp = self.send_request(
            url=f"{self.host}/api/projects/?{urlencode(kwargs)}",
            method="get",
            headers=self.headers,
        )
        return resp.json()["results"]

    def list_ml_models(self, project_id: int, type: str = "project", **kwargs) -> list:
        kwargs["project"] = project_id
        kwargs["type"] = type
        resp = self.send_request(
            url=f"{self.host}/api/ml_models/?{urlencode(kwargs)}",
            method="get",
            headers=self.headers,
        )
        return resp.json()["results"]

    def get_ml_model(self, model_id: int) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/ml_models/{model_id}/",
            method="get",
            headers=self.headers,
        )
        return resp.json()

    def get_ml_model_labels(
        self, model_id: int, timeout: int = 3000
    ) -> requests.models.Response:
        resp = self.send_request(
            url=f"{self.host}/api/ml_models/{model_id}/labels/",
            method="get",
            headers=self.headers,
            stream=True,
            timeout=timeout,
        )
        return resp

    def get_ml_model_file(
        self, model_id: int, timeout: int = 3000, model_format: str = "triton", **kwargs
    ) -> requests.models.Response:
        kwargs["model_format"] = model_format
        resp = self.send_request(
            url=f"{self.host}/api/ml_models/{model_id}/model/?{urlencode(kwargs)}",
            method="get",
            headers=self.headers,
            stream=True,
            timeout=timeout,
        )
        return resp

    def create_dataset(
        self,
        name: str,
        data_source: str,
        project_id: int,
        sensor_ids: list[int],
        type: str,
        annotation_format: str,
        storage_url: str,
        data_folder: str,
        sequential: bool = False,
        generate_metadata: bool = False,
        auto_tagging: Optional[list] = None,
        render_pcd: bool = False,
        container_name: Optional[str] = None,
        sas_token: Optional[str] = None,
        description: Optional[str] = None,
        annotations: Optional[list[str]] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
    ) -> dict:
        if auto_tagging is None:
            auto_tagging = []
        if annotations is None:
            annotations = []
        payload_data = {
            "name": name,
            "project_id": project_id,
            "sensor_ids": sensor_ids,
            "data_source": data_source,
            "storage_url": storage_url,
            "container_name": container_name,
            "data_folder": data_folder,
            "sas_token": sas_token,
            "type": type,
            "sequential": sequential,
            "annotation_format": annotation_format,
            "generate_metadata": generate_metadata,
            "auto_tagging": auto_tagging,
            "render_pcd": render_pcd,
            "description": description if description else "",
            "annotations": annotations if annotations else [],
        }

        aws_access_key = {secret_access_key, access_key_id}
        if not (all(aws_access_key) or not any(aws_access_key)):
            raise ValueError("Need to assign both secret_access_key and access_key_id")
        if secret_access_key and access_key_id:
            payload_data.update(
                {"secret_access_key": secret_access_key, "access_key_id": access_key_id}
            )

        resp = self.send_request(
            url=f"{self.host}/api/datasets/",
            method="post",
            headers=self.headers,
            data=payload_data,
        )
        return resp.json()

    def get_dataset(self, dataset_id: int):
        resp = self.send_request(
            url=f"{self.host}/api/datasets/{dataset_id}/",
            method="get",
            headers=self.headers,
        )

        return resp.json()

    def upload_files(
        self,
        dataset_id: int,
        container_name: str,
        is_finished: bool,
        file_dict: dict[str, bytes],
    ):
        file_dict["json"] = (
            None,
            json.dumps(
                {
                    "dataset_id": dataset_id,
                    "container_name": container_name,
                    "is_finished": is_finished,
                }
            ),
            "application/json",
        )

        resp = self.send_request(
            url=f"{self.host}/api/datasets/upload-files/",
            method="post",
            headers={"Authorization": self.headers["Authorization"]},
            files=file_dict,
        )
        return resp

    def update_dataset(self, dataset_id: int, **kwargs):
        resp = self.send_request(
            url=f"{self.host}/api/datasets/{dataset_id}/",
            method="patch",
            headers=self.headers,
            data=kwargs,
        )
        return resp
