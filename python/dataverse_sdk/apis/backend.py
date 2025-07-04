import inspect
import json
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Optional, Union
from urllib.parse import urlencode

import httpx
import requests
from httpx import AsyncClient, Response, Timeout
from requests import sessions
from requests.adapters import HTTPAdapter, Retry

from ..exceptions.client import DataverseExceptionBase
from ..utils.utils import chunks

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
        service_id: str,
        access_token: str = "",
    ):
        # TODO: Support api versioning
        self.host = host
        self.headers = {
            "Content-Type": "application/json",
            "X-Request-Service-Id": service_id,
        }
        self.access_token = access_token
        self.email = email
        self.password = password
        self.login(email=email, password=password)

    def get_host(self):
        return self.host

    def send_request(
        self,
        url: str,
        method: str,
        data: Optional[Union[str, dict]] = None,
        timeout: int = 3000,
        **kwargs,
    ):
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
        if resp.status_code in (401, 403, 404):
            logger.exception(f"[{parent_func}] request forbidden.")
            raise DataverseExceptionBase(status_code=resp.status_code, **resp.json())

        if resp.status_code == 400:
            logger.exception(f"[{parent_func}] got bad request")
            raise DataverseExceptionBase(status_code=resp.status_code, **resp.json())

        if resp.status_code == 500:
            logger.exception(f"[{parent_func}] got api error")
            raise DataverseExceptionBase(status_code=resp.status_code)

        if not 200 <= resp.status_code <= 299:
            raise DataverseExceptionBase(status_code=resp.status_code, **resp.json())
        return resp

    def login(self, email: str, password: str):
        if email and password:
            resp = self.send_request(
                url=f"{self.host}/auth/users/jwt/",
                method="post",
                headers={"Content-Type": "application/json"},
                data={"email": email, "password": password},
            )
            json_data = resp.json()
            self.set_auth(access_token=json_data["access_token"])
            return

        if self.access_token:
            self.set_auth(access_token=self.access_token)
            return

        if email is None:
            raise ValueError("Can't login with null email.")
        if password is None:
            raise ValueError("Can't login with null password.")

    def set_auth(self, access_token: str) -> None:
        self.headers["Authorization"] = f"Bearer {access_token}"

    def get_user(self) -> dict:
        return self.send_request(
            url=f"{self.host}/auth/users/me/",
            method="get",
            headers=self.headers,
        ).json()

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

    def create_vqa_project(
        self,
        name: str,
        sensor_name: str,
        ontology_name: str,
        question_answer: list,
        description: Optional[str] = None,
    ):
        resp = self.send_request(
            url=f"{self.host}/api/projects/vqa/",
            method="post",
            headers=self.headers,
            data={
                "name": name,
                "sensor_name": sensor_name,
                "ontology_name": ontology_name,
                "question_answer": question_answer,
                "description": description,
            },
        )
        return resp.json()

    def edit_vqa_ontology(self, project_id: int, edit_vqa_data: dict):
        resp = self.send_request(
            url=f"{self.host}/api/projects/{project_id}/update-or-create-vqa-ontology/",
            method="post",
            headers=self.headers,
            data=edit_vqa_data,
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
            url=f"{self.host}/api/projects/{project_id}/",
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

    def list_datasets(self, project_id: int, **kwargs) -> list:
        kwargs["project"] = project_id
        resp = self.send_request(
            url=f"{self.host}/api/datasets/?{urlencode(kwargs)}",
            method="get",
            headers=self.headers,
        )
        return resp.json()["results"]

    def list_dataslices(self, project_id: int, **kwargs) -> list:
        kwargs["project"] = project_id
        resp = self.send_request(
            url=f"{self.host}/api/dataslices/basic/?{urlencode(kwargs)}",
            method="get",
            headers=self.headers,
        )
        return resp.json()["results"]

    def get_dataslice(self, dataslice_id: int) -> list:
        resp = self.send_request(
            url=f"{self.host}/api/dataslices/{dataslice_id}/",
            method="get",
            headers=self.headers,
        )
        return resp.json()

    def update_alias(
        self,
        project_id: int,
        alias_list: list,
    ) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/projects/{project_id}/bulk-upsert-alias/",
            method="post",
            headers=self.headers,
            json=alias_list,
        )
        return resp

    def export_dataslice(
        self,
        dataslice_id: int,
        export_format: str,
        annotation_name: str,
        is_sequential: bool = False,
    ) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/dataslices/{dataslice_id}/export/",
            method="post",
            headers=self.headers,
            data={
                "is_sequential": is_sequential,
                "export_format": export_format,
                "export_to": "direct_download",
                "annotation_name": annotation_name,
            },
        )
        return resp.json()

    def list_ml_models(self, project_id: int, type: str = "trained", **kwargs) -> list:
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

    def get_convert_record(self, convert_record_id: int) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/convert_record/{convert_record_id}/",
            method="get",
            headers=self.headers,
        )
        return resp.json()

    def get_convert_model_labels(
        self, convert_record_id: int, timeout: int = 3000
    ) -> requests.models.Response:
        resp = self.send_request(
            url=f"{self.host}/api/convert_record/{convert_record_id}/label/",
            method="get",
            headers=self.headers,
            stream=True,
            timeout=timeout,
        )
        return resp

    def get_convert_onnx_model(
        self, convert_record_id: int, timeout: int = 3000
    ) -> requests.models.Response:
        resp = self.send_request(
            url=f"{self.host}/api/convert_record/{convert_record_id}/model/",
            method="get",
            headers=self.headers,
            stream=True,
            timeout=timeout,
        )
        return resp

    def get_convert_model_file(
        self,
        convert_record_id: int,
        timeout: int = 3000,
        triton_format: bool = True,
        permission: str = "",
        **kwargs,
    ) -> requests.models.Response:
        headers = self.headers.copy()
        kwargs["triton"] = triton_format
        if permission:
            headers["X-Request-Source"] = permission
        resp = self.send_request(
            url=f"{self.host}/api/convert_record/{convert_record_id}/model-observ/?{urlencode(kwargs)}",
            method="get",
            headers=headers,
            stream=True,
            timeout=timeout,
        )
        return resp

    def create_dataset(
        self,
        name: str,
        data_source: str,
        project_id: int,
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
        create_dataset_uuid: Optional[str] = None,
    ) -> dict:
        if auto_tagging is None:
            auto_tagging = []
        if annotations is None:
            annotations = []
        payload_data = {
            "name": name,
            "project_id": project_id,
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

        if create_dataset_uuid:
            payload_data.update({"create_dataset_uuid": create_dataset_uuid})

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

    def generate_presigned_url(
        self,
        file_paths: list,
        create_dataset_uuid: Optional[str],
        data_source: str,
    ):
        payload = {"filenames": file_paths, "data_source": data_source}
        if create_dataset_uuid:
            payload["create_dataset_uuid"] = create_dataset_uuid
        resp = self.send_request(
            url=f"{self.host}/api/datasets/upload-file-information/",
            method="post",
            headers=self.headers,
            data=payload,
        )
        return resp.json()

    def update_dataset(self, dataset_id: int, **kwargs):
        resp = self.send_request(
            url=f"{self.host}/api/datasets/{dataset_id}/",
            method="patch",
            headers=self.headers,
            data=kwargs,
        )
        return resp.json()


class AsyncBackendAPI:
    def __init__(
        self,
        host: str,
        email: str,
        password: str,
        service_id: str,
        access_token: str = "",
    ):
        self.host = host
        self.headers = {
            "Content-Type": "application/json",
            "X-Request-Service-Id": service_id,
        }
        self.access_token = access_token
        self.email = email
        self.password = password

        self.client = AsyncClient(timeout=Timeout(30))
        self.sync_login(email=email, password=password)

    async def async_send_request(
        self,
        url: str,
        method: str,
        data: Optional[Union[str, dict]] = None,
        timeout: int = 30,
        **kwargs,
    ) -> dict:
        """
        Asynchronous version of `send_request` that sends HTTP requests.
        """

        if (
            isinstance(data, dict)
            and kwargs.get("headers", {}).get("Content-Type") == "application/json"
        ):
            data = json.dumps(data)

        try:
            response: Response = await self.client.request(
                method=method, url=url, data=data, timeout=timeout, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
            raise
        except httpx.TimeoutException:
            logger.warning(f"Request timeout: {method} {url}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected exception: {repr(e)}")
            raise

    def sync_send_request(
        self,
        url: str,
        method: str,
        data: Optional[Union[str, dict]] = None,
        timeout: int = 30,
        headers: Optional[dict] = None,
        **kwargs,
    ):
        """
        Synchronous version of `send_request` for login and authentication.
        """
        if (
            isinstance(data, dict)
            and headers
            and headers.get("Content-Type") == "application/json"
        ):
            data = json.dumps(data)

        parent_func = inspect.stack()[2][3]
        try:
            with sessions.Session() as session:
                resp = session.request(
                    method=method,
                    url=url,
                    data=data,
                    timeout=timeout,
                    headers=self.headers,
                    **kwargs,
                )
                resp.raise_for_status()
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout: {method} {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {repr(e)}")
            raise

        if resp.status_code in (401, 403, 404):
            logger.exception(f"[{parent_func}] request forbidden.")
            raise Exception(f"Forbidden: {resp.status_code}")

        return resp

    def sync_login(self, email: str, password: str):
        if email and password:
            resp = self.sync_send_request(
                url=f"{self.host}/auth/users/jwt/",
                method="POST",
                data={"email": email, "password": password},
                headers={"Content-Type": "application/json"},
            )
            json_data = resp.json()
            self.set_auth(access_token=json_data["access_token"])
        elif self.access_token:
            self.set_auth(access_token=self.access_token)
        else:
            raise ValueError("Invalid credentials: Email and password required.")

    def set_auth(self, access_token: str) -> None:
        self.access_token = access_token
        self.headers["Authorization"] = f"Bearer {access_token}"

    async def get_user(self) -> dict:
        return await self.async_send_request(
            url=f"{self.host}/auth/users/me/",
            method="GET",
            headers=self.headers,
        )

    async def generate_presigned_url(
        self,
        file_paths: list[str],
        create_dataset_uuid: Optional[str],
        data_source: str,
    ) -> dict:
        payload = {"filenames": file_paths, "data_source": data_source}
        if create_dataset_uuid:
            payload["create_dataset_uuid"] = create_dataset_uuid

        return await self.async_send_request(
            url=f"{self.host}/api/datasets/upload-file-information/",
            method="POST",
            headers=self.headers,
            json=payload,
        )

    async def get_project(self, project_id: str) -> dict:
        try:
            resp = await self.client.get(
                url=f"{self.host}/api/projects/{project_id}/",
                headers=self.headers,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            print(f"Request failed: {str(e)}")
            return None

    async def get_datarows(
        self,
        batch_size: int = 20,
        order_by: str = "id",
        id_set_list: Optional[list] = None,
        **kwargs,
    ) -> AsyncGenerator[list[dict]]:
        if "offset" in kwargs or "limit" in kwargs:
            raise ValueError("Specifying offset or limit directly is not allowed.")
        kwargs["order_by"] = order_by
        id_gt = 0
        if id_set_list:
            for id_chunks in chunks(id_set_list, batch_size):
                while True:
                    kwargs.update(
                        {
                            "id_set": ",".join([str(id_) for id_ in id_chunks]),
                            "limit": batch_size,
                            "id__gt": id_gt,
                        }
                    )
                    url = f"{self.host}/api/datarows/?{urlencode(kwargs)}"
                    resp: dict = await self.async_send_request(
                        url=url,
                        method="get",
                        headers=self.headers,
                    )
                    json_data = resp
                    datarows = json_data["results"]
                    if not datarows:
                        break
                    # Get last datarow id
                    id_gt = datarows[-1]["id"]
                    yield datarows
        else:
            dataslice_set = kwargs.pop("dataslice_set", [])
            query_params = {
                **kwargs,
                "dataslice_set": dataslice_set,
                "limit": batch_size,
            }
            query_string = urlencode(query_params, doseq=True)

            id_gt = 0
            while True:
                url = f"{self.host}/api/datarows/?{query_string}&id__gt={id_gt}"
                resp: dict = await self.async_send_request(
                    url=url,
                    method="get",
                    headers=self.headers,
                )
                json_data = resp
                if not json_data["results"]:
                    break
                # Get last datarow id
                datarows = json_data["results"]
                id_gt = datarows[-1]["id"]
                yield datarows

    async def get_datarows_flat_parent(
        self, batch_size: int = 20, order_by: str = "id", **kwargs
    ) -> AsyncIterator[list[dict]]:
        id_gt = 0
        limit = batch_size
        kwargs["order_by"] = order_by
        while True:
            kwargs.update({"id__gt": id_gt, "limit": limit})
            url = f"{self.host}/api/datarows/flat-parent/?{urlencode(kwargs)}"
            resp: dict = await self.async_send_request(
                url=url,
                method="get",
                headers=self.headers,
            )
            json_data: list[dict] = resp
            if not json_data["results"]:
                break

            # Get last datarow id
            datarows = json_data["results"]
            id_gt = datarows[-1]["id"]
            yield datarows

    async def get_dataslice(self, dataslice_id: int) -> dict:
        url = f"{self.host}/api/dataslices/{dataslice_id}/"
        resp: dict = await self.async_send_request(
            url=url,
            method="get",
            headers=self.headers,
        )
        return resp
