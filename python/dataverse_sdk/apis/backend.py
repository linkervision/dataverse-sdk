import inspect
import json
import logging
from typing import Optional, Union

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
        email: Optional[str] = None,
        password: Optional[str] = None,
        access_token: str = None,
        refresh_token: str = None,
    ):
        # TODO: Support api versioning
        self.host = host
        self.headers = {"Content-Type": "application/json"}
        self.access_token = None
        self.refresh_token = None

        if access_token:
            self.set_auth(access_token=access_token, refresh_token=refresh_token)
        else:
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
        **kwargs,
    ):
        if attempts > max_attempts:
            msg = "Exceeds max attempts."
            logger.error(msg)
            raise Exception(msg)

        if isinstance(data, dict):
            data = json.dumps(data)

        parent_func = inspect.stack()[2][3]
        try:
            with sessions.Session() as session:
                session.mount("http://", self.adapter)
                session.mount("https://", self.adapter)
                resp = session.request(method=method, url=url, data=data, **kwargs)
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

        if resp.status_code == 403:
            logger.info(f"[{parent_func}] request forbidden.")
            if not self.access_token or not self.refresh_token:
                raise Exception("Invalid credential.")
            logger.info(f"[{parent_func}] start to refresh access token.")
            self.refresh_access_token()
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
            url=f"{self.host}/auth/jwt/create/",
            method="post",
            headers=self.headers,
            data={"email": email, "password": password},
        )
        json_data = resp.json()
        self.set_auth(
            access_token=json_data.get("access"), refresh_token=json_data.get("refresh")
        )

    def refresh_access_token(self, refresh_token: Optional[str] = None):
        if not refresh_token:
            refresh_token = self.refresh_token
        resp = self.send_request(
            url=f"{self.host}/auth/jwt/refresh/",
            method="post",
            headers=self.headers,
            data={"refresh": refresh_token},
        )
        self.set_auth(access_token=resp.json()["access"])

    def set_auth(self, access_token: str, refresh_token: Optional[str] = None):
        self.access_token = access_token
        if refresh_token:
            self.refresh_token = refresh_token
        self.headers["Authorization"] = f"Bearer {access_token}"

    def create_project(self, name: str, ontology_data: dict, sensor_data: dict) -> dict:
        resp = self.send_request(
            url=f"{self.host}/api/projects/",
            method="post",
            headers=self.headers,
            data={
                "name": name,
                "ontology_data": ontology_data,
                "sensor_data": sensor_data,
            },
        )

        return resp.json()
