import logging
from typing import Optional

import requests
from aiohttp.client_reqrep import ClientResponse
from aiohttp_retry import ExponentialRetry, RetryClient
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)


class ThirdPartyAPI:
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=10, backoff_factor=5, status_forcelist=[500, 502, 503, 504, 104]
        )
    )
    retry_options: dict = {
        "attempts": 5,
        "start_timeout": 5,
        "max_timeout": 5 * (2**5),
        "statuses": {500, 502, 503, 504},
    }

    @classmethod
    def send_request(
        cls,
        url: str,
        method: str,
        timeout: int = 10,
        custom_retry_options: Optional[dict] = None,
        **kwargs,
    ):
        adapter = cls.adapter
        if custom_retry_options:
            adapter = HTTPAdapter(max_retries=Retry(**custom_retry_options))

        with requests.Session() as session:
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            try:
                response = session.request(
                    method=method, url=url, timeout=timeout, **kwargs
                )
            except Exception as e:
                logger.error(f"third party request error : {str(e)}")
                raise

            if not 200 <= response.status_code <= 299:
                raise Exception(
                    f"status code: {response.status_code}, response detail: {response.__dict__}"
                )
        return response

    @classmethod
    async def async_send_request(
        cls, url: str, method: str, **kwargs
    ) -> tuple[RetryClient, ClientResponse]:
        retry_options = ExponentialRetry(**cls.retry_options)
        retry_client = RetryClient(raise_for_status=False, retry_options=retry_options)
        try:
            response = await retry_client.request(method=method, url=url, **kwargs)
        except Exception as e:
            logger.error(f"third party request error : {e}")
            raise

        if not 200 <= response.status <= 299:
            raise Exception(
                f"status code: {response.status}, response detail: {response.__dict__}"
            )

        return retry_client, response

    @classmethod
    async def async_download_file(cls, url: str, method: str, **kwargs) -> bytes:
        client, resp = await cls.async_send_request(url, method, **kwargs)
        data = await resp.read()
        await client.close()
        return data
