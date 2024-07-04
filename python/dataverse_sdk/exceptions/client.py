from typing import Optional


class ClientConnectionError(Exception):
    pass


class DataverseExceptionBase(Exception):
    def __init__(
        self,
        status_code: Optional[int] = None,
        type: Optional[str] = None,
        detail: Optional[str] = None,
        error: Optional[str | dict] = None,
        **args
    ):
        self.type = type
        self.status_code = status_code
        self.detail = detail
        self.error = error


class AsyncThirdPartyAPIException(Exception):
    def __init__(
        self, status_code: Optional[int] = None, detail: Optional[str] = "", **args
    ):
        self.status_code = status_code
        self.detail = detail
