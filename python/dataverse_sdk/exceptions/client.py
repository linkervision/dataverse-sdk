from typing import Optional


class InvalidProcessError(Exception):
    pass


class APIValidationError(Exception):
    pass


class ClientConnectionError(Exception):
    pass


class DataverseExceptionBase(Exception):
    def __init__(
        self,
        status_code: Optional[int] = None,
        type: Optional[str] = "",
        detail: Optional[str] = "",
        error: Optional[str | dict] = "",
        **args,
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
