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
        message: Optional[str] = None,
        **args,
    ):
        self.type = type
        self.status_code = status_code
        self.detail = detail
        self.error = error
        self.message = (
            message
            or detail
            or (str(error) if error else "")
            or f"Request failed with status code: {status_code}"
        )
        super().__init__(self.message)

    def __str__(self):
        return f"""
        DataverseExceptionBase(
            status_code={self.status_code},
            message='{self.message}',
            type='{self.type}',
            detail='{self.detail}',
            error='{self.error}'
        )"""

    def __repr__(self):
        return f"""
        DataverseExceptionBase(
            status_code={self.status_code},
            message='{self.message}',
            type='{self.type}',
            detail='{self.detail}',
            error='{self.error}'
        )"""


class AsyncThirdPartyAPIException(Exception):
    def __init__(
        self, status_code: Optional[int] = None, detail: Optional[str] = "", **args
    ):
        self.status_code = status_code
        self.detail = detail
        self.message = detail or f"Request failed with status code: {status_code}"
        super().__init__(self.message)

    def __str__(self):
        return f"""
        AsyncThirdPartyAPIException(
            status_code={self.status_code},
            message='{self.message}',
            detail='{self.detail}'
        )"""

    def __repr__(self):
        return f"""
        AsyncThirdPartyAPIException(
            status_code={self.status_code},
            message='{self.message}',
            detail='{self.detail}'
        )"""
