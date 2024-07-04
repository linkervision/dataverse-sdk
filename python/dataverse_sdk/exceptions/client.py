class ClientConnectionError(Exception):
    pass


class DataverseExceptionBase(Exception):
    def __init__(
        self,
        type: str = None,
        detail: str = None,
        error: str = None,
        status_code: int = None,
        **args
    ):
        self.type = type
        self.status_code = status_code
        self.detail = detail
        self.error = error


class AsyncThirdPartyAPIException(Exception):
    def __init__(self, detail: str = None, status_code: int = None, **args):
        self.status_code = status_code
        self.detail = detail
