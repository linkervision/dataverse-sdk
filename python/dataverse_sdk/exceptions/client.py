class ClientConnectionError(Exception):
    pass


class BadRequest(Exception):
    def __init__(self, data: dict):
        self.data = data
