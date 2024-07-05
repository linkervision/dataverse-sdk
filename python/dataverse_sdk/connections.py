"""Inspired by elaticsearch-dsl-py"""


class Connections:
    """
    Class responsible for holding connections to different clusters. Used as a
    singleton in this module.
    """

    def __init__(self):
        self._conns = {}

    def add_connection(self, alias, conn, force=True):
        """
        Add a connection object, it will be passed through as-is.
        """
        if force is False:
            if alias in self._conns:
                raise ValueError(f"The connection alias, {alias}, is already exist")
        self._conns[alias] = conn

    def create_connection(self, alias="default", force=True, **kwargs):
        """
        Construct an instance of ``dataverse_sdk.DataverseClient`` and register
        it under given alias.
        """
        if force is False:
            if alias in self._conns:
                raise ValueError(f"The connection alias, {alias}, is already exist")
        from .client import DataverseClient

        conn = self._conns[alias] = DataverseClient(**kwargs)
        return conn

    def get_connection(self, alias="default"):
        """
        Retrieve a connection, construct it if necessary (only configuration
        was passed to us). If a non-string alias has been passed through we
        assume it's already a client instance and will just return it as-is.

        Raises ``KeyError`` if no client (or its definition) is registered
        under the alias.
        """
        if not isinstance(alias, str):
            return alias

        # connection already established
        try:
            return self._conns[alias]
        except KeyError:
            raise KeyError(
                f"You must create the connection with the given alias {alias} in advance."
            )


connections = Connections()
add_connection = connections.add_connection
create_connection = connections.create_connection
get_connection = connections.get_connection
