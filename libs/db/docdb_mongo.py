from pymongo import MongoClient as PyMongoClient

from libs.common import load_tls_ca_bundle

from .mongo import MongoClient


class DocumentDbMongoClient(MongoClient):
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        protocol: str = "mongodb",
        ssl: bool = False,
        collection: str = None,
    ):
        if host == "localhost" or host == "mongo":
            super().__init__(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                protocol=protocol,
                collection=collection,
            )
        else:
            use_tls = ssl
            filename = load_tls_ca_bundle() if use_tls else None

            connection_uri = (
                f"{protocol}://{host}"
                if protocol == "mongodb+srv"
                else f"{protocol}://{host}:{port}"
            )

            self._conn = PyMongoClient(
                connection_uri,
                username=user,
                password=password,
                retryWrites=False,
                tls=use_tls,
                tlsCAFile=filename,
            )

            if self._conn is None:
                raise Exception(
                    "Could not connect to " + host + ":" + port + " was not found."
                )

            self._db = self._conn[database]
            if collection is not None:
                # set initial collection
                self.use_collection(collection)
