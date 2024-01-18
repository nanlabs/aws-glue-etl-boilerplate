from typing import Any, Dict, List, Tuple, Union

from pymongo import MongoClient as PyMongoClient

from libs.common.logconfig import LogConfig


class MongoClient:
    logger = LogConfig().get_logger()

    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        protocol: str = "mongodb",
        collection: str = None,
        throw_error: bool = True,
    ) -> None:
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
        )

        if self._conn is None and throw_error:
            raise Exception(
                "Could not connect to " + host + ":" + port + " was not found."
            )

        self._db = self._conn[database]
        self.logger.info("Connected to database: " + database)
        if collection is not None:
            # set initial collection
            self.use_collection(collection)

    def use(self, database: str) -> None:
        self._db = self._conn[database]
        self.logger.info("Connected to database: " + database)

    def use_collection(self, collection: str) -> None:
        self._collection = self._db[collection]
        self.logger.info("Connected to collection: " + collection)

    def collection(self) -> str:
        return self._collection.name

    def insert(self, data: Dict[str, Any]) -> None:
        self._collection.insert_one(data)

    def insert_many(self, data: List[Dict[str, Any]]) -> None:
        self._collection.insert_many(data)

    def find(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(self._collection.find(query))

    def find_sorted(
        self, query: Dict[str, Any], sort: List[Tuple[str, int]]
    ) -> List[Dict[str, Any]]:
        return list(self._collection.find(query).sort(sort))

    def find_sorted_limit(
        self,
        query: Dict[str, Any],
        sort: List[Tuple[str, int]],
        limit: int,
        projection: Dict[str, Any] = None,
    ) -> List[Dict[str, Any]]:
        return list(self._collection.find(query, projection).sort(sort).limit(limit))

    def find_sorted_limit_skip(
        self,
        query: Dict[str, Any],
        sort: List[Tuple[str, int]],
        limit: int,
        skip: int,
        projection: Dict[str, Any] = None,
    ) -> List[Dict[str, Any]]:
        return list(
            self._collection.find(query, projection).sort(sort).limit(limit).skip(skip)
        )

    def find_one(self, query: Dict[str, Any]) -> Dict[str, Any]:
        return self._collection.find_one(query)

    def find_one_and_update(
        self, query: Dict[str, Any], update: Dict[str, Any]
    ) -> Dict[str, Any]:
        return self._collection.find_one_and_update(query, update)

    def find_one_and_delete(self, query: Dict[str, Any]) -> Dict[str, Any]:
        return self._collection.find_one_and_delete(query)

    def update(self, query: Dict[str, Any], update: Dict[str, Any]) -> None:
        self._collection.update_one(query, update)

    def update_many(self, query: Dict[str, Any], update: Dict[str, Any]) -> None:
        self._collection.update_many(query, update)

    def delete(self, query: Dict[str, Any]) -> None:
        self._collection.delete_one(query)

    def delete_many(self, query: Dict[str, Any]) -> None:
        self._collection.delete_many(query)

    def count(self, query: Dict[str, Any]) -> int:
        return self._collection.count_documents(query)

    def distinct(self, field: str, query: Dict[str, Any]) -> List[Union[str, int]]:
        return self._collection.distinct(field, query)

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(self._collection.aggregate(pipeline))

    def drop(self) -> None:
        self._collection.drop()

    def drop_database(self) -> None:
        self._conn.drop_database(self._db.name)

    def close(self) -> None:
        self._conn.close()

    def __del__(self) -> None:
        self.close()
