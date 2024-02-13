import io
import json
import logging
import environ

from azure.cosmos import CosmosClient, PartitionKey
from six import string_types

from ..exceptions import InitializationError
from .base import Backend

# Module-level logger
log = logging.getLogger(__name__)


class CosmosBackend(Backend):

    @environ.config(prefix="COSMOS")
    class Config(object):
        uri = environ.var()

    def __init__(self, **kwargs):
        try:
            self.client = CosmosClient.from_connection_string(
                kwargs.get("uri")
            )

            # unless clearing the db has been explicitly specified,
            # don't initialize if the discovery_database exits
            # the discovery_databases is a minimally viable database,
            if not self.database_established() or kwargs.get("clear_db"):
                self.clear_db()

                file_name = kwargs.get("filename")
                if file_name:
                    log.info(
                        f"Initializing Cosmos DB with data from {file_name}"
                    )
                    self.initialize_cosmos_with_data(file_name)
                    self.object_manifest_check()

            # super(CosmosBackend, self).__init__(**kwargs)
        except ConnectionError:
            log.error(
                "Unable to establish a connection to MongoDB server {}".format(
                    kwargs.get("uri")
                )
            )

    def database_established(self):
        """
        Checks to see if a medallion database exists
        """
        return "discovery_database" in self.list_databases()

    def _get_api_root_statuses(self, api_root):
        raise NotImplementedError()

    def server_discovery(self):
        discovery_db = self.client.get_database_client("discovery_database")
        discovery_info = discovery_db.get_container_client(
            "discovery_information"
        )
        info = list(
            discovery_info.query_items(
                "SELECT top 1 * FROM c", enable_cross_partition_query=True
            )
        )
        if info and len(info) > 0:
            return info[0]

    def get_collections(self, api_root):
        raise NotImplementedError()

    def get_collection(self, api_root, collection_id):
        raise NotImplementedError()

    def get_object_manifest(
        self, api_root, collection_id, filter_args, allowed_filters, limit
    ):
        raise NotImplementedError()

    def object_manifest_check(self):
        """
        Checks for manifests in each object, throws an error if not present.
        """
        objects_exist = False

        for api_root in self.list_databases():
            containers = [
                container["id"]
                for container in self.client.get_database_client(
                    api_root
                ).list_containers()
            ]

            if "objects" not in containers:
                continue

            objects_exist = True

            api_root_db = self.client.get_database_client(api_root)
            objects = api_root_db.get_container_client("objects")

            for result in objects.read_all_items():
                if "_manifest" not in result:
                    field_to_use = "created"

                    if "modified" in result:
                        field_to_use = "modified"

                    raise InitializationError(
                        "Object {} from {} is missing a manifest".format(
                            result["id"], result[field_to_use]
                        ),
                        408,
                    )

                if not result["_manifest"]:
                    field_to_use = "created"
                    if "modified" in result:
                        field_to_use = "modified"
                    raise InitializationError(
                        "Object {} from {} has a null manifest".format(
                            result["id"], result[field_to_use]
                        ),
                        408,
                    )

            if not objects_exist:
                raise InitializationError(
                    "Could not find any objects in database", 408
                )

    def get_api_root_information(self, api_root):
        raise NotImplementedError()

    def get_objects(
        self, api_root, collection_id, filter_args, allowed_filters, limit
    ):
        raise NotImplementedError()

    def add_objects(self, api_root, collection_id, objs, request_time):
        raise NotImplementedError()

    def get_object(
        self,
        api_root,
        collection_id,
        object_id,
        filter_args,
        allowed_filters,
        limit,
    ):
        raise NotImplementedError()

    def delete_object(
        self, api_root, collection_id, object_id, filter_args, allowed_filters
    ):
        raise NotImplementedError()

    def get_object_versions(
        self,
        api_root,
        collection_id,
        object_id,
        filter_args,
        allowed_filters,
        limit,
    ):

        raise NotImplementedError()

    def _pop_expired_sessions(self):

        raise NotImplementedError()

    def _pop_old_statuses(self):

        raise NotImplementedError()

    def load_data_from_file(self, filename):
        try:
            if isinstance(filename, string_types):
                with io.open(filename, "r", encoding="utf-8") as infile:
                    self.json_data = json.load(infile)
            else:
                self.json_data = json.load(filename)
        except Exception as e:
            raise InitializationError(
                "Problem loading initialization data from {0}".format(
                    filename
                ),
                408,
                e,
            )

    def initialize_cosmos_with_data(self, file_name):
        self.load_data_from_file(file_name)
        if "/discovery" in self.json_data:
            db = self.client.get_database_client("discovery_database")
            db.get_container_client("discovery_information").upsert_item(
                self.json_data["/discovery"],
            )

    def list_databases(self):
        return [database["id"] for database in self.client.list_databases()]

    def clear_db(self):
        log.info("Clearing Cosmos DB")
        for database in self.list_databases():
            log.info(f"Deleting database {database}")
            self.client.delete_database(database)

        log.info("Creating empty database")
        discovery_db = self.client.create_database("discovery_database")
        discovery_db.create_container(
            "discovery_information", partition_key=PartitionKey(path="/id")
        )
        discovery_db.create_container(
            "api_root_info", partition_key=PartitionKey(path="/id")
        )
        return discovery_db
