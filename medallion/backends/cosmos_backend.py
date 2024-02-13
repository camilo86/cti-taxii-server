import io
import json
import logging
import environ

from azure.cosmos import CosmosClient, PartitionKey
from six import string_types

from ..common import (
    APPLICATION_INSTANCE,
    create_resource,
    datetime_to_float,
    datetime_to_string,
    datetime_to_string_stix,
    determine_spec_version,
    determine_version,
    float_to_datetime,
    generate_status,
    generate_status_details,
    get_application_instance_config_values,
    get_custom_headers,
    get_timestamp,
    parse_request_parameters,
    string_to_datetime,
)
from ..exceptions import InitializationError
from .base import Backend

# Module-level logger
log = logging.getLogger(__name__)


def find_manifest_entries_for_id(obj, manifest):
    for m in manifest:
        if m["id"] == obj["id"]:
            if "modified" in obj:
                if m["version"] == obj["modified"]:
                    return m
            else:
                # handle data markings
                if m["version"] == obj["created"]:
                    return m


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
        if api_root not in self.list_databases():
            return None

        api_root_db = self.client.get_database_client(api_root)
        collection_info = api_root_db.get_container_client("collections")
        collections = list(collection_info.read_all_items())

        if get_application_instance_config_values(
            APPLICATION_INSTANCE, "taxii", "interop_requirements"
        ):
            collections = sorted(collections, key=lambda x: x["id"])

        return create_resource("collections", collections)

    def get_collection(self, api_root, collection_id):
        if api_root not in self.list_databases():
            return None

        api_root_db = self.client.get_database_client(api_root)
        collection_info = api_root_db.get_container_client("collections")
        info = collection_info.read_item(collection_id, collection_id)

        return info

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
        db = self.client.get_database_client("discovery_database")
        api_root_info = db.get_container_client("api_root_info")
        info = list(
            api_root_info.query_items(
                query="SELECT * FROM c WHERE c._name=@api_root",
                parameters=[{"name": "@api_root", "value": api_root}],
                enable_cross_partition_query=True,
            )
        )

        if info and len(info) > 0:
            return info[0]

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
        else:
            raise InitializationError(
                "No discovery information provided when initializing the Mongo"
            )

        api_root_info_db = db.get_container_client("api_root_info")

        for api_root_name, api_root_data in self.json_data.items():
            if api_root_name == "/discovery":
                continue

            url = list(
                filter(
                    lambda a: api_root_name in a,
                    self.json_data["/discovery"]["api_roots"],
                )
            )[0]
            api_root_data["information"]["_url"] = url
            api_root_data["information"]["_name"] = api_root_name

            api_root_info_db.upsert_item(api_root_data["information"])

            api_db = self.client.create_database(api_root_name)
            status_container = api_db.create_container(
                "status", partition_key=PartitionKey(path="/id")
            )

            if api_root_data["status"]:
                for status in api_root_data["status"]:
                    status_container.upsert_item(status)

            collection_container = api_db.create_container(
                "collections", partition_key=PartitionKey(path="/id")
            )
            object_container = api_db.create_container(
                "objects", partition_key=PartitionKey(path="/id")
            )

            for collection in api_root_data["collections"]:
                collection_id = collection["id"]
                objects = collection["objects"]
                manifest = collection["manifest"]

                collection.pop("objects")
                collection.pop("manifest")

                collection_container.upsert_item(collection)

                for obj in objects:
                    obj["_collection_id"] = collection_id
                    obj["_manifest"] = find_manifest_entries_for_id(
                        obj, manifest
                    )
                    obj["_manifest"]["date_added"] = datetime_to_float(
                        string_to_datetime(obj["_manifest"]["date_added"])
                    )
                    obj["_manifest"]["version"] = datetime_to_float(
                        string_to_datetime(obj["_manifest"]["version"])
                    )
                    obj["created"] = datetime_to_float(
                        string_to_datetime(obj["created"])
                    )

                    if "modified" in obj:
                        # not for data markings
                        obj["modified"] = datetime_to_float(
                            string_to_datetime(obj["modified"])
                        )

                    object_container.upsert_item(obj)

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
