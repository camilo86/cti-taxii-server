import environ
from azure.cosmos import CosmosClient

from .base import Backend


class CosmosBackend(Backend):

    @environ.config(prefix="COSMOS")
    class Config(object):
        uri = environ.var()
        key = environ.var()

    def __init__(self, **kwargs):
        self.client = CosmosClient(kwargs.get("uri"), kwargs.get("key"))

    def _get_api_root_statuses(self, api_root):
        raise NotImplementedError()

    def server_discovery(self):
        raise NotImplementedError()

    def get_collections(self, api_root):
        raise NotImplementedError()

    def get_collection(self, api_root, collection_id):
        raise NotImplementedError()

    def get_object_manifest(
        self, api_root, collection_id, filter_args, allowed_filters, limit
    ):
        raise NotImplementedError()

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
