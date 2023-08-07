import httpx

from ... import rest

from .api import SyncApi, AsyncApi
from .errors import ApiError


class BaseClient:
    """
    Base class for sync and async clients.
    """
    api_class = None

    def __init__(
        self,
        /,
        # This is the name of the field manager for server-side apply
        default_field_manager = "easykube",
        default_namespace = "default",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.default_field_manager = default_field_manager
        self.default_namespace = default_namespace
        # Cache of API version -> API objects
        self.apis = {}
        # Cache of API group -> API version using preferred version
        self.preferred_versions = {}

    def build_request(self, method, url, **kwargs):
        # For patch requests, set the content-type as merge-patch unless otherwise specified
        if method.lower() == "patch":
            headers = kwargs.get("headers") or {}
            headers.setdefault("Content-Type", "application/merge-patch+json")
            kwargs["headers"] = headers
        return super().build_request(method, url, **kwargs)

    def api(self, api_version):
        """
        Returns an API object for the specified API version.
        """
        if api_version not in self.apis:
            self.apis[api_version] = self.api_class(self, api_version)
        return self.apis[api_version]


class SyncClient(BaseClient, rest.SyncClient):
    """
    Sync client for Kubernetes.
    """
    api_class = SyncApi

    def raise_for_status(self, response):
        # Convert response errors into ApiErrors for better messages
        try:
            super().raise_for_status(response)
        except httpx.HTTPStatusError as source:
            raise ApiError(source)

    def api_preferred_version(self, group):
        """
        Returns an API object for the given group at the preferred version.
        """
        if group not in self.preferred_versions:
            response = self.get(f"/apis/{group}")
            api_version = response.json()["preferredVersion"]["groupVersion"]
            self.preferred_versions[group] = api_version
        return self.api(self.preferred_versions[group])

    def create_object(self, object):
        """
        Create the given object.
        """
        resource = self.api(object["apiVersion"]).resource(object["kind"])
        namespace = object["metadata"].get("namespace")
        return resource.create(object, namespace = namespace)

    def replace_object(self, object):
        """
        Replace the given object with the specified state.
        """
        resource = self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return resource.replace(name, object, namespace = namespace)

    def patch_object(self, object, patch):
        """
        Apply the given patch to the specified object.
        """
        resource = self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return resource.patch(name, patch, namespace = namespace)

    def delete_object(self, object):
        """
        Delete the specified object.
        """
        resource = self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return resource.delete(name, namespace = namespace)

    def apply_object(self, object, /, field_manager = None, force = False):
        """
        Applies the given object using server-side apply.

        See https://kubernetes.io/docs/reference/using-api/server-side-apply/.
        """
        resource = self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return resource.server_side_apply(
            name,
            object,
            field_manager = field_manager,
            namespace = namespace,
            force = force
        )

    def client_side_apply_object(self, object):
        """
        Create or update the given object, equivalent to "kubectl apply".
        """
        resource = self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return resource.create_or_replace(name, object, namespace = namespace)


class AsyncClient(BaseClient, rest.AsyncClient):
    """
    Async client for Kubernetes.
    """
    api_class = AsyncApi

    async def raise_for_status(self, response):
        """
        Raise the relevant exception for the response, if required.
        """
        try:
            await super().raise_for_status(response)
        except httpx.HTTPStatusError as source:
            raise ApiError(source)

    async def api_preferred_version(self, group):
        """
        Returns an API object for the given group at the preferred version.
        """
        if group not in self.preferred_versions:
            response = await self.get(f"/apis/{group}")
            api_version = response.json()["preferredVersion"]["groupVersion"]
            self.preferred_versions[group] = api_version
        return self.api(self.preferred_versions[group])

    async def create_object(self, object):
        """
        Create the given object.
        """
        resource = await self.api(object["apiVersion"]).resource(object["kind"])
        namespace = object["metadata"].get("namespace")
        return await resource.create(object, namespace = namespace)

    async def replace_object(self, object):
        """
        Replace the given object with the specified state.
        """
        resource = await self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.replace(name, object, namespace = namespace)

    async def patch_object(self, object, patch):
        """
        Apply the given patch to the specified object.
        """
        resource = await self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.patch(name, patch, namespace = namespace)

    async def delete_object(self, object):
        """
        Delete the specified object.
        """
        resource = await self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.delete(name, namespace = namespace)

    async def apply_object(self, object, /, field_manager = None, force = False):
        """
        Applies the given object using server-side apply.

        See https://kubernetes.io/docs/reference/using-api/server-side-apply/.
        """
        resource = await self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.server_side_apply(
            name,
            object,
            field_manager = field_manager,
            namespace = namespace,
            force = force
        )

    async def client_side_apply_object(self, object):
        """
        Create or update the given object, equivalent to "kubectl apply".
        """
        resource = await self.api(object["apiVersion"]).resource(object["kind"])
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.create_or_replace(name, object, namespace = namespace)
