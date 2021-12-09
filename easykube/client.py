import asyncio
import copy
import dataclasses
import json
import typing

from . import rest


class ApiError(Exception):
    """
    Exception that is raised when a Kubernetes API error occurs that is in the 4xx range.
    """
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data
        super().__init__(data.get("message", str(data)))


@dataclasses.dataclass
class ResourceSpec:
    """
    Specification for a Kubernetes API resource.
    """
    #: The API version, including group, of the resource
    api_version: str
    #: The name of the resource
    name: str
    #: The kind of the resource
    kind: typing.Optional[str] = None
    #: Whether or not the resource is namespaced
    namespaced: typing.Optional[bool] = None

    def __call__(self, client):
        """
        Return a resource instance for the given client.
        """
        return Resource(client, self.api_version, self.name, self.kind, self.namespaced)


class Client(rest.Client):
    """
    REST client for Kubernetes.
    """
    def __init__(self, default_namespace = "default", **kwargs):
        self.default_namespace = default_namespace
        super().__init__(**kwargs)
        # Cache of API version -> API objects
        self.apis = {}
        # Cache of API group -> API version using preferred version
        self.preferred_versions = {}

    def raise_for_status(self, response):
        # For client errors, the response will always be JSON with extra info
        if response.is_client_error:
            raise ApiError(response.status_code, response.json())
        else:
            super().raise_for_status(response)

    async def request(self, method, url, **kwargs):
        # For patch requests, set the content-type as merge-patch unless otherwise specified
        if method.lower() == "patch":
            headers = kwargs.setdefault("headers", {})
            headers.setdefault("Content-Type", "application/merge-patch+json")
        return await super().request(method, url, **kwargs)

    def api(self, api_version):
        """
        Returns an API object for the specified API version.
        """
        if api_version not in self.apis:
            self.apis[api_version] = Api(self, api_version)
        return self.apis[api_version]

    async def api_preferred_version(self, group):
        """
        Returns an API object for the given group at the preferred version.
        """
        if group not in self.preferred_versions:
            response = await self.get(f"/apis/{group}")
            api_version = response.json()["preferredVersion"]["groupVersion"]
            self.preferred_versions[group] = api_version
        return self.api(self.preferred_versions[group])

    async def _resource_for_object(self, object):
        """
        Returns a resource for the given object.
        """
        api_version = object["apiVersion"]
        kind = object["kind"]
        # We need to get the name for the kind
        api = self.api(api_version)
        api_resources = await api.resources
        try:
            name = next(r["name"] for r in api_resources.values() if r["kind"] == kind)
        except StopIteration:
            raise TypeError(f"API '{api_version}' has no resource '{kind}'")
        else:
            return api.resource(name)

    async def create_object(self, object):
        """
        Create the given object.
        """
        resource = await self._resource_for_object(object)
        namespace = object["metadata"].get("namespace")
        return await resource.create(object, namespace = namespace)

    async def patch_object(self, object, patch):
        """
        Apply the given patch to the specified object.
        """
        resource = await self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.patch(name, patch, namespace = namespace)

    async def delete_object(self, object):
        """
        Delete the specified object.
        """
        resource = await self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.delete(name, namespace = namespace)

    async def apply_object(self, object):
        """
        Create or update the given object, equivalent to "kubectl apply".
        """
        resource = await self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return await resource.create_or_patch(name, object, namespace = namespace)

    @classmethod
    def from_environment(cls, **kwargs):
        """
        Return a client created from the available information in the environment.

        If running in cluster, this will use the details of the service account used by the pod.

        If not running in a cluster, it relies on "kubectl proxy" to be running.
        """
        kwargs.setdefault("base_url", "http://127.0.0.1:8001")
        return cls(**kwargs)


class Api:
    """
    Class for a Kubernetes API.
    """
    def __init__(self, client, api_version):
        self.client = client
        self.api_version = api_version
        # Create a task that will load the resources for the API once
        self.resources = asyncio.create_task(self._fetch_resources())

    async def _fetch_resources(self):
        """
        Return the resources for the API indexed by name.
        """
        prefix = "/apis" if "/" in self.api_version else "/api"
        response = await self.client.get(f"{prefix}/{self.api_version}")
        return { r["name"]: r for r in response.json()["resources"] }

    def resource(self, name):
        """
        Return a resource for the given name.
        """
        return Resource(self.client, self.api_version, name)


class Resource(rest.Resource):
    """
    Class for Kubernetes REST resources.
    """
    def __init__(self, client, api_version, name, kind = None, namespaced = None):
        self.client = client
        # If a falsely API version is given, assume the core API is intended
        self.api_version = api_version or "v1"
        self.name = name
        self.kind = kind
        self.namespaced = namespaced

    async def ensure_initialised(self):
        """
        Ensures that the resource is fully initialised including any async operations.
        """
        # For the core API, the given API version should be "v1"
        # For any other API, the given API version will be "<group>" or "<group>/<version>"
        # We only need to query the preferred version if we were given a group without a version
        api_version_is_complete = self.api_version == "v1" or "/" in self.api_version
        # If we already have all the information we need, there is nothing to do
        if api_version_is_complete and self.kind is not None and self.namespaced is not None:
            return
        # Load the API object for our API version
        if api_version_is_complete:
            api = self.client.api(self.api_version)
        else:
            api = await self.client.api_preferred_version(self.api_version)
        # Update the stored API version with the fully discovered one
        self.api_version = api.api_version
        # Get the kind and whether the resource is namespaced from the API resources
        api_resources = await api.resources
        try:
            resource = api_resources[self.name]
        except KeyError:
            raise TypeError(f"API '{self.api_version}' has no resource '{self.name}'")
        else:
            self.kind = resource["kind"]
            self.namespaced = resource["namespaced"]

    def prepare_path(self, id = None, **params):
        namespace = params.pop("namespace", None) or self.client.default_namespace
        all_namespaces = params.pop("all_namespaces", False)
        if "labels" in params:
            params["labelSelector"] = ",".join(
                f"{k}={v}"
                for k, v in params.pop("labels").items()
            )
        if "fields" in params:
            params["fieldSelector"] = ",".join(
                f"{k}={v}"
                for k, v in params.pop("fields").items()
            )
        # Begin with either /api or /apis depending whether the api version is the core API
        prefix = "/apis" if "/" in self.api_version else "/api"
        if self.namespaced and not all_namespaces:
            path_namespace = f"/namespaces/{namespace}"
        else:
            path_namespace = ""
        path = f"{prefix}/{self.api_version}{path_namespace}/{self.name}"
        return f"{path}/{id}" if id else path, params

    def extract_list(self, response):
        return response.json()["items"]

    def prepare_data(self, data, id = None, **params):
        data = copy.deepcopy(data)
        # Update the data with the known api version and kind
        data.setdefault("apiVersion", self.api_version)
        data.setdefault("kind", self.kind)
        # Set the name in metadata to the given id
        if id:
            data.setdefault("metadata", {}).update(name = id)
        return data

    async def create(self, data, *, namespace = None):
        return await super().create(data, namespace = namespace)

    async def fetch(self, id, *, namespace = None):
        return await super().fetch(id, namespace = namespace)

    async def replace(self, id, data, *, namespace = None):
        return await super().replace(id, data, namespace = namespace)

    async def patch(self, id, data, *, namespace = None):
        return await super().patch(id, data, namespace = namespace)

    async def create_or_replace(self, id, data, *, namespace = None):
        return await super().create_or_replace(id, data, namespace = namespace)

    async def create_or_patch(self, id, data, *, namespace = None):
        return await super().create_or_patch(id, data, namespace = namespace)

    async def delete(self, id, *, namespace = None):
        return await super().delete(id, namespace = namespace)

    async def delete_all(self, **params):
        """
        Deletes a collection of resources.
        """
        await self.ensure_initialised()
        path, params = self.prepare_path(**params)
        await self.client.delete(path, params = params)

    async def watch_list(self, **params):
        """
        Watches the list of resource instances for changes.

        Returns a tuple of (initial state, async iterator of watch events).
        """
        # This behaviour mimics "kubectl get <resource> -w"
        await self.ensure_initialised()
        path, params = self.prepare_path(**params)
        # Get the initial state and the resource version using the same logic as list
        # We can't actually use list because we need the full response, not just the items
        initial_state = []
        resource_version = None
        async for response in self.client.paginate(path, params = params):
            resource_version = response.json()["metadata"]["resourceVersion"]
            initial_state.extend(self.wrap_instance(i) for i in self.extract_list(response))
        # Define the async iterator for the watch events
        async def watch_events():
            watch_params = params.copy()
            watch_params.update({ "watch": 1, "resourceVersion": resource_version })
            stream = self.client.stream("GET", path, params = watch_params, timeout = None)
            async with stream as response:
                async for chunk in response.aiter_bytes():
                    yield json.loads(chunk)
        # Return the (initial state, watch events) tuple
        return initial_state, watch_events()

    async def watch_one(self, id, *, namespace = None):
        """
        Watches a single resource instance for changes.

        Returns a tuple of (initial state, async iterator of watch events).
        """
        # Just watch the list but with a field selector
        # We also extract the single object from the initial state
        initial_state, events = await self.watch_list(
            fields = { "metadata.name": id },
            namespace = namespace
        )
        return next(iter(initial_state), None), events
