import copy
import dataclasses
import json
import typing

import httpx

from . import rest


class ApiError(httpx.HTTPStatusError):
    """
    Exception that is raised when a Kubernetes API error occurs that is in the 4xx range.
    """
    def __init__(self, source):
        try:
            message = source.response.json()["message"]
        except (json.JSONDecodeError, KeyError):
            message = source.response.text
        super().__init__(message, request = source.request, response = source.response)


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

    @classmethod
    def from_crd(cls, crd):
        """
        Returns a resource spec for the given CRD definition.
        """
        api_group = crd["spec"]["group"]
        preferred_version = next(
            version["name"]
            for version in crd["spec"]["versions"]
            if version.get("storage", False)
        )
        api_version = f"{api_group}/{preferred_version}"
        name = crd["spec"]["names"]["plural"]
        kind = crd["spec"]["names"]["kind"]
        namespaced = crd["spec"]["scope"] == "Namespaced"
        return cls(api_version, name, kind, namespaced)


class ClientMixin:
    """
    Mixin defining additional methods for sync and async clients.
    """
    def __init__(self, *, default_namespace = "default", **kwargs):
        super().__init__(**kwargs)
        self.default_namespace = default_namespace
        # Cache of API version -> API objects
        self.apis = {}
        # Cache of API group -> API version using preferred version
        self.preferred_versions = {}

    def raise_for_status(self, response):
        # Convert response errors into ApiErrors for better messages
        try:
            super().raise_for_status(response)
        except httpx.HTTPStatusError as source:
            raise ApiError(source)

    def build_request(self, method, url, **kwargs):
        # For patch requests, set the content-type as merge-patch unless otherwise specified
        if method.lower() == "patch":
            headers = kwargs.setdefault("headers", {})
            headers.setdefault("Content-Type", "application/merge-patch+json")
        return super().build_request(method, url, **kwargs)

    def api(self, api_version):
        """
        Returns an API object for the specified API version.
        """
        if api_version not in self.apis:
            self.apis[api_version] = Api(self, api_version)
        return self.apis[api_version]

    @rest.flow
    def api_preferred_version(self, group):
        """
        Returns an API object for the given group at the preferred version.
        """
        if group not in self.preferred_versions:
            response = yield self.get(f"/apis/{group}")
            api_version = response.json()["preferredVersion"]["groupVersion"]
            self.preferred_versions[group] = api_version
        return self.api(self.preferred_versions[group])

    def _resource_for_object(self, object):
        """
        Returns a resource for the given object.
        """
        # Get the resource using the api version and kind from the object
        api_version = object["apiVersion"]
        kind = object["kind"]
        return (yield self.api(api_version).resource(kind))

    @rest.flow
    def create_object(self, object):
        """
        Create the given object.
        """
        resource = yield self._resource_for_object(object)
        namespace = object["metadata"].get("namespace")
        return (yield resource.create(object, namespace = namespace))

    @rest.flow
    def replace_object(self, object):
        """
        Replace the given object with the specified state.
        """
        resource = yield self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return (yield resource.replace(name, object, namespace = namespace))

    @rest.flow
    def patch_object(self, object, patch):
        """
        Apply the given patch to the specified object.
        """
        resource = yield self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return (yield resource.patch(name, patch, namespace = namespace))

    @rest.flow
    def delete_object(self, object):
        """
        Delete the specified object.
        """
        resource = yield self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return (yield resource.delete(name, namespace = namespace))

    @rest.flow
    def apply_object(self, object):
        """
        Create or update the given object, equivalent to "kubectl apply".
        """
        resource = yield self._resource_for_object(object)
        name = object["metadata"]["name"]
        namespace = object["metadata"].get("namespace")
        return (yield resource.create_or_replace(name, object, namespace = namespace))


class SyncClient(ClientMixin, rest.SyncClient):
    """
    Sync client for Kubernetes.
    """


class AsyncClient(ClientMixin, rest.AsyncClient):
    """
    Async client for Kubernetes.
    """


class Client(rest.Client):
    """
    Client for Kubernetes.
    """
    __sync_client_class__ = SyncClient
    __async_client_class__ = AsyncClient

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
        self._client = client
        self._api_version = api_version
        self._resources = None

    @property
    def api_version(self):
        """
        Returns the API version for the API.
        """
        return self._api_version

    def _ensure_resources(self):
        """
        Ensures that the resources have been loaded.
        """
        if self._resources is None:
            prefix = "/apis" if "/" in self._api_version else "/api"
            response = yield self._client.get(f"{prefix}/{self._api_version}")
            self._resources = { r["name"]: r for r in response.json()["resources"] }
        return self._resources

    @rest.flow
    def resource(self, name):
        """
        Returns a resource for the given name.

        The given name can be either the plural name, the singular name or the kind.
        Lookups by plural name will be faster as that is the key that is indexed.
        """
        resources = yield self._ensure_resources()
        # First try a lookup by plural name
        try:
            resource = resources[name]
        except KeyError:
            # Then try a lookup by singular name or kind
            try:
                resource = next(
                    r
                    for r in resources.values()
                    if r["kind"] == name or r["singularName"] == name
                )
            except StopIteration:
                raise ValueError(f"API '{self._api_version}' has no resource '{name}'")
        return Resource(
            self._client,
            self._api_version,
            resource["name"],
            resource["kind"],
            resource["namespaced"]
        )


class ListResponseIterator(rest.ListResponseIterator):
    """
    Iterator for list responses.
    """
    def __init__(self, client, resource, params):
        super().__init__(client, resource, params)
        self._resource_version = None

    @property
    def resource_version(self):
        """
        The last seen resource version.
        """
        return self._resource_version

    def _extract_list(self, response):
        # Store the resource version from the response as well as returning the data
        data = response.json()
        self._resource_version = data["metadata"]["resourceVersion"]
        return data["items"]


class WatchEvents(rest.StreamIterator):
    """
    Stream iterator that yields watch events.
    """
    def __init__(self, client, path, params, initial_resource_version):
        self._client = client
        self._path = path
        self._params = params.copy()
        self._params.update({
            "watch": 1,
            "resourceVersion": initial_resource_version,
            "allowWatchBookmarks": "true",
        })

    def _stream(self):
        return self._client.stream("GET", self._path, params = self._params, timeout = None)

    def _process_chunk(self, chunk):
        event = json.loads(chunk)
        # Each event contains a resourceVersion, which we track so that we can restart
        # the watch from that version if it fails
        self._params["resourceVersion"] = event["object"]["metadata"]["resourceVersion"]
        # Bookmark events are just for us to save a resource version
        # They should not be emitted
        if event["type"] == "BOOKMARK":
            raise self.SuppressItem
        else:
            return event

    def _should_resume(self, exception):
        # In the case where an event is not valid JSON, it is likely that the API
        # server is still healthy but the read has just timed out
        #
        # In this case, we should be able to restart the watch from the last known
        # resource version
        #
        # In all other cases, the exception should be allowed to bubble, e.g.:
        #   * The API server responds with 410 Gone, in which case the watch needs
        #     to restart from scratch
        #   * We cannot connect to the API server
        return not exception or isinstance(exception, json.JSONDecodeError)


class Resource(rest.Resource):
    """
    Class for Kubernetes REST resources.
    """
    __iterator_class__ = ListResponseIterator

    def __init__(self, client, api_version, name, kind, namespaced):
        super().__init__(client, name)
        self._api_version = api_version
        self._kind = kind
        self._namespaced = namespaced

    def _prepare_path(self, id = None, params = None):
        namespace = params.pop("namespace", None) or self._client.default_namespace
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
        prefix = "/apis" if "/" in self._api_version else "/api"
        if self._namespaced and not all_namespaces:
            path_namespace = f"/namespaces/{namespace}"
        else:
            path_namespace = ""
        path, _ = super()._prepare_path(id)
        return f"{prefix}/{self._api_version}{path_namespace}{path}", params

    def _prepare_data(self, data, id = None, params = None):
        data = copy.deepcopy(data)
        # Update the data with the known api version and kind
        data.setdefault("apiVersion", self._api_version)
        data.setdefault("kind", self._kind)
        # Set the name in metadata to the given id
        if id:
            data.setdefault("metadata", {}).update(name = id)
        return data

    def create(self, data, *, namespace = None):
        return super().create(data, namespace = namespace)

    def fetch(self, id, *, namespace = None):
        return super().fetch(id, namespace = namespace)

    def replace(self, id, data, *, namespace = None):
        return super().replace(id, data, namespace = namespace)

    def patch(self, id, data, *, namespace = None):
        return super().patch(id, data, namespace = namespace)

    @rest.flow
    def create_or_replace(self, id, data, *, namespace = None):
        # This is intended to replicate "kubectl apply"
        # So we fetch the latest resourceVersion before executing if required
        resource_version = data.get("metadata", {}).get("resourceVersion")
        if not resource_version:
            try:
                latest = yield self.fetch(id, namespace = namespace)
            except ApiError as exc:
                if exc.response.status_code == 404:
                    return (yield self.create(data, namespace = namespace))
                else:
                    raise
            else:
                latest_version = latest["metadata"]["resourceVersion"]
                data.setdefault("metadata", {})["resourceVersion"] = latest_version
        return (yield self.replace(id, data, namespace = namespace))

    def create_or_patch(self, id, data, *, namespace = None):
        return super().create_or_patch(id, data, namespace = namespace)

    def delete(self, id, *, namespace = None):
        return super().delete(id, namespace = namespace)

    @rest.flow
    def delete_all(self, **params):
        """
        Deletes a collection of resources.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(params = params)
        yield self._client.delete(path, params = params)

    @rest.flow
    def watch_list(self, **params):
        """
        Watches a set of resource instances, as specified by the given parameters, for changes.

        Returns a tuple of (initial state, watch events).
        """
        yield self._ensure_initialised()
        # Accumulate the inital state by looping through the list iterator
        iterator = self.list(**params)
        initial_state = []
        while True:
            try:
                next_item = yield iterator._next_item()
            except ListResponseIterator.StopIteration:
                break
            else:
                initial_state.append(next_item)
        # Get the path to use for the watch
        path, params = self._prepare_path(params = params)
        # Use the final resource version from the iterator for the watch
        return initial_state, WatchEvents(self._client, path, params, iterator.resource_version)

    @rest.flow
    def watch_one(self, id, *, namespace = None):
        """
        Watches a single resource instance for changes.

        Returns a tuple of (initial state, async iterator of watch events).
        """
        # Just watch the list but with a field selector
        # We also extract the single object from the initial state
        initial_state, events = yield self._watch_list(
            fields = { "metadata.name": id },
            namespace = namespace
        )
        return next(iter(initial_state), None), events
