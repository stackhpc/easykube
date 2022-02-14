import copy

from ... import rest
from ...flow import flow

from .errors import ApiError
from .iterators import ListResponseIterator, WatchEvents


#: Sentinel object indicating that the presence of a label is required with any value
PRESENT = object()
#: Sentinel object indicating that a label must not be present
ABSENT = object()


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
            # Add the selectors for the labels to any explicit label selector
            # This allows the use of set selectors with the labels keyword
            if "labelSelector" in params:
                label_selectors = [params["labelSelector"]]
            else:
                label_selectors = []
            for k, v in params.pop("labels").items():
                if v is PRESENT:
                    label_selectors.append(k)
                elif v is ABSENT:
                    label_selectors.append(f"!{k}")
                elif isinstance(v, (list, tuple)):
                    values_text = v.join(",")
                    label_selectors.append(f"{k} in ({values_text})")
                else:
                    label_selectors.append(f"{k}={v}")
            params["labelSelector"] = ",".join(label_selectors)
        if "fields" in params:
            if "fieldSelector" in params:
                field_selectors = [params["fieldSelector"]]
            else:
                field_selectors = []
            field_selectors.extend(
                f"{k}={v}"
                for k, v in params.pop("fields").items()
            )
            params["fieldSelector"] = ",".join(field_selectors)
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

    @flow
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

    @flow
    def delete_all(self, **params):
        """
        Deletes a collection of resources.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(params = params)
        yield self._client.delete(path, params = params)

    @flow
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

    @flow
    def watch_one(self, id, *, namespace = None):
        """
        Watches a single resource instance for changes.

        Returns a tuple of (initial state, async iterator of watch events).
        """
        # Just watch the list but with a field selector
        # We also extract the single object from the initial state
        initial_state, events = yield self.watch_list(
            fields = { "metadata.name": id },
            namespace = namespace
        )
        return next(iter(initial_state), None), events