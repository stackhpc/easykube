from ...flow import Flowable, flow

from .resource import Resource


class Api(Flowable):
    """
    Class for a Kubernetes API.
    """
    def __init__(self, client, api_version):
        self._client = client
        self._api_version = api_version
        self._resources = None

    def get_flow_executor(self):
        """
        Returns the flow executor to use.
        """
        return self._client.get_flow_executor()

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

    @flow
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
