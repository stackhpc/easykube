from .resource import SyncResource, AsyncResource


class BaseApi:
    """
    Base class for a Kubernetes API.
    """
    def __init__(self, client, api_version):
        self._client = client
        self._api_version = api_version
        prefix = "/apis" if "/" in self._api_version else "/api"
        self._api_path = f"{prefix}/{self._api_version}"
        self._resources = None

    @property
    def api_version(self):
        """
        Returns the API version for the API.
        """
        return self._api_version
    
    def _set_resources(self, response):
        """
        Processes the resources from a successful response.
        """
        self._resources = { r["name"]: r for r in response.json()["resources"] }

    def _lookup_resource(self, name):
        """
        Looks up the named resource in the stored resources.
        """
        # First try a lookup by plural name
        try:
            return self._resources[name]
        except KeyError:
            # Then try a lookup by singular name or kind
            try:
                return next(
                    r
                    for r in self._resources.values()
                    if r["kind"] == name or r["singularName"] == name
                )
            except StopIteration:
                raise ValueError(f"API '{self._api_version}' has no resource '{name}'")


class SyncApi(BaseApi):
    """
    API object for use with sync clients.
    """
    def resource(self, name):
        """
        Returns a resource for the given name.

        The given name can be either the plural name, the singular name or the kind.
        Lookups by plural name will be faster as that is the key that is indexed.
        """
        if self._resources is None:
            self._set_resources(self._client.get(self._api_path))
        resource = self._lookup_resource(name)
        return SyncResource(
            self._client,
            self._api_version,
            resource["name"],
            resource["kind"],
            resource["namespaced"]
        )


class AsyncApi(BaseApi):
    """
    API object for use with sync clients.
    """
    async def resource(self, name):
        """
        Returns a resource for the given name.

        The given name can be either the plural name, the singular name or the kind.
        Lookups by plural name will be faster as that is the key that is indexed.
        """
        if self._resources is None:
            self._set_resources(await self._client.get(self._api_path))
        resource = self._lookup_resource(name)
        return AsyncResource(
            self._client,
            self._api_version,
            resource["name"],
            resource["kind"],
            resource["namespaced"]
        )
