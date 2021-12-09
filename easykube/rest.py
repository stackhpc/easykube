import logging

import httpx


class PropertyDict(dict):
    """
    Dictionary implementation that also supports property-based access (read-only).
    """
    def __getitem__(self, key):
        value = super().__getitem__(key)
        return self.__class__(value) if isinstance(value, dict) else value

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")


class Client(httpx.AsyncClient):
    """
    Class for a REST client.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

    def get_next_page(self, response):
        """
        Given a response for a paginated request, return a tuple of (URL, parameters)
        for the next page.
        """
        # By default, assume there is no pagination
        return None, {}

    def raise_for_status(self, response):
        """
        Raise an exception for the given response if required.
        """
        # By default, raise the httpx exception
        response.raise_for_status()

    async def request(self, method, url, *, raise_exceptions = True, **kwargs):
        response = await super().request(method, url, **kwargs)
        params = kwargs.get("params")
        if params:
            self.logger.info("%s %s %s %s", method, url, response.status_code, params)
        else:
            self.logger.info("%s %s %s", method, url, response.status_code)
        if raise_exceptions:
            self.raise_for_status(response)
        return response

    def stream(self, method, url, **kwargs):
        params = kwargs.get("params")
        if params:
            self.logger.info("[STREAM] %s %s %s", method, url, params)
        else:
            self.logger.info("[STREAM] %s %s", method, url)
        return super().stream(method, url, **kwargs)

    async def get(self, url, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def options(self, url, **kwargs):
        return await self.request("OPTIONS", url, **kwargs)

    async def head(self, url, **kwargs):
        return await self.request("HEAD", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def put(self, url, **kwargs):
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url, **kwargs):
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self.request("DELETE", url, **kwargs)

    async def paginate(self, url, **kwargs):
        """
        Given a URL, return an async iterator of responses for the pages.
        """
        response = await self.get(url, **kwargs)
        while True:
            yield response
            next_url, params = self.get_next_page(response)
            if next_url:
                response = await self.get(next_url, params = params)
            else:
                break

    def resource(self, name):
        """
        Returns a resource for the given name.
        """
        return Resource(self, name)


class Resource:
    """
    Class for a REST resource.
    """
    def __init__(self, client, path):
        self.client = client
        self.path = "/" + path.strip("/")

    async def ensure_initialised(self):
        """
        Ensures that the resource is fully initialised including any async operations.
        """
        # By default, this is a noop

    def prepare_path(self, id = None, **params):
        """
        Prepare the path to use for the given parameters.

        Returns a (path, params) tuple where the path parameters have interpolated
        and the remaining params should be given as URL parameters.
        """
        # By default, there are no path parameters
        return f"{self.path}/{id}" if id else self.path, params

    def extract_list(self, response):
        """
        Extracts a list of instances from a list response.
        """
        return response.json()

    def extract_one(self, response):
        """
        Extracts a single instance from a fetch, create or update response.
        """
        return response.json()

    def wrap_instance(self, instance):
        """
        Receives an instance and wraps the instance if required.
        """
        # By default, wrap the instance in a property dict
        return PropertyDict(instance)

    def prepare_data(self, data, id = None, **params):
        """
        Prepare data for submitting as part of a create, replace or patch request.
        """
        return data

    async def list(self, **params):
        """
        Returns an async iterable of the resource instances that match the given parameters.
        """
        await self.ensure_initialised()
        path, params = self.prepare_path(**params)
        async for response in self.client.paginate(path, params = params):
            for instance in self.extract_list(response):
                yield self.wrap_instance(instance)

    async def first(self, **params):
        """
        Returns the first instance of a resource that matches the parameters, or None if
        one does not exist.
        """
        async for instance in self.list(**params):
            return instance

    async def create(self, data, **params):
        """
        Creates an instance of the resource and returns it.
        """
        await self.ensure_initialised()
        path, _ = self.prepare_path(**params)
        data = self.prepare_data(data, **params)
        response = await self.client.post(path, json = data)
        data = self.extract_one(response)
        return self.wrap_instance(data)

    async def fetch(self, id, **params):
        """
        Returns the data for the specified instance.
        """
        await self.ensure_initialised()
        path, _ = self.prepare_path(id, **params)
        response = await self.client.get(path)
        data = self.extract_one(response)
        return self.wrap_instance(data)

    async def replace(self, id, data, **params):
        """
        Replaces the specified instance with the given data.
        """
        await self.ensure_initialised()
        path, _ = self.prepare_path(id, **params)
        data = self.prepare_data(data, id, **params)
        response = await self.client.put(path, json = data)
        data = self.extract_one(response)
        return self.wrap_instance(data)

    async def patch(self, id, data, **params):
        """
        Patches the specified instance with the given data.
        """
        await self.ensure_initialised()
        path, _ = self.prepare_path(id, **params)
        data = self.prepare_data(data, id, **params)
        response = await self.client.patch(path, json = data)
        data = self.extract_one(response)
        return self.wrap_instance(data)

    async def _create_or_update(self, update_http_method, id, data, params):
        """
        Attempts to update the specified instance of the resource with the given data and method.

        If it does not exist, a new instance is created with the given data instead.
        """
        await self.ensure_initialised()
        path, _ = self.prepare_path(id, **params)
        data = self.prepare_data(data, id, **params)
        response = await self.client.request(
            update_http_method,
            path,
            json = data,
            raise_exceptions = False
        )
        if response.is_success:
            return self.wrap_instance(self.extract_one(response))
        elif response.status_code == 404:
            return await self.create(data, **params)
        else:
            self.client.raise_for_status(response)

    async def create_or_replace(self, id, data, **params):
        """
        Attempts to replace the specified instance of the resource with the given data.
        If it does not exist, a new instance is created with the given data instead.
        """
        return await self._create_or_update("PUT", id, data, params)

    async def create_or_patch(self, id, data, **params):
        """
        Attempts to replace the specified instance of the resource with the given data.
        If it does not exist, a new instance is created with the given data instead.
        """
        return await self._create_or_update("PATCH", id, data, params)

    async def delete(self, id, **params):
        """
        Delete the specified instance.
        """
        await self.ensure_initialised()
        path, _ = self.prepare_path(id, **params)
        response = await self.client.delete(path, raise_exceptions = False)
        # Suppress 404s as the desired state has been reached
        if not response.is_success and response.status_code != 404:
            self.client.raise_for_status(response)
