import functools
import inspect
import json

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

    def __setattr__(self, name, value):
        self[name] = value


class ClientMixin:
    """
    Mixin class containing common methods for sync and async clients.
    """
    def raise_for_status(self, response):
        """
        Raise the relevant exception for the response, if required.
        """
        response.raise_for_status()


class SyncClient(ClientMixin, httpx.Client):
    """
    Class for a sync REST client.
    """
    def send(self, request, **kwargs):
        response = super().send(request, **kwargs)
        self.raise_for_status(response)
        return response

    def execute_flow(self, flow):
        """
        Executes a flow (generator that yields requests).
        """
        try:
            action = flow.send
            to_send = None
            while True:
                try:
                    yielded_obj = action(to_send)
                except StopIteration as exc:
                    # If the generator returns, we are done
                    return exc.value
                try:
                    if isinstance(yielded_obj, httpx.Request):
                        to_send = self.send(yielded_obj)
                    elif inspect.isgenerator(yielded_obj):
                        to_send = self.execute_flow(yielded_obj)
                    else:
                        to_send = yielded_obj
                except Exception as exc:
                    action = flow.throw
                    to_send = exc
                else:
                    action = flow.send
        finally:
            flow.close()


class AsyncClient(ClientMixin, httpx.AsyncClient):
    """
    Class for a REST client.
    """
    async def send(self, request, **kwargs):
        response = await super().send(request, **kwargs)
        self.raise_for_status(response)
        return response

    async def execute_flow(self, flow):
        """
        Executes a flow (generator that yields requests).
        """
        try:
            action = flow.send
            to_send = None
            while True:
                try:
                    yielded_obj = action(to_send)
                except StopIteration as exc:
                    # If the generator returns, we are done
                    return exc.value
                try:
                    if isinstance(yielded_obj, httpx.Request):
                        to_send = await self.send(yielded_obj)
                    elif inspect.isgenerator(yielded_obj):
                        to_send = await self.execute_flow(yielded_obj)
                    elif inspect.isawaitable(yielded_obj):
                        to_send = await yielded_obj
                    else:
                        to_send = yielded_obj
                except Exception as exc:
                    action = flow.throw
                    to_send = exc
                else:
                    action = flow.send
        finally:
            flow.close()


class Client:
    """
    Class for a REST client that is both a sync and an async context manager, yielding the
    sync or async client depending which context is entered.
    """
    __sync_client_class__ = SyncClient
    __async_client_class__ = AsyncClient

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._sync_client = None
        self._async_client = None

    def __enter__(self):
        if self._sync_client is None:
            self._sync_client = self.__sync_client_class__(*self._args, **self._kwargs)
            self._sync_client.__enter__()
        return self._sync_client

    def __exit__(self, exc_type, exc_value, traceback):
        if self._sync_client is not None:
            try:
                return self._sync_client.__exit__(exc_type, exc_value, traceback)
            finally:
                self._sync_client = None

    async def __aenter__(self):
        if self._async_client is None:
            self._async_client = self.__async_client_class__(*self._args, **self._kwargs)
            await self._async_client.__aenter__()
        return self._async_client

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._async_client is not None:
            try:
                return (await self._async_client.__aexit__(exc_type, exc_value, traceback))
            finally:
                self._async_client = None


def flow(method):
    """
    Decorator that marks a method as a flow that can be resolved synchronously or asynchronously
    depending on the client that is in use.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        gen = method(self, *args, **kwargs)
        if isinstance(self, (SyncClient, AsyncClient)):
            return self.execute_flow(gen)
        else:
            return self._client.execute_flow(gen)
    return wrapper


class ListResponseIterator:
    """
    Iterator for list responses.

    Can be used as either a sync or async iterator depending on the client that is given.
    """
    class StopIteration(Exception):
        """
        Exception raised to indicate that a list response has finished iterating.
        """

    def __init__(self, client, resource, params):
        self._client = client
        self._resource = resource
        self._data = []
        self._next_index = 0
        self._next_url, self._next_params = resource._prepare_path(params = params)

    def _extract_list(self, response):
        """
        Extracts a list of instances from a list response.

        By default, this is deferred to the resource method so cases where a custom iterator
        is required are rare.
        """
        return self._resource._extract_list(response)

    def _extract_next_page(self, response):
        """
        Return a (URL, params) tuple for the next page or None if there is no next page.

        By default, this is deferred to the resource method so cases where a custom iterator
        is required are rare.
        """
        return self._resource._extract_next_page(response)

    @flow
    def _next_item(self):
        """
        Flow that returns the next item in the iterator.
        """
        yield self._resource._ensure_initialised()
        # If we have run out of data, try to load some more
        if self._next_index >= len(self._data) and self._next_url:
            response = yield self._client.get(self._next_url, params = self._next_params)
            self._data = self._extract_list(response)
            self._next_index = 0
            self._next_url, self._next_params = self._extract_next_page(response) or (None, None)
        # Return the item at the next index before incrementing it
        try:
            next_item = self._data[self._next_index]
        except IndexError:
            raise self.StopIteration
        else:
            self._next_index = self._next_index + 1
            return self._resource._wrap_instance(next_item)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self._next_item()
        except self.StopIteration:
            raise StopIteration

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self._next_item()
        except self.StopIteration:
            raise StopAsyncIteration


class StreamIterator:
    """
    Iterator over items in a stream.

    Can be used as a sync or async iterator depending on the client that is given.
    """
    class SuppressItem(Exception):
        """
        Exception that can be raised to suppress an item from being yielded.
        """

    def __init__(self, client, method, url, **kwargs):
        self._client = client
        self._method = method
        self._url = url
        self._kwargs = kwargs

    def _stream(self):
        """
        Returns the stream for the iterator.
        """
        return self._client.stream(self._method, self._url, **self._kwargs)

    def _process_chunk(self, chunk):
        """
        Process the given chunk of bytes and return the item to be yielded.

        To suppress the yielding of an item for a chunk, raise the special exception
        StreamIterator.SuppressItem.

        By default, chunks are processed as JSON.
        """
        return json.loads(chunk)

    def _should_resume(self, exception):
        """
        Receives the exception that caused the stream to exit, or None if the stream
        terminated without error, and returns a truthy value if the stream should be
        resumed.
        """
        # By default, this is a NOOP which causes the iterator to exit

    def __iter__(self):
        while True:
            try:
                with self._stream() as response:
                    for chunk in response.iter_bytes():
                        try:
                            yield self._process_chunk(chunk)
                        except self.SuppressItem:
                            continue
            except Exception as exc:
                if not self._should_resume(exc):
                    raise
            else:
                if not self._should_resume(None):
                    break

    async def __aiter__(self):
        while True:
            try:
                async with self._stream() as response:
                    async for chunk in response.aiter_bytes():
                        try:
                            yield self._process_chunk(chunk)
                        except self.SuppressItem:
                            continue
            except Exception as exc:
                if not self._should_resume(exc):
                    raise
            else:
                if not self._should_resume(None):
                    break


class Resource:
    """
    Class for a REST resource.
    """
    __iterator_class__ = ListResponseIterator

    def __init__(self, client, name, prefix = None):
        self._client = client
        self._name = name.strip("/")
        self._prefix = prefix or "/"

    def _ensure_initialised(self):
        """
        Ensures that the resource is fully initialised including any HTTP operations.

        This will be called multiple times, so should become a no-op in the case where
        the resource is already initialised.
        """
        # By default, this is a noop

    def _prepare_path(self, id = None, params = None):
        """
        Prepare the path to use for the given parameters.

        Returns a (path, params) tuple where the path parameters have interpolated
        and the remaining params should be given as URL parameters.
        """
        if id:
            parts = self._name.split("/", maxsplit = 1)
            parts.insert(1, id)
            return self._prefix + "/".join(parts), params
        else:
            return self._prefix + self._name, params

    def _prepare_data(self, data, id = None, params = None):
        """
        Prepare data for submitting as part of a create, replace or patch request.
        """
        return data

    def _extract_list(self, response):
        """
        Extracts a list of instances from a list response.
        """
        return response.json()

    def _extract_next_page(self, response):
        """
        Given a list response, return a (URL, params) tuple for the next page or None
        if there is no next page.
        """
        # By default, assume there is no pagination
        return None

    def _extract_one(self, response):
        """
        Extracts a single instance from a fetch, create or update response.
        """
        content_type = response.headers.get("content-type")
        if content_type == "application/json":
            return response.json()
        elif content_type == "text/plain":
            return response.text
        else:
            return response.content

    def _wrap_instance(self, instance):
        """
        Receives an instance and wraps the instance if required.
        """
        # By default, wrap the instance in a property dict
        return PropertyDict(instance) if isinstance(instance, dict) else instance

    def list(self, **params):
        """
        Returns an iterable of the resource instances that match the given parameters.
        """
        return self.__iterator_class__(self._client, self, params)

    @flow
    def first(self, **params):
        """
        Returns the first instance of a resource that matches the parameters, or None if one
        does not exist.
        """
        try:
            return (yield self.list(**params)._next())
        except ListResponseIterator.StopIteration:
            return None

    @flow
    def create(self, data, **params):
        """
        Creates an instance of the resource and returns it.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(params = params)
        data = self._prepare_data(data, params = params)
        response = yield self._client.post(path, json = data, params = params)
        return self._wrap_instance(self._extract_one(response))

    @flow
    def fetch(self, id, **params):
        """
        Returns the data for the specified instance.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(id, params)
        response = yield self._client.get(path, params = params)
        return self._wrap_instance(self._extract_one(response))

    @flow
    def replace(self, id, data, **params):
        """
        Replaces the specified instance with the given data.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(id, params)
        data = self._prepare_data(data, id, params)
        response = yield self._client.put(path, json = data, params = params)
        return self._wrap_instance(self._extract_one(response))

    @flow
    def patch(self, id, data, **params):
        """
        Patches the specified instance with the given data.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(id, params)
        data = self._prepare_data(data, id, params)
        response = yield self._client.patch(path, json = data, params = params)
        return self._wrap_instance(self._extract_one(response))

    def _create_or_update(self, method, id, data, params):
        """
        Flow that attempts to update an instance using the given data and method. If the
        instance does not exist, it is created with the given data.
        """
        try:
            return (yield method(id, data, **params))
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return (yield self.create(data, **params))
            else:
                raise

    @flow
    def create_or_replace(self, id, data, **params):
        """
        Attempts to replace the specified instance of the resource with the given data.
        If it does not exist, a new instance is created with the given data instead.
        """
        return (yield self._create_or_update(self.replace, id, data, params))

    @flow
    def create_or_patch(self, id, data, **params):
        """
        Attempts to patch the specified instance of the resource with the given data.
        If it does not exist, a new instance is created with the given data instead.
        """
        return (yield self._create_or_update(self.patch, id, data, params))

    @flow
    def delete(self, id, **params):
        """
        Delete the specified instance.
        """
        yield self._ensure_initialised()
        path, params = self._prepare_path(id, params)
        try:
            yield self._client.delete(path, params = params)
        except httpx.HTTPStatusError as exc:
            # Suppress 404s as the desired state has been reached
            if exc.response.status_code != 404:
                raise
