import asyncio
import json
import logging

import httpx


logger = logging.getLogger(__name__)


class BaseClient:
    """
    Base class for sync and async REST clients.
    """
    def __init__(self, /, json_encoder = None, **kwargs):
        event_hooks = kwargs.setdefault("event_hooks", {})
        event_hooks.setdefault("response", []).append(self.raise_for_status)
        super().__init__(**kwargs)
        self._json_encoder = json_encoder

    def raise_for_status(self, response):
        raise NotImplementedError

    def build_request(self, method, url, **kwargs):
        # Use the specified JSON-encoder to encode the JSON object
        content = kwargs.get("content")
        json_obj = kwargs.pop("json", None)
        if content is None and json_obj is not None:
            kwargs["content"] = json.dumps(json_obj, default = self._json_encoder)
        return super().build_request(method, url, **kwargs)


class SyncClient(BaseClient, httpx.Client):
    """
    Class for a sync REST client.
    """
    def raise_for_status(self, response):
        """
        Raise the relevant exception for the response, if required.
        """
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            exc.response.read()
            raise exc

    def delete(self, url, **kwargs):
        """
        Sends a delete request.

        We override the delete from HTTPX in order to be able to send a request body.
        """
        return self.request("DELETE", url, **kwargs)


class AsyncClient(BaseClient, httpx.AsyncClient):
    """
    Class for a REST client.
    """
    async def raise_for_status(self, response):
        """
        Raise the relevant exception for the response, if required.
        """
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            await exc.response.aread()
            raise exc

    async def delete(self, url, **kwargs):
        """
        Sends a delete request.

        We override the delete from HTTPX in order to be able to send a request body.
        """
        return await self.request("DELETE", url, **kwargs)

    async def _send_single_request(self, request):
        # anyio.fail_after seems to raise a TimeoutError even when the task is cancelled
        # from the outside rather than because it reaches the deadline
        # This is then translated into a PoolTimeout by httpx, even though the connection
        # pool has plenty of availability
        # We can prevent this while still allowing the request to appear cancelled from
        # the outside by shielding the request coroutine
        # This will mean that the request will continue until it is fulfilled, even if
        # nothing is waiting for it, but means PoolTimeouts will only be raised when
        # we genuinely fail to get a connection from the pool
        # As per the advice in the docs for shield, we maintain a reference to the
        # wrapped coroutine to avoid it being garbage collected too early
        coro = super()._send_single_request(request)
        response = await asyncio.shield(coro)
        return response
