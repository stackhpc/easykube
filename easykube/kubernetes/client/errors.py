import json

import httpx


class ApiError(httpx.HTTPStatusError):
    """
    Exception that is raised when a Kubernetes API error occurs that is in the 4xx range.
    """
    def __init__(self, source):
        try:
            data = source.response.json()
        except (json.JSONDecodeError, KeyError):
            message = source.response.text
            reason = None
        else:
            message = data["message"]
            reason = data.get("reason")
        super().__init__(message, request = source.request, response = source.response)
        self.status_code = source.response.status_code
        self.reason = reason
