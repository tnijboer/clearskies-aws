from .lambda_api_gateway import LambdaAPIGateway
import json
from clearskies.handlers.exceptions import ClientError
class LambdaInvocation(LambdaAPIGateway):
    def __init__(
        self,
        event,
        context,
        method=None,
        url=None,
    ):
        self._event = event
        self._context = context
        self._path = url if url else ''
        self._request_method = method.upper() if method else 'GET'
        self._query_parameters = {}
        self._path_parameters = []
        self._request_headers = {}

    def has_body(self):
        return True

    def get_body(self):
        return self._event

    def json_body(self, required=True):
        if required and not self._event:
            raise ClientError("Request body was not valid JSON")
        return self._event

    def respond(self, body, status_code=200):
        return body.decode('utf-8') if type(body) == bytes else body
