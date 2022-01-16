from .lambda_api_gateway import LambdaAPIGateway
from clearskies.handlers.exceptions import ClientError
class LambdaInvocation(LambdaAPIGateway):
    def __init__(self, event, context):
        self._event = event
        self._context = context
        self._path = ''
        self._request_method = 'GET'
        self._query_parameters = {}
        self._path_parameters = []
        self._request_headers = {}

    def json_body(self, required=True):
        if required and not self._event:
            raise ClientError("Request body was not valid JSON")
        return self._event
