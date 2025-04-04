from .lambda_api_gateway import LambdaAPIGateway
from clearskies.handlers.exceptions import ClientError
import json
class LambdaSqsStandard(LambdaAPIGateway):
    def __init__(self, record, event, context, url=None, method=None):
        self._record = record
        self._context = context
        self._event = event
        self._path = url if url else ''
        self._request_method = method.upper() if method else 'GET'

    def respond(self, body, status_code=200):
        pass

    def get_body(self):
        return self._record

    def has_body(self):
        return True

    def request_data(self, required=True, allow_non_json_bodies=False):
        return self.json_body(required=required, allow_non_json_bodies=allow_non_json_bodies)

    def json_body(self, required=True, allow_non_json_bodies=False):
        if not self._record:
            if required:
                raise ClientError("SQS message was not valid JSON")
            return {}

        try:
            return json.loads(self._record)
        except json.JSONDecodeError:
            raise ClientError("SQS message was not valid JSON")

    def get_query_string(self):
        raise NotImplementedError("The query string doesn't exist in an SQS context")

    def get_content_type(self):
        raise NotImplementedError("Content type doesn't exist in an SQS context")

    def get_protocol(self):
        raise NotImplementedError("A request protocol is not defined in an SQS context")

    def has_request_header(self, header_name):
        raise NotImplementedError("SQS contexts don't have request headers")

    def get_request_header(self, header_name, silent=True):
        raise NotImplementedError("SQS contexts don't have request headers")

    def get_query_parameter(self, key):
        raise NotImplementedError("SQS contexts don't have query parameters")

    def get_query_parameters(self):
        raise NotImplementedError("SQS contexts don't have query parameters")
