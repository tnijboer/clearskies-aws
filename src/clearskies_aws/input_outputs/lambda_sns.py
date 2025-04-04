from .lambda_api_gateway import LambdaAPIGateway
from clearskies.handlers.exceptions import ClientError
import json
class LambdaSns(LambdaAPIGateway):
    def __init__(self, event, context, url=None, method=None):
        self._event = event
        self._context = context
        self._path = url if url else ''
        self._request_method = method.upper() if method else 'GET'
        record = event['Records'][0]['Sns']['Message']
        try:
            self._record = json.loads(record)
        except json.JSONDecodeError as e:
            raise ClientError("The message from AWS was not a serialized JSON string.  The lambda_sns context for clearskies only accepts serialized JSON")

    def respond(self, body, status_code=200):
        pass

    def get_body(self):
        return self._record

    def request_data(self, required=True, allow_non_json_bodies=False):
        return self.json_body(required=required, allow_non_json_bodies=allow_non_json_bodies)

    def json_body(self, required=True, allow_non_json_bodies=False):
        if not self._record:
            if required:
                raise ClientError("No SNS message found")
            return {}

        return self._record

    def get_query_string(self):
        raise NotImplementedError("The query string doesn't exist in an SNS context")

    def get_content_type(self):
        raise NotImplementedError("Content type doesn't exist in an SNS context")

    def get_protocol(self):
        raise NotImplementedError("A request protocol is not defined in an SNS context")

    def has_request_header(self, header_name):
        raise NotImplementedError("SNS contexts don't have request headers")

    def get_request_header(self, header_name, silent=True):
        raise NotImplementedError("SNS contexts don't have request headers")

    def get_query_parameter(self, key):
        raise NotImplementedError("SNS contexts don't have query parameters")

    def get_query_parameters(self):
        raise NotImplementedError("SNS contexts don't have query parameters")
