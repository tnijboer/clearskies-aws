from .lambda_api_gateway import LambdaAPIGateway
from clearskies.handlers.exceptions import ClientError
import json
class LambdaSns(LambdaAPIGateway):
    def __init__(self, record, context):
        self._record = record
        self._context = context

    def respond(self, body, status_code=200):
        pass

    def get_body(self):
        json_body = self.json_body()
        if 'Message' in json_body:
            return json.loads(json_body['Message'])

        raise ValueError(f"Cannot retrieve body out of SNS event.")

    def request_data(self, required=True):
        return self.json_body(required=required)

    def json_body(self, required=True):
        if not self._record:
            if required:
                raise ClientError("SNS message was not valid JSON")
            return {}

        try:
            return json.loads(self._record)
        except json.JSONDecodeError:
            raise ClientError("SNS message was not valid JSON")

    def get_request_method(self):
        raise NotImplementedError("Request methods don't exist in an SNS context")

    def get_script_name(self):
        raise NotImplementedError("Script names doesn't exist in an SNS context")

    def get_path_info(self):
        raise NotImplementedError("Path info doesn't exist in an SNS context")

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
