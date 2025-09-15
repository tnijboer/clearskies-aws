import base64
import json
from typing import Any, Literal, Optional, Union, cast
from urllib.parse import urlencode

from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.parser.models import (
    APIGatewayProxyEventModel,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from clearskies.input_outputs.input_output import InputOutput
from pydantic import ValidationError
from pydantic.networks import IPvAnyNetwork


class LambdaAPIGateway(InputOutput):
    _event: APIGatewayProxyEventModel
    _context: LambdaContext
    _request_headers: dict[str, str]
    _request_method: Literal["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    _resource = None
    _query_parameters: dict[str, Union[str, list[str]]] = {}
    _path_parameters: dict[str, str] = {}
    _cached_body = None
    _body_was_cached = False

    def __init__(self, event: dict, context: LambdaContext):
        try:
            # Manually parse the incoming event into MyEvent model
            self._event = parse(model=APIGatewayProxyEventModel, event=event)
        except ValidationError as e:
            # Catch validation errors and return a 400 response
            raise ValueError(f"Failed to parse event from ApiGateway: {e}")
        self._context = context
        self._request_method = self._event.httpMethod
        self._path = self._event.path
        self._resource = self._event.resource
        self._query_parameters = {
            **(self._event.queryStringParameters or {}),
            **(self._event.multiValueQueryStringParameters or {}),
        }
        self._path_parameters = self._event.pathParameters if self._event.pathParameters else {}
        self._request_headers = {}
        for key, value in {
            **self._event.headers,
            **self._event.multiValueHeaders,
        }.items():
            self._request_headers[key.lower()] = str(value)

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        if "content-type" not in self.response_headers:
            self.response_headers.content_type = "application/json; charset=UTF-8"

        is_base64 = False

        if isinstance(body, bytes):
            is_base64 = True
            final_body = base64.encodebytes(body).decode("utf8")
        elif isinstance(body, str):
            final_body = body
        else:
            final_body = json.dumps(body)

        return {
            "isBase64Encoded": is_base64,
            "statusCode": status_code,
            "headers": self.response_headers,
            "body": final_body,
        }

    def has_body(self) -> bool:
        return bool(self.get_body())

    def get_body(self) -> Any:
        if not self._body_was_cached:
            self._cached_body = self._event.body
            if self._cached_body is not None and self._event.isBase64Encoded and isinstance(self._cached_body, str):
                self._cached_body = base64.decodebytes(self._cached_body.encode("utf-8")).decode("utf-8")
        return self._cached_body

    def get_request_method(self) -> str:
        return self._request_method

    def get_script_name(self) -> str:
        return ""

    def get_path_info(self) -> str:
        return self._path

    def get_query_string(self) -> str:
        return urlencode(self._query_parameters) if self._query_parameters else ""

    def get_content_type(self) -> str:
        return str(self.get_request_header("content-type", True))

    def get_protocol(self) -> str:
        return "https"

    def has_request_header(self, header_name: str) -> bool:
        return header_name.lower() in self._request_headers

    def get_request_header(self, header_name: str, silent: bool = False) -> Union[list[str], str]:
        if header_name.lower() not in self._request_headers:
            if not silent:
                raise KeyError(f"HTTP header '{header_name}' was not found in request")
            return ""
        return self._request_headers[header_name.lower()]

    def get_query_parameter(self, key: str) -> list[str]:
        if not self._query_parameters or key not in self._query_parameters:
            return []

        # Convert to list if it's a string
        value = self._query_parameters[key]
        if isinstance(value, str):
            return [value]
        return value

    def get_query_parameters(self) -> dict[str, Union[str, list[str]]]:
        return self._query_parameters

    def context_specifics(self) -> dict[str, Any]:
        return {
            "event": self._event,
            "context": self._context,
        }

    def get_client_ip(self) -> IPvAnyNetwork:
        # I haven't actually tested with an API gateway yet to figure out which of these works...
        if hasattr(self._event, "requestContext") and hasattr(self._event.requestContext, "identity"):
            if hasattr(self._event.requestContext.identity, "sourceIp"):
                return cast(IPvAnyNetwork, self._event.requestContext.identity.sourceIp)

        return cast(IPvAnyNetwork, self.get_request_header("x-forwarded-for", silent=True))
