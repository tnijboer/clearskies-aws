import base64
import json
from typing import Any, Literal, Optional, TypeVar, Union, cast
from urllib.parse import urlencode

from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.parser.models import AlbModel
from aws_lambda_powertools.utilities.typing import LambdaContext
from clearskies.input_outputs.input_output import InputOutput
from pydantic import ValidationError

import clearskies_aws


class LambdaALB(clearskies_aws.input_outputs.LambdaAPIGateway):
    # Override the parent class's type annotation
    _event: AlbModel  # type: ignore[assignment]

    def __init__(self, event: dict, context: LambdaContext):
        try:
            # Manually parse the incoming event into AlbModel
            self._event = parse(model=AlbModel, event=event)
        except ValidationError as e:
            # Catch validation errors and return a 400 response
            raise ValueError(f"Failed to parse event from ApiGateway: {e}")
        self._context = context

        # Ensure the method is one of the allowed literals
        http_method = self._event.httpMethod.upper()
        valid_methods = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
        if http_method in valid_methods:
            self._request_method = cast(
                Literal["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
                http_method,
            )
        else:
            raise ValueError(f"Unsupported HTTP method: {http_method}")

        self._path = self._event.path

        # Initialize query parameters to match parent class pattern
        # ALB events don't have multiValueQueryStringParameters, so we only use queryStringParameters
        self._query_parameters = {
            **(self._event.queryStringParameters or {}),
        }

        # Initialize path parameters (ALB events don't have path parameters)
        self._path_parameters = {}

        # Initialize headers in the same way as parent class
        self._request_headers = {}
        for key, value in self._event.headers.items():
            self._request_headers[key.lower()] = value

    def get_client_ip(self):
        return self.get_request_header("x-forwarded-for")
