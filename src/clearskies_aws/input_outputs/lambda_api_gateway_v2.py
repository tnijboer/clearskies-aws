import base64
import json
from typing import Any, Literal, Optional, Union
from urllib.parse import urlencode

from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.parser.models import (
    APIGatewayProxyEventV2Model,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from pydantic import ValidationError

import clearskies_aws


class LambdaAPIGatewayV2(clearskies_aws.input_outputs.LambdaAPIGateway):
    _event: APIGatewayProxyEventV2Model  # type: ignore[assignment]
    _query_parameters: dict[str, str] = {}  # type: ignore[assignment]

    def __init__(self, event: dict, context: LambdaContext):
        try:
            # Manually parse the incoming event into MyEvent model
            self._event = parse(model=APIGatewayProxyEventV2Model, event=event)
        except ValidationError as e:
            # Catch validation errors and return a 400 response
            raise ValueError(f"Failed to parse event from ApiGateway: {e}")
        self._context = context
        self._request_method = self._event.requestContext.http.method
        self._path = self._event.requestContext.http.path
        self._query_parameters = self._event.queryStringParameters or {}
        self._path_parameters = self._event.pathParameters
        self._request_headers = {}
        for key, value in self._event.headers.items():
            self._request_headers[key.lower()] = value

    def get_protocol(self):
        return self._event.requestContext.http.protocol

    def get_client_ip(self):
        # I haven't actually tested with an API gateway yet to figure out which of these works...
        if hasattr(self._event, "requestContext") and hasattr(self._event.requestContext, "http"):
            if hasattr(self._event.requestContext.http, "sourceIp"):
                return self._event.requestContext.http.sourceIp

        return self.get_request_header("x-forwarded-for", silent=True)
