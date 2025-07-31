import base64
import json
from typing import Any, Literal, Optional, Union
from urllib.parse import urlencode

from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.parser.models import (
    APIGatewayWebSocketConnectEventModel,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from pydantic import ValidationError

import clearskies_aws


class LambdaAPIGatewayWebSocket(clearskies_aws.input_outputs.LambdaAPIGateway):
    _event: APIGatewayWebSocketConnectEventModel

    def __init__(self, event: dict, context: LambdaContext):
        try:
            # Manually parse the incoming event into MyEvent model
            self._event = parse(model=APIGatewayWebSocketConnectEventModel, event=event)
        except ValidationError as e:
            # Catch validation errors and return a 400 response
            raise ValueError(
                f"Failed to parse event from APIGatewayWebSocketConnectEventModel: {e}"
            )
        self._context = context
        self._request_method = self._event.request_context.http.method
        self._path = self._event.requestContext.http.path
        self._query_parameters = self._event.queryStringParameters
        self._path_parameters = self._event.pathParameters
        self._request_headers = {}
        for key, value in self._event.headers.items():
            self._request_headers[key.lower()] = value

    def context_specifics(self):
        return {
            "event": self._event,
            "context": self._context,
            "connection_id": self._event["requestContext"]["connectionId"],
        }
