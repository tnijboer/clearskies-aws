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
    _event: APIGatewayWebSocketConnectEventModel  # type: ignore[assignment]
    _request_method: str  # type: ignore[assignment]

    def __init__(self, event: dict, context: LambdaContext):
        try:
            # Manually parse the incoming event into MyEvent model
            self._event = parse(model=APIGatewayWebSocketConnectEventModel, event=event)
        except ValidationError as e:
            # Catch validation errors and return a 400 response
            raise ValueError(f"Failed to parse event from APIGatewayWebSocketConnectEventModel: {e}")
        self._context = context
        self._request_method = self._event.request_context.route_key
        self._path = ""
        self._query_parameters = {}
        self._path_parameters = {}
        self._request_headers = {}

    def context_specifics(self):
        return {
            "event": self._event,
            "context": self._context,
            "connection_id": self._event.request_context.connection_id,
        }
