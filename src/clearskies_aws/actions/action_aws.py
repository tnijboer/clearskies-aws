import boto3
import json
import logging

from abc import ABC
from boto3 import client
from botocore.exceptions import ClientError
from clearskies.environment import Environment
from clearskies.models import Models
from collections import OrderedDict
from typing import Callable, Optional

from ..di import StandardDependencies
from .assume_role import AssumeRole
class ActionAws(ABC):

    _logging = logging.getLogger(__name__)
    _client: Optional[boto3.client] = None
    _name: Optional[str] = None

    def __init__(self, environment: Environment, boto3: boto3, di: StandardDependencies) -> None:
        """Setup action."""
        self.environment = environment
        self.boto3 = boto3
        self.di = di

    def configure(
        self,
        message_callable: Optional[Callable] = None,
        when: Optional[Callable] = None,
        assume_role: Optional[AssumeRole] = None,
    ) -> None:
        """Configues the Action."""
        self.when = when
        self.message_callable = message_callable
        self.assume_role = assume_role

        if self.message_callable and not callable(self.message_callable):
            raise ValueError(
                "'message_callable' should be a callable that returns the message for the queue, but a callable was not passed."
            )

        if when and not callable(when):
            raise ValueError("'when' must be a callable but something else was found")

        if not self._name:
            raise ValueError(f"Name of client not set.")

    def __call__(self, model: Models) -> None:
        """Send a notification as configured."""
        if self.when and not self.di.call_function(self.when, model=model):
            return

        try:
            client = self._getClient()
            self._execute_action(client, model)
        except ClientError as e:
            self._logging.exception(f"Failed to retrieve client for {self._name}")
            raise e

    def _getClient(self) -> boto3.client:
        """Retrieve the boto3 client."""
        if self._client:
            return self._client

        if self.assume_role:
            boto3 = self.assume_role(self.boto3)
        else:
            boto3 = self.boto3

        self._client = boto3.client(self._name)
        return self._client

    def _execute_action(self, client: boto3.client, model: Models) -> None:
        """Run the action."""
        pass

    def get_message_body(self, model: Models) -> str:
        """Retrieve the message for the action."""
        if self.message_callable:
            result = self.di.call_function(self.message_callable, model=model)
            if isinstance(result, dict) or isinstance(result, list):
                return json.dumps(result)
            if not isinstance(result, str):
                raise TypeError(
                    f"The return value from the message callable for the {__name__} action must be a string, dictionary, or list. I received a "
                    + f"{type(result)} after calling '{self.message_callable.__name__}'"
                )
            return result

        model_data = OrderedDict()
        for (column_name, column) in model.columns().items():
            if not column.is_readable:
                continue
            model_data[column_name] = column.to_json(model)
        return json.dumps(model_data)
