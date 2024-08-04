import boto3
import json
import datetime

from botocore.exceptions import ClientError
from clearskies.environment import Environment
from clearskies.model import Model
from collections.abc import Sequence
from collections import OrderedDict
from types import ModuleType
from typing import List, Optional, Callable, Union

from ..di import StandardDependencies
from . import assume_role
from .action_aws import ActionAws
class SQS(ActionAws):
    _name = "sqs"

    def __init__(self, environment: Environment, boto3: boto3, di: StandardDependencies) -> None:
        """Setup action."""
        super().__init__(environment, boto3, di)

    def configure(
        self,
        queue_url: str = '',
        queue_url_environment_key: str = '',
        queue_url_callable: Optional[Callable] = None,
        message_callable: Optional[Callable] = None,
        when: Optional[Callable] = None,
        assume_role: Optional[assume_role.AssumeRole] = None,
        message_group_id: Optional[Union[str, Callable]] = None,
    ) -> None:
        super().configure(message_callable=message_callable, when=when, assume_role=assume_role)

        self.queue_url = queue_url
        self.queue_url_environment_key = queue_url_environment_key
        self.queue_url_callable = queue_url_callable
        self.message_group_id = message_group_id

        queue_urls = 0
        for value in [queue_url, queue_url_environment_key, queue_url_callable]:
            if value:
                queue_urls += 1
        if queue_urls > 1:
            raise ValueError(
                "You can only provide one of 'queue_url', 'queue_url_environment_key', or 'queue_url_callable', but more than one were provided."
            )
        if not queue_urls:
            raise ValueError(
                "You must provide at least one of 'queue_url', 'queue_url_environment_key', or 'queue_url_callable'."
            )
        if message_group_id and not callable(message_group_id) and not isinstance(message_group_id, str):
            raise ValueError(
                "If provided, 'message_group_id' must be a string or callable, but the provided value was neither."
            )

    def _execute_action(self, client: ModuleType, model: Model) -> None:
        """Send a notification as configured."""
        params = {
            "QueueUrl": self.get_queue_url(model),
            "MessageBody": self.get_message_body(model),
        }
        if not params["QueueUrl"]:
            return

        if self.message_group_id:
            if callable(self.message_group_id):
                message_group_id = self.di.call_function(self.message_group_id, model=model)
                if not isinstance(message_group_id, str):
                    raise ValueError(f"I called the message_group_id function for SQS for model '{model.__class__.__name__}' but the value it returned was not a string.  The message group id must be a string.")
            else:
                message_group_id = self.message_group_id
            params["MessageGroupId"] = message_group_id

        client.send_message(**params)

    def get_queue_url(self, model: Model):
        if self.queue_url:
            return self.queue_url
        if self.queue_url_environment_key:
            return self.environment.get(self.queue_url_environment_key)
        return self.di.call_function(self.queue_url_callable, model=model)
