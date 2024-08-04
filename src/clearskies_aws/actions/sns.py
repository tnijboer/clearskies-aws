import boto3
import clearskies
import datetime
import json

from botocore.exceptions import ClientError
from collections.abc import Sequence
from clearskies.environment import Environment
from clearskies.models import Models
from types import ModuleType
from typing import List, Optional, Callable, cast

from ..di import StandardDependencies
from .assume_role import AssumeRole
from .action_aws import ActionAws
class SNS(ActionAws):
    _name = "sns"

    def __init__(self, environment: Environment, boto3: boto3, di: StandardDependencies) -> None:
        super().__init__(environment, boto3, di)

    def configure(
        self,
        topic=None,
        topic_environment_key=None,
        topic_callable: Optional[Callable] = None,
        message_callable: Optional[Callable] = None,
        when: Optional[Callable] = None,
        assume_role: Optional[AssumeRole] = None,
    ) -> None:
        """Configures the action for SNS."""
        super().configure(message_callable=message_callable, when=when, assume_role=assume_role)

        self.topic = topic
        self.topic_environment_key = topic_environment_key
        self.topic_callable = topic_callable

        topics = 0
        for value in [topic, topic_environment_key, topic_callable]:
            if value:
                topics += 1
        if topics > 1:
            raise ValueError(
                "You can only provide one of 'topic', 'topic_environment_key', or 'topic_callable', but more than one were provided."
            )
        if not topics:
            raise ValueError("You must provide at least one of 'topic', 'topic_environment_key', or 'topic_callable'.")

    def _execute_action(self, client: ModuleType, model: Models) -> None:
        """Send a notification as configured."""
        topic_arn = self.get_topic_arn(model)
        if not topic_arn:
            return
        client.publish(
            TopicArn=self.get_topic_arn(model),
            Message=self.get_message_body(model),
        )

    def get_topic_arn(self, model: Models) -> str:
        if self.topic:
            return self.topic
        if self.topic_environment_key:
            return self.environment.get(self.topic_environment_key)
        return self.di.call_function(self.topic_callable, model=model)
