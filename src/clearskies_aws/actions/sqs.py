from typing import List
import datetime
import clearskies
from botocore.exceptions import ClientError
from collections.abc import Sequence
from collections import OrderedDict
import json
class SQS:
    def __init__(self, environment, boto3, di):
        self.environment = environment
        self.boto3 = boto3
        self.di = di

    def configure(
        self,
        queue_url=None,
        queue_url_environment_key=None,
        queue_url_callable=None,
        message_callable=None,
        when=None,
        assume_role=None,
    ) -> None:
        self.when = when
        self.message_callable = message_callable
        self.queue_url = queue_url
        self.queue_url_environment_key = queue_url_environment_key
        self.queue_url_callable = queue_url_callable
        self.assume_role = assume_role

        if self.message_callable and not callable(self.message_callable):
            raise ValueError(
                "'message_callable' should be a callable that returns the message for the queue, but a callable was not passed."
            )
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
        if when and not callable(when):
            raise ValueError("'when' must be a callable but something else was found")

    def __call__(self, model) -> None:
        """Send a notification as configured."""
        if self.when and not self.di.call_function(self.when, model=model):
            return

        if self.assume_role:
            boto3 = self.assume_role(self.boto3)
        else:
            boto3 = self.boto3

        boto3.client("sqs").send_message(
            QueueUrl=self.get_queue_url(model),
            MessageBody=self.get_message_body(model),
        )

    def get_queue_url(self, model):
        if self.queue_url:
            return self.queue_url
        if self.queue_url_environment_key:
            return self.environment.get(self.queue_url_environment_key)
        return self.di.call_function(self.queue_url_callable, model=model)

    def get_message_body(self, model):
        if self.message_callable:
            result = self.di.call_function(self.message_callable, model=model)
            if type(result) == dict or type(result) == list:
                return json.dumps(result)
            if type(result) != str:
                raise TypeError(
                    "The return value from the message callable for the SQS action must be a string, dictionary, or list.   I received a "
                    + type(result) + " after calling '" + self.message_callable.__name__ + "'"
                )
            return result

        model_data = OrderedDict()
        for (column_name, column) in model.columns().items():
            if not column.is_readable:
                continue
            model_data[column_name] = column.to_json(model)
        return json.dumps(model_data)
