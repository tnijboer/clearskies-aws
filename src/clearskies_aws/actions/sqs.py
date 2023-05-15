from typing import List
import datetime
import clearskies
from botocore.exceptions import ClientError
from collections.abc import Sequence
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
    ) -> None:
        self.when = when
        self.message_callable = message_callable
        self.queue_url = queue_url
        self.queue_url_environment_key = self.queue_url_environment_key
        self.queue_url_callable = self.queue_url_callable

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

    def get_queue_url(self):
        if self.queue_url:
            return self.queue_url
        if self.queue_url_environment_key:
            return self.environment.get(self.queue_url_environment_key)
        return self.di.call_function(self.queue_url_callable, model=model)

    def get_message_body(self, model):
        if self.message_callable:
            return self.di.call_function(self.message_callable, model=model)
        #json = OrderedDict()
        #for (output_name, column) in self._as_json_map.items():
        #column_data = column.to_json(model)
        #if type(column_data) == dict:
        #for (key, value) in column_data.items():
        #json[self.auto_case_column_name(key, True)] = value
        #else:
        #json[output_name] = column_data
        #return json
