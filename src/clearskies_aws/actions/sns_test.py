import unittest
import boto3

from unittest.mock import MagicMock, call
from .sns import SNS
import clearskies
from ..di import StandardDependencies
import json
from collections import OrderedDict
class User(clearskies.Model):
    def __init__(self, memory_backend, columns):
        super().__init__(memory_backend, columns)

    def columns_configuration(self):
        return OrderedDict([
            clearskies.column_types.string('name'),
            clearskies.column_types.email('email'),
        ])
class SNSTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind('environment', {'AWS_REGION': 'us-east-2'})
        self.users = self.di.build(User)
        self.sns = MagicMock()
        self.sns.publish = MagicMock()
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.sns)
        self.when = None
        self.environment = MagicMock()
        self.environment.get = MagicMock(return_value='us-east-1')

    def always(self, model):
        self.when = model
        return True

    def never(self, model):
        self.when = model
        return False

    def test_send(self):
        sns = SNS(self.environment, self.boto3, self.di)
        sns.configure(
            topic='arn:aws:my-topic',
            when=self.always,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        sns(user)
        self.sns.publish.assert_has_calls([
            call(
                TopicArn='arn:aws:my-topic',
                Message=json.dumps({
                    "id": "1-2-3-4",
                    "name": "Jane",
                    "email": "jane@example.com",
                }),
            ),
        ])
        self.assertEqual(id(user), id(self.when))

    def test_not_now(self):
        sns = SNS(self.environment, self.boto3, self.di)
        sns.configure(
            topic='arn:aws:my-topic',
            when=self.never,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        sns(user)
        self.sns.publish.assert_not_called()
        self.assertEqual(id(user), id(self.when))
