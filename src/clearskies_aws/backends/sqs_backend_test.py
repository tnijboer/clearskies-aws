import unittest
import json
from unittest.mock import MagicMock
from collections import OrderedDict
from types import SimpleNamespace
from .sqs_backend import SqsBackend
import clearskies
from ..di import StandardDependencies
class User(clearskies.Model):
    def __init__(self, sqs_backend, columns):
        super().__init__(sqs_backend, columns)

    id_column_name = 'name'

    def columns_configuration(self):
        return OrderedDict([
            clearskies.column_types.string('name'),
        ])
class SqsBackendTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind('environment', {'AWS_REGION': 'us-east-2'})
        self.sqs = SimpleNamespace(send_message=MagicMock())
        self.boto3 = SimpleNamespace(client=MagicMock(return_value=self.sqs))
        self.di.bind('boto3', self.boto3)

    def test_send(self):
        user = self.di.build(User)
        user.save({'name': 'sup'})
        self.boto3.client.assert_called_with('sqs', region_name='us-east-2')
        self.sqs.send_message.assert_called_with(QueueUrl='users', MessageBody=json.dumps({"name": "sup"}))
