import unittest
from unittest.mock import MagicMock, call
from .sqs import SQS
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
class SQSTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind('environment', {'AWS_REGION': 'us-east-2'})
        self.users = self.di.build(User)
        self.sqs = MagicMock()
        self.sqs.send_message = MagicMock()
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.sqs)
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
        sqs = SQS(self.environment, self.boto3, self.di)
        sqs.configure(
            queue_url='https://queue.example.com',
            when=self.always,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        sqs(user)
        self.sqs.send_message.assert_has_calls([
            call(
                QueueUrl='https://queue.example.com',
                MessageBody=json.dumps({
                    "id": "1-2-3-4",
                    "name": "Jane",
                    "email": "jane@example.com",
                }),
            ),
        ])
        self.assertEqual(id(user), id(self.when))

    def test_send_message_group_id(self):
        sqs = SQS(self.environment, self.boto3, self.di)
        sqs.configure(
            queue_url='https://queue.example.com',
            when=self.always,
            message_group_id='heysup',
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        sqs(user)
        self.sqs.send_message.assert_has_calls([
            call(
                QueueUrl='https://queue.example.com',
                MessageGroupId='heysup',
                MessageBody=json.dumps({
                    "id": "1-2-3-4",
                    "name": "Jane",
                    "email": "jane@example.com",
                }),
            ),
        ])
        self.assertEqual(id(user), id(self.when))

    def test_send_message_group_id_callable(self):
        sqs = SQS(self.environment, self.boto3, self.di)
        sqs.configure(
            queue_url='https://queue.example.com',
            when=self.always,
            message_group_id=lambda model: model.id,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        sqs(user)
        self.sqs.send_message.assert_has_calls([
            call(
                QueueUrl='https://queue.example.com',
                MessageGroupId='1-2-3-4',
                MessageBody=json.dumps({
                    "id": "1-2-3-4",
                    "name": "Jane",
                    "email": "jane@example.com",
                }),
            ),
        ])
        self.assertEqual(id(user), id(self.when))

    def test_not_now(self):
        sqs = SQS(self.environment, self.boto3, self.di)
        sqs.configure(
            queue_url='https://queue.example.com',
            when=self.never,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        sqs(user)
        self.sqs.send_message.assert_not_called()
        self.assertEqual(id(user), id(self.when))
