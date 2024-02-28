import unittest
from unittest.mock import MagicMock, call
from .step_function import StepFunction
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
class StepFunctionTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind('environment', {'AWS_REGION': 'us-east-2'})
        self.users = self.di.build(User)
        self.step_function = MagicMock()
        self.step_function.start_execution = MagicMock()
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.step_function)
        self.when = None
        self.environment = MagicMock()
        self.environment.get = MagicMock(return_value='us-east-1')

    def always(self, model):
        self.when = model
        return True

    def never(self, model):
        self.when = model
        return False

    def test_execute(self):
        step_function = StepFunction(self.environment, self.boto3, self.di)
        step_function.configure(
            arn='aws::arn::step/asdf-er',
            when=self.always,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        step_function(user)
        self.step_function.start_execution.assert_has_calls([
            call(
                StateMachineArn='aws::arn::step/asdf-er',
                Message=json.dumps({
                    "id": "1-2-3-4",
                    "name": "Jane",
                    "email": "jane@example.com",
                }),
            ),
        ])
        self.assertEqual(id(user), id(self.when))

    def test_not_now(self):
        step_function = StepFunction(self.environment, self.boto3, self.di)
        step_function.configure(
            arn='arn::aws:step-function/asdf-er',
            when=self.never,
        )
        user = self.users.model({
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        })
        step_function(user)
        self.step_function.start_execution.assert_not_called()
        self.assertEqual(id(user), id(self.when))
