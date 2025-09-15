import json
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, call

import clearskies

from clearskies_aws.actions.step_function import StepFunction
from clearskies_aws.di import StandardDependencies


class User(clearskies.Model):
    def __init__(self, memory_backend, columns):
        super().__init__(memory_backend, columns)

    def columns_configuration(self):
        return OrderedDict(
            [
                clearskies.column_types.email("email"),
                clearskies.column_types.string("execution_arn"),
            ]
        )


class StepFunctionTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind("environment", {"AWS_REGION": "us-east-2"})
        memory_backend = self.di.build("memory_backend")
        self.users = self.di.build(User)
        self.user = self.users.create(
            {
                "id": "1-2-3-4",
                "email": "blah@example.com",
            }
        )
        self.step_function = MagicMock()
        self.step_function.start_execution = MagicMock(return_value={"executionArn": "aws:arn:execution"})
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.step_function)
        self.when = None
        self.environment = MagicMock()
        self.environment.get = MagicMock(return_value="us-east-2")

    def always(self, model):
        self.when = model
        return True

    def never(self, model):
        self.when = model
        return False

    def test_execute(self):
        step_function = StepFunction(self.environment, self.boto3, self.di)
        step_function.configure(
            arn="aws::arn:us-east-2:step/asdf-er",
            when=self.always,
            column_to_store_execution_arn="execution_arn",
        )
        step_function(self.user)
        self.step_function.start_execution.assert_has_calls(
            [
                call(
                    stateMachineArn="aws::arn:us-east-2:step/asdf-er",
                    input=json.dumps(
                        {
                            "id": self.user.id,
                            "email": self.user.email,
                            "execution_arn": None,
                        }
                    ),
                ),
            ]
        )
        self.assertEqual(id(self.user), id(self.when))
        self.assertEqual("aws:arn:execution", self.user.execution_arn)

    def test_region_switch(self):
        self.boto3.client.side_effect = [self.step_function, self.step_function]
        step_function = StepFunction(self.environment, self.boto3, self.di)
        step_function.configure(
            arn="aws::arn:eu-west-1:step/asdf-er",
            when=self.always,
            column_to_store_execution_arn="execution_arn",
        )
        step_function(self.user)
        self.step_function.start_execution.assert_has_calls(
            [
                call(
                    stateMachineArn="aws::arn:eu-west-1:step/asdf-er",
                    input=json.dumps(
                        {
                            "id": self.user.id,
                            "email": self.user.email,
                            "execution_arn": None,
                        }
                    ),
                ),
            ]
        )
        self.assertEqual(id(self.user), id(self.when))
        self.assertEqual("aws:arn:execution", self.user.execution_arn)
        self.boto3.client.assert_has_calls(
            [
                call("stepfunctions", region_name="us-east-2"),
                call("stepfunctions", region_name="eu-west-1"),
            ]
        )

    def test_not_now(self):
        step_function = StepFunction(self.environment, self.boto3, self.di)
        step_function.configure(
            arn="aws::arn:us-east-2:step-function/asdf-er",
            when=self.never,
        )
        user = self.users.model(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )
        step_function(user)
        self.step_function.start_execution.assert_not_called()
        self.assertEqual(id(user), id(self.when))
