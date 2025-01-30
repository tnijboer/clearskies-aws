import boto3

from clearskies.environment import Environment
from clearskies.models import Models
from types import ModuleType
from typing import List, Optional, Callable, cast

from ..di import StandardDependencies
from .assume_role import AssumeRole
from .action_aws import ActionAws
class StepFunction(ActionAws):
    _name = "stepfunctions"

    def __init__(self, environment: Environment, boto3: boto3, di: StandardDependencies) -> None:
        super().__init__(environment, boto3, di)

    def configure(
        self,
        arn: Optional[str]=None,
        arn_environment_key: Optional[str] = None,
        arn_callable: Optional[Callable] = None,
        column_to_store_execution_arn: Optional[str] = None,
        message_callable: Optional[Callable] = None,
        when: Optional[Callable] = None,
        assume_role: Optional[AssumeRole] = None,
    ) -> None:
        """Configures the action for the step function."""
        super().configure(message_callable=message_callable, when=when, assume_role=assume_role)

        self.arn = arn
        self.arn_environment_key = arn_environment_key
        self.arn_callable = arn_callable
        self.column_to_store_execution_arn = column_to_store_execution_arn

        arns = 0
        for value in [arn, arn_environment_key, arn_callable]:
            if value:
                arns += 1
        if arns > 1:
            raise ValueError(
                "You can only provide one of 'arn', 'arn_environment_key', or 'arn_callable', but more than one was provided."
            )
        if not arns:
            raise ValueError("You must provide at least one of 'arn', 'arn_environment_key', or 'arn_callable'.")

    def _execute_action(self, client: ModuleType, model: Models) -> None:
        """Send a notification as configured."""
        arn = self.get_arn(model)
        default_region = self.default_region()
        arn_region = arn.split(':')[3]
        if default_region and default_region != arn_region:
            client = self._getClient(region=arn_region)
        response = client.start_execution(
            stateMachineArn=self.get_arn(model),
            input=self.get_message_body(model),
        )

        if self.column_to_store_execution_arn:
            model.save({self.column_to_store_execution_arn: response['executionArn']})

    def get_arn(self, model: Models) -> str:
        if self.arn:
            return self.arn
        if self.arn_environment_key:
            return self.environment.get(self.arn_environment_key)
        return self.di.call_function(self.arn_callable, model=model)
