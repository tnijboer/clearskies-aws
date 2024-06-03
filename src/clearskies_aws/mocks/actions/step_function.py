from types import ModuleType
from clearskies import Model
from ...actions.step_function import StepFunction as BaseStepFunction
class StepFunction(BaseStepFunction):
    calls = None

    def __init__(self, environment, boto3, di):
        super().__init__(environment, boto3, di)

    @classmethod
    def mock(cls, di):
        StepFunction.calls = []
        di.mock_class(BaseStepFunction, StepFunction)

    def _execute_action(self, client: ModuleType, model: Model) -> None:
        """Send a notification as configured."""
        if StepFunction.calls == None:
            StepFunction.calls = []

        StepFunction.calls.append({
            "stateMachineArn": self.get_arn(model),
            "input": self.get_message_body(model),
        })

        if self.column_to_store_execution_arn:
            model.save({self.column_to_store_execution_arn: "mock_execution_arn"})
