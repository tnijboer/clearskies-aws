from types import ModuleType
from clearskies import Model
from ...actions.sqs import SQS as BaseSQS
class SQS(BaseSQS):
    calls = None

    def __init__(self, environment, boto3, di):
        super().__init__(environment, boto3, di)

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSQS, SQS)

    def _execute_action(self, client: ModuleType, model: Model) -> None:
        """Send a notification as configured."""
        if SQS.calls == None:
            SQS.calls = []

        SQS.calls.append({
            "QueueUrl": self.get_queue_url(model),
            "MessageBody": self.get_message_body(model),
        })
