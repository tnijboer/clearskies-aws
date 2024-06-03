from types import ModuleType
from clearskies import Model
from ...actions.sns import SNS as BaseSNS
class SNS(BaseSNS):
    calls = None

    def __init__(self, environment, boto3, di):
        super().__init__(environment, boto3, di)

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSNS, SNS)

    def _execute_action(self, client: ModuleType, model: Model) -> None:
        """Send a notification as configured."""
        if SNS.calls == None:
            SNS.calls = []

        SNS.calls.append({
            "TopicArn": self.get_topic_arn(model),
            "Message": self.get_message_body(model),
        })
