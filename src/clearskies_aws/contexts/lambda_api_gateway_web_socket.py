from clearskies.contexts.context import Context

from ..di import Di
from ..input_outputs import (
    LambdaAPIGatewayWebSocket as LambdaAPIGatewayWebSocketInputOutput,
)


class LambdaAPIGatewayWebSocket(Context):
    def __init__(self, di):
        super().__init__(di)

    def __call__(self, event, context):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaAPIGatewayWebSocket context without first configuring it")

        return self.execute_application(LambdaAPIGatewayWebSocketInputOutput(event, context))
