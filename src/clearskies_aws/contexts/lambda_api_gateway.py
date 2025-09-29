from clearskies_aws.contexts import Context
from clearskies_aws.di import Di
from clearskies_aws.input_outputs import LambdaAPIGateway as LambdaAPIGatewayInputOutput


class LambdaAPIGateway(Context):
    def __call__(self, event, context):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaAPIGateway context without first configuring it")

        return self.execute_application(LambdaAPIGatewayInputOutput(event, context))
