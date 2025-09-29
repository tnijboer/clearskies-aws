from clearskies_aws.contexts.context import Context
from clearskies_aws.di import Di
from clearskies_aws.input_outputs import LambdaHTTPGateway as LambdaHTTPGatewayInputOutput


class LambdaHTTPGateway(Context):
    def __call__(self, event, context):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaHTTPGateway context without first configuring it")

        return self.execute_application(LambdaHTTPGatewayInputOutput(event, context))
