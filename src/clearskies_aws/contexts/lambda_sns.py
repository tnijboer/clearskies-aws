from clearskies.authentication import Public

from clearskies_aws.contexts.context import Context
from clearskies_aws.input_outputs import LambdaSns as LambdaSnsInputOutput


class LambdaSns(Context):
    def finalize_handler_config(self, config):
        return {
            "authentication": Public(),
            **config,
        }

    def __call__(self, event, context, method=None, url=None):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaSnsEvent context without first configuring it")

        try:
            return self.execute_application(LambdaSnsInputOutput(event, context, method=method, url=url))
        except Exception as e:
            print("Failed message " + event["Records"][0]["Sns"]["MessageId"] + ". Error error: " + str(e))
            raise e
