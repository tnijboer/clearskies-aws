from clearskies.authentication import Public

from clearskies_aws.contexts.context import Context
from clearskies_aws.input_outputs import LambdaInvocation as LambdaInvocationInputOutput


class LambdaInvocation(Context):
    def finalize_handler_config(self, config):
        return {
            "authentication": Public(),
            **config,
        }

    def __call__(
        self,
        event,
        context,
        method=None,
        url=None,
    ):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaInvocation context without first configuring it")

        return self.execute_application(
            LambdaInvocationInputOutput(
                event,
                context,
                method=method,
                url=url,
            )
        )
