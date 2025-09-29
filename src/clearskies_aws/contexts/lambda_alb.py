from clearskies_aws.contexts.context import Context

from ..input_outputs import LambdaALB as LambdaAlbInputOutput


class LambdaALB(Context):
    def __call__(self, event, context):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaALB context without first configuring it")

        return self.execute_application(LambdaAlbInputOutput(event, context))
