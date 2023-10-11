from clearskies.authentication import public
from ..input_outputs import LambdaInvocation as LambdaInvocationInputOutput
from ..di import StandardDependencies
from clearskies.contexts.build_context import build_context
from clearskies.contexts.context import Context
class LambdaInvocation(Context):
    def __init__(self, di):
        super().__init__(di)

    def finalize_handler_config(self, config):
        return {
            'authentication': public(),
            **config,
        }

    def __call__(
        self,
        event,
        context,
        method=None,
        url=None,
    ):
        if self.handler is None:
            raise ValueError("Cannot execute LambdaInvocation context without first configuring it")

        return self.handler(LambdaInvocationInputOutput(
            event,
            context,
            method=method,
            url=url,
        ))
def lambda_invocation(
    application,
    di_class=StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return build_context(
        LambdaInvocation,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
