from ..input_outputs import LambdaAPIGateway as LambdaAPIGatewayInputOutput
from ..di import StandardDependencies
from clearskies.contexts.build_context import build_context
from clearskies.contexts.context import Context
class LambdaAPIGateway(Context):
    def __init__(self, di):
        super().__init__(di)

    def __call__(self, event, context):
        if self.handler is None:
            raise ValueError("Cannot execute LambdaAPIGateway context without first configuring it")

        return self.handler(LambdaAPIGatewayInputOutput(event, context))
def lambda_api_gateway(
    application,
    di_class=StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return build_context(
        LambdaAPIGateway,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
