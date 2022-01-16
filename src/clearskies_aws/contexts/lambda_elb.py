from ..input_outputs import LambdaELB as LambdaELBInputOutput
from ..di import StandardDependencies
from clearskies.contexts.build_context import build_context
from clearskies.contexts.context import Context
class LambdaELB(Context):
    def __init__(self, di):
        super().__init__(di)

    def __call__(self, event, context):
        if self.handler is None:
            raise ValueError("Cannot execute LambdaELB context without first configuring it")

        return self.handler(LambdaELBInputOutput(event, context))
def lambda_elb(
    application,
    di_class=StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return build_context(
        LambdaELB,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
