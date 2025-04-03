from ..input_outputs import LambdaSns as LambdaSnsInputOutput
from ..di import StandardDependencies
from clearskies.contexts.build_context import build_context
from clearskies.contexts.context import Context
from clearskies.authentication import public

class LambdaSns(Context):
    def __init__(self, di):
        super().__init__(di)

    def finalize_handler_config(self, config):
        return {
            'authentication': public(),
            **config,
        }

    def __call__(self, event, context, method=None, url=None):
        if self.handler is None:
            raise ValueError("Cannot execute LambdaSnsEvent context without first configuring it")

        try:
            return self.handler(LambdaSnsInputOutput(event, context, method=method, url=url))
        except Exception as e:
            print('Failed message ' + event['Records'][0]['Sns']['MessageId'] + '. Error error: ' + str(e))
            raise e

def lambda_sns(
    application,
    di_class=StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return build_context(
        LambdaSns,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
