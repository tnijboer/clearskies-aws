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

    def __call__(self, event, context):
        if self.handler is None:
            raise ValueError("Cannot execute LambdaSnsEvent context without first configuring it")

        for record in event['Records'][0]['Sns']:
            try:
                self.handler(LambdaSnsInputOutput(record['Message'], context))
            except Exception as e:
                print('Failed message ' + record['MessageId'] + '. Error error: ' + str(e))

def lambda_sns_event(
    application,
    di_class=StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return build_context(
        LambdaSnsEvent,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
