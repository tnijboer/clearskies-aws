from ..input_outputs import LambdaSqsStandard as LambdaSqsStandardInputOutput
from ..di import StandardDependencies
from clearskies.contexts.build_context import build_context
from clearskies.contexts.context import Context
from clearskies.authentication import public
class LambdaSqsStandardPartialBatch(Context):
    def __init__(self, di):
        super().__init__(di)

    def finalize_handler_config(self, config):
        return {
            'authentication': public(),
            **config,
        }

    def __call__(self, event, context):
        if self.handler is None:
            raise ValueError("Cannot execute LambdaELB context without first configuring it")

        item_failures = []
        for record in event['Records']:
            try:
                self.handler(LambdaSqsStandardInputOutput(record['body'], event, context))
            except Exception as e:
                print('Failed message ' + record['messageId'] + ' being returned for retry.  Error error: ' + str(e))
                item_failures.append({'itemIdentifier': record['messageId']})

        if item_failures:
            return {
                "batchItemFailures": item_failures,
            }
        return {}
def lambda_sqs_standard_partial_batch(
    application,
    di_class=StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return build_context(
        LambdaSqsStandardPartialBatch,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
