import traceback

from clearskies.authentication import Public

from clearskies_aws.contexts.context import Context
from clearskies_aws.input_outputs import LambdaSqsStandard as LambdaSqsStandardInputOutput


class LambdaSqsStandardPartialBatch(Context):
    def finalize_handler_config(self, config):
        return {
            "authentication": Public(),
            **config,
        }

    def __call__(self, event, context, url=None, method=None):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaELB context without first configuring it")

        item_failures = []
        for record in event["Records"]:
            try:
                self.execute_application(
                    LambdaSqsStandardInputOutput(record["body"], event, context, url=url, method=method)
                )
            except Exception as e:
                print("Failed message " + record["messageId"] + " being returned for retry.  Error error: " + str(e))
                traceback.print_tb(e.__traceback__)
                item_failures.append({"itemIdentifier": record["messageId"]})

        if item_failures:
            return {
                "batchItemFailures": item_failures,
            }
        return {}
