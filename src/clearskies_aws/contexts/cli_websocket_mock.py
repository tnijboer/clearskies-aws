import clearskies
import clearskies_aws
from ..input_outputs import CLIWebsocketMock as CLIWebsocketMockInputOutput
from clearskies_aws.contexts.cli import CLI
class CLIWebsocketMock(CLI):
    def __init__(self, di):
        super().__init__(di)

    def __call__(self):
        if self.handler is None:
            raise ValueError("Cannot execute CLIWebsocketMock context without first configuring it")

        try:
            return self.handler(self.di.build(CLIWebsocketMockInputOutput))
        except clearskies.input_outputs.exceptions.CLINotFound:
            print("help (aka 404 not found)!")
def cli_websocket_mock(
    application,
    di_class=clearskies_aws.di.StandardDependencies,
    bindings=None,
    binding_classes=None,
    binding_modules=None,
    additional_configs=None,
):
    return clearskies.contexts.build_context(
        CLIWebsocketMock,
        application,
        di_class=di_class,
        bindings=bindings,
        binding_classes=binding_classes,
        binding_modules=binding_modules,
        additional_configs=additional_configs,
    )
