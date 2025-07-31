import clearskies_aws

from ..input_outputs import CLIWebsocketMock as CLIWebsocketMockInputOutput


class CLIWebsocketMock(clearskies_aws.contexts.Cli):

    def __call__(self):
        return self.execute_application(CLIWebsocketMockInputOutput())
