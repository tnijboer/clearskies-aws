import clearskies_aws

from ..input_outputs import CLIWebSocketMock as CLIWebSocketMockInputOutput


class CLIWebSocketMock(clearskies_aws.contexts.Cli):
    def __call__(self):
        return self.execute_application(CLIWebSocketMockInputOutput())
