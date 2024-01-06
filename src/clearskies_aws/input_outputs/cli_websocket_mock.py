import clearskies
class CLIWebsocketMock(clearskies.input_outputs.CLI):
    def context_specifics(self):
        connection_id = self.json_body().get("connection_id")
        if not connection_id:
            raise KeyError("When using the CLIWebsocketMock you must provide connection_id in the request body")

        return {
            "event": {},
            "context": {},
            "connection_id": connection_id,
        }
