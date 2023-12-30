from abc import abstractmethod
import json

import clearskies


class WebSocketConnectionModel(clearskies.Model):

    id_column_name = "connection_id"

    def __init__(self, backend, columns, boto3, input_output):
        super().__init__(backend, columns)
        self._boto3 = boto3
        self._input_output = input_output

    def _build_model(self):
        model_class = self.model_class()
        return model_class(self._backend, self._columns, self._boto3, self._input_output)

    def send(self, message):
        if not self.exists:
            raise ValueError("Cannot send message to non-existent connection.")
        if not self.data.get("connection_id"):
            raise ValueError(
                f"Hmmm... I couldn't find the connection id for the {self.__class__.__name__}.  I'm picky about id column names.  Can you please make sure I have a column called connection_id and that it contains the connection id?"
            )

        event = self._input_output.context_specifics()["event"]
        domain = event.get("requestContext", {}).get("domainName")
        stage = event.get("requestContext", {}).get("stage")
        # only include the stage if we're using the default AWS domain - not with a custom domain
        if ".amazonaws.com" in domain:
            endpoint_url = f"https://{domain}/{stage}"
        else:
            endpoint_url = f"https://{domain}"
        api_gateway = self._boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)

        bytes_message = json.dumps(message).encode("utf-8")
        try:
            response = api_gateway.post_to_connection(Data=bytes_message, ConnectionId=self.connection_id)
        except api_gateway.exceptions.GoneException:
            self.delete()
        return response
