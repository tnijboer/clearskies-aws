import json

import clearskies

import clearskies_aws


class WebSocketConnectionModel(clearskies.Model):

    id_column_name = "connection_id"

    connection_id = clearskies.columns.String()

    boto3 = clearskies_aws.di.inject.Boto3()
    backend = clearskies_aws.backends.DummyBackend()
    input_output = clearskies.di.inject.ByClass(
        clearskies_aws.input_outputs.LambdaAPIGatewayWebSocket
    )

    def send(self, message):
        if not self:
            raise ValueError("Cannot send message to non-existent connection.")
        if not self.connection_id:
            raise ValueError(
                f"Hmmm... I couldn't find the connection id for the {self.__class__.__name__}.  I'm picky about id column names.  Can you please make sure I have a column called connection_id and that it contains the connection id?"
            )

        event = self.input_output.context_specifics()["event"]
        domain = event.get("requestContext", {}).get("domainName")
        stage = event.get("requestContext", {}).get("stage")
        # only include the stage if we're using the default AWS domain - not with a custom domain
        if ".amazonaws.com" in domain:
            endpoint_url = f"https://{domain}/{stage}"
        else:
            endpoint_url = f"https://{domain}"
        api_gateway = self.boto3.client(
            "apigatewaymanagementapi", endpoint_url=endpoint_url
        )

        bytes_message = json.dumps(message).encode("utf-8")
        try:
            response = api_gateway.post_to_connection(Data=bytes_message, ConnectionId=self.connection_id)
        except api_gateway.exceptions.GoneException:
            self.delete()
        return response
