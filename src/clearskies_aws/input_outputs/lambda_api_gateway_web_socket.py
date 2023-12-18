from .lambda_api_gateway import LambdaAPIGateway
class LambdaAPIGatewayWebSocket(LambdaAPIGateway):
    def context_specifics(self):
        return {
            "event": self._event,
            "context": self._context,
            "connection_id": self._event["requestContext"]["connectionId"],
        }
