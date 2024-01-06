import clearskies


class SimpleBodyRouting(clearskies.handlers.Routing):
    def __init__(self, di):
        super().__init__(di)

    _configuration_defaults = {
        "routes": {},
        "route_key": "route",
    }

    def handler_classes(self, configuration):
        # not actually used but required by base
        return []

    def _check_configuration(self, configuration):
        for config_name in ["route_key", "routes"]:
            if not configuration.get(config_name):
                raise KeyError(f"Missing required configuration for SimpleBodyRouting handler: '{config_name}'")
        if not isinstance(configuration["routes"], dict):
            raise ValueError(f"COnfiguration 'routes' for handler SimpleBodyRouting must be a dictionary, but instead I got something else.")

    def handle(self, input_output):
        body = input_output.json_body(required=True)
        if not body or not body.get(self.configuration("route_key")):
            return self.error(input_output, "Not Found", 404)

        route = body[self.configuration("route_key")]
        if route not in self.configuration("routes"):
            return self.error(input_output, "Not Found", 404)
        return input_output.respond(self._di.call_function(
            self.configuration("routes")[route],
            request_data=body,
            **input_output.context_specifics(),
        ), 200)

    def documentation(self):
        return []
