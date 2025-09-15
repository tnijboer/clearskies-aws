import datetime
from types import ModuleType
from typing import Any, Callable

import clearskies
from clearskies.contexts.wsgi import Wsgi as WsgiDependency
from clearskies.di.additional_config import AdditionalConfig

import clearskies_aws


class Wsgi(WsgiDependency):
    """
    Connect your application to a WSGI server.

    Extend from the core WSGI context,
    but with an override of the DI to use clearskies_aws.di.Di().
    """

    def __init__(
        self,
        application: Callable | clearskies.endpoint.Endpoint | clearskies.endpoint_group.EndpointGroup,
        classes: type | list[type] = [],
        modules: ModuleType | list[ModuleType] = [],
        bindings: dict[str, Any] = {},
        additional_configs: AdditionalConfig | list[AdditionalConfig] = [],
        class_overrides: dict[type, Any] = {},
        overrides: dict[str, type] = {},
        now: datetime.datetime | None = None,
        utcnow: datetime.datetime | None = None,
    ):
        self.di = clearskies_aws.di.Di(
            classes=classes,
            modules=modules,
            bindings=bindings,
            additional_configs=additional_configs,
            class_overrides=class_overrides,
            overrides=overrides,
            now=now,
            utcnow=utcnow,
        )
        self.application = application
