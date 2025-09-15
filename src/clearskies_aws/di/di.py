import datetime
from types import ModuleType
from typing import Any

import boto3 as boto3_module
from clearskies import Environment
from clearskies.di import Di as DefaultDI
from clearskies.di.additional_config import AdditionalConfig

from clearskies_aws.secrets import ParameterStore


class Di(DefaultDI):
    """
    Provide a DI with AWS modules built-in.

    This DI auto injects boto3, boto3 Session and the parameter store.
    """

    def __init__(
        self,
        classes: type | list[type] = [],
        modules: ModuleType | list[ModuleType] = [],
        bindings: dict[str, Any] = {},
        additional_configs: AdditionalConfig | list[AdditionalConfig] = [],
        class_overrides: dict[type, Any] = {},
        overrides: dict[str, type] = {},
        now: datetime.datetime | None = None,
        utcnow: datetime.datetime | None = None,
    ):
        super().__init__(
            classes=classes,
            modules=modules,
            bindings=bindings,
            additional_configs=additional_configs,
            class_overrides=class_overrides,
            overrides=overrides,
            now=now,
            utcnow=utcnow,
        )

    def provide_boto3(self) -> ModuleType:
        import boto3

        return boto3

    def provide_parameter_store(self, boto3: ModuleType, environment: Environment) -> ParameterStore:
        # This is just here so that we can auto-inject the secrets into the environment without having
        # to force the developer to define a secrets manager
        return ParameterStore(boto3, environment)

    def provide_boto3_session(self, boto3: ModuleType, environment: Environment) -> boto3_module.session.Session:
        if not environment.get("AWS_REGION", True):
            raise ValueError(
                "To use AWS Session you must use set AWS_REGION in the .env file or an environment variable"
            )

        session = boto3.session.Session(region_name=environment.get("AWS_REGION", True))
        return session
