from clearskies import BindingConfig

from . import additional_configs
from .akeyless_with_ssm_cache import AkeylessWithSsmCache
from .parameter_store import ParameterStore
from .secrets_manager import SecretsManager


def akeyless_with_ssm_cache(*args, **kwargs):
    return BindingConfig(AkeylessWithSsmCache, *args, **kwargs)

__all__ = [
    "ParameterStore",
    "SecretsManager",
    "akeyless_with_ssm_cache",
    "additional_configs",
    "AkeylessWithSsmCache",
]
