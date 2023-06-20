from .parameter_store import ParameterStore
from .secrets_manager import SecretsManager
from .akeyless_with_ssm_cache import AkeylessWithSsmCache
from . import additional_configs
def akeyless_with_cache(*args, **kwargs):
    return BindingConfig(AkeylessWithCache, *args, **kwargs)
