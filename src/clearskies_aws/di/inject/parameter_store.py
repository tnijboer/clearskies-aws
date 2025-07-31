from clearskies.di.injectable import Injectable

from clearskies_aws.secrets.parameter_store import (
    ParameterStore as ParameterStoreDependency,
)


class ParameterStore(Injectable):
    def __init__(self, cache: bool = True):
        self.cache = cache

    def __get__(self, instance, parent) -> ParameterStoreDependency:
        if instance is None:
            return self  # type: ignore
        return self._di.build_from_name("parameter_store", cache=self.cache)
