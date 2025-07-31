from types import ModuleType

from clearskies.di.injectable import Injectable


class Boto3Session(Injectable):
    def __init__(self, cache: bool = True):
        self.cache = cache

    def __get__(self, instance, parent) -> ModuleType:
        if instance is None:
            return self  # type: ignore
        return self._di.build_from_name("boto3_session", cache=self.cache)
