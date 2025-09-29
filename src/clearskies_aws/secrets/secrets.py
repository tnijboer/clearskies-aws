from clearskies.di.inject import Environment
from clearskies.secrets import Secrets as BaseSecrets

from clearskies_aws.di import inject


class Secrets(BaseSecrets):
    boto3 = inject.Boto3()
    environment = Environment()

    def __init__(self):
        if not self.environment.get("AWS_REGION", True):
            raise ValueError("To use secrets manager you must use set the 'AWS_REGION' environment variable")
