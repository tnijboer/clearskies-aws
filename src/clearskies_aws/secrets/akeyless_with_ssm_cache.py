import re

from clearskies.secrets.akeyless import Akeyless
from types_boto3_ssm import SSMClient

from clearskies_aws.secrets import secrets


class AkeylessWithSsmCache(secrets.Secrets, Akeyless):
    ssm: SSMClient

    def __init__(self):
        if not self.environment.get("AWS_REGION", True):
            raise ValueError("To use parameter store you must use set the 'AWS_REGION' environment variable")
        self.ssm = self.boto3.client("ssm", region_name="us-east-1")

    def get(self, path, refresh=False):
        # AWS SSM parameter paths only allow a-z, A-Z, 0-9, -, _, ., /, @, and :
        # Replace any disallowed characters with hyphens
        ssm_name = re.sub(r"[^a-zA-Z0-9\-_\./@:]", "-", path)
        # if we're not forcing a refresh, then see if it is in paramater store
        if not refresh:
            missing = False
            try:
                response = self.ssm.get_parameter(Name=ssm_name, WithDecryption=True)
            except self.ssm.exceptions.ParameterNotFound:
                missing = True
            if not missing:
                value = response["Parameter"].get("Value", "")
                if value:
                    return value

        # otherwise get it out of Akeyless
        value = super().get(path)

        # and make sure and store the new value in parameter store
        if value:
            self.ssm.put_parameter(
                Name=ssm_name,
                Value=value,
                Type="SecureString",
                Overwrite=True,
            )

        return value
