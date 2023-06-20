from clearskies.secrets import AKeyless
class AkeylessWithSsmCache(AKeyless):
    _boto3 = None
    def __init__(self, requests, environment, boto3):
        super().__init__(requests, environment)
        self._boto3 = boto3
        if not self._environment.get('AWS_REGION', True):
            raise ValueError("To use parameter store you must use set the 'AWS_REGION' environment variable")

    def get(self, path, refresh=False):
        ssm = self._boto3.client('ssm', region_name='us-east-1')
        # we have spaces in our akeyless names but parameter store doesn't allow that
        ssm_name = path.replace(' ', '-')
        # if we're not forcing a refresh, then see if it is in paramater store
        if not refresh:
            missing = False
            try:
                response = ssm.get_parameter(Name=ssm_name, WithDecryption=True)
            except ssm.exceptions.ParameterNotFound:
                missing = True
            if not missing:
                value = response['Parameter']['Value']
                if value:
                    return value

        # otherwise get it out of Akeyless
        value = super().get(path)

        # and make sure and store the new value in parameter store
        if value:
            ssm.put_parameter(
                Name=ssm_name,
                Value=value,
                Type='SecureString',
                Overwrite=True,
            )

        return value
