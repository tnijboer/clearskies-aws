class ParameterStore:
    _boto3 = None
    _environment = None
    _ssm = None

    def __init__(self, boto3, environment):
        self._boto3 = boto3
        self._environment = environment
        if not self._environment.get('AWS_REGION', True):
            raise ValueError("To use parameter store you must use set the 'AWS_REGION' environment variable")
        self._ssm = self._boto3.client('ssm', region_name=self._environment.get('AWS_REGION'))

    def get(self, path):
        result = self._ssm.get_parameter(Name=path, WithDecryption=True)
        return result['Parameter']['Value']
