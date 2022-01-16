class SecretsManager:
    _boto3 = None
    _environment = None
    _secrets_manager = None

    def __init__(self, boto3, environment):
        self._boto3 = boto3
        self._environment = environment
        if not self._environment.get('AWS_REGION', True):
            raise ValueError("To use secrets manager you must use set the 'AWS_REGION' environment variable")
        self._secrets_manager = self._boto3.client('secretsmanager', region_name=self._environment.get('AWS_REGION'))

    def get(self, secret_id, version_id=None, version_stage=None):
        calling_parameters = {
            'SecretId': secret_id,
            'VersionId': version_id,
            'VersionStage': version_stage,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        result = self._secrets_manager.get_secret_value(**calling_parameters)
        if result.get('SecretString'):
            return result.get('SecretString')
        return result.get('SecretBinary')
