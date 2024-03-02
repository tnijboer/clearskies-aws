from botocore.exceptions import ClientError
from clearskies.secrets.exceptions import NotFound
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

    def create(self, secret_id, value, kms_key_id=None):
        calling_parameters = {
            'SecretId': secret_id,
            'SecretString': value,
            'KmsKeyId': kms_key_id,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        result = self._secrets_manager.create_secret(**calling_parameters)

    def get(self, secret_id, version_id=None, version_stage=None, silent_if_not_found=False):
        calling_parameters = {
            'SecretId': secret_id,
            'VersionId': version_id,
            'VersionStage': version_stage,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        try:
            result = self._secrets_manager.get_secret_value(**calling_parameters)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                if silent_if_not_found:
                    return None
                raise NotFound(
                    f"Cound not find secret '{secret_id}' with version '{version_id}' and stage '{version_stage}'"
                )
            raise e
        if result.get('SecretString'):
            return result.get('SecretString')
        return result.get('SecretBinary')

    def list_secrets(self, path):
        results = self._secrets_manager.list_secrets(Filters=[
            {
                'Key': 'name',
                'Values': [path],
            },
        ], )
        return results['SecretList']

    def update(self, secret_id, value, kms_key_id=None):
        calling_parameters = {
            'SecretId': secret_id,
            'SecretString': value,
            'KmsKeyId': kms_key_id,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        result = self._secrets_manager.update_secret(**calling_parameters)

    def upsert(self, secret_id, value, kms_key_id=None):
        calling_parameters = {
            'SecretId': secret_id,
            'SecretString': value,
            'KmsKeyId': kms_key_id,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        result = self._secrets_manager.put_secret_value(**calling_parameters)

    def list_sub_folders(self, path, value):
        raise NotImplementedError(
            "Secrets Manager doesn't support list_sub_folders."
        )
