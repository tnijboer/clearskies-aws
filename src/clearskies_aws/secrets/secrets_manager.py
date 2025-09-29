from typing import Any

from botocore.exceptions import ClientError
from clearskies.secrets.exceptions.not_found import NotFound
from types_boto3_secretsmanager import SecretsManagerClient
from types_boto3_secretsmanager.type_defs import SecretListEntryTypeDef

from clearskies_aws.secrets import secrets


class SecretsManager(secrets.Secrets):
    _secrets_manager: SecretsManagerClient

    def __init__(self):
        super().__init__()
        self._secrets_manager = self.boto3.client("secretsmanager", region_name=self.environment.get("AWS_REGION"))

    def create(self, secret_id: str, value: Any, kms_key_id: str | None = None) -> bool:
        calling_parameters = {
            "SecretId": secret_id,
            "SecretString": value,
            "KmsKeyId": kms_key_id,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        result = self._secrets_manager.create_secret(**calling_parameters)
        return bool(result.get("ARN"))

    def get(  # type: ignore[override]
        self,
        secret_id: str,
        version_id: str | None = None,
        version_stage: str | None = None,
        silent_if_not_found: bool = False,
    ) -> str | bytes | None:
        calling_parameters = {"SecretId": secret_id}

        # Only add optional parameters if they are not None
        if version_id:
            calling_parameters["VersionId"] = version_id
        if version_stage:
            calling_parameters["VersionStage"] = version_stage

        try:
            result = self._secrets_manager.get_secret_value(**calling_parameters)
        except ClientError as e:
            error = e.response.get("Error", {})
            if error.get("Code") == "ResourceNotFoundException":
                if silent_if_not_found:
                    return None
                raise NotFound(
                    f"Could not find secret '{secret_id}' with version '{version_id}' and stage '{version_stage}'"
                )
            raise e
        if result.get("SecretString"):
            return result.get("SecretString")
        return result.get("SecretBinary")

    def list_secrets(self, path: str) -> list[SecretListEntryTypeDef]:  # type: ignore[override]
        results = self._secrets_manager.list_secrets(
            Filters=[
                {
                    "Key": "name",
                    "Values": [path],
                },
            ],
        )
        return results["SecretList"]

    def update(self, secret_id: str, value: str, kms_key_id: str | None = None) -> bool:  # type: ignore[override]
        calling_parameters = {
            "SecretId": secret_id,
            "SecretString": value,
        }
        if kms_key_id:
            # If no KMS key is provided, we should not include it in the parameters
            calling_parameters["KmsKeyId"] = kms_key_id

        result = self._secrets_manager.update_secret(**calling_parameters)
        return bool(result.get("ARN"))

    def upsert(self, secret_id: str, value: str, kms_key_id: str | None = None) -> bool:  # type: ignore[override]
        calling_parameters = {
            "SecretId": secret_id,
            "SecretString": value,
        }
        if kms_key_id:
            # If no KMS key is provided, we should not include it in the parameters
            calling_parameters["KmsKeyId"] = kms_key_id

        result = self._secrets_manager.put_secret_value(**calling_parameters)
        return bool(result.get("ARN"))

    def list_sub_folders(self, path: str, value: str) -> list[str]:  # type: ignore[override]
        raise NotImplementedError("Secrets Manager doesn't support list_sub_folders.")
