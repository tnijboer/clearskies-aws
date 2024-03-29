import json

import clearskies
from clearskies.handlers.exceptions import ClientError
from clearskies.handlers.base import Base
import botocore

class SecretsManagerRotation(Base, clearskies.handlers.SchemaHelper):
    _steps = ["createSecret", "setSecret", "testSecret", "finishSecret"]

    current = "AWSCURRENT"
    pending = "AWSPENDING"

    _configuration_defaults = {
        "createSecret": None,
        "setSecret": None,
        "testSecret": None,
        "finishSecret": None,
        "schema": [],
    }

    def __init__(self, boto3, di):
        super().__init__(di)
        self.boto3 = boto3

    def _check_configuration(self, configuration):
        super()._check_configuration(configuration)
        class_name = self.__class__.__name__
        if not configuration.get("createSecret"):
            raise KeyError(f"Missing required configuration 'createSecret' for handler {class_name}")

        for config_name in self._steps:
            config = configuration.get(config_name)
            if config is None:
                continue
            if not callable(config):
                raise ValueError(f"Misconfiguration for handler {class_name}: configuration '{config_name}' is not callable")

        if configuration.get("schema") is not None:
            self._check_schema(configuration["schema"], None, f"Misconfiguration for handler {class_name}")

    def _finalize_configuration(self, configuration):
        if configuration.get('schema'):
            configuration['schema'] = self._schema_to_columns(configuration['schema'])
        return super()._finalize_configuration(configuration)

    def handle(self, input_output):
        request_data = input_output.json_body()

        arn = request_data.get('SecretId')
        request_token = request_data.get('ClientRequestToken')
        step = request_data.get('Step')
        secretsmanager = self.boto3.client('secretsmanager')
        metadata = secretsmanager.describe_secret(SecretId=arn)

        self._validate_secret_and_request(step, arn, metadata, request_token)

        current_secret_data = {}
        pending_secret_data = {}

        current_secret = secretsmanager.get_secret_value(SecretId=arn, VersionStage=self.current)
        current_secret_data = json.loads(current_secret['SecretString'])

        # validate the current secret
        secret_errors = {
            **self._extra_column_errors(current_secret_data),
            **self._find_input_errors(current_secret_data),
        }
        if secret_errors:
            raise ValueError(f"The current secret did not match the configured schema: {secret_errors}")

        # check for a pending secret.  Note that this is not always available.  In the event that we are retrying a failed
        # rotation it will already be set, in which case we need to skip the createSecret step.
        try:
            pending_secret = secretsmanager.get_secret_value(SecretId=arn, VersionId=request_token, VersionStage=self.pending)
            pending_secret_data = json.loads(pending_secret['SecretString'])
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'ResourceNotFoundException':
                pending_secret_data = None
            else:
                raise error

        # we can't call the createSecret step if we already have a pending secret or this will generate an error from AWS.
        if step == "createSecret" and pending_secret_data is not None:
            return

        # call the appropriate step and pass along *everything*.
        getattr(self, step)(
            current_secret_data=current_secret_data,
            pending_secret_data=pending_secret_data,
            secretsmanager=secretsmanager,
            metadata=metadata,
            request_token=request_token,
            arn=arn,
        )

    def _validate_secret_and_request(self, step, arn, metadata, request_token):
        """ This function does some basic checks suggested by AWS of both the request and the secret to make sure everything is on the up-and-up. """
        if step not in self._steps:
            raise ClientError(f"Invalid step: {step}")

        if not metadata.get('RotationEnabled'):
            raise ValueError("Secret %s is not enabled for rotation" % arn)

        versions = metadata["VersionIdsToStages"]
        prefix = f"Rotation config error for version '{request_token}' of secret '{arn}': "
        if request_token not in versions:
            raise ValueError(f"{prefix} we don't have a stage for rotation")
        if self.current in versions[request_token]:
            raise ValueError(f"{prefix} it's already the current version, which shouldn't happen.  I'm quitting with prejudice.")
        elif self.pending not in versions[request_token]:
            raise ValueError(f"{prefix} it hasn't been set to pending yet, which makes no sense!")

    def createSecret(self, **kwargs):
        new_secret_data = self._di.call_function(self._configuration["createSecret"], **kwargs)
        if new_secret_data is None:
            raise ValueError(f"I called the configured createSecret function but it didn't return anything.  It has to return the new secret data.")
        if not isinstance(new_secret_data, dict):
            raise ValueError(f"I called the configured createSecret function but it didn't return a dictionary.  The createSecret function must return a dictionary.")

        secret_errors = {
            **self._extra_column_errors(new_secret_data),
            **self._find_input_errors(new_secret_data),
        }
        if secret_errors:
            raise ValueError(f"The secret data returned by the call to createSecret did not match the configured schema: {secret_errors}")

        # if we get this far we can store the new data
        secretsmanager = kwargs["secretsmanager"]
        request_token = kwargs["request_token"]
        arn = kwargs["arn"]
        secretsmanager.put_secret_value(
            SecretId=arn,
            SecretString=json.dumps(new_secret_data),
            ClientRequestToken=request_token,
            VersionStages=[self.pending],
        )

    def setSecret(self, **kwargs):
        if not self._configuration.get("setSecret"):
            return
        self._di.call_function(self._configuration["setSecret"], **kwargs)

    def testSecret(self, **kwargs):
        if not self._configuration.get("testSecret"):
            return
        self._di.call_function(self._configuration["testSecret"], **kwargs)

    def finishSecret(self, **kwargs):
        if self._configuration.get("finishSecret"):
            self._di.call_function(self._configuration["finishSecret"], **kwargs)

        secretsmanager = kwargs["secretsmanager"]
        request_token = kwargs["request_token"]
        arn = kwargs["arn"]
        metadata = kwargs["metadata"]
        current_version = None
        for version in metadata["VersionIdsToStages"]:
            if self.current not in metadata["VersionIdsToStages"][version]:
                continue

            if version == request_token:
                return

            current_version = version
            break

        # finish the rotation by taking the new version and making it current.
        secretsmanager.update_secret_version_stage(
            SecretId=arn,
            VersionStage=self.current,
            MoveToVersionId=request_token,
            RemoveFromVersionId=current_version
        )
