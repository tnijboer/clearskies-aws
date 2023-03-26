from clearskies.di import StandardDependencies as DefaultStandardDependencies
from ..backends import DynamoDBBackend, SqsBackend
from ..secrets import ParameterStore
class StandardDependencies(DefaultStandardDependencies):
    def provide_dynamo_db_backend(self, boto3, environment):
        return DynamoDBBackend(boto3, environment)

    def provide_sqs_backend(self, boto3, environment):
        return SqsBackend(boto3, environment)

    def provide_boto3(self):
        import boto3
        return boto3

    def provide_secrets(self, boto3, environment):
        # This is just here so that we can auto-inject the secrets into the environment without having
        # to force the developer to define a secrets manager
        return ParameterStore(boto3, environment)
