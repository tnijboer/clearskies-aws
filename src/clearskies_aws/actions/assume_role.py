from __future__ import annotations
from types import ModuleType
from typing import Optional
class AssumeRole:
    """
    Used by the various actions if you need to assume a role before making an AWS call.

    Note that, in all cases, this class and the actions assume that you already have AWS credentials
    properly configured/findable by boto3 in the standard way.  If you just have static IAM credentials
    that you are trying to use... well, you can do that with some undocumented hackery, but that's not
    really the goal for any of these classes.

    Example:
        Here's a basic usage example with an SQS action on a model trigger::

            class User(clearskies.Model):
                def __init__(self, memory_backend, columns):
                    super().__init__(memory_backend, columns)
                def columns_configuration(self):
                    return OrderedDict([
                        clearskies.column_types.string(
                            'name',
                            on_change=[
                                clearskies_aws.actions.sqs(
                                    queue_url='https://queue.url.example.aws.com',
                                    assume_role=clearskies_aws.actions.assume_role(
                                        role_arn='arn:aws:iam:role/name',
                                        external_id='12345',
                                    )
                                )
                            ],
                        ),
                    ])

    Example:
        Here's a more complicated example with a double-assumme-role to show how to combine them::

            first_assume_role = clearskies_aws.actions.assume_role(
                role_arn='arn:aws:123456789012:iam:role/name',
                external_id='12345',
            )
            final_assume_role = clearskies_aws.actions.assume_role(
                role_arn='arn:aws:210987654321:iam:role/name-2',
                external_id='54321',
                source=first_assume_role,
            )
            class User(clearskies.Model):
                def __init__(self, memory_backend, columns):
                    super().__init__(memory_backend, columns)
                def columns_configuration(self):
                    return OrderedDict([
                        clearskies.column_types.string(
                            'name',
                            on_change=[clearskies_aws.actions.sqs(
                                queue_url='https://queue.url.example.aws.com',
                                assume_role=final_assume_role,
                            )],
                        ),
                    ])

    """
    role_arn = ""
    external_id = ""
    role_session_name = ""
    duration = 3600
    source: Optional[AssumeRole] = None

    def __init__(
        self,
        role_arn: str,
        external_id: str = "",
        role_session_name: str = "",
        duration: int = 3600,
        source: Optional[AssumeRole] = None
    ):
        """Assume a role."""
        self.role_arn = role_arn
        self.external_id = external_id
        self.role_session_name = role_session_name
        self.duration = duration
        self.source = source

    def __call__(self, boto3: ModuleType) -> ModuleType:
        # chaining!
        if self.source:
            boto3 = self.source(boto3)

        calling_params = {
            "RoleArn": self.role_arn,
            "RoleSessionName": self.role_session_name if self.role_session_name else "clearkies-aws",
            "DurationSeconds": self.duration,
        }
        if self.external_id:
            calling_params['ExternalId'] = self.external_id
        credentials = boto3.client("sts").assume_role(**calling_params)["Credentials"]

        # now let's make a new session using those
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
