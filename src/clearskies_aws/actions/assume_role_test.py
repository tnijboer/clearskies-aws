import unittest
from unittest.mock import MagicMock, call
from .assume_role import AssumeRole
class AssumeRoleTest(unittest.TestCase):
    def test_with_external_id(self):
        sts = MagicMock()
        sts.assume_role = MagicMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "access-key",
                    "SecretAccessKey": "secret-key",
                    "SessionToken": "session-token",
                }
            }
        )
        boto3 = MagicMock()
        boto3.client = MagicMock(return_value=sts)
        boto3.Session = MagicMock(return_value='MOAR BOTO')

        assume_role = AssumeRole(role_arn='aws:arn:role/name', external_id='12345')
        self.assertEquals('MOAR BOTO', assume_role(boto3))
        boto3.client.assert_called_with("sts")
        boto3.Session.assert_called_with(
            aws_access_key_id="access-key",
            aws_secret_access_key="secret-key",
            aws_session_token="session-token",
        )
        sts.assume_role.assert_called_with(
            RoleArn='aws:arn:role/name',
            RoleSessionName="clearkies-aws",
            DurationSeconds=3600,
            ExternalId="12345",
        )

    def test_with_source(self):
        sts = MagicMock()
        sts.assume_role = MagicMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "access-key",
                    "SecretAccessKey": "secret-key",
                    "SessionToken": "session-token",
                }
            }
        )
        boto3 = MagicMock()
        boto3.client = MagicMock(return_value=sts)
        boto3.Session = MagicMock(return_value='MOAR BOTO')
        source = MagicMock(return_value=boto3)

        assume_role = AssumeRole(
            role_arn='aws:arn:role/name',
            source=source,
            role_session_name="sup",
            duration=7200,
        )
        self.assertEquals('MOAR BOTO', assume_role("not-boto3"))
        boto3.client.assert_called_with("sts")
        boto3.Session.assert_called_with(
            aws_access_key_id="access-key",
            aws_secret_access_key="secret-key",
            aws_session_token="session-token",
        )
        sts.assume_role.assert_called_with(
            RoleArn='aws:arn:role/name',
            RoleSessionName="sup",
            DurationSeconds=7200,
        )
        source.assert_called_with("not-boto3")
