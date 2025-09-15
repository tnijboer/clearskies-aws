import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from clearskies_aws.secrets.secrets_manager import SecretsManager


class SecretsManagerTest(unittest.TestCase):
    def setUp(self):
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))

    def test_get(self):
        secretsmanager = SimpleNamespace(get_secret_value=MagicMock(return_value={"SecretString": "sup"}))
        boto3 = SimpleNamespace(client=MagicMock(return_value=secretsmanager))
        secrets_manager = SecretsManager(boto3, self.environment)
        self.assertEqual("sup", secrets_manager.get("/my/item"))
        secretsmanager.get_secret_value.assert_called_with(SecretId="/my/item")
        boto3.client.assert_called_with("secretsmanager", region_name="us-east-1")
