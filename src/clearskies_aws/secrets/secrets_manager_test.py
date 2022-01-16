import unittest
from unittest.mock import MagicMock
from .secrets_manager import SecretsManager
from types import SimpleNamespace
class SecretsManagerTest(unittest.TestCase):
    def setUp(self):
        self.environment = SimpleNamespace(get=MagicMock(return_value='us-east-1'))

    def test_get(self):
        secretsmanager = SimpleNamespace(get_secret_value=MagicMock(return_value={'SecretString': 'sup'}))
        boto3 = SimpleNamespace(client=MagicMock(return_value=secretsmanager))
        secrets_manager = SecretsManager(boto3, self.environment)
        self.assertEquals('sup', secrets_manager.get('/my/item'))
        secretsmanager.get_secret_value.assert_called_with(SecretId='/my/item')
        boto3.client.assert_called_with('secretsmanager', region_name='us-east-1')
