import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from clearskies_aws.secrets.parameter_store import ParameterStore


class ParameterStoreTest(unittest.TestCase):
    def setUp(self):
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))

    def test_get(self):
        ssm = SimpleNamespace(get_parameter=MagicMock(return_value={"Parameter": {"Value": "sup"}}))
        boto3 = SimpleNamespace(client=MagicMock(return_value=ssm))
        parameter_store = ParameterStore(boto3, self.environment)
        self.assertEqual("sup", parameter_store.get("/my/item"))
        ssm.get_parameter.assert_called_with(Name="/my/item", WithDecryption=True)
        boto3.client.assert_called_with("ssm", region_name="us-east-1")
