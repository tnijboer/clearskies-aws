import unittest
from collections import OrderedDict
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies
from boto3.dynamodb import conditions as dynamodb_conditions

from clearskies_aws.di import StandardDependencies


class User(clearskies.Model):
    def __init__(self, dynamo_db_backend, columns):
        super().__init__(dynamo_db_backend, columns)

    def columns_configuration(self):
        return OrderedDict(
            [
                clearskies.column_types.string("name"),
                clearskies.column_types.string("category_id"),
                clearskies.column_types.integer("age"),
            ]
        )


class Users(clearskies.Models):
    def __init__(self, dynamo_db_backend, columns):
        super().__init__(dynamo_db_backend, columns)

    def model_class(self):
        return User


class DynamoDBBackendTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind("environment", {"AWS_REGION": "us-east-2"})
        self.dynamo_db_table = SimpleNamespace(
            key_schema=[{"KeyType": "HASH", "AttributeName": "id"}],
            global_secondary_indexes=[
                {
                    "IndexName": "category_id-name-index",
                    "KeySchema": [
                        {"KeyType": "HASH", "AttributeName": "category_id"},
                        {"KeyType": "RANGE", "AttributeName": "name"},
                    ],
                },
                {
                    "IndexName": "category_id-age-index",
                    "KeySchema": [
                        {"KeyType": "RANGE", "AttributeName": "age"},
                        {"KeyType": "HASH", "AttributeName": "category_id"},
                    ],
                },
            ],
            local_secondary_indexes=[
                {
                    "IndexName": "id-category_id-index",
                    "KeySchema": [
                        {"KeyType": "RANGE", "AttributeName": "category_id"},
                        {"KeyType": "HASH", "AttributeName": "id"},
                    ],
                }
            ],
        )
        self.dynamo_db = SimpleNamespace(
            Table=MagicMock(return_value=self.dynamo_db_table),
        )
        self.boto3 = SimpleNamespace(
            resource=MagicMock(return_value=self.dynamo_db),
        )
        self.di.bind("boto3", self.boto3)

    def test_create(self):
        self.dynamo_db_table.put_item = MagicMock()
        user = self.di.build(User)
        user.save({"name": "sup", "age": 5, "category_id": "1-2-3-4"})
        self.boto3.resource.assert_called_with("dynamodb", region_name="us-east-2")
        self.dynamo_db.Table.assert_called_with("users")
        self.assertEqual(1, len(self.dynamo_db_table.put_item.call_args_list))
        call = self.dynamo_db_table.put_item.call_args_list[0]
        self.assertEqual((), call.args)
        self.assertEqual(1, len(call.kwargs))
        self.assertTrue("Item" in call.kwargs)
        saved_data = call.kwargs["Item"]
        # we're doing this a bit weird because the UUIDs will generate random values.
        # I could mock it, or I could just be lazy and grab it from the data I was given.
        self.assertEqual(
            {
                "id": saved_data["id"],
                "name": "sup",
                "age": 5,
                "category_id": "1-2-3-4",
            },
            saved_data,
        )
        self.assertEqual(saved_data["id"], user.id)
        self.assertEqual(5, user.age)
        self.assertEqual("1-2-3-4", user.category_id)
        self.assertEqual("sup", user.name)

    def test_update(self):
        self.dynamo_db_table.update_item = MagicMock(
            return_value={
                "Attributes": {
                    "id": "1-2-3-4",
                    "name": "hello",
                    "age": Decimal("10"),
                    "category_id": "1-2-3-5",
                }
            }
        )
        user = self.di.build(User)
        user.data = {"id": "1-2-3-4", "name": "sup", "age": 5, "category_id": "1-2-3-5"}
        user.save({"name": "hello", "age": 10})
        self.boto3.resource.assert_called_with("dynamodb", region_name="us-east-2")
        self.dynamo_db.Table.assert_called_with("users")
        self.dynamo_db_table.update_item.assert_called_with(
            Key={"id": "1-2-3-4"},
            UpdateExpression="SET #name = :name, #age = :age",
            ExpressionAttributeValues={":name": "hello", ":age": 10},
            ExpressionAttributeNames={"#name": "name", "#age": "age"},
            ReturnValues="ALL_NEW",
        )
        self.assertEqual("1-2-3-4", user.id)
        self.assertEqual("hello", user.name)
        self.assertEqual(10, user.age)
        self.assertEqual("1-2-3-5", user.category_id)

    def test_delete(self):
        self.dynamo_db_table.delete_item = MagicMock()
        user = self.di.build(User)
        user.data = {"id": "1-2-3-4", "name": "sup", "age": 5, "category_id": "1-2-3-5"}
        user.delete()
        self.dynamo_db_table.delete_item.assert_called_with(
            Key={"id": "1-2-3-4"},
        )

    def test_fetch_by_id(self):
        self.dynamo_db_table.query = MagicMock(
            return_value={"Items": [{"id": "1-2-3-4", "age": Decimal(10), "category_id": "4-5-6"}]}
        )
        users = self.di.build(Users)
        user = users.where("id=1-2-3-4").first()
        self.assertTrue(user.exists)
        self.assertEqual("1-2-3-4", user.id)
        self.assertEqual(10, user.age)
        self.assertEqual("4-5-6", user.category_id)
        self.assertEqual(1, len(self.dynamo_db_table.query.call_args_list))
        call = self.dynamo_db_table.query.call_args_list[0]
        self.assertEqual(3, len(call.kwargs))
        self.assertEqual("ALL_ATTRIBUTES", call.kwargs["Select"])
        self.assertEqual(True, call.kwargs["ScanIndexForward"])
        key_condition = call.kwargs["KeyConditionExpression"]

        # dynamodb does not make it easy to verify that the key expression was built properly,
        # and I don't think that mocking will make this any better.  Hoepfully they don't
        # change their library often...
        key_column = key_condition.get_expression()["values"][0]
        self.assertEqual("id", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Key))
        self.assertEqual("=", key_condition.expression_operator)
        self.assertEqual("1-2-3-4", key_condition.get_expression()["values"][1])

    def test_fetch_by_id_with_sort(self):
        self.dynamo_db_table.query = MagicMock(
            return_value={"Items": [{"id": "1-2-3-4", "age": Decimal(10), "category_id": "4-5-6"}]}
        )
        users = self.di.build(Users)
        users = users.where("id=1-2-3-4").sort_by("category_id", "desc").__iter__()
        self.assertEqual(1, len(self.dynamo_db_table.query.call_args_list))
        call = self.dynamo_db_table.query.call_args_list[0]
        self.assertEqual(4, len(call.kwargs))
        self.assertEqual("ALL_ATTRIBUTES", call.kwargs["Select"])
        self.assertEqual("id-category_id-index", call.kwargs["IndexName"])
        self.assertEqual(False, call.kwargs["ScanIndexForward"])
        key_condition = call.kwargs["KeyConditionExpression"]

        # dynamodb does not make it easy to verify that the key expression was built properly,
        # and I don't think that mocking will make this any better.  Hoepfully they don't
        # change their library often...
        key_column = key_condition.get_expression()["values"][0]
        self.assertEqual("id", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Key))
        self.assertEqual("=", key_condition.expression_operator)
        self.assertEqual("1-2-3-4", key_condition.get_expression()["values"][1])

    def test_fetch_by_secondary_index_twice(self):
        self.dynamo_db_table.query = MagicMock(
            return_value={"Items": [{"id": "1-2-3-4", "age": Decimal(10), "category_id": "4-5-6"}]}
        )
        users = self.di.build(Users)
        users = users.where("category_id=1-2-3-4").where("age>10").__iter__()
        self.assertEqual(1, len(self.dynamo_db_table.query.call_args_list))
        call = self.dynamo_db_table.query.call_args_list[0]
        self.assertEqual(4, len(call.kwargs))
        self.assertEqual("ALL_ATTRIBUTES", call.kwargs["Select"])
        self.assertEqual("category_id-age-index", call.kwargs["IndexName"])
        self.assertEqual(True, call.kwargs["ScanIndexForward"])
        key_condition = call.kwargs["KeyConditionExpression"]
        self.assertTrue(isinstance(key_condition, dynamodb_conditions.And))
        gt_condition = key_condition.get_expression()["values"][0]
        equal_condition = key_condition.get_expression()["values"][1]

        self.assertTrue(isinstance(equal_condition, dynamodb_conditions.Equals))
        key_column = equal_condition.get_expression()["values"][0]
        self.assertEqual("category_id", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Key))
        self.assertEqual("=", equal_condition.expression_operator)
        self.assertEqual("1-2-3-4", equal_condition.get_expression()["values"][1])

        self.assertTrue(isinstance(gt_condition, dynamodb_conditions.GreaterThan))
        key_column = gt_condition.get_expression()["values"][0]
        self.assertEqual("age", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Key))
        self.assertEqual(">", gt_condition.expression_operator)
        self.assertEqual(Decimal("10"), gt_condition.get_expression()["values"][1])

    def test_index_and_scan(self):
        self.dynamo_db_table.query = MagicMock(
            return_value={"Items": [{"id": "1-2-3-4", "age": Decimal(10), "category_id": "4-5-6"}]}
        )
        users = self.di.build(Users)
        users = users.where("category_id=1-2-3-4").where("age is not null").__iter__()
        self.assertEqual(1, len(self.dynamo_db_table.query.call_args_list))
        call = self.dynamo_db_table.query.call_args_list[0]
        self.assertEqual(5, len(call.kwargs))
        self.assertEqual("ALL_ATTRIBUTES", call.kwargs["Select"])
        self.assertEqual("category_id-name-index", call.kwargs["IndexName"])
        self.assertEqual(True, call.kwargs["ScanIndexForward"])

        # our key condition should be an equal search on category_id
        key_condition = call.kwargs["KeyConditionExpression"]
        key_column = key_condition.get_expression()["values"][0]
        self.assertEqual("category_id", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Key))
        self.assertEqual("=", key_condition.expression_operator)
        self.assertEqual("1-2-3-4", key_condition.get_expression()["values"][1])

        # and we should have a FilterExpression which is an 'is not null' condition
        filter_condition = call.kwargs["FilterExpression"]
        key_column = filter_condition.get_expression()["values"][0]
        self.assertEqual("age", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Attr))
        self.assertEqual("attribute_not_exists", filter_condition.expression_operator)

    def test_scan_only(self):
        self.dynamo_db_table.scan = MagicMock(
            return_value={"Items": [{"id": "1-2-3-4", "age": Decimal(10), "category_id": "4-5-6"}]}
        )
        users = self.di.build(Users)
        users = users.where("category_id>1-2-3-4").__iter__()
        self.assertEqual(1, len(self.dynamo_db_table.scan.call_args_list))
        call = self.dynamo_db_table.scan.call_args_list[0]
        self.assertEqual(2, len(call.kwargs))
        self.assertEqual("ALL_ATTRIBUTES", call.kwargs["Select"])

        # and we should have a FilterExpression which is an 'is not null' condition
        filter_condition = call.kwargs["FilterExpression"]
        key_column = filter_condition.get_expression()["values"][0]
        self.assertEqual("category_id", key_column.name)
        self.assertTrue(isinstance(key_column, dynamodb_conditions.Attr))
        self.assertEqual(">", filter_condition.expression_operator)
