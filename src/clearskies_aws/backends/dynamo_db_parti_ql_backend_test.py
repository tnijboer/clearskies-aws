import base64
import json
import re
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

from boto3.session import Session as Boto3Session
from botocore.exceptions import ClientError
from clearskies import Model
from clearskies.autodoc.schema import String as AutoDocString

from clearskies_aws.backends.dynamo_db_parti_ql_backend import (
    DynamoDBPartiQLBackend,
    DynamoDBPartiQLCursor,
)


@patch("clearskies_aws.backends.dynamo_db_parti_ql_backend.logger")
class TestDynamoDBPartiQLBackend(unittest.TestCase):

    def setUp(self):
        """Set up the test environment before each test method."""
        self.mock_boto3_session = MagicMock(spec=Boto3Session)
        self.mock_dynamodb_client = MagicMock()
        self.mock_boto3_session.client.return_value = self.mock_dynamodb_client

        self.cursor_under_test = DynamoDBPartiQLCursor(self.mock_boto3_session)

        self.backend = DynamoDBPartiQLBackend(self.cursor_under_test)
        self.mock_model = MagicMock(spec=Model)
        self.mock_model.get_table_name = MagicMock(return_value="my_test_table")
        self.mock_model.id_column_name = "id"

        self.mock_model.schema = MagicMock()
        self.mock_model.schema.return_value.indexes = {}

        self.backend._get_table_description = MagicMock()
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [],
        }

    def _get_base_config(self, table_name="test_table", **overrides):
        """Helper to create a base configuration dictionary with defaults."""
        config = {
            "table_name": table_name,
            "wheres": [],
            "sorts": [],
            "limit": None,
            "pagination": {},
            "model_columns": [],
            "select_all": False,
            "selects": [],
            "group_by_column": None,
            "joins": [],
        }
        config.update(overrides)
        return config

    def test_as_sql_simple_select_all(self, mock_logger_arg):
        """Test SQL generation for a simple 'SELECT *' statement."""
        config = self._get_base_config(table_name="users", select_all=True)
        config["_chosen_index_name"] = None
        statement, params, limit, next_token = self.backend.as_sql(config)
        self.assertEqual('SELECT * FROM "users"', statement)
        self.assertEqual([], params)
        self.assertIsNone(limit)
        self.assertEqual(next_token, config.get("pagination", {}).get("next_token"))

    def test_as_sql_select_specific_columns(self, mock_logger_arg):
        """Test SQL generation for selecting specific columns."""
        config = self._get_base_config(table_name="products", selects=["name", "price"])
        config["_chosen_index_name"] = None
        statement, params, limit, next_token = self.backend.as_sql(config)
        self.assertEqual('SELECT "name", "price" FROM "products"', statement)
        self.assertEqual([], params)

    def test_as_sql_select_all_and_specific_columns_uses_specific(
        self, mock_logger_arg
    ):
        """Test SQL generation uses specific columns if both select_all and selects are given."""
        config = self._get_base_config(
            table_name="inventory", select_all=True, selects=["item_id", "stock_count"]
        )
        config["_chosen_index_name"] = None
        statement, params, limit, next_token = self.backend.as_sql(config)
        expected_sql = 'SELECT "item_id", "stock_count" FROM "inventory"'
        self.assertEqual(expected_sql, statement)
        mock_logger_arg.warning.assert_any_call(
            "Both 'select_all=True' and specific 'selects' were provided. Using specific 'selects'."
        )

    def test_as_sql_default_select_if_no_select_all_or_selects(self, mock_logger_arg):
        """Test SQL generation defaults to 'SELECT *' if no specific columns are given."""
        config = self._get_base_config(table_name="orders")
        config["_chosen_index_name"] = None
        statement, params, limit, next_token = self.backend.as_sql(config)
        self.assertEqual('SELECT * FROM "orders"', statement)
        self.assertEqual([], params)

    def test_as_sql_with_wheres(self, mock_logger_arg):
        """Test SQL generation with WHERE clauses."""
        config = self._get_base_config(
            table_name="customers",
            select_all=True,
            wheres=[
                {"column": "city", "operator": "=", "values": ["New York"]},
                {"column": "age", "operator": ">", "values": [30]},
            ],
        )
        config["_chosen_index_name"] = None
        statement, params, limit, next_token = self.backend.as_sql(config)
        expected_statement = 'SELECT * FROM "customers" WHERE "city" = ? AND "age" > ?'
        expected_parameters = [{"S": "New York"}, {"N": "30"}]
        self.assertEqual(expected_statement, statement)
        self.assertEqual(expected_parameters, params)

    def test_as_sql_with_sorts(self, mock_logger_arg):
        """Test SQL generation with ORDER BY clauses (no table prefix for columns)."""
        config = self._get_base_config(
            table_name="items",
            select_all=True,
            sorts=[
                {"column": "name", "direction": "ASC"},
                {"column": "created_at", "direction": "DESC"},
            ],
        )
        config["_chosen_index_name"] = None
        statement, params, limit, next_token = self.backend.as_sql(config)
        expected_statement = (
            'SELECT * FROM "items" ORDER BY "name" ASC, "created_at" DESC'
        )
        self.assertEqual(expected_statement, statement)

    def test_as_sql_with_index_name(self, mock_logger_arg):
        """Test SQL generation uses index name in FROM clause if provided."""
        config = self._get_base_config(table_name="my_table", select_all=True)
        config["_chosen_index_name"] = "my_gsi"

        statement, params, limit, next_token = self.backend.as_sql(config)
        self.assertEqual('SELECT * FROM "my_table"."my_gsi"', statement)

    def test_as_sql_ignores_group_by_and_joins(self, mock_logger_arg):
        """Test that GROUP BY and JOIN configurations are ignored for SQL but logged."""
        config = self._get_base_config(
            table_name="log_data", group_by_column="level", joins=["some_join_info"]
        )
        config["_chosen_index_name"] = None

        statement, _, _, _ = self.backend.as_sql(config)
        self.assertNotIn("GROUP BY", statement.upper())
        self.assertNotIn("JOIN", statement.upper())
        mock_logger_arg.warning.assert_any_call(
            "Configuration included 'group_by_column=level', "
            "but GROUP BY is not supported by this DynamoDB PartiQL backend and will be ignored for SQL generation."
        )
        mock_logger_arg.warning.assert_any_call(
            "Configuration included 'joins=['some_join_info']', "
            "but JOINs are not supported by this DynamoDB PartiQL backend and will be ignored for SQL generation."
        )

    def test_check_query_configuration_sort_with_base_table_hash_key_equality(
        self, mock_logger_arg
    ):
        """Test _check_query_configuration allows sort if base table hash key equality exists."""
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [],
        }
        config = self._get_base_config(
            table_name="my_test_table",
            sorts=[{"column": "name", "direction": "ASC"}],
            wheres=[{"column": "id", "operator": "=", "values": ["some_id"]}],
        )
        processed_config = self.backend._check_query_configuration(
            config, self.mock_model
        )
        self.assertIsNone(processed_config.get("_chosen_index_name"))
        self.assertEqual(processed_config.get("_partition_key_for_target"), "id")

    def test_check_query_configuration_sort_raises_error_if_no_hash_key_equality(
        self, mock_logger_arg
    ):
        """Test _check_query_configuration raises ValueError for sort without hash key equality."""
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [],
        }
        config = self._get_base_config(
            table_name="my_test_table",
            sorts=[{"column": "name", "direction": "ASC"}],
            wheres=[{"column": "status", "operator": "=", "values": ["active"]}],
        )
        expected_error_message = "DynamoDB PartiQL queries with ORDER BY on 'my_test_table' require an equality condition on its partition key ('id') in the WHERE clause."
        with self.assertRaisesRegex(ValueError, re.escape(expected_error_message)):
            self.backend._check_query_configuration(config, self.mock_model)

    def test_check_query_configuration_sort_uses_gsi_if_partition_key_matches(
        self, mock_logger_arg
    ):
        """Test _check_query_configuration selects GSI if its partition key matches WHERE and can sort."""
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "domain-status-index",
                    "KeySchema": [
                        {"AttributeName": "domain", "KeyType": "HASH"},
                        {"AttributeName": "status", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        }
        config = self._get_base_config(
            table_name="my_test_table",
            sorts=[{"column": "status", "direction": "DESC"}],
            wheres=[{"column": "domain", "operator": "=", "values": ["example.com"]}],
        )
        processed_config = self.backend._check_query_configuration(
            config, self.mock_model
        )
        self.assertEqual(
            processed_config.get("_chosen_index_name"), "domain-status-index"
        )
        self.assertEqual(processed_config.get("_partition_key_for_target"), "domain")

    def test_count_uses_native_query_with_pk_condition(self, mock_logger_arg):
        """Test count() uses native DDB query when PK equality is present."""
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        }
        config = self._get_base_config(
            table_name="users",
            wheres=[{"column": "id", "operator": "=", "values": ["user123"]}],
        )
        self.mock_dynamodb_client.query.return_value = {"Count": 10, "Items": []}

        count = self.backend.count(config, self.mock_model)
        self.assertEqual(count, 10)
        self.mock_dynamodb_client.query.assert_called_once()
        self.mock_dynamodb_client.scan.assert_not_called()
        called_args = self.mock_dynamodb_client.query.call_args[1]
        self.assertEqual(called_args.get("TableName"), "users")
        self.assertEqual(called_args.get("Select"), "COUNT")
        self.assertIn("KeyConditionExpression", called_args)

    def test_count_uses_native_scan_without_pk_condition(self, mock_logger_arg):
        """Test count() uses native DDB scan when PK equality is NOT present."""
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        }
        config = self._get_base_config(
            table_name="users",
            wheres=[{"column": "status", "operator": "=", "values": ["active"]}],
        )
        self.mock_dynamodb_client.scan.return_value = {"Count": 5, "Items": []}

        count = self.backend.count(config, self.mock_model)
        self.assertEqual(count, 5)
        self.mock_dynamodb_client.scan.assert_called_once()
        self.mock_dynamodb_client.query.assert_not_called()
        called_args = self.mock_dynamodb_client.scan.call_args[1]
        self.assertEqual(called_args.get("TableName"), "users")
        self.assertEqual(called_args.get("Select"), "COUNT")
        self.assertIn("FilterExpression", called_args)

    def test_count_paginates_native_results(self, mock_logger_arg):
        """Test count() paginates and sums results from native DDB operations."""
        self.backend._get_table_description.return_value = {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        }
        config = self._get_base_config(table_name="large_table")

        self.mock_dynamodb_client.scan.side_effect = [
            {"Count": 100, "LastEvaluatedKey": {"id": {"S": "page1_end"}}},
            {"Count": 50, "LastEvaluatedKey": {"id": {"S": "page2_end"}}},
            {"Count": 25},
        ]
        count = self.backend.count(config, self.mock_model)
        self.assertEqual(count, 175)
        self.assertEqual(self.mock_dynamodb_client.scan.call_count, 3)

    def test_records_simple_fetch(self, mock_logger_arg):
        """Test records() fetching a single page of results without limit or pagination."""
        config = self._get_base_config(table_name="users", select_all=True)
        expected_statement = 'SELECT * FROM "users"'
        ddb_items = [
            {"id": {"S": "user1"}, "name": {"S": "Alice"}, "age": {"N": "30"}},
            {"id": {"S": "user2"}, "name": {"S": "Bob"}, "age": {"N": "24"}},
        ]
        self.mock_dynamodb_client.execute_statement.return_value = {"Items": ddb_items}

        results = list(self.backend.records(config, self.mock_model))

        expected_call_kwargs = {
            "Statement": expected_statement,
        }
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            **expected_call_kwargs
        )
        self.assertEqual(len(results), 2)
        # Assert based on what _map_from_boto3 currently does
        self.assertEqual(
            results[0], {"id": "user1", "name": "Alice", "age": Decimal("30")}
        )
        self.assertEqual(
            results[1], {"id": "user2", "name": "Bob", "age": Decimal("24")}
        )
        self.assertIsNone(config["pagination"].get("next_page_token_for_response"))

    def test_records_with_limit(self, mock_logger_arg):
        """Test records() respects the server-side limit passed to DynamoDB."""
        config = self._get_base_config(table_name="products", limit=1, select_all=True)
        expected_statement = 'SELECT * FROM "products"'
        ddb_items = [{"id": {"S": "prod1"}, "price": {"N": "10.99"}}]
        ddb_next_token = "fakeDDBNextToken"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items,
            "NextToken": ddb_next_token,
        }
        next_page_data = {}
        results = list(self.backend.records(config, self.mock_model, next_page_data))

        expected_call_kwargs = {
            "Statement": expected_statement,
            "Limit": 1,
        }
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            **expected_call_kwargs
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"id": "prod1", "price": Decimal("10.99")})
        expected_client_token = self.backend.serialize_next_token_for_response(
            ddb_next_token
        )
        self.assertEqual(
            next_page_data["next_token"],
            expected_client_token,
        )

    def test_records_pagination_flow(self, mock_logger_arg):
        """Test records() handling of client-provided token and returning DDB's next token."""
        initial_ddb_token = "start_token_from_ddb_previously"
        client_sends_this_token = self.backend.serialize_next_token_for_response(
            initial_ddb_token
        )
        config1 = self._get_base_config(
            table_name="events",
            select_all=True,
            pagination={"next_token": client_sends_this_token},
        )
        expected_statement = 'SELECT * FROM "events"'
        ddb_items_page1 = [{"event_id": {"S": "evt1"}}]
        ddb_next_token_page1 = "ddb_token_for_page2"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items_page1,
            "NextToken": ddb_next_token_page1,
        }
        next_page_data = {}
        results_page1 = list(
            self.backend.records(config1, self.mock_model, next_page_data)
        )

        expected_call_kwargs1 = {
            "Statement": expected_statement,
            "NextToken": initial_ddb_token,
        }
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            **expected_call_kwargs1
        )
        self.assertEqual(len(results_page1), 1)
        self.assertEqual(results_page1[0], {"event_id": "evt1"})
        client_token_for_next_call = next_page_data["next_token"]
        self.assertIsNotNone(client_token_for_next_call)
        self.assertEqual(
            self.backend.restore_next_token_from_config(client_token_for_next_call),
            ddb_next_token_page1,
        )

        self.mock_dynamodb_client.execute_statement.reset_mock()
        config2 = self._get_base_config(
            table_name="events",
            select_all=True,
            pagination={"next_token": client_token_for_next_call},
        )
        ddb_items_page2 = [{"event_id": {"S": "evt2"}}]
        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items_page2
        }
        next_page_data = {}
        results_page2 = list(
            self.backend.records(config2, self.mock_model, next_page_data)
        )
        expected_call_kwargs2 = {
            "Statement": expected_statement,
            "NextToken": ddb_next_token_page1,
        }
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            **expected_call_kwargs2
        )
        self.assertEqual(len(results_page2), 1)
        self.assertEqual(results_page2[0], {"event_id": "evt2"})
        self.assertEqual(next_page_data, {})

    def test_records_no_items_returned_with_next_token(self, mock_logger_arg):
        """Test records() when DDB returns no items but provides a NextToken."""
        config = self._get_base_config(table_name="filtered_items", select_all=True)
        expected_statement = 'SELECT * FROM "filtered_items"'
        ddb_next_token = "ddb_has_more_but_current_page_empty_after_filter"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": [],
            "NextToken": ddb_next_token,
        }
        next_page_data = {}
        results = list(self.backend.records(config, self.mock_model, next_page_data))

        expected_call_kwargs = {"Statement": expected_statement}
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            **expected_call_kwargs
        )
        self.assertEqual(len(results), 0)
        expected_client_token = self.backend.serialize_next_token_for_response(
            ddb_next_token
        )
        self.assertEqual(
            next_page_data["next_token"],
            expected_client_token,
        )

    def test_records_limit_cuts_off_ddb_page(self, mock_logger_arg):
        """Test when server-side limit means fewer items are returned than a full DDB page."""
        config = self._get_base_config(
            table_name="many_items", limit=1, select_all=True
        )
        expected_statement = 'SELECT * FROM "many_items"'
        ddb_items_returned_by_limit = [{"id": {"S": "item1"}}]
        ddb_next_token_after_limit = "ddb_still_has_more_after_limit"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items_returned_by_limit,
            "NextToken": ddb_next_token_after_limit,
        }
        next_page_data = {}
        results = list(self.backend.records(config, self.mock_model, next_page_data))

        expected_call_kwargs = {
            "Statement": expected_statement,
            "Limit": 1,
        }
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            **expected_call_kwargs
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"id": "item1"})
        expected_client_token = self.backend.serialize_next_token_for_response(
            ddb_next_token_after_limit
        )
        self.assertEqual(
            next_page_data["next_token"],
            expected_client_token,
        )

    def test_create_record(self, mock_logger_arg):
        """Test create() inserts a record and returns the input data."""
        data_to_create = {"id": "new_user_123", "name": "Jane Doe", "age": 28}
        # Updated expected statement and parameters to match the new PartiQL format
        expected_statement = (
            "INSERT INTO \"my_test_table\" VALUE {'id': ?, 'name': ?, 'age': ?}"
        )
        expected_ddb_parameters = [
            {"S": "new_user_123"},
            {"S": "Jane Doe"},
            {"N": "28"},
        ]

        self.mock_dynamodb_client.execute_statement.return_value = {}

        created_data = self.backend.create(data_to_create, self.mock_model)

        self.assertEqual(created_data, data_to_create)
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement,
            Parameters=expected_ddb_parameters,
        )

    def test_update_record(self, mock_logger_arg):
        """Test update() modifies a record and returns the updated data."""
        record_id = "user_to_update"
        update_data = {"age": 35, "status": "active"}

        expected_set_params = [{"N": "35"}, {"S": "active"}]
        expected_id_param = {"S": "user_to_update"}
        expected_ddb_parameters = expected_set_params + [expected_id_param]

        updated_item_from_db = {
            "id": {"S": record_id},
            "name": {"S": "Original Name"},
            "age": {"N": "35"},
            "status": {"S": "active"},
        }
        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": [updated_item_from_db]
        }

        updated_data_response = self.backend.update(
            record_id, update_data, self.mock_model
        )

        expected_statement = 'UPDATE "my_test_table" SET "age" = ?, "status" = ? WHERE "id" = ? RETURNING ALL NEW *'
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=expected_ddb_parameters
        )
        self.assertEqual(
            updated_data_response,
            {
                "id": "user_to_update",
                "name": "Original Name",
                "age": Decimal("35"),
                "status": "active",
            },
        )

    def test_delete_record(self, mock_logger_arg):
        """Test delete() removes a record."""
        record_id = "user_to_delete"
        expected_ddb_parameters = [{"S": "user_to_delete"}]
        self.mock_dynamodb_client.execute_statement.return_value = {}

        result = self.backend.delete(record_id, self.mock_model)

        self.assertTrue(result)
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement='DELETE FROM "my_test_table" WHERE "id" = ?',
            Parameters=expected_ddb_parameters,
        )


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
