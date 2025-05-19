import base64
import json
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

from boto3.session import Session as Boto3Session
from botocore.exceptions import ClientError
from clearskies import Model  # For mocking model instances
from clearskies.autodoc.schema import String as AutoDocString

# Imports for the classes under test and their dependencies
from clearskies_aws.backends.dynamo_db_parti_ql_backend import (
    DynamoDBPartiQLBackend,
    DynamoDBPartiQLCursor,
)

# No longer using a global mock_logger here. It will be injected by @patch.


@patch(
    "clearskies_aws.backends.dynamo_db_parti_ql_backend.logger"
)  # Patch the logger at the module level of the backend
class TestDynamoDBPartiQLBackend(unittest.TestCase):

    def setUp(self):
        """Set up the test environment before each test method."""
        self.mock_boto3_session = MagicMock(spec=Boto3Session)
        self.mock_dynamodb_client = MagicMock()  # This is the client used by the cursor
        self.mock_boto3_session.client.return_value = self.mock_dynamodb_client

        # Instantiate the actual cursor with the mocked Boto3 client setup
        # We will test the actual .execute() method of this cursor instance.
        self.cursor_under_test = DynamoDBPartiQLCursor(self.mock_boto3_session)

        self.backend = DynamoDBPartiQLBackend(self.cursor_under_test)
        self.mock_model = MagicMock(spec=Model)
        # Configure common model attributes needed by backend methods
        self.mock_model.table_name = "my_test_table"
        self.mock_model.id_column_name = "id"

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
        }
        config.update(overrides)
        return config

    def test_as_sql_simple_select_all(self, mock_logger_arg):
        """Test SQL generation for a simple 'SELECT *' statement."""
        config = self._get_base_config(table_name="users", select_all=True)
        statement, params, limit, next_token = self.backend.as_sql(config)
        self.assertEqual('SELECT "users".* FROM "users"', statement)
        self.assertEqual([], params)
        self.assertIsNone(limit)
        self.assertIsNone(next_token)

    def test_as_sql_select_specific_columns(self, mock_logger_arg):
        """Test SQL generation for selecting specific columns."""
        config = self._get_base_config(table_name="products", selects=["name", "price"])
        statement, params, limit, next_token = self.backend.as_sql(config)
        self.assertEqual('SELECT "name", "price" FROM "products"', statement)
        self.assertEqual([], params)

    def test_as_sql_select_all_and_specific_columns(self, mock_logger_arg):
        """Test SQL generation when both select_all and specific columns are requested."""
        config = self._get_base_config(
            table_name="inventory", select_all=True, selects=["item_id", "stock_count"]
        )
        statement, params, limit, next_token = self.backend.as_sql(config)
        expected_sql = 'SELECT "inventory".*, "item_id", "stock_count" FROM "inventory"'
        self.assertEqual(expected_sql, statement)
        self.assertEqual([], params)

    def test_as_sql_default_select_if_no_select_all_or_selects(self, mock_logger_arg):
        """Test SQL generation defaults to 'SELECT *' if no specific columns are given."""
        config = self._get_base_config(table_name="orders")
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
        statement, params, limit, next_token = self.backend.as_sql(config)
        expected_statement = (
            'SELECT "customers".* FROM "customers" WHERE "city" = ? AND "age" > ?'
        )
        expected_parameters = [{"S": "New York"}, {"N": "30"}]
        self.assertEqual(expected_statement, statement)
        self.assertEqual(expected_parameters, params)

    def test_as_sql_with_sorts(self, mock_logger_arg):
        """Test SQL generation with ORDER BY clauses."""
        config = self._get_base_config(
            table_name="items",
            select_all=True,
            sorts=[
                {"column": "name", "direction": "ASC"},
                {"column": "created_at", "direction": "DESC", "table": "items"},
            ],
        )
        statement, params, limit, next_token = self.backend.as_sql(config)
        expected_statement = 'SELECT "items".* FROM "items" ORDER BY "name" ASC, "items"."created_at" DESC'
        self.assertEqual(expected_statement, statement)

    def test_as_sql_with_limit_and_pagination(self, mock_logger_arg):
        """Test SQL generation with LIMIT and pagination (NextToken)."""
        config = self._get_base_config(
            table_name="logs",
            select_all=True,
            limit=50,
            pagination={"next_token": "some_encoded_token_string"},
        )
        statement, params, limit_val, next_token_val = self.backend.as_sql(config)
        self.assertEqual('SELECT "logs".* FROM "logs"', statement)
        self.assertEqual(50, limit_val)
        self.assertEqual("some_encoded_token_string", next_token_val)

    def test_map_from_boto3_value_decimal(self, mock_logger_arg):
        """Test the _map_from_boto3_value method for Decimal conversion."""
        self.assertEqual(123.45, self.backend._map_from_boto3_value(Decimal("123.45")))

    def test_map_from_boto3_record(self, mock_logger_arg):
        """Test the _map_from_boto3 method for processing a record."""
        record = {
            "amount": Decimal("99.90"),
            "name": "Test",
            "id": {"S": "value-from-dynamodb"},
        }
        expected_mapped_record = {
            "amount": 99.90,
            "name": "Test",
            "id": {"S": "value-from-dynamodb"},
        }
        self.assertEqual(expected_mapped_record, self.backend._map_from_boto3(record))

    def test_check_query_configuration_valid(self, mock_logger_arg):
        """Test _check_query_configuration with a valid configuration."""
        config = self._get_base_config(
            table_name="my_table", model_columns=["id", "name"]
        )
        config_copy = config.copy()
        processed_config = self.backend._check_query_configuration(
            config_copy, self.mock_model
        )
        self.assertIn("table_name", processed_config)
        self.assertEqual([], processed_config.get("wheres"))
        self.assertEqual(False, processed_config.get("select_all"))

    def test_check_query_configuration_missing_required_table_name(
        self, mock_logger_arg
    ):
        """Test _check_query_configuration raises KeyError if table_name is missing."""
        config = self._get_base_config(table_name=None)
        del config["table_name"]
        with self.assertRaisesRegex(
            KeyError, "Missing required configuration key table_name"
        ):
            self.backend._check_query_configuration(config, self.mock_model)

    def test_check_query_configuration_empty_table_name(self, mock_logger_arg):
        """Test _check_query_configuration raises KeyError if table_name is empty."""
        config = self._get_base_config(table_name="")
        with self.assertRaisesRegex(
            KeyError, "Missing required configuration key table_name"
        ):
            self.backend._check_query_configuration(config, self.mock_model)

    def test_check_query_configuration_invalid_key(self, mock_logger_arg):
        """Test _check_query_configuration raises KeyError for an unsupported config key."""
        config = self._get_base_config()
        config["unsupported_key_for_backend"] = True
        with self.assertRaisesRegex(
            KeyError,
            "DynamoDBBackend does not support config 'unsupported_key_for_backend'",
        ):
            self.backend._check_query_configuration(config, self.mock_model)

    def test_validate_pagination_kwargs_valid(self, mock_logger_arg):
        """Test validate_pagination_kwargs with a valid next_token."""
        token_data = {"last_id": "item123"}
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()
        self.assertEqual(
            "",
            self.backend.validate_pagination_kwargs({"next_token": token}, lambda x: x),
        )

    def test_validate_pagination_kwargs_invalid_key(self, mock_logger_arg):
        """Test validate_pagination_kwargs with an invalid pagination key."""
        case_mapping = str.upper
        key_name_nt = case_mapping("next_token")
        expected_msg = (
            f"Invalid pagination key(s): 'offset'. Only '{key_name_nt}' is allowed"
        )
        valid_token = base64.urlsafe_b64encode(json.dumps({"id": 1}).encode()).decode()
        self.assertEqual(
            expected_msg,
            self.backend.validate_pagination_kwargs(
                {"next_token": valid_token, "offset": 1}, case_mapping
            ),
        )

    def test_validate_pagination_kwargs_missing_token(self, mock_logger_arg):
        """Test validate_pagination_kwargs when the next_token is missing."""
        case_mapping = str.lower
        key_name_nt = case_mapping("next_token")
        self.assertEqual(
            f"You must specify '{key_name_nt}' when setting pagination",
            self.backend.validate_pagination_kwargs({}, case_mapping),
        )

    def test_validate_pagination_kwargs_invalid_token_non_string(self, mock_logger_arg):
        """Test validate_pagination_kwargs with a next_token that is not a string."""
        case_mapping = str.lower
        key_name_nt = case_mapping("next_token")
        self.assertEqual(
            f"The provided '{key_name_nt}' appears to be invalid.",
            self.backend.validate_pagination_kwargs(
                {"next_token": 12345}, case_mapping
            ),
        )

    def test_validate_pagination_kwargs_invalid_token_empty_string(
        self, mock_logger_arg
    ):
        """Test validate_pagination_kwargs with an empty string as next_token."""
        case_mapping = str.lower
        key_name_nt = case_mapping("next_token")
        self.assertEqual(
            f"The provided '{key_name_nt}' appears to be invalid.",
            self.backend.validate_pagination_kwargs({"next_token": ""}, case_mapping),
        )

    def test_serialize_and_restore_next_token(self, mock_logger_arg):
        """Test serialization and subsequent restoration of a next_token."""
        original_ddb_token = "opaqueDynamoDBNextTokenString"
        serialized_for_client = self.backend.serialize_next_token_for_response(
            original_ddb_token
        )
        self.assertIsInstance(serialized_for_client, str)
        restored_for_ddb = self.backend.restore_next_token_from_config(
            serialized_for_client
        )
        self.assertEqual(original_ddb_token, restored_for_ddb)

    def test_serialize_next_token_for_none(self, mock_logger_arg):
        """Test that serializing a None key results in None."""
        self.assertIsNone(self.backend.serialize_next_token_for_response(None))

    def test_restore_next_token_from_config_none_or_invalid(self, mock_logger_arg):
        """Test restoring next_token from None or invalid string values."""
        self.assertIsNone(self.backend.restore_next_token_from_config(None))
        self.assertIsNone(self.backend.restore_next_token_from_config(""))
        self.assertIsNone(
            self.backend.restore_next_token_from_config("this is not valid base64 json")
        )
        self.assertIsNone(self.backend.restore_next_token_from_config(12345))

    def test_documentation_methods(self, mock_logger_arg):
        """Test the output of documentation helper methods for pagination."""
        case_mapping = lambda x: f"custom_{x}"
        expected_doc_response = [AutoDocString(name=case_mapping("next_token"))]
        actual_doc_response = self.backend.documentation_pagination_next_page_response(
            case_mapping
        )
        self.assertEqual(len(expected_doc_response), len(actual_doc_response))
        self.assertEqual(expected_doc_response[0].name, actual_doc_response[0].name)

        expected_doc_example = {case_mapping("next_token"): ""}
        self.assertEqual(
            expected_doc_example,
            self.backend.documentation_pagination_next_page_example(case_mapping),
        )

        expected_doc_params = [
            (
                AutoDocString(name=case_mapping("next_token"), example=""),
                "A token to fetch the next page of results",
            )
        ]
        actual_doc_params = self.backend.documentation_pagination_parameters(
            case_mapping
        )
        self.assertEqual(len(expected_doc_params), len(actual_doc_params))
        self.assertEqual(expected_doc_params[0][0].name, actual_doc_params[0][0].name)
        self.assertEqual(
            expected_doc_params[0][0].example, actual_doc_params[0][0].example
        )
        self.assertEqual(expected_doc_params[0][1], actual_doc_params[0][1])

    def test_cursor_execute_called_correctly(self, mock_logger_arg):
        """Test that the cursor's execute method is called correctly by the backend."""
        statement_to_run = 'SELECT * FROM "my_table" WHERE "id" = ?'
        params_to_run = [{"S": "123"}]
        expected_boto_response = {
            "Items": [{"id": {"S": "123"}, "data": {"S": "value"}}]
        }
        self.mock_dynamodb_client.execute_statement.return_value = (
            expected_boto_response
        )

        result = self.cursor_under_test.execute(
            statement_to_run, params_to_run, ConsistentRead=True
        )

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=statement_to_run, Parameters=params_to_run, ConsistentRead=True
        )
        self.assertEqual(expected_boto_response, result)

    def test_cursor_execute_client_error_resource_not_found(self, mock_logger_arg):
        """Test cursor's execute method handling for ResourceNotFoundException."""
        error_response = {
            "Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}
        }
        self.mock_dynamodb_client.execute_statement.side_effect = ClientError(
            error_response, "ExecuteStatement"
        )
        with self.assertRaises(ClientError) as cm:
            self.cursor_under_test.execute('SELECT * FROM "non_existent_table"', [])

        self.assertEqual(
            cm.exception.response["Error"]["Code"], "ResourceNotFoundException"
        )
        mock_logger_arg.error.assert_any_call(
            "Couldn't execute PartiQL '%s' because the table does not exist.",
            'SELECT * FROM "non_existent_table"',
        )

    def test_cursor_execute_client_error_other(self, mock_logger_arg):
        """Test cursor's execute method handling for other ClientErrors."""
        error_response = {
            "Error": {"Code": "ValidationException", "Message": "Invalid query"}
        }
        self.mock_dynamodb_client.execute_statement.side_effect = ClientError(
            error_response, "ExecuteStatement"
        )
        with self.assertRaises(ClientError) as cm:
            self.cursor_under_test.execute('SELECT ??? FROM "bad_query"', [])

        self.assertEqual(cm.exception.response["Error"]["Code"], "ValidationException")
        mock_logger_arg.error.assert_any_call(
            "Couldn't execute PartiQL '%s'. Here's why: %s: %s",
            'SELECT ??? FROM "bad_query"',
            "ValidationException",
            "Invalid query",
        )

    def test_records_simple_fetch(self, mock_logger_arg):
        """Test records() fetching a single page of results without limit or pagination."""
        config = self._get_base_config(table_name="users", select_all=True)
        expected_statement = 'SELECT "users".* FROM "users"'
        ddb_items = [
            {"id": {"S": "user1"}, "name": {"S": "Alice"}, "age": Decimal("30")},
            {"id": {"S": "user2"}, "name": {"S": "Bob"}, "age": Decimal("24")},
        ]
        self.mock_dynamodb_client.execute_statement.return_value = {"Items": ddb_items}

        results = list(self.backend.records(config, self.mock_model))

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=[]
        )
        self.assertEqual(len(results), 2)
        # Expecting _map_from_boto3 to convert Decimal to float
        self.assertEqual(
            results[0], {"id": {"S": "user1"}, "name": {"S": "Alice"}, "age": 30.0}
        )
        self.assertEqual(
            results[1], {"id": {"S": "user2"}, "name": {"S": "Bob"}, "age": 24.0}
        )
        self.assertIsNone(config["pagination"].get("next_page_token_for_response"))

    def test_records_with_limit(self, mock_logger_arg):
        """Test records() respects the server-side limit passed to DynamoDB."""
        config = self._get_base_config(table_name="products", limit=1, select_all=True)
        expected_statement = 'SELECT "products".* FROM "products"'
        ddb_items = [{"id": {"S": "prod1"}, "price": Decimal("10.99")}]
        ddb_next_token = "fakeDDBNextToken"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items,
            "NextToken": ddb_next_token,
        }

        results = list(self.backend.records(config, self.mock_model))

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=[], Limit=1
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"id": {"S": "prod1"}, "price": 10.99})
        expected_client_token = self.backend.serialize_next_token_for_response(
            ddb_next_token
        )
        self.assertEqual(
            config["pagination"].get("next_page_token_for_response"),
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
        expected_statement = 'SELECT "events".* FROM "events"'
        ddb_items_page1 = [{"event_id": {"S": "evt1"}}]  # Data as it comes from DDB
        ddb_next_token_page1 = "ddb_token_for_page2"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items_page1,
            "NextToken": ddb_next_token_page1,
        }

        results_page1 = list(self.backend.records(config1, self.mock_model))

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=[], NextToken=initial_ddb_token
        )
        self.assertEqual(len(results_page1), 1)
        self.assertEqual(
            results_page1[0], {"event_id": {"S": "evt1"}}
        )  # Expecting mapped data
        client_token_for_next_call = config1["pagination"].get(
            "next_page_token_for_response"
        )
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
        ddb_items_page2 = [{"event_id": {"S": "evt2"}}]  # Data as it comes from DDB
        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items_page2
        }

        results_page2 = list(self.backend.records(config2, self.mock_model))

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=[], NextToken=ddb_next_token_page1
        )
        self.assertEqual(len(results_page2), 1)
        self.assertEqual(
            results_page2[0], {"event_id": {"S": "evt2"}}
        )  # Expecting mapped data
        self.assertIsNone(config2["pagination"].get("next_page_token_for_response"))

    def test_records_no_items_returned_with_next_token(self, mock_logger_arg):
        """Test records() when DDB returns no items but provides a NextToken."""
        config = self._get_base_config(table_name="filtered_items", select_all=True)
        expected_statement = 'SELECT "filtered_items".* FROM "filtered_items"'
        ddb_next_token = "ddb_has_more_but_current_page_empty_after_filter"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": [],
            "NextToken": ddb_next_token,
        }

        results = list(self.backend.records(config, self.mock_model))

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=[]
        )
        self.assertEqual(len(results), 0)
        expected_client_token = self.backend.serialize_next_token_for_response(
            ddb_next_token
        )
        self.assertEqual(
            config["pagination"].get("next_page_token_for_response"),
            expected_client_token,
        )

    def test_records_limit_cuts_off_ddb_page(self, mock_logger_arg):
        """Test when server-side limit means fewer items are returned than a full DDB page."""
        config = self._get_base_config(
            table_name="many_items", limit=1, select_all=True
        )
        expected_statement = 'SELECT "many_items".* FROM "many_items"'
        ddb_items_returned_by_limit = [{"id": {"S": "item1"}}]
        ddb_next_token_after_limit = "ddb_still_has_more_after_limit"

        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": ddb_items_returned_by_limit,
            "NextToken": ddb_next_token_after_limit,
        }

        results = list(self.backend.records(config, self.mock_model))

        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement=expected_statement, Parameters=[], Limit=1
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"id": {"S": "item1"}})  # Expecting mapped data
        expected_client_token = self.backend.serialize_next_token_for_response(
            ddb_next_token_after_limit
        )
        self.assertEqual(
            config["pagination"].get("next_page_token_for_response"),
            expected_client_token,
        )

    def test_count_simple(self, mock_logger_arg):
        """Test count() with no where conditions."""
        config = self._get_base_config(table_name="users")
        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": [{"count": {"N": "150"}}]
        }

        count = self.backend.count(config, self.mock_model)

        self.assertEqual(count, 150)
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement='SELECT COUNT(*) AS count FROM "users"', Parameters=[]
        )

    def test_count_with_wheres(self, mock_logger_arg):
        """Test count() with where conditions."""
        config = self._get_base_config(
            table_name="users",
            wheres=[{"column": "status", "operator": "=", "values": ["active"]}],
        )
        self.mock_dynamodb_client.execute_statement.return_value = {
            "Items": [{"_1": {"N": "75"}}]
        }

        count = self.backend.count(config, self.mock_model)

        self.assertEqual(count, 75)
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement='SELECT COUNT(*) AS count FROM "users" WHERE "status" = ?',
            Parameters=[{"S": "active"}],
        )

    def test_create_record(self, mock_logger_arg):
        """Test create() inserts a record and returns the input data."""
        data_to_create = {"id": "new_user_123", "name": "Jane Doe", "age": 28}
        # Corrected expectation: Parameters is a list containing ONE item, which is the record itself.
        expected_ddb_parameters = [
            {"id": {"S": "new_user_123"}, "name": {"S": "Jane Doe"}, "age": {"N": "28"}}
        ]

        self.mock_dynamodb_client.execute_statement.return_value = {}

        created_data = self.backend.create(data_to_create, self.mock_model)

        self.assertEqual(created_data, data_to_create)
        self.mock_dynamodb_client.execute_statement.assert_called_once_with(
            Statement='INSERT INTO "my_test_table" VALUE ?',
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
            "age": {"N": "35"},  # Data as it comes from DDB
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
        # Assert based on _map_from_boto3 (which only converts Decimal to float)
        self.assertEqual(
            updated_data_response,
            {
                "id": {"S": record_id},
                "name": {"S": "Original Name"},
                "age": {
                    "N": "35"
                },  # Expecting this to remain as DDB typed value if not Decimal
                "status": {"S": "active"},
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
