import base64
import binascii
import json
import logging
from decimal import Decimal
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

from boto3.session import Session as Boto3Session
from botocore.exceptions import ClientError
from clearskies import Model as ClearSkiesModel
from clearskies.autodoc.schema import String as AutoDocString
from clearskies.backends import CursorBackend
from types_boto3_dynamodb.client import DynamoDBClient
from types_boto3_dynamodb.type_defs import (
    AttributeValueTypeDef,
    ExecuteStatementInputTypeDef,
    ExecuteStatementOutputTypeDef,
)

from clearskies_aws.backends.dynamo_db_condition_parser import DynamoDBConditionParser

logger = logging.getLogger(__name__)


class DynamoDBPartiQLCursor:
    """
    Cursor for executing PartiQL statements against DynamoDB.

    This class wraps a Boto3 DynamoDB client to provide a simplified interface
    for statement execution and error handling.
    """

    def __init__(self, boto3_session: Boto3Session) -> None:
        """
        Initializes the DynamoDBPartiQLCursor.

        Args:
            boto3_session: An initialized Boto3 Session object.
        """
        self._session: Boto3Session = boto3_session
        self._client: DynamoDBClient = self._session.client("dynamodb")

    def execute(
        self,
        statement: str,
        parameters: Optional[List[AttributeValueTypeDef]] = None,
        Limit: Optional[int] = None,
        NextToken: Optional[str] = None,
        ConsistentRead: Optional[bool] = None,
    ) -> ExecuteStatementOutputTypeDef:
        """
        Execute a PartiQL statement against DynamoDB.

        Args:
            statement: The PartiQL statement string to execute.
            parameters: An optional list of parameters for the PartiQL statement.
            Limit: Optional limit for the number of items DynamoDB evaluates.
            NextToken: Optional token for paginating results from DynamoDB.
            ConsistentRead: Optional flag for strongly consistent reads.

        Returns:
            The output from the boto3 client's execute_statement method.

        Raises:
            ClientError: If the execution fails due to a client-side error.
        """
        try:
            call_args: ExecuteStatementInputTypeDef = {"Statement": statement}
            if parameters is not None:
                call_args["Parameters"] = parameters
            if Limit is not None:
                call_args["Limit"] = Limit
            if NextToken is not None:
                call_args["NextToken"] = NextToken
            if ConsistentRead is not None:
                call_args["ConsistentRead"] = ConsistentRead

            output: ExecuteStatementOutputTypeDef = self._client.execute_statement(
                **call_args
            )
        except ClientError as err:
            error_response: Dict[str, Any] = err.response.get("Error", {})
            error_code: str = error_response.get("Code", "UnknownCode")
            error_message: str = error_response.get("Message", "Unknown error")

            if error_code == "ResourceNotFoundException":
                logger.error(
                    "Couldn't execute PartiQL '%s' because the table does not exist.",
                    statement,
                )
            else:
                logger.error(
                    "Couldn't execute PartiQL '%s'. Here's why: %s: %s",
                    statement,
                    error_code,
                    error_message,
                )
            raise
        else:
            return output


class DynamoDBPartiQLBackend(CursorBackend):
    """
    DynamoDB backend implementation that uses PartiQL for database interactions.
    """

    _allowed_configs: List[str] = [
        "table_name",
        "wheres",
        "sorts",
        "limit",
        "pagination",
        "model_columns",
        "selects",
        "select_all",
        "group_by_column",
    ]
    _required_configs: List[str] = ["table_name"]

    def __init__(self, dynamo_db_parti_ql_cursor: DynamoDBPartiQLCursor) -> None:
        """
        Initializes the DynamoDBPartiQLBackend.
        """
        super().__init__(dynamo_db_parti_ql_cursor)
        self.condition_parser: DynamoDBConditionParser = DynamoDBConditionParser()

    def _table_escape_character(self) -> str:
        """Returns the character used to escape table names."""
        return '"'

    def _column_escape_character(self) -> str:
        """Returns the character used to escape column names."""
        return '"'

    def _finalize_table_name(self, table_name: str) -> str:
        """Escapes a table name for use in a query."""
        if not table_name:
            return ""
        esc: str = self._table_escape_character()
        return f"{esc}{table_name.strip(esc)}{esc}"

    def _conditions_as_wheres_and_parameters(
        self, conditions: List[Dict[str, Any]], default_table_name: str
    ) -> Tuple[str, List[AttributeValueTypeDef]]:
        """
        Converts where conditions into a PartiQL WHERE clause and parameters.
        """
        if not conditions:
            return "", []

        where_parts: List[str] = []
        parameters: List[AttributeValueTypeDef] = []

        for where in conditions:
            if not isinstance(where, dict):
                logger.warning(f"Skipping non-dictionary where condition: {where}")
                continue

            column: Optional[str] = where.get("column")
            operator: Optional[str] = where.get("operator")
            values: Optional[List[Any]] = where.get("values")

            if not column or not operator or values is None:
                logger.warning(
                    f"Skipping malformed structured where condition: {where}"
                )
                continue

            value_parts: List[str] = []
            for v in values:
                if isinstance(v, str):
                    value_parts.append(f"'{v}'")
                elif isinstance(v, bool):
                    value_parts.append(str(v).lower())
                elif isinstance(v, (int, float, Decimal, type(None))):
                    value_parts.append(str(v))
                else:
                    value_parts.append(f"'{str(v)}'")

            condition_string: str = ""
            op_lower: str = operator.lower()
            if op_lower == "in":
                condition_string = f"{column} {operator} ({', '.join(value_parts)})"
            elif op_lower in self.condition_parser.operators_without_placeholders:
                condition_string = f"{column} {operator}"
            else:
                condition_string = (
                    f"{column} {operator} {value_parts[0] if value_parts else ''}"
                )

            try:
                parsed: Dict[str, Any] = self.condition_parser.parse_condition(
                    condition_string
                )
                where_parts.append(parsed["parsed"])
                parameters.extend(parsed["values"])
            except ValueError as e:
                logger.error(f"Error parsing condition '{condition_string}': {e}")
                continue

        if not where_parts:
            return "", []
        return " WHERE " + " AND ".join(where_parts), parameters

    def as_sql(
        self, configuration: Dict[str, Any]
    ) -> Tuple[str, List[AttributeValueTypeDef], Optional[int], Optional[str]]:
        """
        Constructs a PartiQL statement and parameters from a query configuration.
        """
        # _check_query_configuration is called in records(), count() etc. before this.

        escape: str = self._column_escape_character()
        table_name: str = configuration.get("table_name", "")

        wheres, parameters = self._conditions_as_wheres_and_parameters(
            configuration.get("wheres", []), table_name
        )

        select_parts: List[str] = []
        finalized_table: str = self._finalize_table_name(table_name)

        if configuration.get("select_all") and finalized_table:
            select_parts.append(f"{finalized_table}.*")

        selects: Optional[List[str]] = configuration.get("selects")
        if selects:
            select_parts.extend([f"{escape}{s.strip(escape)}{escape}" for s in selects])

        select: str = ", ".join(select_parts) if select_parts else "*"

        order_by: str = ""
        sorts: Optional[List[Dict[str, str]]] = configuration.get("sorts")
        if sorts:
            sort_parts: List[str] = []
            for sort in sorts:
                table_for_sort: Optional[str] = sort.get("table")
                column_name: str = sort["column"]
                direction: str = sort.get("direction", "ASC").upper()
                prefix: str = (
                    f"{self._finalize_table_name(table_for_sort)}."
                    if table_for_sort
                    else ""
                )
                sort_parts.append(
                    f"{prefix}{escape}{column_name.strip(escape)}{escape} {direction}"
                )
            if sort_parts:
                order_by = " ORDER BY " + ", ".join(sort_parts)

        # GROUP BY is not supported by DynamoDB PartiQL for typical aggregations.
        # Log a warning if group_by_column is provided but ignore it for SQL construction.
        group_by_column_config: Optional[Union[str, List[str]]] = configuration.get(
            "group_by_column"
        )
        if group_by_column_config:
            logger.warning(
                f"Configuration included 'group_by_column={group_by_column_config}', "
                "but GROUP BY is not supported by this DynamoDB PartiQL backend and will be ignored."
            )
        group_by: str = ""  # Ensure GROUP BY clause is not generated

        limit: Optional[int] = configuration.get("limit")
        if limit is not None:
            limit = int(limit)

        pagination: Dict[str, Any] = configuration.get("pagination", {})
        next_token: Optional[str] = pagination.get("next_token")
        if next_token is not None:
            next_token = str(next_token)

        if not finalized_table:
            raise ValueError("Table name is required for constructing SQL query.")

        statement: str = (
            f"SELECT {select} FROM {finalized_table}{wheres}{group_by}{order_by}".strip()
        )

        return statement, parameters, limit, next_token

    def records(
        self,
        configuration: Dict[str, Any],
        model: ClearSkiesModel,
        next_page_data: Optional[Any] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetches records from DynamoDB based on the provided configuration using PartiQL.
        """
        configuration = self._check_query_configuration(configuration, model)

        statement, params, limit, client_next_token_from_as_sql = self.as_sql(
            configuration
        )

        current_client_token = (
            next_page_data
            if next_page_data is not None
            else client_next_token_from_as_sql
        )

        ddb_token_for_this_call: Optional[str] = self.restore_next_token_from_config(
            current_client_token
        )

        cursor_limit: Optional[int] = None
        if limit is not None and limit > 0:
            cursor_limit = limit

        try:
            response: ExecuteStatementOutputTypeDef = self._cursor.execute(
                statement=statement,
                parameters=params,
                Limit=cursor_limit,
                NextToken=ddb_token_for_this_call,
            )
        except Exception as e:
            logger.error(
                f"Error executing PartiQL statement in records(): {statement}, error: {e}"
            )
            configuration["pagination"]["next_page_token_for_response"] = None
            raise

        items_from_response: List[Dict[str, Any]] = response.get("Items", [])

        configuration["pagination"]["next_page_token_for_response"] = None

        for item_raw in items_from_response:
            yield self._map_from_boto3(item_raw)

        next_token_from_ddb: Optional[str] = response.get("NextToken")
        if next_token_from_ddb:
            configuration["pagination"]["next_page_token_for_response"] = (
                self.serialize_next_token_for_response(next_token_from_ddb)
            )

    def count(self, configuration: Dict[str, Any], model: ClearSkiesModel) -> int:
        """
        Counts records in DynamoDB based on the provided configuration using PartiQL.
        """
        configuration = self._check_query_configuration(configuration, model)
        table_name: str = self._finalize_table_name(configuration["table_name"])

        count_configuration = {
            **configuration,
            "limit": None,
            "pagination": {},
            "select_all": False,
            "selects": [],
            "sorts": [],
            "group_by_column": None,
        }

        wheres, params = self._conditions_as_wheres_and_parameters(
            count_configuration.get("wheres", []), configuration["table_name"]
        )

        statement = f"SELECT COUNT(*) AS count FROM {table_name}{wheres}".strip()

        try:
            response = self._cursor.execute(statement=statement, parameters=params)
            items = response.get("Items", [])
            if items and "count" in items[0] and "N" in items[0]["count"]:
                return int(items[0]["count"]["N"])
            elif items and "_1" in items[0] and "N" in items[0]["_1"]:
                return int(items[0]["_1"]["N"])
            logger.warning(f"Could not parse count from response: {items}")
            return 0
        except Exception as e:
            logger.error(
                f"Error executing COUNT PartiQL statement: {statement}, error: {e}"
            )
            raise
        return 0

    def create(self, data: Dict[str, Any], model: ClearSkiesModel) -> Dict[str, Any]:
        """
        Creates a new record in DynamoDB using PartiQL INSERT.
        """
        table_name: str = self._finalize_table_name(model.table_name)  # type: ignore

        item_to_insert: Dict[str, AttributeValueTypeDef] = {
            key: self.condition_parser.to_dynamodb_attribute_value(value)
            for key, value in data.items()
        }

        parameters: List[AttributeValueTypeDef] = [item_to_insert]  # type: ignore
        statement = f"INSERT INTO {table_name} VALUE ?"

        try:
            self._cursor.execute(statement=statement, parameters=parameters)
            return data
        except Exception as e:
            logger.error(
                f"Error executing INSERT PartiQL statement: {statement}, data: {data}, error: {e}"
            )
            raise

    def update(
        self, id_value: Any, data: Dict[str, Any], model: ClearSkiesModel
    ) -> Dict[str, Any]:
        """
        Updates an existing record in DynamoDB using PartiQL UPDATE.
        """
        table_name: str = self._finalize_table_name(model.table_name)  # type: ignore
        id_column_name: str = model.id_column_name  # type: ignore
        escaped_id_column: str = (
            f"{self._column_escape_character()}{id_column_name}{self._column_escape_character()}"
        )

        if not data:
            logger.warning(
                f"Update called with empty data for ID {id_value}. Returning ID only."
            )
            return {id_column_name: id_value}

        set_clauses: List[str] = []
        parameters: List[AttributeValueTypeDef] = []
        col_esc: str = self._column_escape_character()

        for key, value in data.items():
            if key == id_column_name:
                continue
            set_clauses.append(f"{col_esc}{key}{col_esc} = ?")
            parameters.append(self.condition_parser.to_dynamodb_attribute_value(value))

        if not set_clauses:
            logger.warning(
                f"Update called for ID {id_value} but no updatable fields found in data. Returning ID only."
            )
            return {id_column_name: id_value}

        parameters.append(self.condition_parser.to_dynamodb_attribute_value(id_value))

        set_statement: str = ", ".join(set_clauses)
        statement: str = (
            f"UPDATE {table_name} SET {set_statement} WHERE {escaped_id_column} = ? RETURNING ALL NEW *"
        )

        try:
            response = self._cursor.execute(statement=statement, parameters=parameters)
            items = response.get("Items", [])
            if items:
                return self._map_from_boto3(items[0])
            logger.warning(
                f"UPDATE statement did not return items for ID {id_value}. Returning input data merged with ID."
            )
            return {**data, id_column_name: id_value}

        except Exception as e:
            logger.error(
                f"Error executing UPDATE PartiQL statement: {statement}, data: {data}, id: {id_value}, error: {e}"
            )
            raise

    def delete(self, id_value: Any, model: ClearSkiesModel) -> bool:
        """
        Deletes a record from DynamoDB using PartiQL DELETE.
        """
        table_name: str = self._finalize_table_name(model.table_name)  # type: ignore
        id_column_name: str = model.id_column_name  # type: ignore
        escaped_id_column: str = (
            f"{self._column_escape_character()}{id_column_name}{self._column_escape_character()}"
        )

        parameters: List[AttributeValueTypeDef] = [self.condition_parser.to_dynamodb_attribute_value(id_value)]  # type: ignore
        statement: str = f"DELETE FROM {table_name} WHERE {escaped_id_column} = ?"

        try:
            self._cursor.execute(statement=statement, parameters=parameters)
            return True
        except Exception as e:
            logger.error(
                f"Error executing DELETE PartiQL statement: {statement}, id: {id_value}, error: {e}"
            )
            raise

    def _map_from_boto3(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Maps a raw record from Boto3 to a dictionary with Python-native types.
        """
        return {
            key: self._map_from_boto3_value(value) for (key, value) in record.items()
        }

    def _map_from_boto3_value(self, value: Any) -> Any:
        """
        Converts a single DynamoDB Decimal value to a Python float.
        """
        if isinstance(value, Decimal):
            return float(value)
        return value

    def _check_query_configuration(
        self, configuration: Dict[str, Any], model: ClearSkiesModel
    ) -> Dict[str, Any]:
        """
        Validates the query configuration and applies default values.
        """
        for key in list(configuration.keys()):
            if key not in self._allowed_configs:
                raise KeyError(
                    f"DynamoDBBackend does not support config '{key}'. You may be using the wrong backend"
                )
        for key in self._required_configs:
            if not configuration.get(key):
                raise KeyError(f"Missing required configuration key {key}")

        if "wheres" not in configuration:
            configuration["wheres"] = []
        if "sorts" not in configuration:
            configuration["sorts"] = []
        if "selects" not in configuration:
            configuration["selects"] = []
        if "model_columns" not in configuration:
            configuration["model_columns"] = []
        if "pagination" not in configuration:
            configuration["pagination"] = {}
        if "limit" not in configuration:
            configuration["limit"] = None
        if "select_all" not in configuration:
            configuration["select_all"] = False
        if "group_by_column" not in configuration:
            configuration["group_by_column"] = None

        return configuration

    def validate_pagination_kwargs(
        self, kwargs: Dict[str, Any], case_mapping: Callable[[str], str]
    ) -> str:
        """
        Validates pagination keyword arguments.
        """
        extra_keys: set[str] = set(kwargs.keys()) - set(self.allowed_pagination_keys())
        key_name: str = case_mapping("next_token")
        if len(extra_keys):
            return (
                f"Invalid pagination key(s): '{','.join(sorted(list(extra_keys)))}'. "
                f"Only '{key_name}' is allowed"
            )
        if "next_token" not in kwargs:
            return f"You must specify '{key_name}' when setting pagination"
        try:
            token: Any = kwargs["next_token"]
            if not isinstance(token, str) or not token:
                raise ValueError("Token must be a non-empty string.")
            json.loads(base64.urlsafe_b64decode(token))
        except (TypeError, ValueError, binascii.Error, json.JSONDecodeError):
            return f"The provided '{key_name}' appears to be invalid."
        return ""

    def allowed_pagination_keys(self) -> List[str]:
        """
        Returns a list of allowed keys for pagination.
        """
        return ["next_token"]

    def restore_next_token_from_config(
        self, next_token: Optional[str]
    ) -> Optional[Any]:
        """
        Decodes a base64 encoded JSON string (next_token) into its original form.
        """
        if not next_token or not isinstance(next_token, str):
            return None
        try:
            decoded_bytes: bytes = base64.urlsafe_b64decode(next_token)
            restored_token: Any = json.loads(decoded_bytes)
            return restored_token
        except (TypeError, ValueError, binascii.Error, json.JSONDecodeError):
            logger.warning(f"Failed to restore next_token: {next_token}")
            return None

    def serialize_next_token_for_response(
        self, ddb_next_token: Optional[str]
    ) -> Optional[str]:
        """
        Serializes a DynamoDB PartiQL NextToken string into a base64 encoded JSON string.
        """
        if ddb_next_token is None:
            return None
        try:
            json_string: str = json.dumps(ddb_next_token)
            encoded_bytes: bytes = base64.urlsafe_b64encode(json_string.encode("utf-8"))
            return encoded_bytes.decode("utf8")
        except (TypeError, ValueError) as e:
            logger.error(
                f"Error serializing DDB next_token: {ddb_next_token}, error: {e}"
            )
            return None

    def documentation_pagination_next_page_response(
        self, case_mapping: Callable[[str], str]
    ) -> List[AutoDocString]:
        """
        Provides documentation for the 'next_page' (pagination token) in API responses.
        """
        return [AutoDocString(case_mapping("next_token"))]

    def documentation_pagination_next_page_example(
        self, case_mapping: Callable[[str], str]
    ) -> Dict[str, str]:
        """
        Provides an example value for the 'next_page' (pagination token) in API responses.
        """
        return {case_mapping("next_token"): ""}

    def documentation_pagination_parameters(
        self, case_mapping: Callable[[str], str]
    ) -> List[Tuple[AutoDocString, str]]:
        """
        Provides documentation for pagination parameters in API requests.
        """
        return [
            (
                AutoDocString(case_mapping("next_token"), example=""),
                "A token to fetch the next page of results",
            )
        ]
