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
    GlobalSecondaryIndexDescriptionTypeDef,
    KeySchemaElementTypeDef,
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
            # Only include 'Parameters' if it's not None AND not empty
            if (
                parameters
            ):  # This implies parameters is not None and parameters is not an empty list
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

            parameters_str = str(parameters) if parameters is not None else "None"

            if error_code == "ResourceNotFoundException":
                logger.error(
                    "Couldn't execute PartiQL '%s' with parameters '%s' because the table or index does not exist.",
                    statement,
                    parameters_str,
                )
            else:
                logger.error(
                    "Couldn't execute PartiQL '%s' with parameters '%s'. Here's why: %s: %s",
                    statement,
                    parameters_str,
                    error_code,
                    error_message,
                )
            raise
        else:
            return output


class DynamoDBPartiQLBackend(CursorBackend):
    """
    DynamoDB backend implementation that uses PartiQL for database interactions.
    Supports querying base tables and attempts to use Global Secondary Indexes (GSIs)
    when appropriate based on query conditions and sorting.
    The count() method uses native DynamoDB Query/Scan operations for accuracy.
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
        "joins",
    ]
    _required_configs: List[str] = ["table_name"]

    def __init__(self, dynamo_db_parti_ql_cursor: DynamoDBPartiQLCursor) -> None:
        """
        Initializes the DynamoDBPartiQLBackend.
        """
        super().__init__(dynamo_db_parti_ql_cursor)
        self.condition_parser: DynamoDBConditionParser = DynamoDBConditionParser()
        self._table_descriptions_cache: Dict[str, Dict[str, Any]] = {}

    def _get_table_description(self, table_name: str) -> Dict[str, Any]:
        """
        Retrieves and caches the DynamoDB table description.
        """
        if table_name not in self._table_descriptions_cache:
            try:
                self._table_descriptions_cache[table_name] = self._cursor._client.describe_table(TableName=table_name)  # type: ignore
            except ClientError as e:
                logger.error(f"Failed to describe table '{table_name}': {e}")
                raise
        return self._table_descriptions_cache[table_name].get("Table", {})

    def _table_escape_character(self) -> str:
        """Returns the character used to escape table/index names."""
        return '"'

    def _column_escape_character(self) -> str:
        """Returns the character used to escape column names."""
        return '"'

    def _finalize_table_name(
        self, table_name: str, index_name: Optional[str] = None
    ) -> str:
        """
        Escapes a table name and optionally an index name for use in a PartiQL FROM clause.
        """
        if not table_name:
            return ""
        esc: str = self._table_escape_character()
        final_name = f"{esc}{str(table_name).strip(esc)}{esc}"
        if index_name:
            final_name += f".{esc}{index_name.strip(esc)}{esc}"
        return final_name

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
        escape: str = self._column_escape_character()
        table_name: str = configuration.get("table_name", "")
        chosen_index_name: Optional[str] = configuration.get("_chosen_index_name")

        wheres, parameters = self._conditions_as_wheres_and_parameters(
            configuration.get("wheres", []), table_name
        )

        from_clause_target: str = self._finalize_table_name(
            table_name, chosen_index_name
        )

        selects: Optional[List[str]] = configuration.get("selects")
        select_clause: str
        if selects:
            select_clause = ", ".join(
                [f"{escape}{s.strip(escape)}{escape}" for s in selects]
            )
            if configuration.get("select_all"):
                logger.warning(
                    "Both 'select_all=True' and specific 'selects' were provided. Using specific 'selects'."
                )
        else:
            select_clause = "*"

        order_by: str = ""
        sorts: Optional[List[Dict[str, str]]] = configuration.get("sorts")
        if sorts:
            sort_parts: List[str] = []
            for sort in sorts:
                column_name: str = sort["column"]
                direction: str = sort.get("direction", "ASC").upper()
                sort_parts.append(
                    f"{escape}{column_name.strip(escape)}{escape} {direction}"
                )
            if sort_parts:
                order_by = " ORDER BY " + ", ".join(sort_parts)

        if configuration.get("group_by_column"):
            logger.warning(
                f"Configuration included 'group_by_column={configuration.get("group_by_column")}', "
                "but GROUP BY is not supported by this DynamoDB PartiQL backend and will be ignored for SQL generation."
            )

        if configuration.get("joins"):
            logger.warning(
                f"Configuration included 'joins={configuration.get("joins")}', "
                "but JOINs are not supported by this DynamoDB PartiQL backend and will be ignored for SQL generation."
            )

        limit: Optional[int] = configuration.get("limit")
        if limit is not None:
            limit = int(limit)

        pagination: Dict[str, Any] = configuration.get("pagination", {})
        next_token: Optional[str] = pagination.get("next_token")
        if next_token is not None:
            next_token = str(next_token)

        if not from_clause_target:
            raise ValueError("Table name is required for constructing SQL query.")

        statement: str = (
            f"SELECT {select_clause} FROM {from_clause_target}{wheres}{order_by}".strip()
        )

        return statement, parameters, limit, next_token

    def records(
        self,
        configuration: Dict[str, Any],
        model: ClearSkiesModel,
        next_page_data: dict[str, Any] = {},
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetches records from DynamoDB based on the provided configuration using PartiQL.
        """
        configuration = self._check_query_configuration(configuration, model)

        statement, params, limit, client_next_token_from_as_sql = self.as_sql(
            configuration
        )

        ddb_token_for_this_call: Optional[str] = self.restore_next_token_from_config(
            client_next_token_from_as_sql
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
            next_page_data = {}
            raise

        items_from_response: List[Dict[str, Any]] = response.get("Items", [])

        for item_raw in items_from_response:
            yield self._map_from_boto3(item_raw)

        next_token_from_ddb: Optional[str] = response.get("NextToken")
        if next_token_from_ddb:
            next_page_data["next_token"] = self.serialize_next_token_for_response(
                next_token_from_ddb
            )

    def _wheres_to_native_dynamo_expressions(
        self,
        conditions: List[Dict[str, Any]],
        partition_key_name: Optional[str],
        sort_key_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Converts a list of 'where' condition dictionaries into DynamoDB native
        expression strings and attribute maps.
        This is a simplified implementation.
        """
        expression_attribute_names: Dict[str, str] = {}
        expression_attribute_values: Dict[str, AttributeValueTypeDef] = {}
        key_condition_parts: List[str] = []
        filter_expression_parts: List[str] = []

        name_counter = 0
        value_counter = 0

        partition_key_condition_found = False
        if partition_key_name:
            processed_indices = set()  # To avoid processing a condition twice
            for i, cond in enumerate(conditions):
                if i in processed_indices:
                    continue
                if (
                    cond.get("column") == partition_key_name
                    and cond.get("operator") == "="
                    and cond.get("values")
                ):
                    name_placeholder = f"#pk{name_counter}"
                    value_placeholder = f":pk_val{value_counter}"
                    expression_attribute_names[name_placeholder] = partition_key_name
                    expression_attribute_values[value_placeholder] = (
                        self.condition_parser.to_dynamodb_attribute_value(
                            cond["values"][0]
                        )
                    )
                    key_condition_parts.append(
                        f"{name_placeholder} = {value_placeholder}"
                    )
                    name_counter += 1
                    value_counter += 1
                    partition_key_condition_found = True
                    processed_indices.add(i)
                    break

            # Example for sort key condition (simplified)
            if (
                sort_key_name and partition_key_condition_found
            ):  # Sort key only relevant if PK is there
                for i, cond in enumerate(conditions):
                    if i in processed_indices:
                        continue
                    if cond.get("column") == sort_key_name and cond.get("values"):
                        # Simplified: only handle '=' for sort key for now
                        if cond.get("operator") == "=":
                            name_placeholder = f"#sk{name_counter}"
                            value_placeholder = f":sk_val{value_counter}"
                            expression_attribute_names[name_placeholder] = sort_key_name
                            expression_attribute_values[value_placeholder] = (
                                self.condition_parser.to_dynamodb_attribute_value(
                                    cond["values"][0]
                                )
                            )
                            key_condition_parts.append(
                                f"{name_placeholder} = {value_placeholder}"
                            )
                            name_counter += 1
                            value_counter += 1
                            processed_indices.add(i)
                            break  # Assuming one sort key condition for KeyConditionExpression

        # Remaining conditions go to FilterExpression
        for i, cond in enumerate(conditions):
            if i in (
                locals().get("processed_indices") or set()
            ):  # Check if already processed for KeyCondition
                continue

            col_name = cond.get("column")
            op = cond.get("operator")
            vals = cond.get("values")

            if not col_name or not op or vals is None:
                continue

            # This is a very simplified filter builder. A real one would handle all operators.
            if op == "=" and vals:  # Example: only equality
                name_placeholder = f"#fn{name_counter}"
                value_placeholder = f":fv{value_counter}"
                expression_attribute_names[name_placeholder] = col_name
                expression_attribute_values[value_placeholder] = (
                    self.condition_parser.to_dynamodb_attribute_value(vals[0])
                )
                filter_expression_parts.append(
                    f"{name_placeholder} = {value_placeholder}"
                )
                name_counter += 1
                value_counter += 1
            # Add more operator handling here for a complete filter expression builder

        result: Dict[str, Any] = {}
        if (
            key_condition_parts and partition_key_condition_found
        ):  # Only add KeyConditionExpression if PK equality was found
            result["KeyConditionExpression"] = " AND ".join(key_condition_parts)
        elif (
            key_condition_parts
        ):  # If we built key_condition_parts but PK equality wasn't the one, move to filter
            filter_expression_parts.extend(key_condition_parts)

        if filter_expression_parts:
            result["FilterExpression"] = " AND ".join(filter_expression_parts)
        if expression_attribute_names:
            result["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            result["ExpressionAttributeValues"] = expression_attribute_values

        return result

    def count(self, configuration: Dict[str, Any], model: ClearSkiesModel) -> int:
        """
        Counts records in DynamoDB using native Query or Scan operations.
        """
        configuration = self._check_query_configuration(configuration, model)

        table_name: str = configuration["table_name"]
        chosen_index_name: Optional[str] = configuration.get("_chosen_index_name")
        partition_key_for_target: Optional[str] = configuration.get(
            "_partition_key_for_target"
        )
        # Get sort key for the chosen target (base table or GSI)
        sort_key_for_target: Optional[str] = None
        table_description = self._get_table_description(table_name)
        if chosen_index_name:
            gsi_definitions: List[GlobalSecondaryIndexDescriptionTypeDef] = (
                table_description.get("GlobalSecondaryIndexes", [])
            )
            for gsi in gsi_definitions:
                if gsi["IndexName"] == chosen_index_name:
                    for key_element in gsi["KeySchema"]:
                        if key_element["KeyType"] == "RANGE":
                            sort_key_for_target = key_element["AttributeName"]
                            break
                    break
        else:
            base_table_key_schema: List[KeySchemaElementTypeDef] = (
                table_description.get("KeySchema", [])
            )
            for key_element in base_table_key_schema:
                if key_element["KeyType"] == "RANGE":
                    sort_key_for_target = key_element["AttributeName"]
                    break

        wheres_config = configuration.get("wheres", [])

        native_expressions = self._wheres_to_native_dynamo_expressions(
            wheres_config, partition_key_for_target, sort_key_for_target
        )

        params_for_native_call: Dict[str, Any] = {  # Renamed from params
            "TableName": table_name,
            "Select": "COUNT",
        }
        if chosen_index_name:
            params_for_native_call["IndexName"] = chosen_index_name

        can_use_query_for_count = False
        if (
            native_expressions.get("KeyConditionExpression")
            and partition_key_for_target
            and any(
                w.get("column") == partition_key_for_target and w.get("operator") == "="
                for w in wheres_config
                if isinstance(w, dict)
            )
        ):
            can_use_query_for_count = True
            params_for_native_call["KeyConditionExpression"] = native_expressions[
                "KeyConditionExpression"
            ]
            if native_expressions.get(
                "FilterExpression"
            ):  # Query can also have FilterExpression
                params_for_native_call["FilterExpression"] = native_expressions[
                    "FilterExpression"
                ]
        else:  # Fall back to Scan
            # If KeyConditionExpression was built but not for the PK, it becomes part of Filter for Scan
            all_filter_parts = []
            if native_expressions.get("KeyConditionExpression"):
                all_filter_parts.append(native_expressions["KeyConditionExpression"])
            if native_expressions.get("FilterExpression"):
                all_filter_parts.append(native_expressions["FilterExpression"])
            if all_filter_parts:
                params_for_native_call["FilterExpression"] = " AND ".join(
                    all_filter_parts
                )

        if native_expressions.get("ExpressionAttributeNames"):
            params_for_native_call["ExpressionAttributeNames"] = native_expressions[
                "ExpressionAttributeNames"
            ]
        if native_expressions.get("ExpressionAttributeValues"):
            params_for_native_call["ExpressionAttributeValues"] = native_expressions[
                "ExpressionAttributeValues"
            ]

        total_count = 0
        exclusive_start_key: Optional[Dict[str, AttributeValueTypeDef]] = None

        while True:
            if exclusive_start_key:
                params_for_native_call["ExclusiveStartKey"] = exclusive_start_key

            try:
                if can_use_query_for_count:
                    logger.debug(
                        f"Executing native DynamoDB Query (for count) with params: {params_for_native_call}"
                    )
                    response = self._cursor._client.query(**params_for_native_call)  # type: ignore
                else:
                    logger.debug(
                        f"Executing native DynamoDB Scan (for count) with params: {params_for_native_call}"
                    )
                    response = self._cursor._client.scan(**params_for_native_call)  # type: ignore
            except ClientError as e:
                logger.error(
                    f"Error executing native DynamoDB operation for count: {e}. Params: {params_for_native_call}"
                )
                raise

            total_count += response.get("Count", 0)
            exclusive_start_key = response.get("LastEvaluatedKey")
            if not exclusive_start_key:
                break

        return total_count

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
        Maps a raw record from DynamoDB (which uses AttributeValueTypeDef for values)
        to a dictionary with Python-native types.

        Args:
            record: A dictionary representing a record item from DynamoDB,
                    where values are in AttributeValueTypeDef format.

        Returns:
            A dictionary with values unwrapped to Python native types.
        """
        return {
            key: self._map_from_boto3_value(value) for (key, value) in record.items()
        }

    def _map_from_boto3_value(self, attribute_value: AttributeValueTypeDef) -> Any:
        """
        Converts a single DynamoDB AttributeValueTypeDef to its Python native equivalent.

        Args:
            attribute_value: A DynamoDB AttributeValueTypeDef dictionary.

        Returns:
            The unwrapped Python native value.
        """
        if not isinstance(attribute_value, dict):
            return attribute_value

        if "S" in attribute_value:
            return attribute_value["S"]
        if "N" in attribute_value:
            try:
                return Decimal(attribute_value["N"])
            except DecimalException:
                logger.warning(
                    f"Could not convert N value '{attribute_value['N']}' to Decimal."
                )
                return attribute_value["N"]
        if "BOOL" in attribute_value:
            return attribute_value["BOOL"]
        if "NULL" in attribute_value:
            return None
        if "B" in attribute_value:
            return base64.b64decode(attribute_value["B"])
        if "L" in attribute_value:
            return [self._map_from_boto3_value(item) for item in attribute_value["L"]]
        if "M" in attribute_value:
            return {
                key: self._map_from_boto3_value(val)
                for key, val in attribute_value["M"].items()
            }
        if "SS" in attribute_value:
            return set(attribute_value["SS"])
        if "NS" in attribute_value:
            try:
                return set(Decimal(n_val) for n_val in attribute_value["NS"])
            except DecimalException:
                logger.warning(
                    f"Could not convert one or more NS values in '{attribute_value['NS']}' to Decimal."
                )
                return set(attribute_value["NS"])
        if "BS" in attribute_value:
            return set(base64.b64decode(b_val) for b_val in attribute_value["BS"])

        logger.warning(f"Unrecognized DynamoDB attribute type: {attribute_value}")
        return attribute_value

    def _check_query_configuration(
        self, configuration: Dict[str, Any], model: ClearSkiesModel
    ) -> Dict[str, Any]:
        """
        Validates the query configuration, applies default values, and attempts to
        select an appropriate GSI if sorting is requested and conditions allow.
        It also stores the determined partition key for the target in the configuration.
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
        if "joins" not in configuration:
            configuration["joins"] = []

        configuration["_chosen_index_name"] = None
        configuration["_partition_key_for_target"] = None

        if configuration.get("sorts") or configuration.get(
            "wheres"
        ):  # Check for index even if not sorting, for count
            table_name_from_config: str = configuration.get("table_name", "")
            table_description = self._get_table_description(table_name_from_config)

            wheres = configuration.get("wheres", [])
            sort_column = (
                configuration.get("sorts")[0]["column"]
                if configuration.get("sorts")
                else None
            )

            key_to_check_for_equality: Optional[str] = None
            target_name_for_error_msg: str = table_name_from_config
            chosen_index_for_query: Optional[str] = None
            partition_key_for_chosen_target: Optional[str] = None

            gsi_definitions: List[GlobalSecondaryIndexDescriptionTypeDef] = (
                table_description.get("GlobalSecondaryIndexes", [])
            )
            if gsi_definitions:
                for gsi in gsi_definitions:
                    gsi_name: str = gsi["IndexName"]
                    gsi_key_schema: List[KeySchemaElementTypeDef] = gsi["KeySchema"]
                    gsi_partition_key: Optional[str] = None
                    gsi_sort_key: Optional[str] = None

                    for key_element in gsi_key_schema:
                        if key_element["KeyType"] == "HASH":
                            gsi_partition_key = key_element["AttributeName"]
                        elif key_element["KeyType"] == "RANGE":
                            gsi_sort_key = key_element["AttributeName"]

                    if gsi_partition_key and any(
                        w.get("column") == gsi_partition_key
                        and w.get("operator") == "="
                        for w in wheres
                        if isinstance(w, dict)
                    ):
                        if configuration.get("sorts"):
                            if sort_column == gsi_partition_key and not gsi_sort_key:
                                key_to_check_for_equality = gsi_partition_key
                                chosen_index_for_query = gsi_name
                                target_name_for_error_msg = (
                                    f"{table_name_from_config} (index: {gsi_name})"
                                )
                                partition_key_for_chosen_target = gsi_partition_key
                                break
                            if sort_column == gsi_sort_key:
                                key_to_check_for_equality = gsi_partition_key
                                chosen_index_for_query = gsi_name
                                target_name_for_error_msg = (
                                    f"{table_name_from_config} (index: {gsi_name})"
                                )
                                partition_key_for_chosen_target = gsi_partition_key
                                break
                        else:
                            key_to_check_for_equality = gsi_partition_key
                            chosen_index_for_query = gsi_name
                            target_name_for_error_msg = (
                                f"{table_name_from_config} (index: {gsi_name})"
                            )
                            partition_key_for_chosen_target = gsi_partition_key
                            break

            if not chosen_index_for_query:
                base_table_key_schema: List[KeySchemaElementTypeDef] = (
                    table_description.get("KeySchema", [])
                )
                if base_table_key_schema:
                    for key_element in base_table_key_schema:
                        if key_element["KeyType"] == "HASH":
                            key_to_check_for_equality = key_element["AttributeName"]
                            partition_key_for_chosen_target = key_element[
                                "AttributeName"
                            ]
                            break

            configuration["_chosen_index_name"] = chosen_index_for_query
            configuration["_partition_key_for_target"] = partition_key_for_chosen_target

            if configuration.get("sorts"):
                if not key_to_check_for_equality:
                    logger.warning(
                        f"Could not determine the required partition key for table/index '{target_name_for_error_msg}' "
                        f"to validate ORDER BY clause. The query may fail in DynamoDB."
                    )
                else:
                    has_required_key_equality = any(
                        w.get("column") == key_to_check_for_equality
                        and w.get("operator") == "="
                        for w in wheres
                        if isinstance(w, dict)
                    )
                    if not has_required_key_equality:
                        raise ValueError(
                            f"DynamoDB PartiQL queries with ORDER BY on '{target_name_for_error_msg}' require an equality "
                            f"condition on its partition key ('{key_to_check_for_equality}') in the WHERE clause."
                        )
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
