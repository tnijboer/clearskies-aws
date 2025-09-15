import base64
import json
import logging
from decimal import Decimal, DecimalException
from typing import Any, Dict, List, Tuple

from clearskies import ConditionParser

# Ensure AttributeValueTypeDef is imported from the correct boto3 types package
# This is crucial for the "ideal fix".
from types_boto3_dynamodb.type_defs import AttributeValueTypeDef

logger = logging.getLogger(__name__)


class DynamoDBConditionParser(ConditionParser):
    """
    Parses string conditions into a structured format suitable for DynamoDB PartiQL queries.

    This class handles various SQL-like operators and translates them into
    PartiQL compatible expressions and parameters. It also includes a utility
    to convert Python values into the DynamoDB AttributeValue format.
    """

    operator_lengths: Dict[str, int] = {
        "<>": 2,
        "<=": 2,
        ">=": 2,
        "!=": 2,
        ">": 1,
        "<": 1,
        "=": 1,
        "in": 4,
        "is not null": 12,
        "is null": 8,
        "is not": 8,
        "is": 4,
        "like": 6,
        "is not missing": 15,
        "is missing": 11,
        "contains": 9,
        "begins_with": 12,
    }
    operators: List[str] = [
        # Longer operators first to help with matching
        "is not null",
        "is not missing",
        "is null",
        "is missing",
        "begins_with",
        "contains",
        "<>",
        "!=",
        "<=",
        ">=",
        "is not",
        "is",
        "like",
        ">",
        "<",
        "=",
        "in",
    ]
    operators_for_matching: Dict[str, str] = {
        "like": " like ",
        "in": " in ",
        "is not missing": " is not missing",
        "is missing": " is missing",
        "is not null": " is not null",
        "is null": " is null",
        "is": " is ",
        "is not": " is not ",
        "begins_with": " begins_with",
        "contains": " contains",
    }
    operators_with_simple_placeholders: Dict[str, bool] = {
        "<>": True,
        "<=": True,
        ">=": True,
        "!=": True,
        "=": True,
        "<": True,
        ">": True,
        "is": True,
        "is not": True,
    }
    operators_without_placeholders: set[str] = {
        "is not missing",
        "is missing",
    }
    operator_needs_remap: Dict[str, str] = {
        "is not null": "is not missing",
        "is null": "is missing",
    }
    operators_with_special_placeholders: set[str] = {"begins_with", "contains"}

    def parse_condition(self, condition: str) -> Dict[str, Any]:
        """
        Parse a string condition into a structured dictionary.

        The "values" key in the returned dictionary will contain List[AttributeValueTypeDef].

        Args:
            condition: The condition string to parse.

        Returns:
            A dictionary with keys: "table", "column", "operator", "values" (DynamoDB formatted),
            and "parsed" (the SQL fragment).
        """
        lowercase_condition: str = condition.lower()
        matching_operator: str = ""
        matching_index: int = -1
        current_best_match_len: int = 0

        for operator in self.operators:
            try:
                operator_for_match: str = self.operators_for_matching.get(operator, operator)
                index: int = lowercase_condition.index(operator_for_match)

                if matching_index == -1 or index < matching_index:
                    matching_index = index
                    matching_operator = operator
                    current_best_match_len = len(operator_for_match)
                elif index == matching_index:
                    if len(operator_for_match) > current_best_match_len:
                        matching_operator = operator
                        current_best_match_len = len(operator_for_match)
            except ValueError:
                continue

        if not matching_operator:
            raise ValueError(f"No supported operators found in condition {condition}")

        column: str = condition[:matching_index].strip()
        value: str = condition[matching_index + self.operator_lengths[matching_operator] :].strip()

        if len(value) >= 2:
            first_char = value[0]
            last_char = value[-1]
            if (first_char == "'" and last_char == "'") or (first_char == '"' and last_char == '"'):
                value = value[1:-1]

        raw_values: List[str] = []

        if matching_operator == "in":
            raw_values = self._parse_condition_list(value) if value else []
        elif matching_operator not in self.operators_without_placeholders and not (
            matching_operator in self.operator_needs_remap
            and self.operator_needs_remap[matching_operator] in self.operators_without_placeholders
        ):
            raw_values = [value]

        if matching_operator.lower() == "like":
            if value.startswith("%") and value.endswith("%") and len(value) > 1:
                matching_operator = "contains"
                raw_values = [value[1:-1]]
            elif value.endswith("%") and not value.startswith("%"):
                matching_operator = "begins_with"
                raw_values = [value[:-1]]
            elif value.startswith("%") and not value.endswith("%"):
                raise ValueError("DynamoDB PartiQL does not directly support 'ends_with'")
            else:
                matching_operator = "="
                raw_values = [value]

        matching_operator = self.operator_needs_remap.get(matching_operator.lower(), matching_operator)

        table_name: str = ""
        final_column_name: str = column
        if "." in column:
            table_prefix, column_name_part = column.split(".", 1)
            table_name = table_prefix.strip().replace('"', "").replace("`", "")
            final_column_name = column_name_part.strip().replace('"', "").replace("`", "")
        else:
            final_column_name = column.replace('"', "").replace("`", "")

        # This list will now correctly be List[AttributeValueTypeDef]
        parameters: List[AttributeValueTypeDef] = []
        if matching_operator.lower() not in self.operators_without_placeholders:
            for val_item in raw_values:
                parameters.append(self.to_dynamodb_attribute_value(val_item))

        column_for_parsed: str = f"{table_name}.{final_column_name}" if table_name else final_column_name

        return {
            "table": table_name,
            "column": final_column_name,
            "operator": matching_operator.upper(),
            "values": parameters,  # This is now correctly typed for MyPy
            "parsed": self._with_placeholders(
                column_for_parsed,
                matching_operator,
                parameters,
            ),
        }

    def _with_placeholders(
        self,
        column: str,
        operator: str,
        values: List[AttributeValueTypeDef],  # Parameter 'values' is List[AttributeValueTypeDef]
        escape: bool = True,
        escape_character: str = '"',
    ) -> str:
        """Format a SQL fragment with placeholders for a given column, operator, and parameters."""
        quoted_column = column
        if escape:
            parts: List[str] = column.split(".", 1)
            cleaned_parts: List[str] = [part.strip('"`') for part in parts]
            if len(cleaned_parts) == 2:
                quoted_column = (
                    f"{escape_character}{cleaned_parts[0]}{escape_character}"
                    "."
                    f"{escape_character}{cleaned_parts[1]}{escape_character}"
                )
            else:
                quoted_column = f"{escape_character}{cleaned_parts[0]}{escape_character}"

        upper_case_operator: str = operator.upper()
        lower_case_operator: str = operator.lower()

        if lower_case_operator in self.operators_with_simple_placeholders:
            return f"{quoted_column} {upper_case_operator} ?"
        if lower_case_operator in self.operators_without_placeholders:
            return f"{quoted_column} {upper_case_operator}"
        if lower_case_operator in self.operators_with_special_placeholders:
            return f"{lower_case_operator}({quoted_column}, ?)"

        if lower_case_operator == "in":
            placeholders_str: str = ", ".join(["?" for _ in values])
            return f"{quoted_column} IN ({placeholders_str})"

        raise ValueError(f"Unsupported operator for placeholder generation: {operator}")

    def to_dynamodb_attribute_value(self, value: Any) -> AttributeValueTypeDef:  # Return type changed
        """Convert a Python variable into a DynamoDB-formatted attribute value dictionary."""
        if isinstance(value, str):
            if value.lower() == "true":
                return {"BOOL": True}
            if value.lower() == "false":
                return {"BOOL": False}
            if value.lower() == "null":
                return {"NULL": True}
            try:
                if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                    return {"N": str(int(value))}
                return {"N": str(Decimal(value))}
            except (ValueError, TypeError, json.JSONDecodeError, DecimalException):
                return {"S": value}
        elif isinstance(value, bool):
            return {"BOOL": value}
        elif isinstance(value, (int, float, Decimal)):
            return {"N": str(value)}
        elif value is None:
            return {"NULL": True}
        elif isinstance(value, bytes):
            return {"B": base64.b64encode(value).decode("utf-8")}
        elif isinstance(value, list):
            # Each item will be AttributeValueTypeDef, so the list is List[AttributeValueTypeDef]
            return {"L": [self.to_dynamodb_attribute_value(item) for item in value]}
        elif isinstance(value, dict):
            # Each value in the map will be AttributeValueTypeDef
            return {"M": {str(k): self.to_dynamodb_attribute_value(v) for k, v in value.items()}}
        elif isinstance(value, set):
            if not value:
                raise ValueError("Cannot determine DynamoDB Set type from an empty Python set.")
            if all(isinstance(item, str) for item in value):
                return {"SS": sorted(list(value))}
            elif all(isinstance(item, (int, float, Decimal)) for item in value):
                return {"NS": sorted([str(item) for item in value])}
            elif all(isinstance(item, bytes) for item in value):
                return {"BS": sorted([base64.b64encode(item).decode("utf-8") for item in value])}
            raise ValueError("Set contains mixed types or unsupported types for DynamoDB Sets.")
        else:
            raise TypeError(f"Unsupported Python type for DynamoDB conversion: {type(value)}")

    def _parse_condition_list(self, list_string: str) -> List[str]:
        """Parse a string representation of a list into a list of strings."""
        if not list_string.strip():
            return []

        if list_string.startswith("(") and list_string.endswith(")"):
            list_string = list_string[1:-1]
            if not list_string.strip():
                return []

        items: List[str] = []
        current_item: str = ""
        in_quotes: bool = False
        quote_char: str = ""
        for char in list_string:
            if char in ("'", '"'):
                if in_quotes and char == quote_char:
                    in_quotes = False
                elif not in_quotes:
                    in_quotes = True
                    quote_char = char
                else:
                    current_item += char
            elif char == "," and not in_quotes:
                stripped_item = current_item.strip()
                if stripped_item:
                    items.append(stripped_item)
                current_item = ""
            else:
                current_item += char

        stripped_current_item = current_item.strip()
        if stripped_current_item:
            items.append(stripped_current_item)

        final_items = []
        for item in items:
            processed_item = item
            if len(processed_item) >= 2:
                if processed_item.startswith("'") and processed_item.endswith("'"):
                    processed_item = processed_item[1:-1]
                elif processed_item.startswith('"') and processed_item.endswith('"'):
                    processed_item = processed_item[1:-1]

            if processed_item:
                final_items.append(processed_item)
        return final_items
